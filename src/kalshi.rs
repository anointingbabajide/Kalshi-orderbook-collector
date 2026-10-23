//! Kalshi side of the collector: command builders and message -> BookEvent.

use crate::book::Book;
use crate::conn::{self, Ws};
use crate::decode::{DecodeError, Event, decode};
use crate::event::{BookEvent, BookSide};
use crate::exchange::{Codec, Decoded, Exchange};
use crate::feed::{Feed, Outcome};
use ed25519_dalek::SigningKey;
use serde_json::json;

#[derive(Default)]
pub struct Kalshi {
    feed: Feed,
    sid: Option<i64>,
    last_ts_us: i64,
}

impl Kalshi {
    pub fn subscribe_msg(id: i64, tickers: &[String]) -> String {
        json!({"id": id, "cmd": "subscribe",
               "params": {"channels": ["orderbook_delta"], "market_tickers": tickers}})
        .to_string()
    }

    /// None until a `subscribed` message has given us a sid.
    pub fn unsubscribe_msg(&self, id: i64) -> Option<String> {
        self.sid.map(|sid| {
            json!({"id": id, "cmd": "unsubscribe", "params": {"sids": [sid]}}).to_string()
        })
    }

    pub fn has_sid(&self) -> bool {
        self.sid.is_some()
    }

    pub fn update_msg(&self, id: i64, action: &str, tickers: &[String]) -> Option<String> {
        self.sid.map(|sid| {
            json!({"id": id, "cmd": "update_subscription",
               "params": {"sids": [sid], "market_tickers": tickers, "action": action}})
            .to_string()
        })
    }

    pub fn forget(&mut self, ticker: &str) {
        self.feed.remove(ticker);
    }

    pub fn is_synced(&self) -> bool {
        self.feed.is_synced()
    }

    /// A full snapshot of every book we hold. Empty while out of sync, so a
    /// stale book is never emitted. The exchange timestamp is that of the
    /// last message seen, since a periodic snapshot has no exchange time of its own.
    pub fn snapshot_all(&self) -> Vec<BookEvent> {
        if !self.feed.is_synced() {
            return Vec::new();
        }
        self.feed
            .books()
            .map(|(symbol, book)| {
                let (bids, asks) = book.levels();
                BookEvent::Snapshot {
                    symbol: symbol.clone(),
                    bids,
                    asks,
                    exchange_ts_us: self.last_ts_us,
                }
            })
            .collect()
    }

    pub fn on_message(&mut self, raw: &str) -> Result<Decoded, DecodeError> {
        let ev = decode(raw)?;
        let unsubscribed = matches!(ev, Event::Unsubscribed);
        if let Event::Subscribed { sid } = &ev {
            self.sid = Some(*sid);
        }
        // Capture what we need before the feed consumes the event.
        let snap_ts = match &ev {
            Event::Snapshot { sending_ts_ms, .. } => Some(*sending_ts_ms),
            _ => None,
        };
        let delta = match &ev {
            Event::Delta {
                side,
                price,
                sending_ts_ms,
                ..
            } => Some((*side, *price, *sending_ts_ms)),
            _ => None,
        };

        let mut out = Decoded::default();
        out.unsubscribed = unsubscribed;
        match self.feed.on_event(ev) {
            Outcome::Applied { ticker } => {
                let Some(book) = self.feed.book(&ticker) else {
                    return Ok(out);
                };
                if let Some(ts) = snap_ts {
                    let (bids, asks) = book.levels();
                    out.events.push(BookEvent::Snapshot {
                        symbol: ticker,
                        bids,
                        asks,
                        exchange_ts_us: ts * 1000,
                    });
                } else if let Some((side, price, ts)) = delta {
                    let (is_bid, yes_price) = Book::to_yes_axis(side, price);
                    out.events.push(BookEvent::Update {
                        symbol: ticker,
                        side: if is_bid { BookSide::Bid } else { BookSide::Ask },
                        price: yes_price,
                        quantity: book.qty(side, price),
                        exchange_ts_us: ts * 1000,
                    });
                }
            }
            Outcome::Resync(_) => out.resync = true,
            Outcome::Ignored => {}
        }
        for e in &out.events {
            self.last_ts_us = match e {
                BookEvent::Snapshot { exchange_ts_us, .. }
                | BookEvent::Update { exchange_ts_us, .. } => *exchange_ts_us,
            };
        }
        Ok(out)
    }
}

// ---- generic-collector glue ----

impl Codec for Kalshi {
    fn subscribe_msg(id: i64, tickers: &[String]) -> String {
        Kalshi::subscribe_msg(id, tickers)
    }
    fn add_msg(&self, id: i64, t: &[String]) -> Option<String> {
        self.update_msg(id, "add_markets", t)
    }
    fn remove_msg(&self, id: i64, t: &[String]) -> Option<String> {
        self.update_msg(id, "delete_markets", t)
    }
    fn unsubscribe_all_msg(&self, id: i64) -> Option<String> {
        self.unsubscribe_msg(id)
    }
    fn on_message(&mut self, text: &str) -> anyhow::Result<Decoded> {
        Kalshi::on_message(self, text).map_err(|e| anyhow::anyhow!("{e:?}"))
    }
    fn can_update(&self) -> bool {
        self.has_sid()
    }
    fn is_synced(&self) -> bool {
        Kalshi::is_synced(self)
    }
    fn snapshot_all(&self) -> Vec<BookEvent> {
        Kalshi::snapshot_all(self)
    }
    fn forget(&mut self, t: &str) {
        Kalshi::forget(self, t)
    }
}

pub struct KalshiExchange {
    pub key_id: String,
    pub key: SigningKey,
    pub ws_url: &'static str,
    pub rest_base: &'static str,
}

impl Exchange for KalshiExchange {
    type Codec = Kalshi;
    async fn connect(&self) -> anyhow::Result<Ws> {
        conn::connect(self.ws_url, &self.key_id, &self.key).await
    }
    async fn list_markets(&self, n: usize) -> anyhow::Result<Vec<String>> {
        conn::list_open_markets(self.rest_base, n).await
    }
    fn max_groups(&self) -> usize {
        50
    }
    fn min_shard_size(&self) -> usize {
        200
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshot_then_no_delta_becomes_ask_update() {
        let mut k = Kalshi::default();
        k.on_message(r#"{"type":"subscribed","id":1,"msg":{"channel":"orderbook_delta","sid":1},"sending_ts_ms":1}"#).unwrap();
        let snap = r#"{"type":"orderbook_snapshot","sid":1,"seq":1,"msg":{"market_ticker":"T","yes_dollars_fp":[["0.4100","30.00"]],"no_dollars_fp":[["0.3500","25.00"]]},"sending_ts_ms":3}"#;
        let out = k.on_message(snap).unwrap();
        assert_eq!(
            out.events,
            vec![BookEvent::Snapshot {
                symbol: "T".into(),
                bids: vec![(410_000, 30_000_000)],
                asks: vec![(650_000, 25_000_000)],
                exchange_ts_us: 3000,
            }]
        );
        let delta = r#"{"type":"orderbook_delta","sid":1,"seq":2,"msg":{"market_ticker":"T","price_dollars":"0.3500","delta_fp":"10.00","side":"no","ts_ms":5},"sending_ts_ms":7}"#;
        let out = k.on_message(delta).unwrap();
        assert_eq!(
            out.events,
            vec![BookEvent::Update {
                symbol: "T".into(),
                side: BookSide::Ask,
                price: 650_000,
                quantity: 35_000_000,
                exchange_ts_us: 7000,
            }]
        );
        assert_eq!(k.unsubscribe_msg(9).unwrap().contains("\"sids\":[1]"), true);
    }
}
