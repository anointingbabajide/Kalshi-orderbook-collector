//! A fake exchange with an invented text format, used to show the collector
//! is generic.
//!   client -> server:  "sub FAKE-1,FAKE-2"
//!   server -> client:  "snap <SYM> <price> <qty> <ts_us>"
//!                      "upd  <SYM> <price> <qty> <ts_us>"

use std::collections::BTreeMap;

use crate::conn::Ws;
use crate::event::{BookEvent, BookSide};
use crate::exchange::{Codec, Decoded, Exchange};

#[derive(Default)]
pub struct FakeCodec {
    books: BTreeMap<String, BTreeMap<i64, i64>>,
    synced: bool,
    last_ts: i64,
}

impl Codec for FakeCodec {
    fn subscribe_msg(_id: i64, tickers: &[String]) -> String {
        format!("sub {}", tickers.join(","))
    }
    fn add_msg(&self, _id: i64, t: &[String]) -> Option<String> {
        Some(format!("add {}", t.join(",")))
    }
    fn remove_msg(&self, _id: i64, t: &[String]) -> Option<String> {
        Some(format!("del {}", t.join(",")))
    }
    fn unsubscribe_all_msg(&self, _id: i64) -> Option<String> {
        Some("unsub".to_string())
    }
    fn on_message(&mut self, text: &str) -> anyhow::Result<Decoded> {
        let bad = || anyhow::anyhow!("bad fake message: {text}");
        let mut it = text.split_whitespace();
        let kind = it.next().ok_or_else(bad)?;
        let sym = it.next().ok_or_else(bad)?.to_string();
        let price: i64 = it.next().ok_or_else(bad)?.parse()?;
        let qty: i64 = it.next().ok_or_else(bad)?.parse()?;
        let ts: i64 = it.next().ok_or_else(bad)?.parse()?;
        self.last_ts = ts;

        let mut out = Decoded::default();
        match kind {
            "snap" => {
                let book = self.books.entry(sym.clone()).or_default();
                book.clear();
                book.insert(price, qty);
                self.synced = true;
                out.events.push(BookEvent::Snapshot {
                    symbol: sym,
                    bids: vec![(price as _, qty as _)],
                    asks: vec![],
                    exchange_ts_us: ts as _,
                });
            }
            "upd" => {
                self.books
                    .entry(sym.clone())
                    .or_default()
                    .insert(price, qty);
                out.events.push(BookEvent::Update {
                    symbol: sym,
                    side: BookSide::Bid,
                    price: price as _,
                    quantity: qty as _,
                    exchange_ts_us: ts as _,
                });
            }
            _ => {}
        }
        Ok(out)
    }
    fn can_update(&self) -> bool {
        true
    }
    fn is_synced(&self) -> bool {
        self.synced
    }
    fn snapshot_all(&self) -> Vec<BookEvent> {
        if !self.synced {
            return Vec::new();
        }
        self.books
            .iter()
            .map(|(sym, book)| BookEvent::Snapshot {
                symbol: sym.clone(),
                bids: book.iter().map(|(p, q)| (*p as _, *q as _)).collect(),
                asks: vec![],
                exchange_ts_us: self.last_ts as _,
            })
            .collect()
    }
    fn forget(&mut self, ticker: &str) {
        self.books.remove(ticker);
    }
}

pub struct FakeExchange {
    pub addr: String,
}

impl FakeExchange {
    pub fn new(addr: impl Into<String>) -> Self {
        Self { addr: addr.into() }
    }
}

impl Exchange for FakeExchange {
    type Codec = FakeCodec;
    async fn connect(&self) -> anyhow::Result<Ws> {
        let (ws, _) = tokio_tungstenite::connect_async(format!("ws://{}", self.addr)).await?;
        Ok(ws)
    }
    async fn list_markets(&self, n: usize) -> anyhow::Result<Vec<String>> {
        Ok(vec!["FAKE-1".to_string(), "FAKE-2".to_string()]
            .into_iter()
            .take(n)
            .collect())
    }
    fn max_groups(&self) -> usize {
        1
    }
    fn min_shard_size(&self) -> usize {
        200
    }
}
