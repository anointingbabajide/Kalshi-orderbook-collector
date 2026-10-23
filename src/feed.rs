//! One subscription's worth of books, with per-subscription seq checking.
//!
//! seq is one counter per sid, so a gap means the whole subscription is
//! suspect. After a gap we stop applying events until the caller
//! resubscribes, which produces a `Subscribed` event and resets everything.

use crate::book::Book;
use crate::decode::Event;
use std::collections::{HashMap, HashSet};

#[derive(Debug, PartialEq)]
pub enum Resync {
    Gap {
        expected: i64,
        got: i64,
    },
    /// A delta arrived for a market we never got a snapshot for.
    UnknownMarket(String),
}

#[derive(Debug, PartialEq)]
pub enum Outcome {
    Applied {
        ticker: String,
    },
    Ignored,
    /// Caller must resubscribe this whole subscription.
    Resync(Resync),
}

#[derive(Default)]
pub struct Feed {
    sid: Option<i64>,
    last_seq: Option<i64>,
    books: HashMap<String, Book>,
    /// Markets we unsubscribed from; late deltas for them are not a gap.
    removed: HashSet<String>,
    desynced: bool,
}

impl Feed {
    pub fn book(&self, ticker: &str) -> Option<&Book> {
        self.books.get(ticker)
    }
    pub fn books(&self) -> impl Iterator<Item = (&String, &Book)> {
        self.books.iter()
    }
    /// Stop tracking a market. Late deltas for it are ignored, not a resync.
    pub fn remove(&mut self, ticker: &str) {
        self.books.remove(ticker);
        self.removed.insert(ticker.to_owned());
    }

    /// False before the first `subscribed`, and after a gap until resubscribe.
    pub fn is_synced(&self) -> bool {
        self.sid.is_some() && !self.desynced
    }

    pub fn on_event(&mut self, ev: Event) -> Outcome {
        match ev {
            Event::Subscribed { sid } => {
                self.sid = Some(sid);
                self.last_seq = None;
                self.books.clear();
                self.removed.clear();
                self.desynced = false;
                Outcome::Ignored
            }
            Event::Snapshot {
                sid,
                seq,
                ticker,
                yes,
                no,
                ..
            } => {
                if let Some(o) = self.check(sid, seq) {
                    return o;
                }
                // A snapshot for a market means we are tracking it again.
                self.removed.remove(&ticker);
                self.books
                    .entry(ticker.clone())
                    .or_default()
                    .load_snapshot(yes, no);
                Outcome::Applied { ticker }
            }
            Event::Delta {
                sid,
                seq,
                ticker,
                side,
                price,
                delta,
                ..
            } => {
                if let Some(o) = self.check(sid, seq) {
                    return o;
                }
                match self.books.get_mut(&ticker) {
                    Some(b) => {
                        b.apply_delta(side, price, delta);
                        Outcome::Applied { ticker }
                    }
                    None if self.removed.contains(&ticker) => Outcome::Ignored,
                    None => {
                        self.desynced = true;
                        Outcome::Resync(Resync::UnknownMarket(ticker))
                    }
                }
            }
            // The reply to update_subscription takes a seq number like any message.
            Event::Ok { sid, seq } => self.check(sid, seq).unwrap_or(Outcome::Ignored),

            Event::Other | Event::Unsubscribed => Outcome::Ignored,
        }
    }

    /// Returns Some(outcome) if the event must not be applied.
    fn check(&mut self, sid: i64, seq: i64) -> Option<Outcome> {
        if self.desynced || self.sid.is_some_and(|s| s != sid) {
            return Some(Outcome::Ignored);
        }
        if let Some(last) = self.last_seq {
            let expected = last + 1;
            if seq != expected {
                self.desynced = true;
                return Some(Outcome::Resync(Resync::Gap { expected, got: seq }));
            }
        }
        // The first message after subscribing is accepted at whatever seq it has.
        self.last_seq = Some(seq);
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::book::Side;

    fn snap(seq: i64, t: &str) -> Event {
        Event::Snapshot {
            sid: 1,
            seq,
            ticker: t.into(),
            yes: vec![(420_000, 100_000_000)],
            no: vec![(580_000, 50_000_000)],
            sending_ts_ms: 0,
        }
    }

    fn delta(seq: i64, t: &str, d: i64) -> Event {
        Event::Delta {
            sid: 1,
            seq,
            ticker: t.into(),
            side: Side::Yes,
            price: 420_000,
            delta: d,
            ts_ms: 0,
            sending_ts_ms: 0,
        }
    }

    #[test]
    fn applies_in_order() {
        let mut f = Feed::default();
        f.on_event(Event::Subscribed { sid: 1 });
        assert!(matches!(f.on_event(snap(1, "A")), Outcome::Applied { .. }));
        assert!(matches!(
            f.on_event(delta(2, "A", 50_000_000)),
            Outcome::Applied { .. }
        ));
        let (bids, _) = f.book("A").unwrap().levels();
        assert_eq!(bids, vec![(420_000, 150_000_000)]);
    }

    #[test]
    fn gap_desyncs_whole_subscription_until_resubscribe() {
        let mut f = Feed::default();
        f.on_event(Event::Subscribed { sid: 1 });
        f.on_event(snap(1, "A"));
        f.on_event(snap(2, "B"));
        assert_eq!(
            f.on_event(delta(4, "A", 10)),
            Outcome::Resync(Resync::Gap {
                expected: 3,
                got: 4
            })
        );
        // Even a "valid" next seq is ignored while desynced, for any market.
        assert_eq!(f.on_event(delta(5, "B", 10)), Outcome::Ignored);
        f.on_event(Event::Subscribed { sid: 2 });
        assert!(f.book("A").is_none());
    }

    #[test]
    fn delta_for_unknown_market_requests_resync() {
        let mut f = Feed::default();
        f.on_event(Event::Subscribed { sid: 1 });
        assert_eq!(
            f.on_event(delta(1, "Z", 10)),
            Outcome::Resync(Resync::UnknownMarket("Z".into()))
        );
    }

    #[test]
    fn ok_reply_consumes_a_seq() {
        let mut f = Feed::default();
        f.on_event(Event::Subscribed { sid: 1 });
        f.on_event(snap(1, "A"));
        assert_eq!(f.on_event(Event::Ok { sid: 1, seq: 2 }), Outcome::Ignored);
        assert!(matches!(
            f.on_event(delta(3, "A", 10)),
            Outcome::Applied { .. }
        ));
    }

    #[test]
    fn late_delta_for_removed_market_is_ignored() {
        let mut f = Feed::default();
        f.on_event(Event::Subscribed { sid: 1 });
        f.on_event(snap(1, "A"));
        f.remove("A");
        assert_eq!(f.on_event(delta(2, "A", 10)), Outcome::Ignored);
        // seq still advanced, so the next message is in order
        f.on_event(snap(3, "A"));
        assert!(f.book("A").is_some());
    }
}
