//! Single writer thread: takes stamped BookEvents from a channel and writes
//! them as SBE through AlignedSbeWriter. Flushes when every sender is dropped.

use crate::event::BookEvent;
use crate::util::AlignedSbeWriter;
use crate::util::dtos::challenge_orderbook::{
    PriceLevelUpdateDomain, SnapshotAsksEntryDomain, SnapshotBidsEntryDomain, SnapshotDomain,
};
use anyhow::Context;
use chrono::{DateTime, Utc};
use crossbeam::channel::Receiver;

/// Chunk size used by the repo's example, and the reader's fixed default.
pub const CHUNK_SIZE: usize = 64 << 20;

/// An event plus the time we read it off the socket (unix microseconds).
/// The reader stamps this at the socket read, before decoding.
#[derive(Debug, Clone, PartialEq)]
pub struct Stamped {
    pub received_at_us: i64,
    pub event: BookEvent,
}

fn ts(us: i64) -> anyhow::Result<DateTime<Utc>> {
    DateTime::from_timestamp_micros(us).context("timestamp out of range")
}

pub fn write_one(w: &mut AlignedSbeWriter, s: &Stamped) -> anyhow::Result<()> {
    let received_at = ts(s.received_at_us)?;
    match &s.event {
        BookEvent::Snapshot {
            symbol,
            bids,
            asks,
            exchange_ts_us,
        } => {
            let msg = SnapshotDomain {
                received_at,
                exchange_timestamp: ts(*exchange_ts_us)?,
                bids: bids
                    .iter()
                    .map(|&(price_level, quantity)| SnapshotBidsEntryDomain {
                        price_level,
                        quantity,
                    })
                    .collect(),
                asks: asks
                    .iter()
                    .map(|&(price_level, quantity)| SnapshotAsksEntryDomain {
                        price_level,
                        quantity,
                    })
                    .collect(),
                symbol: symbol.clone(),
            };
            w.write_msg(&msg)
        }
        BookEvent::Update {
            symbol,
            price,
            quantity,
            exchange_ts_us,
            ..
        } => {
            // The only place that touches the update schema. If the schema
            // gains a `side` field, map `side` (Bid/Ask) to it here.
            let msg = PriceLevelUpdateDomain {
                backtest_timestamp: received_at,
                exchange_timestamp: ts(*exchange_ts_us)?,
                price_level: *price,
                quantity: *quantity,
                symbol: symbol.clone(),
            };
            w.write_msg(&msg)
        }
    }
}

/// Runs until every Sender is dropped, then flushes the last partial chunk.
pub fn run(rx: Receiver<Stamped>, mut w: AlignedSbeWriter) -> anyhow::Result<()> {
    for s in rx.iter() {
        write_one(&mut w, &s)?;
    }
    w.flush()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::BookSide;
    use crate::util::dtos::challenge_orderbook::AnyMessage;
    use crate::util::{AlignedSbeReader, OrderbookSchema};

    #[test]
    fn writes_and_reads_back() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let w = AlignedSbeWriter::create(file.path(), CHUNK_SIZE).unwrap();
        let (tx, rx) = crossbeam::channel::unbounded();
        tx.send(Stamped {
            received_at_us: 2_000_000,
            event: BookEvent::Snapshot {
                symbol: "T".into(),
                bids: vec![(410_000, 30_000_000)],
                asks: vec![(650_000, 25_000_000)],
                exchange_ts_us: 1_000_000,
            },
        })
        .unwrap();
        tx.send(Stamped {
            received_at_us: 3_000_000,
            event: BookEvent::Update {
                symbol: "T".into(),
                side: BookSide::Ask,
                price: 650_000,
                quantity: 35_000_000,
                exchange_ts_us: 2_500_000,
            },
        })
        .unwrap();
        drop(tx);
        run(rx, w).unwrap();

        let mut r = AlignedSbeReader::<OrderbookSchema>::open(file.path()).unwrap();
        let Some(AnyMessage::Snapshot(d)) = r.read_msg().unwrap() else {
            panic!("snapshot")
        };
        let snap = SnapshotDomain::try_from_decoder(d).unwrap();
        assert_eq!(snap.symbol, "T");
        assert_eq!(snap.bids[0].price_level, 410_000);
        assert_eq!(snap.asks[0].quantity, 25_000_000);
        let Some(AnyMessage::PriceLevelUpdate(d)) = r.read_msg().unwrap() else {
            panic!("update")
        };
        let upd = PriceLevelUpdateDomain::try_from_decoder(d).unwrap();
        assert_eq!((upd.price_level, upd.quantity), (650_000, 35_000_000));
        assert!(r.read_msg().unwrap().is_none());
    }
}
