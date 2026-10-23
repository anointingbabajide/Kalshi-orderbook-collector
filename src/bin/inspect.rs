//! inspect <file> [symbol]: summarise an SBE output file back.
//! With a symbol, also prints every snapshot and update for that symbol.

use std::collections::BTreeMap;

use engineering_challenge::util::dtos::challenge_orderbook::{
    AnyMessage, PriceLevelUpdateDomain, SnapshotDomain,
};
use engineering_challenge::util::{AlignedSbeReader, OrderbookSchema};

/// `v` must be sorted and non-empty.
fn pct(v: &[i64], p: usize) -> i64 {
    v[(v.len() - 1) * p / 100]
}

fn main() -> anyhow::Result<()> {
    let mut args = std::env::args().skip(1);
    let path = args
        .next()
        .ok_or_else(|| anyhow::anyhow!("usage: inspect <file> [symbol]"))?;
    let dump: Option<String> = args.next();
    let mut r = AlignedSbeReader::<OrderbookSchema>::open(&path)?;

    let (mut snaps, mut updates) = (0u64, 0u64);
    let mut per_symbol: BTreeMap<String, (u64, u64)> = BTreeMap::new();
    let mut lags: Vec<i64> = Vec::new();
    // Lag grouped by 10 s of exchange time since the first update.
    let mut windows: BTreeMap<i64, Vec<i64>> = BTreeMap::new();
    let mut t0 = None;

    while let Some(msg) = r.read_msg()? {
        match msg {
            AnyMessage::Snapshot(d) => {
                let s = SnapshotDomain::try_from_decoder(d)?;
                if snaps == 0 && dump.is_none() {
                    println!(
                        "first snapshot: {} bids={} asks={} best_bid={:?} best_ask={:?}",
                        s.symbol,
                        s.bids.len(),
                        s.asks.len(),
                        s.bids.first().map(|l| (l.price_level, l.quantity)),
                        s.asks.first().map(|l| (l.price_level, l.quantity)),
                    );
                }
                if dump.as_deref() == Some(s.symbol.as_str()) {
                    println!("SNAPSHOT {}", s.symbol);
                    for l in &s.bids {
                        println!("  bid {} x {}", l.price_level, l.quantity);
                    }
                    for l in &s.asks {
                        println!("  ask {} x {}", l.price_level, l.quantity);
                    }
                }
                snaps += 1;
                per_symbol.entry(s.symbol).or_default().0 += 1;
            }
            AnyMessage::PriceLevelUpdate(d) => {
                let u = PriceLevelUpdateDomain::try_from_decoder(d)?;
                let lag = (u.backtest_timestamp - u.exchange_timestamp).num_milliseconds();
                if updates < 3 && dump.is_none() {
                    println!(
                        "update: {} price={} qty={} lag_ms={}",
                        u.symbol, u.price_level, u.quantity, lag
                    );
                }
                if dump.as_deref() == Some(u.symbol.as_str()) {
                    println!(
                        "UPDATE price={} qty={} exch={} recv={} lag_ms={}",
                        u.price_level, u.quantity, u.exchange_timestamp, u.backtest_timestamp, lag
                    );
                }
                let base = *t0.get_or_insert(u.exchange_timestamp);
                let win = (u.exchange_timestamp - base).num_seconds() / 10;
                windows.entry(win).or_default().push(lag);
                lags.push(lag);
                updates += 1;
                per_symbol.entry(u.symbol).or_default().1 += 1;
            }
            _ => {}
        }
    }

    println!(
        "snapshots={snaps} updates={updates} symbols={}",
        per_symbol.len()
    );
    if !lags.is_empty() {
        lags.sort_unstable();
        println!(
            "lag ms: p50={} p90={} p99={} max={}",
            pct(&lags, 50),
            pct(&lags, 90),
            pct(&lags, 99),
            lags[lags.len() - 1]
        );
        println!("lag by 10s window of exchange time:");
        for (w, mut v) in windows {
            v.sort_unstable();
            println!(
                "  {:>4}s  n={:<6} p50={:<6} p99={:<6} max={}",
                w * 10,
                v.len(),
                pct(&v, 50),
                pct(&v, 99),
                v[v.len() - 1]
            );
        }
    }
    let mut busiest: Vec<_> = per_symbol.into_iter().collect();
    busiest.sort_by_key(|(_, (_, u))| std::cmp::Reverse(*u));
    for (sym, (s, u)) in busiest.iter().take(5) {
        println!("  {sym}: snapshots={s} updates={u}");
    }
    Ok(())
}
