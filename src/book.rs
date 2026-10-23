//! In-memory orderbook for one Kalshi market, plus fixed-point parsing.
//!
//! Kalshi sends two bid ladders: YES bids and NO bids. A NO bid at price p is
//! the same as a YES ask at (1 - p), so we store both ladders as received and
//! map NO to the YES price axis only when producing output.

use std::collections::BTreeMap;

/// 1_000_000 = 1 dollar / 1 contract, matching the output schema.
pub const SCALE: i64 = 1_000_000;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Side {
    Yes,
    No,
}

/// Parse a decimal string like "0.0100", "500.00" or "-12.5" into 1e6 fixed
/// point without going through floats. Returns None on bad input or if the
/// value has more than 6 non-zero decimals.
pub fn parse_fp(s: &str) -> Option<i64> {
    let (neg, s) = match s.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, s),
    };
    let (int, frac) = s.split_once('.').unwrap_or((s, ""));
    if int.is_empty() && frac.is_empty() {
        return None;
    }
    if !int.bytes().all(|b| b.is_ascii_digit()) || !frac.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    if frac.len() > 6 && frac[6..].bytes().any(|b| b != b'0') {
        return None;
    }
    let frac6 = &frac[..frac.len().min(6)];
    let int_v: i64 = if int.is_empty() { 0 } else { int.parse().ok()? };
    let mut frac_v: i64 = if frac6.is_empty() {
        0
    } else {
        frac6.parse().ok()?
    };
    for _ in frac6.len()..6 {
        frac_v *= 10;
    }
    let v = int_v.checked_mul(SCALE)?.checked_add(frac_v)?;
    Some(if neg { -v } else { v })
}

#[derive(Default, Debug)]
pub struct Book {
    yes: BTreeMap<i64, i64>, // YES bids: price -> quantity
    no: BTreeMap<i64, i64>,  // NO bids: price -> quantity
}

impl Book {
    fn ladder(&mut self, side: Side) -> &mut BTreeMap<i64, i64> {
        match side {
            Side::Yes => &mut self.yes,
            Side::No => &mut self.no,
        }
    }
    /// Current quantity at a level, 0 if absent.
    pub fn qty(&self, side: Side, price: i64) -> i64 {
        match side {
            Side::Yes => &self.yes,
            Side::No => &self.no,
        }
        .get(&price)
        .copied()
        .unwrap_or(0)
    }

    /// Replace the whole book from a snapshot. Levels with quantity <= 0 are skipped.
    pub fn load_snapshot(
        &mut self,
        yes: impl IntoIterator<Item = (i64, i64)>,
        no: impl IntoIterator<Item = (i64, i64)>,
    ) {
        self.yes.clear();
        self.no.clear();
        self.yes.extend(yes.into_iter().filter(|&(_, q)| q > 0));
        self.no.extend(no.into_iter().filter(|&(_, q)| q > 0));
    }

    /// Add `delta` to the quantity at `price`. Removes the level when it
    /// reaches zero or below. Returns the new total (0 means removed).
    pub fn apply_delta(&mut self, side: Side, price: i64, delta: i64) -> i64 {
        let ladder = self.ladder(side);
        let new = ladder.get(&price).copied().unwrap_or(0) + delta;
        if new <= 0 {
            ladder.remove(&price);
            0
        } else {
            ladder.insert(price, new);
            new
        }
    }

    /// Where a (side, price) level lives on the YES price axis:
    /// returns (is_bid, price_level).
    pub fn to_yes_axis(side: Side, price: i64) -> (bool, i64) {
        match side {
            Side::Yes => (true, price),
            Side::No => (false, SCALE - price),
        }
    }

    /// (bids best-first, asks best-first) on the YES price axis.
    pub fn levels(&self) -> (Vec<(i64, i64)>, Vec<(i64, i64)>) {
        let bids = self.yes.iter().rev().map(|(&p, &q)| (p, q)).collect();
        let asks = self
            .no
            .iter()
            .rev()
            .map(|(&p, &q)| (SCALE - p, q))
            .collect();
        (bids, asks)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_fixed_point() {
        assert_eq!(parse_fp("0.0100"), Some(10_000));
        assert_eq!(parse_fp("500.00"), Some(500_000_000));
        assert_eq!(parse_fp("1"), Some(1_000_000));
        assert_eq!(parse_fp("-12.5"), Some(-12_500_000));
        assert_eq!(parse_fp("0.1234560"), Some(123_456));
        assert_eq!(parse_fp("0.1234567"), None);
        assert_eq!(parse_fp(""), None);
        assert_eq!(parse_fp("+5"), None);
        assert_eq!(parse_fp("1.2x"), None);
    }

    #[test]
    fn delta_adds_and_removes() {
        let mut b = Book::default();
        assert_eq!(b.apply_delta(Side::Yes, 420_000, 100), 100);
        assert_eq!(b.apply_delta(Side::Yes, 420_000, 50), 150);
        assert_eq!(b.apply_delta(Side::Yes, 420_000, -150), 0);
        assert!(b.levels().0.is_empty());
        assert_eq!(b.apply_delta(Side::Yes, 400_000, 30), 30);
    }

    #[test]
    fn snapshot_maps_no_bids_to_asks() {
        let mut b = Book::default();
        b.load_snapshot(
            [(10_000, 500_000_000), (50_000, 125_000_000)],
            [(10_000, 403_000_000), (20_000, 350_000_000)],
        );
        let (bids, asks) = b.levels();
        assert_eq!(bids, vec![(50_000, 125_000_000), (10_000, 500_000_000)]);
        assert_eq!(asks, vec![(980_000, 350_000_000), (990_000, 403_000_000)]);
        assert_eq!(Book::to_yes_axis(Side::No, 20_000), (false, 980_000));
    }
}
