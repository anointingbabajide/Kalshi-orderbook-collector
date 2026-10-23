/// Exchange-agnostic event. Prices and quantities are 1e6 fixed point.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BookSide {
    Bid,
    Ask,
}

#[derive(Debug, Clone, PartialEq)]
pub enum BookEvent {
    Snapshot {
        symbol: String,
        bids: Vec<(i64, i64)>, // best first, YES axis
        asks: Vec<(i64, i64)>, // best first, YES axis
        exchange_ts_us: i64,
    },
    /// Absolute quantity at one level after the change; 0 means the level is gone.
    Update {
        symbol: String,
        side: BookSide,
        price: i64,
        quantity: i64,
        exchange_ts_us: i64,
    },
}
