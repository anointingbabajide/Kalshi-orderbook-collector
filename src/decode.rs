//! Raw Kalshi websocket JSON -> typed events. Prices and quantities are
//! parsed from strings with `parse_fp`, never through floats.

use crate::book::{Side, parse_fp};
use serde_json::Value;

#[derive(Debug, PartialEq)]
pub enum Event {
    Subscribed {
        sid: i64,
    },
    Snapshot {
        sid: i64,
        seq: i64,
        ticker: String,
        yes: Vec<(i64, i64)>, // YES bids: (price, qty), 1e6 scale
        no: Vec<(i64, i64)>,  // NO bids: (price, qty), 1e6 scale
        sending_ts_ms: i64,
    },
    Delta {
        sid: i64,
        seq: i64,
        ticker: String,
        side: Side,
        price: i64,
        delta: i64, // signed change in quantity
        ts_ms: i64, // when the event happened
        sending_ts_ms: i64,
    },
    /// Reply to `update_subscription`. It consumes a seq on the subscription.
    Ok {
        sid: i64,
        seq: i64,
    },
    Unsubscribed,
    /// Any message type we do not handle (errors, pings, other channels).
    Other,
}

#[derive(Debug, PartialEq)]
pub enum DecodeError {
    Json(String),
    Missing(&'static str),
    BadNumber(&'static str),
}

fn get_i64(v: &Value, key: &'static str) -> Result<i64, DecodeError> {
    v.get(key)
        .and_then(Value::as_i64)
        .ok_or(DecodeError::Missing(key))
}

fn get_str<'a>(v: &'a Value, key: &'static str) -> Result<&'a str, DecodeError> {
    v.get(key)
        .and_then(Value::as_str)
        .ok_or(DecodeError::Missing(key))
}

fn get_fp(v: &Value, key: &'static str) -> Result<i64, DecodeError> {
    parse_fp(get_str(v, key)?).ok_or(DecodeError::BadNumber(key))
}

/// A missing ladder means an empty side. Each level is ["price", "qty"].
fn ladder(msg: &Value, key: &'static str) -> Result<Vec<(i64, i64)>, DecodeError> {
    let Some(arr) = msg.get(key) else {
        return Ok(Vec::new());
    };
    let arr = arr.as_array().ok_or(DecodeError::BadNumber(key))?;
    arr.iter()
        .map(|lvl| {
            let p = lvl.get(0).and_then(Value::as_str).and_then(parse_fp);
            let q = lvl.get(1).and_then(Value::as_str).and_then(parse_fp);
            match (p, q) {
                (Some(p), Some(q)) => Ok((p, q)),
                _ => Err(DecodeError::BadNumber(key)),
            }
        })
        .collect()
}

pub fn decode(raw: &str) -> Result<Event, DecodeError> {
    let v: Value = serde_json::from_str(raw).map_err(|e| DecodeError::Json(e.to_string()))?;
    let ty = get_str(&v, "type")?;
    match ty {
        "subscribed" => {
            let msg = v.get("msg").ok_or(DecodeError::Missing("msg"))?;
            Ok(Event::Subscribed {
                sid: get_i64(msg, "sid")?,
            })
        }
        "unsubscribed" => Ok(Event::Unsubscribed),
        "orderbook_snapshot" => {
            let msg = v.get("msg").ok_or(DecodeError::Missing("msg"))?;
            Ok(Event::Snapshot {
                sid: get_i64(&v, "sid")?,
                seq: get_i64(&v, "seq")?,
                ticker: get_str(msg, "market_ticker")?.to_owned(),
                yes: ladder(msg, "yes_dollars_fp")?,
                no: ladder(msg, "no_dollars_fp")?,
                sending_ts_ms: get_i64(&v, "sending_ts_ms")?,
            })
        }

        "orderbook_delta" => {
            let msg = v.get("msg").ok_or(DecodeError::Missing("msg"))?;
            let side = match get_str(msg, "side")? {
                "yes" => Side::Yes,
                "no" => Side::No,
                _ => return Err(DecodeError::BadNumber("side")),
            };
            Ok(Event::Delta {
                sid: get_i64(&v, "sid")?,
                seq: get_i64(&v, "seq")?,
                ticker: get_str(msg, "market_ticker")?.to_owned(),
                side,
                price: get_fp(msg, "price_dollars")?,
                delta: get_fp(msg, "delta_fp")?,
                ts_ms: get_i64(msg, "ts_ms")?,
                sending_ts_ms: get_i64(&v, "sending_ts_ms")?,
            })
        }
        // Reply to update_subscription: carries the subscription's next seq.
        "ok" => match (
            v.get("sid").and_then(Value::as_i64),
            v.get("seq").and_then(Value::as_i64),
        ) {
            (Some(sid), Some(seq)) => Ok(Event::Ok { sid, seq }),
            _ => Ok(Event::Other),
        },
        _ => Ok(Event::Other),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decodes_real_delta() {
        let raw = r#"{"type":"orderbook_delta","sid":1,"seq":1001,"msg":{"market_ticker":"KXTEMPNYCHS-26OCT2219-T62.99","market_id":"01a16598-17fd-794c-b7c8-3ca33c8d379c","price_dollars":"0.7300","delta_fp":"47.00","side":"no","ts":"2026-10-22T22:37:51.749Z","ts_ms":1792708671749},"sending_ts_ms":1792708671755}"#;
        assert_eq!(
            decode(raw),
            Ok(Event::Delta {
                sid: 1,
                seq: 1001,
                ticker: "KXTEMPNYCHS-26OCT2219-T62.99".into(),
                side: Side::No,
                price: 730_000,
                delta: 47_000_000,
                ts_ms: 1792708671749,
                sending_ts_ms: 1792708671755,
            })
        );
    }

    #[test]
    fn decodes_subscribed() {
        let raw = r#"{"type":"subscribed","id":1,"msg":{"channel":"orderbook_delta","sid":1},"sending_ts_ms":1792708671726}"#;
        assert_eq!(decode(raw), Ok(Event::Subscribed { sid: 1 }));
    }

    #[test]
    fn decodes_ok_with_seq() {
        let raw = r#"{"id":5,"sid":1,"seq":222,"type":"ok","market_tickers":["A","B"]}"#;
        assert_eq!(decode(raw), Ok(Event::Ok { sid: 1, seq: 222 }));
    }

    #[test]
    fn rejects_bad_number_and_ignores_unknown() {
        let bad = r#"{"type":"orderbook_delta","sid":1,"seq":2,"msg":{"market_ticker":"X","price_dollars":"abc","delta_fp":"1.00","side":"yes","ts_ms":1},"sending_ts_ms":1}"#;
        assert_eq!(decode(bad), Err(DecodeError::BadNumber("price_dollars")));
        assert_eq!(decode(r#"{"type":"pong"}"#), Ok(Event::Other));
    }
    #[test]
    fn decodes_real_snapshot() {
        let raw = r#"{"type":"orderbook_snapshot","sid":1,"seq":1,"msg":{"market_ticker":"KXNBAAST-26OCT22CLEPHI-CLESMERRILL5-2","market_id":"01a165c8-b1ed-737e-b77a-7af64d6edc6f","yes_dollars_fp":[["0.0100","75.00"],["0.0200","13.00"],["0.0500","1.00"],["0.3500","25.00"],["0.4000","50.00"],["0.4100","30.00"]],"no_dollars_fp":[["0.0100","627.00"],["0.0200","200.00"],["0.0300","800.00"],["0.0400","22.00"],["0.0500","1.00"],["0.2900","29.00"],["0.3400","50.00"],["0.3500","25.00"]]},"sending_ts_ms":1792709201719}"#;
        let ev = decode(raw).unwrap();
        let Event::Snapshot {
            sid,
            seq,
            ticker,
            yes,
            no,
            sending_ts_ms,
        } = ev
        else {
            panic!("expected snapshot");
        };
        assert_eq!((sid, seq, sending_ts_ms), (1, 1, 1792709201719));
        assert_eq!(ticker, "KXNBAAST-26OCT22CLEPHI-CLESMERRILL5-2");
        assert_eq!(
            yes,
            vec![
                (10_000, 75_000_000),
                (20_000, 13_000_000),
                (50_000, 1_000_000),
                (350_000, 25_000_000),
                (400_000, 50_000_000),
                (410_000, 30_000_000),
            ]
        );
        assert_eq!(no.len(), 8);
        assert_eq!(no[7], (350_000, 25_000_000));

        // Through the book: best bid 0.41, best ask is 1 - 0.35 = 0.65.
        let mut b = crate::book::Book::default();
        b.load_snapshot(yes, no);
        let (bids, asks) = b.levels();
        assert_eq!(bids[0], (410_000, 30_000_000));
        assert_eq!(asks[0], (650_000, 25_000_000));
    }
}
