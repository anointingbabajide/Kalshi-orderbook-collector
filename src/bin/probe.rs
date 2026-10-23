//! Step 1: list open Kalshi markets, then print raw orderbook messages from an
//! authenticated websocket to stdout.
//!
//! env: KALSHI_KEY_ID, KALSHI_KEY_PATH (PEM file), KALSHI_ENV=demo|prod (default prod)
//! args: [n_tickers] [seconds]

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, bail};
use base64::Engine;
use ed25519_dalek::pkcs8::DecodePrivateKey;
use ed25519_dalek::{Signer, SigningKey};
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;

const WS_PATH: &str = "/trade-api/ws/v2";

#[derive(Deserialize)]
struct MarketsPage {
    markets: Vec<Market>,
    cursor: Option<String>,
}

#[derive(Deserialize)]
struct Market {
    ticker: String,
}

fn load_key(path: &str) -> anyhow::Result<SigningKey> {
    let pem = std::fs::read_to_string(path).context("reading private key file")?;
    SigningKey::from_pkcs8_pem(&pem).context("not a valid Ed25519 PKCS#8 PEM")
}

fn sign(key: &SigningKey, ts_ms: u128, method: &str, path: &str) -> String {
    // message = timestamp_ms + METHOD + path (no query string)
    let msg = format!("{ts_ms}{method}{path}");
    let sig = key.sign(msg.as_bytes());
    base64::engine::general_purpose::STANDARD.encode(sig.to_bytes())
}
async fn list_open_markets(rest_base: &str, want: usize) -> anyhow::Result<Vec<String>> {
    let client = reqwest::Client::new();
    let mut tickers = Vec::new();
    let mut cursor = String::new();
    loop {
        let mut req = client.get(format!("{rest_base}/markets")).query(&[
            ("status", "open"),
            ("limit", "1000"),
            ("mve_filter", "exclude"),
        ]);
        if !cursor.is_empty() {
            req = req.query(&[("cursor", cursor.as_str())]);
        }
        let resp = req.send().await?;
        if !resp.status().is_success() {
            bail!("GET /markets -> {} {}", resp.status(), resp.text().await?);
        }
        let page: MarketsPage = resp.json().await?;
        tickers.extend(page.markets.into_iter().map(|m| m.ticker));
        if tickers.len() >= want {
            break;
        }
        match page.cursor {
            Some(c) if !c.is_empty() => cursor = c,
            _ => break,
        }
    }
    Ok(tickers)
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let key_id = std::env::var("KALSHI_KEY_ID").context("KALSHI_KEY_ID not set")?;
    let key_path = std::env::var("KALSHI_KEY_PATH").context("KALSHI_KEY_PATH not set")?;
    let demo = std::env::var("KALSHI_ENV")
        .map(|v| v == "demo")
        .unwrap_or(false);
    let (rest_base, ws_url) = if demo {
        (
            "https://demo-api.kalshi.co/trade-api/v2",
            "wss://demo-api.kalshi.co/trade-api/ws/v2",
        )
    } else {
        (
            "https://api.elections.kalshi.com/trade-api/v2",
            "wss://api.elections.kalshi.com/trade-api/ws/v2",
        )
    };
    let mut args = std::env::args().skip(1);
    let n: usize = args.next().and_then(|s| s.parse().ok()).unwrap_or(5);
    let secs: u64 = args.next().and_then(|s| s.parse().ok()).unwrap_or(20);

    let tickers = list_open_markets(rest_base, n).await?;
    eprintln!("fetched {} tickers", tickers.len());
    for t in tickers.iter().take(5) {
        eprintln!("  {t}");
    }
    let chosen: Vec<&String> = tickers.iter().take(n).collect();

    let key = load_key(&key_path)?;
    let ts = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis();
    let mut req = ws_url.into_client_request()?;
    let h = req.headers_mut();
    h.insert("KALSHI-ACCESS-KEY", key_id.parse()?);
    h.insert("KALSHI-ACCESS-TIMESTAMP", ts.to_string().parse()?);
    h.insert(
        "KALSHI-ACCESS-SIGNATURE",
        sign(&key, ts, "GET", WS_PATH).parse()?,
    );

    let (mut ws, resp) = tokio_tungstenite::connect_async(req).await?;
    eprintln!("ws connected: {}", resp.status());

    let sub = serde_json::json!({
        "id": 1,
        "cmd": "subscribe",
        "params": { "channels": ["orderbook_delta"], "market_tickers": chosen },
    });
    ws.send(Message::Text(sub.to_string())).await?;

    let deadline = tokio::time::sleep(Duration::from_secs(secs));
    tokio::pin!(deadline);
    loop {
        tokio::select! {
            _ = &mut deadline => break,
            msg = ws.next() => match msg {
                Some(Ok(Message::Text(t))) => println!("{t}"),
                Some(Ok(Message::Close(f))) => { eprintln!("closed: {f:?}"); break }
                Some(Ok(_)) => {}
                Some(Err(e)) => { eprintln!("ws error: {e}"); break }
                None => break,
            }
        }
    }
    ws.close(None).await.ok();
    Ok(())
}
