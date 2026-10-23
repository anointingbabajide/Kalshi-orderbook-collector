//! Kalshi auth, REST market listing and websocket connect.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, bail};
use base64::Engine;
use ed25519_dalek::pkcs8::DecodePrivateKey;
use ed25519_dalek::{Signer, SigningKey};
use serde::Deserialize;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;

pub const WS_PATH: &str = "/trade-api/ws/v2";

pub type Ws =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

pub struct Endpoints {
    pub rest_base: &'static str,
    pub ws_url: &'static str,
}

/// KALSHI_ENV=demo selects the demo exchange, anything else is prod.
pub fn endpoints() -> Endpoints {
    let demo = std::env::var("KALSHI_ENV")
        .map(|v| v == "demo")
        .unwrap_or(false);
    if demo {
        Endpoints {
            rest_base: "https://demo-api.kalshi.co/trade-api/v2",
            ws_url: "wss://demo-api.kalshi.co/trade-api/ws/v2",
        }
    } else {
        Endpoints {
            rest_base: "https://api.elections.kalshi.com/trade-api/v2",
            ws_url: "wss://api.elections.kalshi.com/trade-api/ws/v2",
        }
    }
}

pub fn load_key(path: &str) -> anyhow::Result<SigningKey> {
    let pem = std::fs::read_to_string(path).context("reading private key file")?;
    SigningKey::from_pkcs8_pem(&pem).context("not a valid Ed25519 PKCS#8 PEM")
}

fn sign(key: &SigningKey, ts_ms: u128, method: &str, path: &str) -> String {
    let msg = format!("{ts_ms}{method}{path}");
    base64::engine::general_purpose::STANDARD.encode(key.sign(msg.as_bytes()).to_bytes())
}

#[derive(Deserialize)]
struct MarketsPage {
    markets: Vec<Market>,
    cursor: Option<String>,
}

#[derive(Deserialize)]
struct Market {
    ticker: String,
}

async fn fetch_page(
    client: &reqwest::Client,
    rest_base: &str,
    cursor: &str,
) -> anyhow::Result<MarketsPage> {
    let mut delay = Duration::from_millis(500);
    let mut attempt = 1;
    loop {
        let mut req = client.get(format!("{rest_base}/markets")).query(&[
            ("status", "open"),
            ("limit", "1000"),
            ("mve_filter", "exclude"),
        ]);
        if !cursor.is_empty() {
            req = req.query(&[("cursor", cursor)]);
        }
        let res: anyhow::Result<MarketsPage> = async {
            let resp = req.send().await?;
            if !resp.status().is_success() {
                bail!("GET /markets -> {} {}", resp.status(), resp.text().await?);
            }
            Ok(resp.json().await?)
        }
        .await;
        match res {
            Ok(p) => return Ok(p),
            Err(e) if attempt < 6 => {
                eprintln!("page fetch failed (attempt {attempt}): {e:#}, retrying in {delay:?}");
                tokio::time::sleep(delay).await;
                delay *= 2;
                attempt += 1;
            }
            Err(e) => return Err(e),
        }
    }
}

pub async fn list_open_markets(rest_base: &str, want: usize) -> anyhow::Result<Vec<String>> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()?;
    let mut tickers = Vec::new();
    let mut cursor = String::new();
    loop {
        let page = fetch_page(&client, rest_base, &cursor).await?;
        tickers.extend(page.markets.into_iter().map(|m| m.ticker));
        eprintln!("fetched {} markets so far", tickers.len());
        if tickers.len() >= want {
            break;
        }
        match page.cursor {
            Some(c) if !c.is_empty() => cursor = c,
            _ => break,
        }
    }
    tickers.truncate(want);
    Ok(tickers)
}
pub async fn connect(ws_url: &str, key_id: &str, key: &SigningKey) -> anyhow::Result<Ws> {
    let ts = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis();
    let mut req = ws_url.into_client_request()?;
    let h = req.headers_mut();
    h.insert("KALSHI-ACCESS-KEY", key_id.parse()?);
    h.insert("KALSHI-ACCESS-TIMESTAMP", ts.to_string().parse()?);
    h.insert(
        "KALSHI-ACCESS-SIGNATURE",
        sign(key, ts, "GET", WS_PATH).parse()?,
    );
    let (ws, resp) = tokio_tungstenite::connect_async(req).await?;
    eprintln!("ws connected: {}", resp.status());
    Ok(ws)
}
