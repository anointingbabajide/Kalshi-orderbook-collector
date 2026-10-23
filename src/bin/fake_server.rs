//! fake_server [addr]  (default 127.0.0.1:9001)
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpListener;
use tokio_tungstenite::tungstenite::Message;

fn now_us() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_micros() as i64
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let addr = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:9001".into());
    let listener = TcpListener::bind(&addr).await?;
    eprintln!("fake exchange listening on ws://{addr}");
    loop {
        let (stream, peer) = listener.accept().await?;
        eprintln!("client connected: {peer}");
        tokio::spawn(async move {
            let Ok(mut ws) = tokio_tungstenite::accept_async(stream).await else {
                return;
            };
            match ws.next().await {
                Some(Ok(Message::Text(t))) => eprintln!("got: {t}"),
                _ => return,
            }
            let ts = now_us();
            ws.send(Message::Text(format!("snap FAKE-1 100 5 {ts}")))
                .await
                .ok();
            ws.send(Message::Text(format!("snap FAKE-2 200 7 {ts}")))
                .await
                .ok();
            let mut i = 0i64;
            loop {
                tokio::time::sleep(Duration::from_millis(200)).await;
                let msg = format!("upd FAKE-1 {} {} {}", 101 + i % 50, 10 + i, now_us());
                if ws.send(Message::Text(msg)).await.is_err() {
                    return;
                }
                i += 1;
            }
        });
    }
}
