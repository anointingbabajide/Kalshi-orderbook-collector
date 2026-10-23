use crate::conn::Ws;
use crate::event::BookEvent;
use std::future::Future;

#[derive(Default)]
pub struct Decoded {
    pub events: Vec<BookEvent>,
    pub unsubscribed: bool,
    pub resync: bool,
}

pub trait Codec: Default + Send + 'static {
    fn subscribe_msg(id: i64, tickers: &[String]) -> String;
    fn add_msg(&self, id: i64, tickers: &[String]) -> Option<String>;
    fn remove_msg(&self, id: i64, tickers: &[String]) -> Option<String>;
    fn unsubscribe_all_msg(&self, id: i64) -> Option<String>;
    fn on_message(&mut self, text: &str) -> anyhow::Result<Decoded>;
    fn can_update(&self) -> bool;
    fn is_synced(&self) -> bool;
    fn snapshot_all(&self) -> Vec<BookEvent>;
    fn forget(&mut self, ticker: &str);
}

pub trait Exchange: Send + Sync + 'static {
    type Codec: Codec;
    fn connect(&self) -> impl Future<Output = anyhow::Result<Ws>> + Send;
    fn list_markets(&self, n: usize) -> impl Future<Output = anyhow::Result<Vec<String>>> + Send;
    fn max_groups(&self) -> usize;
    fn min_shard_size(&self) -> usize;
}
