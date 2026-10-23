//! collector <output_path> [n_tickers (0 = all)] [seconds]
//! env: KALSHI_KEY_ID, KALSHI_KEY_PATH, KALSHI_ENV=demo|prod
//!
//! Each shard is a *group* of two redundant feeds (a and b). Both keep their
//! own socket and order books; a shared gate lets only the active feed write.
//! When a feed becomes the source it first emits a full snapshot.
//!
//! Extension 2: per-symbol activity monitoring (quiet symbols are unsubscribed
//! and resubscribed) and periodic socket recycling (standby first, then swap).
//! Extension 3: exchange-specific code lives behind the Exchange / Codec traits.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::Context;
use crossbeam::channel::Sender;
use engineering_challenge::conn::{self, Ws};
use engineering_challenge::event::BookEvent;
use engineering_challenge::exchange::{Codec, Exchange};
use engineering_challenge::fake::FakeExchange;
use engineering_challenge::kalshi::KalshiExchange;
use engineering_challenge::util::AlignedSbeWriter;
use engineering_challenge::writer::{self, Stamped};

use futures_util::{SinkExt, StreamExt};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};
use tokio::task::JoinHandle;
use tokio::time::{Instant, MissedTickBehavior};
use tokio_tungstenite::tungstenite::Message;

/// No frame at all (data or ping) for this long means the connection is dead.
const IDLE_SECS: u64 = 60;
const SNAPSHOT_SECS: u64 = 60;
/// How often to re-list open markets and look for new/closed ones.
const DISCOVERY_SECS: u64 = 300;
/// The active feed with no data for this long may be replaced by a healthy standby.
const STALE_MS: u64 = 15_000;
/// Minimum time between two source switches in one group (anti-flapping).
const MIN_SWITCH: Duration = Duration::from_secs(10);
/// A send that takes longer than this means the socket is wedged.
const SEND_TIMEOUT: Duration = Duration::from_secs(5);

// ---- Extension 2: socket recycling ----
/// Recycle each group's sockets this often (set to 60 while testing).
const RECYCLE_SECS: u64 = 1800;
/// Give up on a recycle that has not completed in this long.
const RECYCLE_TIMEOUT: Duration = Duration::from_secs(120);
/// A recycled feed must stay healthy this long before it is promoted,
/// so every market's snapshot has time to arrive.
const WARMUP: Duration = Duration::from_secs(10);

// ---- Extension 2: per-symbol monitoring ----
const MON_SECS: u64 = 30;
/// Messages needed before a symbol's rate is trusted.
const MIN_SAMPLES: u32 = 20;
/// Silence must exceed this many times the symbol's usual gap...
const QUIET_FACTOR: f64 = 20.0;
/// ...and at least this many seconds.
const QUIET_FLOOR_S: f64 = 90.0;
const RESUB_COOLDOWN: Duration = Duration::from_secs(600);
/// At most this many symbols are resubscribed per monitoring round.
const MAX_RESUB: usize = 25;

enum Exit {
    /// Time limit or Ctrl-C.
    Stop,
    /// Connection lost; connect again.
    Reconnect,
    /// The writer thread is gone; its own error is reported on join.
    WriterStopped,
}

/// Sent from the discovery task to a shard.
#[derive(Clone)]
enum Cmd {
    Add(Vec<String>),
    Remove(Vec<String>),
}

struct ShardSlot {
    /// One command channel per feed in the group.
    txs: Vec<UnboundedSender<Cmd>>,
    count: usize,
}

impl ShardSlot {
    fn send(&self, c: Cmd) {
        for t in &self.txs {
            t.send(c.clone()).ok();
        }
    }
}

/// What the supervisor can see of one feed.
#[derive(Default)]
struct Health {
    connected: AtomicBool,
    synced: AtomicBool,
    /// Group-epoch milliseconds of the last text (data) frame.
    last_data_ms: AtomicU64,
    /// Set by the supervisor; the feed drops its socket and reconnects.
    recycle: AtomicBool,
}

/// State shared by the two feeds of one shard and its supervisor.
struct Group {
    epoch: Instant,
    /// Id of the feed allowed to write. Held while sending, so a switch can
    /// never interleave with a write from the old source.
    active: Mutex<usize>,
    health: [Health; 2],
}

impl Group {
    fn new() -> Self {
        Group {
            epoch: Instant::now(),
            active: Mutex::new(0),
            health: Default::default(),
        }
    }

    fn ms(&self) -> u64 {
        self.epoch.elapsed().as_millis() as u64
    }

    fn healthy(&self, i: usize) -> bool {
        let h = &self.health[i];
        h.connected.load(Ordering::Relaxed)
            && h.synced.load(Ordering::Relaxed)
            && self
                .ms()
                .saturating_sub(h.last_data_ms.load(Ordering::Relaxed))
                < STALE_MS
    }

    /// Switch only if the source is unhealthy and the standby is healthy.
    /// If both look dead (e.g. a quiet market), do nothing.
    fn maybe_switch(&self, idx: usize, last_switch: &mut Instant) {
        let mut active = self.active.lock().unwrap();
        let (cur, other) = (*active, 1 - *active);
        if self.healthy(cur) || !self.healthy(other) || last_switch.elapsed() < MIN_SWITCH {
            return;
        }
        eprintln!("[shard {idx}] switching source {cur} -> {other}");
        *active = other;
        *last_switch = Instant::now();
    }
}

/// Progress of a recycle the supervisor is waiting on.
#[derive(Clone, Copy)]
struct Wait {
    feed: usize,
    since: Instant,
    /// The feed has actually gone down (so a later "healthy" is the new socket).
    saw_down: bool,
    ready_since: Option<Instant>,
}

async fn supervise(idx: usize, group: Arc<Group>, deadline: Option<Instant>) {
    let mut tick = tokio::time::interval(Duration::from_millis(500));
    tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let mut last_switch = Instant::now() - MIN_SWITCH;
    // Jitter so 50 groups don't all recycle at once.
    let mut next_recycle =
        Instant::now() + Duration::from_secs(RECYCLE_SECS + (idx as u64 * 37) % 300);
    let mut waiting: Option<Wait> = None;
    loop {
        tokio::select! {
            _ = tick.tick() => {}
            _ = until(deadline) => break,
            _ = tokio::signal::ctrl_c() => break,
        }
        group.maybe_switch(idx, &mut last_switch);
        let now = Instant::now();

        match waiting {
            None if now >= next_recycle => {
                let standby = 1 - *group.active.lock().unwrap();
                if group.healthy(standby) {
                    eprintln!("[shard {idx}] recycling standby {standby}");
                    group.health[standby].recycle.store(true, Ordering::Relaxed);
                    waiting = Some(Wait {
                        feed: standby,
                        since: now,
                        saw_down: false,
                        ready_since: None,
                    });
                } else {
                    next_recycle = now + Duration::from_secs(60); // try again soon
                }
            }
            Some(mut w) => {
                let h = &group.health[w.feed];
                if !h.connected.load(Ordering::Relaxed) {
                    w.saw_down = true;
                    w.ready_since = None;
                } else if w.saw_down && group.healthy(w.feed) {
                    w.ready_since.get_or_insert(now);
                } else {
                    w.ready_since = None;
                }
                if w.since.elapsed() > RECYCLE_TIMEOUT {
                    eprintln!("[shard {idx}] recycle of feed {} timed out", w.feed);
                    h.recycle.store(false, Ordering::Relaxed);
                    waiting = None;
                    next_recycle = now + Duration::from_secs(60);
                } else if w.ready_since.is_some_and(|r| r.elapsed() >= WARMUP) {
                    {
                        let mut a = group.active.lock().unwrap();
                        if *a != w.feed {
                            eprintln!("[shard {idx}] promoting recycled feed {}", w.feed);
                            *a = w.feed;
                            last_switch = now;
                        }
                    }
                    // Now recycle the other one; it becomes the new standby.
                    group.health[1 - w.feed]
                        .recycle
                        .store(true, Ordering::Relaxed);
                    waiting = None;
                    next_recycle = now + Duration::from_secs(RECYCLE_SECS);
                } else {
                    waiting = Some(w);
                }
            }
            None => {}
        }
    }
}

#[derive(Debug)]
struct WriterGone;

/// A feed's only path to the writer.
struct Sink {
    id: usize,
    group: Arc<Group>,
    tx: Sender<Stamped>,
    emitting: bool,
}

impl Sink {
    /// Sends `events` if this feed is the source. On becoming the source (or
    /// after losing sync) it sends a full snapshot instead, since the triggering
    /// batch is already reflected in the books and `Update` quantities are absolute.
    fn publish<C: Codec>(
        &mut self,
        codec: &C,
        received_at_us: i64,
        events: Vec<BookEvent>,
        force_snapshot: bool,
    ) -> Result<(), WriterGone> {
        let active = self.group.active.lock().unwrap();
        if *active != self.id || !codec.is_synced() {
            self.emitting = false;
            return Ok(());
        }
        let out = if force_snapshot || !self.emitting {
            self.emitting = true;
            codec.snapshot_all()
        } else {
            events
        };
        for event in out {
            self.tx
                .send(Stamped {
                    received_at_us,
                    event,
                })
                .map_err(|_| WriterGone)?;
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Per-symbol activity monitor
// ---------------------------------------------------------------------------

struct SymStats {
    last: Instant,
    /// Smoothed gap between messages, in seconds.
    ewma_s: f64,
    n: u32,
    /// When this symbol was last resubscribed by the monitor.
    fixed: Option<Instant>,
}

#[derive(Default)]
struct Monitor {
    syms: HashMap<String, SymStats>,
}

impl Monitor {
    fn record(&mut self, sym: &str, now: Instant) {
        match self.syms.get_mut(sym) {
            Some(s) => {
                let mut dt = now.duration_since(s.last).as_secs_f64();
                if s.n >= 5 {
                    // A long gap shouldn't poison the average.
                    dt = dt.min(s.ewma_s * 10.0);
                }
                s.ewma_s = if s.n <= 1 {
                    dt
                } else {
                    0.9 * s.ewma_s + 0.1 * dt
                };
                s.last = now;
                s.n = s.n.saturating_add(1);
            }
            None => {
                self.syms.insert(
                    sym.to_string(),
                    SymStats {
                        last: now,
                        ewma_s: 0.0,
                        n: 1,
                        fixed: None,
                    },
                );
            }
        }
    }

    /// Symbols whose silence is far beyond their own norm.
    fn stalled(&self, now: Instant) -> Vec<String> {
        let (mut quiet, mut eligible) = (Vec::new(), 0usize);
        for (t, s) in &self.syms {
            if s.n < MIN_SAMPLES {
                continue;
            }
            eligible += 1;
            let limit = (QUIET_FACTOR * s.ewma_s).max(QUIET_FLOOR_S);
            let cooled = s
                .fixed
                .map_or(true, |f| now.duration_since(f) > RESUB_COOLDOWN);
            if now.duration_since(s.last).as_secs_f64() > limit && cooled {
                quiet.push(t.clone());
            }
        }
        // Most of the shard quiet = a socket problem, not a symbol problem.
        if eligible > 0 && quiet.len() * 2 > eligible {
            eprintln!(
                "[monitor] {}/{} symbols quiet, leaving it to group health",
                quiet.len(),
                eligible
            );
            return Vec::new();
        }
        quiet.truncate(MAX_RESUB);
        quiet
    }

    fn mark_fixed(&mut self, ts: &[String], now: Instant) {
        for t in ts {
            if let Some(s) = self.syms.get_mut(t) {
                s.last = now;
                s.fixed = Some(now);
            }
        }
    }
}

fn ev_symbol(e: &BookEvent) -> Option<&str> {
    match e {
        BookEvent::Update { symbol, .. } | BookEvent::Snapshot { symbol, .. } => {
            Some(symbol.as_str())
        }
        #[allow(unreachable_patterns)]
        _ => None,
    }
}

// ---------------------------------------------------------------------------

fn now_us() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_micros() as i64
}

async fn until(d: Option<Instant>) {
    match d {
        Some(d) => tokio::time::sleep_until(d).await,
        None => std::future::pending::<()>().await,
    }
}

/// Every websocket send goes through here so a wedged socket can't hang a feed.
async fn send_to(ws: &mut Ws, m: Message) -> anyhow::Result<()> {
    tokio::time::timeout(SEND_TIMEOUT, ws.send(m))
        .await
        .context("websocket send timed out")??;
    Ok(())
}

async fn subscribe<C: Codec>(ws: &mut Ws, id: &mut i64, tickers: &[String]) -> anyhow::Result<()> {
    *id += 1;
    send_to(ws, Message::Text(C::subscribe_msg(*id, tickers))).await
}

/// Update the shard's own ticker list; returns only what actually changed.
fn apply(tickers: &mut Vec<String>, cmd: Cmd) -> Cmd {
    match cmd {
        Cmd::Add(ts) => {
            let new: Vec<String> = ts.into_iter().filter(|t| !tickers.contains(t)).collect();
            tickers.extend(new.iter().cloned());
            Cmd::Add(new)
        }
        Cmd::Remove(ts) => {
            let gone: Vec<String> = ts.into_iter().filter(|t| tickers.contains(t)).collect();
            tickers.retain(|t| !gone.contains(t));
            Cmd::Remove(gone)
        }
    }
}

/// Tell the exchange about a change on the existing subscription.
async fn send_update<C: Codec>(
    ws: &mut Ws,
    codec: &mut C,
    id: &mut i64,
    change: &Cmd,
) -> anyhow::Result<()> {
    let ts = match change {
        Cmd::Add(t) | Cmd::Remove(t) => t,
    };
    if ts.is_empty() {
        return Ok(());
    }
    *id += 1;
    let msg = match change {
        Cmd::Add(t) => codec.add_msg(*id, t),
        Cmd::Remove(t) => codec.remove_msg(*id, t),
    };
    if let Some(m) = msg {
        send_to(ws, Message::Text(m)).await?;
    }
    if let Cmd::Remove(ts) = change {
        for t in ts {
            codec.forget(t);
        }
    }
    Ok(())
}

/// A resubscribe is about to carry the full current list, so queued updates
/// must not also be sent. Removals still have to leave the local books.
fn drop_pending<C: Codec>(codec: &mut C, pending: &mut Vec<Cmd>) {
    for c in pending.drain(..) {
        if let Cmd::Remove(ts) = c {
            for t in &ts {
                codec.forget(t);
            }
        }
    }
}

async fn collect<E: Exchange>(
    idx: &str,
    id: usize,
    group: &Arc<Group>,
    ex: &E,
    tickers: &mut Vec<String>,
    cmd_rx: &mut UnboundedReceiver<Cmd>,
    tx: &Sender<Stamped>,
    deadline: Option<Instant>,
) -> anyhow::Result<Exit> {
    // Fold in anything queued while we were disconnected, so we subscribe once.
    while let Ok(c) = cmd_rx.try_recv() {
        apply(tickers, c);
    }
    if tickers.is_empty() {
        // Everything on this shard closed; nothing to subscribe to.
        return Ok(Exit::Stop);
    }

    let mut ws = ex.connect().await?;
    let health = &group.health[id];
    // Clear any recycle request that was pending while we were down.
    health.recycle.store(false, Ordering::Relaxed);
    health.connected.store(true, Ordering::Relaxed);
    // A fresh Sink per connection: a reconnected feed starts with emitting = false,
    // so if it is still the source it re-announces itself with a snapshot.
    let mut sink = Sink {
        id,
        group: group.clone(),
        tx: tx.clone(),
        emitting: false,
    };
    let mut codec = <E::Codec as Default>::default();
    let mut next_id: i64 = 0;
    subscribe::<E::Codec>(&mut ws, &mut next_id, tickers.as_slice()).await?;

    // Set while we wait for `unsubscribed` during a resync.
    let mut unsub_deadline: Option<Instant> = None;
    let mut snap_tick = tokio::time::interval(Duration::from_secs(SNAPSHOT_SECS));
    snap_tick.tick().await; // the first tick fires immediately; skip it
    // Lets a quiet standby notice it was promoted without waiting for a message.
    let mut role_tick = tokio::time::interval(Duration::from_millis(500));
    role_tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
    // Per-symbol activity monitor.
    let mut mon = Monitor::default();
    let mut mon_tick = tokio::time::interval(Duration::from_secs(MON_SECS));
    mon_tick.tick().await;

    // Outside the loop, so other select arms firing don't reset the idle clock.
    let mut last_rx = Instant::now();
    // Changes made before the `subscribed` reply gave us a sid.
    let mut pending: Vec<Cmd> = Vec::new();
    let mut cmds_open = true;
    let connected_at = Instant::now();

    let exit = loop {
        let d = unsub_deadline;
        let unsub_timeout = until(d);
        let end = until(deadline);
        let idle = tokio::time::sleep_until(last_rx + Duration::from_secs(IDLE_SECS));

        tokio::select! {
            _ = tokio::signal::ctrl_c() => { eprintln!("[shard {idx}] ctrl-c, shutting down"); break Exit::Stop; }
            _ = end => { eprintln!("[shard {idx}] time limit reached"); break Exit::Stop; }
            _ = idle => { eprintln!("[shard {idx}] no data for {IDLE_SECS}s"); break Exit::Reconnect; }
            _ = unsub_timeout => {
                eprintln!("[shard {idx}] no unsubscribed reply in 5s, resubscribing anyway");
                unsub_deadline = None;
                subscribe::<E::Codec>(&mut ws, &mut next_id, tickers.as_slice()).await?;
            }
            cmd = cmd_rx.recv(), if cmds_open => match cmd {
                Some(c) => {
                    let change = apply(tickers, c);
                    if let Cmd::Remove(ts) = &change {
                        for t in ts { mon.syms.remove(t); }
                    }
                    if unsub_deadline.is_some() {
                        // Mid-resync: the resubscribe will carry the new list.
                        if let Cmd::Remove(ts) = &change {
                            for t in ts { codec.forget(t); }
                        }
                    } else if codec.can_update() {
                        send_update(&mut ws, &mut codec, &mut next_id, &change).await?;
                    } else {
                        pending.push(change);
                    }
                }
                None => cmds_open = false,
            },
            _ = role_tick.tick() => {
                if health.recycle.swap(false, Ordering::Relaxed) {
                    eprintln!("[shard {idx}] recycling socket");
                    break Exit::Reconnect;
                }
                if connected_at.elapsed() > Duration::from_secs(20) && !codec.is_synced() {
                eprintln!("[shard {idx}] not synced after 20s, reconnecting");
                break Exit::Reconnect;
                }
                if sink.publish(&codec, now_us(), Vec::new(), false).is_err() {
                    break Exit::WriterStopped;
                }
            }

            _ = snap_tick.tick() => {
                if last_rx.elapsed() > Duration::from_secs(30) {
                    eprintln!("[shard {idx}] skipping snapshot: no frames for {}s", last_rx.elapsed().as_secs());
                    continue;
                }
                // Only the source writes; a standby's publish is a no-op.
                if sink.publish(&codec, now_us(), Vec::new(), true).is_err() {
                    break Exit::WriterStopped;
                }
            }
            _ = mon_tick.tick() => {
                if unsub_deadline.is_none() && codec.can_update() && pending.is_empty() {
                    let stale = mon.stalled(Instant::now());
                    if !stale.is_empty() {
                        eprintln!("[shard {idx}] resubscribing {} quiet symbols", stale.len());
                        send_update(&mut ws, &mut codec, &mut next_id, &Cmd::Remove(stale.clone())).await?;
                        send_update(&mut ws, &mut codec, &mut next_id, &Cmd::Add(stale.clone())).await?;
                        mon.mark_fixed(&stale, Instant::now());
                    }
                }
            }
            msg = ws.next() => {
                last_rx = Instant::now();
                match msg {
                    Some(Ok(Message::Text(t))) => {
                        let received_at_us = now_us(); // stamp at the socket read

                    if t.contains("\"type\":\"error\"") {
                        eprintln!("[shard {idx}] exchange error: {t}");
                    }
                    health.last_data_ms.store(group.ms(), Ordering::Relaxed);
                        let out = match codec.on_message(&t) {
                            Ok(out) => out,
                            Err(e) => { eprintln!("[shard {idx}] decode error: {e:?}"); continue; }
                        };
                        let now = Instant::now();
                        for e in &out.events {
                            if let Some(s) = ev_symbol(e) { mon.record(s, now); }
                        }
                        if sink.publish(&codec, received_at_us, out.events, false).is_err() {
                            break Exit::WriterStopped;
                        }
                        health.synced.store(codec.is_synced(), Ordering::Relaxed);
                        if out.unsubscribed && unsub_deadline.take().is_some() {
                            subscribe::<E::Codec>(&mut ws, &mut next_id, tickers.as_slice()).await?;
                        }
                        if !pending.is_empty() && codec.can_update() && unsub_deadline.is_none() {
                            for change in std::mem::take(&mut pending) {
                                send_update(&mut ws, &mut codec, &mut next_id, &change).await?;
                            }
                        }
                        if out.resync && unsub_deadline.is_none() {
                            eprintln!("[shard {idx}] sequence gap or unknown market, resyncing");
                            drop_pending(&mut codec, &mut pending);
                            next_id += 1;
                            match codec.unsubscribe_all_msg(next_id) {
                                Some(m) => {
                                    send_to(&mut ws, Message::Text(m)).await?;
                                    unsub_deadline = Some(Instant::now() + Duration::from_secs(5));
                                }
                                None => subscribe::<E::Codec>(&mut ws, &mut next_id, tickers.as_slice()).await?,
                            }
                        }
                    }
                    Some(Ok(Message::Ping(p))) => send_to(&mut ws, Message::Pong(p)).await?,
                    Some(Ok(Message::Close(f))) => { eprintln!("[shard {idx}] closed by server: {f:?}"); break Exit::Reconnect; }
                    Some(Ok(_)) => {}
                    Some(Err(e)) => { eprintln!("[shard {idx}] ws error: {e}"); break Exit::Reconnect; }
                    None => { eprintln!("[shard {idx}] stream ended"); break Exit::Reconnect; }
                }
            }
        }
    };
    tokio::time::timeout(Duration::from_secs(2), ws.close(None))
        .await
        .ok();
    Ok(exit)
}

/// One websocket's lifetime: connect, collect, reconnect with backoff.
async fn run_shard<E: Exchange>(
    idx: String,
    id: usize,
    group: Arc<Group>,
    start_delay: Duration,
    ex: Arc<E>,
    mut tickers: Vec<String>,
    mut cmd_rx: UnboundedReceiver<Cmd>,
    tx: Sender<Stamped>,
    deadline: Option<Instant>,
) {
    // Stagger connects so we don't hit the handshake all at once.
    tokio::time::sleep(start_delay).await;
    let mut backoff = Duration::from_secs(1);
    loop {
        let started = Instant::now();
        let res = collect(
            &idx,
            id,
            &group,
            &*ex,
            &mut tickers,
            &mut cmd_rx,
            &tx,
            deadline,
        )
        .await;
        // Whatever happened, this feed is no longer a usable source.
        group.health[id].connected.store(false, Ordering::Relaxed);
        group.health[id].synced.store(false, Ordering::Relaxed);
        match res {
            Ok(Exit::Stop) | Ok(Exit::WriterStopped) => break,
            Ok(Exit::Reconnect) => eprintln!("[shard {idx}] disconnected, reconnecting"),
            Err(e) => eprintln!("[shard {idx}] connection error: {e:#}"),
        }
        if started.elapsed() > Duration::from_secs(30) {
            backoff = Duration::from_secs(1); // it was a healthy connection
        }
        if deadline.is_some_and(|d| Instant::now() >= d) {
            break;
        }
        eprintln!("[shard {idx}] retrying in {}s", backoff.as_secs());
        let jitter = Duration::from_millis((now_us() as u64 / 1000) % 1000);
        tokio::select! {
            _ = tokio::time::sleep(backoff + jitter) => {}
            _ = tokio::signal::ctrl_c() => { eprintln!("[shard {idx}] ctrl-c, shutting down"); break; }
        }
        backoff = (backoff * 2).min(Duration::from_secs(30));
    }
}

/// Spawn both feeds of a group plus its supervisor.
fn spawn_group<E: Exchange>(
    idx: usize,
    start_delay: Duration,
    ex: &Arc<E>,
    tickers: Vec<String>,
    tx: &Sender<Stamped>,
    deadline: Option<Instant>,
) -> (ShardSlot, Vec<JoinHandle<()>>) {
    let group = Arc::new(Group::new());
    let count = tickers.len();
    let mut txs = Vec::new();
    let mut handles = Vec::new();
    for id in 0..2usize {
        let (cmd_tx, cmd_rx) = unbounded_channel();
        txs.push(cmd_tx);
        handles.push(tokio::spawn(run_shard(
            format!("{idx}{}", if id == 0 { 'a' } else { 'b' }),
            id,
            group.clone(),
            start_delay + Duration::from_millis(250 * id as u64),
            ex.clone(),
            tickers.clone(),
            cmd_rx,
            tx.clone(),
            deadline,
        )));
    }
    tokio::spawn(supervise(idx, group, deadline));
    (ShardSlot { txs, count }, handles)
}

/// Periodically re-list open markets and push the differences to the shards.
/// Returns the handles of any shards it had to create.
async fn discover<E: Exchange>(
    n: usize,
    shard_size: usize,
    ex: Arc<E>,
    mut slots: Vec<ShardSlot>,
    mut assigned: HashMap<String, usize>,
    tx: Sender<Stamped>,
    deadline: Option<Instant>,
) -> Vec<JoinHandle<()>> {
    let max_groups = ex.max_groups();
    let mut extra = Vec::new();
    loop {
        tokio::select! {
            _ = tokio::time::sleep(Duration::from_secs(DISCOVERY_SECS)) => {}
            _ = until(deadline) => break,
            _ = tokio::signal::ctrl_c() => break,
        }
        let listed = tokio::select! {
            r = ex.list_markets(n) => r,
            _ = until(deadline) => break,
            _ = tokio::signal::ctrl_c() => break,
        };
        let listed = match listed {
            Ok(l) => l,
            Err(e) => {
                eprintln!("[discovery] listing failed, will retry next round: {e:#}");
                continue;
            }
        };
        let now: HashSet<String> = listed.into_iter().collect();
        // A truncated listing must not look like half the exchange closing.
        if now.len() * 2 < assigned.len() {
            eprintln!(
                "[discovery] listing returned {} vs {} known, ignoring it",
                now.len(),
                assigned.len()
            );
            continue;
        }

        let added: Vec<String> = now
            .iter()
            .filter(|t| !assigned.contains_key(*t))
            .cloned()
            .collect();
        let removed: Vec<String> = assigned
            .keys()
            .filter(|t| !now.contains(*t))
            .cloned()
            .collect();

        // Removals, batched per shard.
        let mut rm: HashMap<usize, Vec<String>> = HashMap::new();
        for t in &removed {
            if let Some(i) = assigned.remove(t) {
                slots[i].count = slots[i].count.saturating_sub(1);
                rm.entry(i).or_default().push(t.clone());
            }
        }
        for (i, ts) in rm {
            slots[i].send(Cmd::Remove(ts));
        }

        // Additions: least-loaded shard with room, otherwise a new group (up to max_groups).
        let mut add: HashMap<usize, Vec<String>> = HashMap::new();
        let mut overflow: Vec<String> = Vec::new();
        for t in added.iter() {
            let least = slots
                .iter()
                .enumerate()
                .min_by_key(|(_, s)| s.count)
                .map(|(i, s)| (i, s.count));
            match least {
                Some((i, c)) if c < shard_size || slots.len() >= max_groups => {
                    slots[i].count += 1;
                    assigned.insert(t.clone(), i);
                    add.entry(i).or_default().push(t.clone());
                }
                _ => overflow.push(t.clone()),
            }
        }

        // New groups may not push us past max_groups; spill the rest onto the
        // least-loaded existing shards.
        let room = max_groups.saturating_sub(slots.len());
        let split = (room * shard_size).min(overflow.len());
        for t in overflow.split_off(split) {
            if let Some(i) = slots
                .iter()
                .enumerate()
                .min_by_key(|(_, s)| s.count)
                .map(|(i, _)| i)
            {
                slots[i].count += 1;
                assigned.insert(t.clone(), i);
                add.entry(i).or_default().push(t);
            }
        }

        for (i, ts) in add {
            slots[i].send(Cmd::Add(ts));
        }
        for (j, chunk) in overflow.chunks(shard_size).enumerate() {
            let idx = slots.len();
            for t in chunk {
                assigned.insert(t.clone(), idx);
            }
            let (slot, hs) = spawn_group(
                idx,
                Duration::from_millis(500 * j as u64),
                &ex,
                chunk.to_vec(),
                &tx,
                deadline,
            );
            slots.push(slot);
            extra.extend(hs);
        }

        eprintln!(
            "[discovery] +{} -{} markets, {} tracked on {} shards",
            added.len(),
            removed.len(),
            assigned.len(),
            slots.len()
        );
    }
    extra
}

async fn run<E: Exchange>(
    ex: Arc<E>,
    out_path: &str,
    n: usize,
    run_for: Option<u64>,
) -> anyhow::Result<()> {
    let tickers = ex.list_markets(n).await?;
    eprintln!("subscribing to {} markets", tickers.len());
    let shard_size = tickers
        .len()
        .div_ceil(ex.max_groups())
        .max(ex.min_shard_size());
    let deadline = run_for.map(|s| Instant::now() + Duration::from_secs(s));
    let w = AlignedSbeWriter::create(out_path, writer::CHUNK_SIZE)?;
    let (tx, rx) = crossbeam::channel::unbounded();
    let writer_thread = std::thread::spawn(move || writer::run(rx, w));

    let mut slots = Vec::new();
    let mut handles = Vec::new();
    let mut assigned: HashMap<String, usize> = HashMap::new();
    for (i, chunk) in tickers.chunks(shard_size).enumerate() {
        for t in chunk {
            assigned.insert(t.clone(), i);
        }
        let (slot, hs) = spawn_group(
            i,
            Duration::from_millis(500 * i as u64),
            &ex,
            chunk.to_vec(),
            &tx,
            deadline,
        );
        slots.push(slot);
        handles.extend(hs);
    }
    eprintln!(
        "{} groups ({} sockets) of up to {shard_size} markets",
        slots.len(),
        handles.len()
    );

    let disc = tokio::spawn(discover(
        n,
        shard_size,
        ex.clone(),
        slots,
        assigned,
        tx.clone(),
        deadline,
    ));

    drop(tx);
    for h in handles {
        h.await.ok();
    }
    let extra = disc.await.unwrap_or_default();
    for h in extra {
        h.await.ok();
    }
    writer_thread
        .join()
        .map_err(|_| anyhow::anyhow!("writer thread panicked"))?
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut args = std::env::args().skip(1);
    let out_path = args
        .next()
        .context("usage: collector <output_path> [n_tickers (0 = all)] [seconds]")?;
    // 0 = every open market;
    let n: usize = match args.next().and_then(|s| s.parse::<usize>().ok()) {
        Some(0) => usize::MAX,
        Some(n) => n,
        None => 100,
    };
    let run_for: Option<u64> = args.next().and_then(|s| s.parse().ok());

    match std::env::var("EXCHANGE").as_deref().unwrap_or("kalshi") {
        "kalshi" => {
            let key_id = std::env::var("KALSHI_KEY_ID").context("KALSHI_KEY_ID not set")?;
            let key_path = std::env::var("KALSHI_KEY_PATH").context("KALSHI_KEY_PATH not set")?;
            let key = conn::load_key(&key_path)?;
            let ep = conn::endpoints();
            let ex = Arc::new(KalshiExchange {
                key_id,
                key,
                ws_url: ep.ws_url,
                rest_base: ep.rest_base,
            });
            run(ex, &out_path, n, run_for).await
        }
        "fake" => {
            let addr = std::env::var("FAKE_ADDR").unwrap_or_else(|_| "127.0.0.1:9001".into());
            run(Arc::new(FakeExchange::new(addr)), &out_path, n, run_for).await
        }
        // "polymarket" => run(Arc::new(PolymarketExchange::new()), &out_path, n, run_for).await,
        other => anyhow::bail!("unknown EXCHANGE {other:?} (expected: kalshi)"),
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use engineering_challenge::event::BookSide;
    use engineering_challenge::kalshi::Kalshi;

    fn synced_kalshi() -> Kalshi {
        let mut k = Kalshi::default();
        k.on_message(r#"{"type":"subscribed","id":1,"msg":{"channel":"orderbook_delta","sid":1},"sending_ts_ms":1}"#).unwrap();
        k.on_message(r#"{"type":"orderbook_snapshot","sid":1,"seq":1,"msg":{"market_ticker":"T","yes_dollars_fp":[["0.4100","30.00"]],"no_dollars_fp":[["0.3500","25.00"]]},"sending_ts_ms":3}"#).unwrap();
        k
    }

    fn ev() -> Vec<BookEvent> {
        vec![BookEvent::Update {
            symbol: "T".into(),
            side: BookSide::Bid,
            price: 1,
            quantity: 1,
            exchange_ts_us: 1,
        }]
    }

    #[test]
    fn switch_snapshots_first_and_silences_old_source() {
        let group = Arc::new(Group::new());
        let (tx, rx) = crossbeam::channel::unbounded();
        let mk = |id| Sink {
            id,
            group: group.clone(),
            tx: tx.clone(),
            emitting: false,
        };
        let (mut a, mut b) = (mk(0), mk(1));
        let (ka, kb) = (synced_kalshi(), synced_kalshi());

        // A is the source: snapshot first, then deltas.
        a.publish(&ka, 0, ev(), false).unwrap();
        assert!(matches!(
            rx.try_recv().unwrap().event,
            BookEvent::Snapshot { .. }
        ));
        a.publish(&ka, 0, ev(), false).unwrap();
        assert!(matches!(
            rx.try_recv().unwrap().event,
            BookEvent::Update { .. }
        ));

        // The standby never writes.
        b.publish(&kb, 0, ev(), false).unwrap();
        assert_eq!(rx.try_iter().count(), 0);

        // Failover: the old source goes silent, the new one opens with a snapshot.
        *group.active.lock().unwrap() = 1;
        a.publish(&ka, 0, ev(), false).unwrap();
        assert_eq!(rx.try_iter().count(), 0);
        b.publish(&kb, 0, ev(), false).unwrap();
        assert!(matches!(
            rx.try_recv().unwrap().event,
            BookEvent::Snapshot { .. }
        ));
    }

    #[test]
    fn monitor_flags_only_the_quiet_symbol() {
        let t0 = Instant::now();
        let mut m = Monitor::default();
        for _ in 0..25 {
            m.record("A", t0);
            m.record("B", t0 + Duration::from_secs(190));
            m.record("C", t0 + Duration::from_secs(190));
        }
        let stale = m.stalled(t0 + Duration::from_secs(200));
        assert_eq!(stale, vec!["A".to_string()]);

        // Once resubscribed it is not flagged again during the cooldown.
        m.mark_fixed(&stale, t0 + Duration::from_secs(200));
        assert!(m.stalled(t0 + Duration::from_secs(400)).is_empty());
    }

    #[test]
    fn monitor_defers_to_group_health_when_most_are_quiet() {
        let t0 = Instant::now();
        let mut m = Monitor::default();
        for _ in 0..25 {
            m.record("A", t0);
            m.record("B", t0);
        }
        assert!(m.stalled(t0 + Duration::from_secs(300)).is_empty());
    }
}
