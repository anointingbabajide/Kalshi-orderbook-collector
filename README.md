# Kalshi orderbook collector

A Rust service that connects to Kalshi's websocket, keeps an in-memory orderbook for every market it follows, and writes snapshots and price level updates to a file in the repo's SBE format. It handles many markets across many sockets, reconnects on its own, and keeps a standby connection for each group of markets so one bad socket does not leave a gap in the output.

How well it performs is written up in [LATENCY.md](LATENCY.md). Short version: it worked in my runs up to 10,000 markets, and it does not keep up when subscribing to every open market (about 165,000) from my laptop.

## Quick start

You need a Kalshi API key ID and the matching Ed25519 private key as a PKCS#8 PEM file.

```
KALSHI_KEY_ID=<key id> KALSHI_KEY_PATH=<path to .pem> \
  cargo run --release --bin engineering-challenge -- <output_path> [n_tickers] [seconds]
```

- `output_path`: where the SBE file is written.
- `n_tickers`: how many open markets to follow. `0` means every open market. Default is 100.
- `seconds`: stop after this long. Leave it out to run until Ctrl-C.
- `KALSHI_ENV=demo` uses Kalshi's demo exchange. The default is production.

Read the output back with the inspector:

```
cargo run --release --bin inspect -- <output_path> [symbol]
```

It prints the number of snapshots and updates, how many symbols appeared, lag percentiles overall and per 10 second window, and the busiest symbols. With a symbol it also prints every message for that symbol.

Run the tests with `cargo test`.

The repo's writer pads its output to 64 MiB chunks, so even a short run produces a file of at least 64 MiB.

## How it works

```
Kalshi websocket(s) -> decode -> per-feed book + sequence check -> BookEvent -> channel -> writer thread -> SBE file
```

**Kalshi to book.** Kalshi sends two bid ladders per market, YES bids and NO bids. A NO bid at price `p` is the same as a YES ask at `1 - p`, so the book stores both ladders as received and maps NO to the YES price axis when producing output. Prices and quantities arrive as decimal strings and are parsed straight into 1e6 fixed point, never through floats.

**Updates carry the new total.** Kalshi sends a signed change in quantity at a price level. The collector applies it to its own book and writes the resulting absolute quantity, with `0` meaning the level is gone. This matches the schema, where `quantity` is "the absolute available quantity".

**Sequence checking.** `seq` is one counter per subscription, not per market. Every message must be exactly one higher than the last, and the reply to `update_subscription` also consumes a number. On a gap, the feed stops applying messages, unsubscribes (waiting up to 5 s for the confirmation), and subscribes again. Kalshi then sends fresh snapshots and the books are rebuilt from them. A delta for a market that was never snapshotted triggers the same recovery. Late messages for a market that was deliberately unsubscribed are ignored.

**Snapshots.** Kalshi's initial snapshot for each market is written as it arrives. Every 60 s each feed also writes a full snapshot of every book it holds. A feed that is out of sync, or that has received nothing for over 30 s, skips the periodic snapshot instead of writing a stale book.

**Sharding.** Markets are split across up to 100 sockets (Kalshi started refusing connections near 195), with at least 200 markets per socket. Shard connections are started 250 ms apart.

**Redundancy and failover.** Each group of markets has two feeds subscribed to the same markets, but only the current source writes to the output. If the source goes silent for 15 s the standby takes over, and a snapshot is written at the switch so a reader does not see a gap. The standby is recycled periodically: it reconnects, warms up for 10 s, and is promoted, and then the old source is recycled the same way. A feed that is connected but not synced after 20 s is reconnected.

**Reconnecting.** A lost connection, a close frame, or 60 s without any frame (Kalshi pings regularly, so silence means the connection is dead) triggers a reconnect with exponential backoff, 1 s doubling to 30 s with jitter. The backoff resets after a connection that lasted more than 30 s.

**Market discovery.** Every 5 minutes the open markets are listed again. New markets are added to the least loaded socket with `update_subscription`, and closed markets are removed. A listing that returns less than half of the known markets is ignored, so a truncated response doesn't look like half the exchange closing.

**Shutdown.** Ctrl-C or the time limit stops the feeds, and the writer thread flushes the last partial chunk before the program exits.

## Output

The file contains the two message types from `sbe_schemas/engineering_challenge/challenge_orderbook.yaml`.

| Message            | Contents                                                                                                      |
| ------------------ | ------------------------------------------------------------------------------------------------------------- |
| `Snapshot`         | `received_at`, `exchange_timestamp`, `symbol`, and the bid and ask ladders (best level first, YES price axis) |
| `PriceLevelUpdate` | `backtest_timestamp`, `exchange_timestamp`, `symbol`, `price_level` and the absolute `quantity`               |

Decisions where the schema leaves room:

- **Timestamps.** `exchange_timestamp` is Kalshi's `sending_ts_ms` converted to microseconds, for snapshots and updates alike. A periodic snapshot uses the exchange timestamp of the last message that feed saw, since it has no exchange time of its own. `received_at` and `backtest_timestamp` are the host clock, stamped when the frame is read off the socket, before decoding.
- **No side on updates.** The update schema has no bid or ask field. In an uncrossed book a price can only be one or the other, because a YES bid at `p` and a NO bid at `1 - p` would match. So a reader can replay updates into a single price to quantity map per symbol. A reader that needs the side for a level that doesn't exist yet has to infer it from the surrounding book.
- **After a reconnect.** The feed rebuilds its books from fresh snapshots, so a reader sees a new snapshot for each symbol in the middle of the stream. That is the signal to reset the book.

## Code layout

| File                 | Purpose                                                                                         |
| -------------------- | ----------------------------------------------------------------------------------------------- |
| `src/main.rs`        | Collector entry point and orchestration                                                         |
| `src/conn.rs`        | Kalshi request signing, market listing, websocket connect                                       |
| `src/kalshi.rs`      | Subscribe, unsubscribe and update commands, and the mapping from Kalshi messages to `BookEvent` |
| `src/decode.rs`      | Raw Kalshi JSON to typed events                                                                 |
| `src/book.rs`        | Orderbook (two ladders) and fixed-point parsing                                                 |
| `src/feed.rs`        | One subscription's books and its sequence checking                                              |
| `src/event.rs`       | The `BookEvent` type that the output side works with                                            |
| `src/writer.rs`      | Writer thread that turns `BookEvent` into the SBE messages                                      |
| `src/bin/probe.rs`   | Prints raw Kalshi messages, used to capture real samples                                        |
| `src/bin/inspect.rs` | Reads an output file back and reports counts and lag                                            |
| `LATENCY.md`         | Latency measurements and what I found                                                           |

The tests in `decode.rs` run against real messages captured from Kalshi (a snapshot, a delta, a subscribe reply and an `update_subscription` reply). The writer has a round trip test through the repo's reader.

## Main settings

These are constants in the code.

| Setting                            | Value |
| ---------------------------------- | ----- |
| Maximum sockets                    | 100   |
| Minimum markets per socket         | 200   |
| Idle timeout (no frame at all)     | 60 s  |
| Periodic snapshot interval         | 60 s  |
| Market discovery interval          | 300 s |
| Failover when the source is silent | 15 s  |

## Known limits

- **Full scale does not work from my laptop.** With all open markets (about 165,000), only 38 to 45% of markets produced data in 120 s and update lag reached 41 to 107 s. I did not find out whether the cause is my process, my connection, or Kalshi. See [LATENCY.md](LATENCY.md).
- **Failover only handles silence.** If the source is late but still sending, nothing switches.
- **Lag numbers are rough.** They compare my clock with Kalshi's, and they include any delay inside my own program.
- **A crash loses buffered data.** The repo's writer only writes to disk when a 64 MiB chunk fills or at shutdown, so a crash loses whatever is in the buffer.
- **Nothing was run from a cloud machine.**

## Background from the template

This repo uses the [FIX SBE standard](https://fixtrading.org/packages/simple-binary-encoding-technical-specification-v2-rc2/)
for writing binary messages. The rust code abstracts this away for you, and you can simply use the helpers.

FIX SBE is defined in XML, which I find extremely cumbersome to edit, especially
when updating schemas frequently. To solve this, I have also introduced some
helper code that takes our own custom YAML format, and turns it into FIX SBE
XML, which in turn is used to generate rust code to encode and decode the
messages. **You don't need to edit any of these files**. This explanation is
simply to help you understand the origins of the various files in this repo.

If you're curious to see the YAML schema, take a look at [challenge_orderbook.yaml](https://github.com/voluntas-io/engineering-challenge/blob/main/sbe_schemas/engineering_challenge/challenge_orderbook.yaml).
We also use a custom timestamp field, so that the Rust struct can use
`chrono::DateTime` in the generated struct, and in SBE schema, it is a uint64
representing the timestamp as the number of microseconds since unix epoch.
