# Latency notes (Kalshi)

I ran the collector against Kalshi's live orderbook feed from a Windows laptop in Nigeria. Nothing was run from a cloud machine, so these numbers describe my setup and probably not a production one.

## Summary

- **Up to 2,000 markets** most runs were healthy, with p99 lag usually between 350 and 700 ms. Those runs were done before I fixed my clock, so the true lag was probably a bit higher.
- **A few 2,000-market runs went wrong.** Lag climbed to 20 to 38 s, or whole groups of markets never produced data. I never found the cause.
- **A 10,000-market run also worked fine.**
- **At full scale (about 165,000 markets) it does not work from this laptop.** Only 38 to 45% of markets produced any data and update lag reached 41 to 107 s. I don't know why yet. The limit is somewhere between 10,000 and 165,000 markets, and I haven't found where.
- **Failover handles a feed that goes silent, but not one that is late.**

## How lag is measured

Each update in the output file carries two timestamps: Kalshi's `sending_ts_ms` and the time my code read the frame off the socket. The `inspect` binary subtracts one from the other and prints percentiles, overall and for each 10 second window of exchange time. Lag is measured on updates only, not snapshots.

This is not true network latency, for two reasons:

1. It compares my clock with Kalshi's, so any offset between them goes straight into the number. If my clock is behind, lag looks too low.
2. The receive time is taken when my code reads the frame. If my program is slow to run, that delay shows up as lag too. So the number is network plus Kalshi plus my own scheduling delay, and I have not separated the three.

## Clock skew

My first runs had a negative p50 lag, which is impossible with correct clocks because a message can't arrive before it was sent.

| Run                       | p50 lag                       |
| ------------------------- | ----------------------------- |
| 2000 markets, 120s        | -84 ms                        |
| 2000 markets, 60s         | -164 ms                       |
| 2000 markets, 60s (later) | -100 ms                       |
| 200 markets, 300s         | about -690 ms in most windows |

The offset changed from run to run, so there was no fixed value to subtract. A fake exchange running on localhost gives 0 ms lag everywhere, which shows the timestamp conversion adds no bias at low load. It says nothing about how the collector behaves under heavy load.

I have since synced the Windows clock (`w32tm /resync`), but it drifts: when I checked later it was about half a second behind a time server. So treat absolute lag numbers as uncertain by up to about 0.5 s. Comparing windows within one run is more reliable than comparing different runs, because the offset moved between runs. All the runs up to 2,000 markets below were done before the sync.

## Results up to 2,000 markets

Most runs were fine: p99 between about 350 and 700 ms and a max of roughly 0.5 to 1.6 s. The very first run was worse (p99 4.4 s, max 4.9 s), and the spikes lined up with startup, when 20 sockets connect and all the snapshots arrive at once.

A few runs in the evening were worse:

| Run                                                                      | What happened                                                                                                                                                                                                                                                                                          |
| ------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| 2000 markets, 60s                                                        | Lag climbed by about one second for every second that passed: p50 of 6 s in the 10s window, 11 s in the 20s window, 17.8 s in the 30s window, max 21.5 s. It was back to normal in the 40s window. That looks like data being buffered somewhere and then draining, not like the exchange going quiet. |
| 2000 markets, 60s, second run                                            | Only 1,200 of the 2,000 symbols showed up, with about 2.4k updates (a healthy run has 25k to 48k). Lag reached 38 s. Four of the ten groups never produced anything, although both feeds in those groups were connected.                                                                               |
| Same command, after I added a "not synced after 20s, reconnect" watchdog | All 2,000 symbols were back, p99 3.3 s, max 3.9 s. The watchdog never fired, so I can't say it was the fix.                                                                                                                                                                                            |
| An earlier run, same command                                             | Only 400 symbols (2 of 10 groups). The same command worked before and after.                                                                                                                                                                                                                           |

After those runs the collector went back to normal behaviour at this scale, and I couldn't reproduce the bad ones on demand. I never found the root cause of the silent groups. It could be the network path, Kalshi being slow to accept that many subscriptions at once, or a rate limit from opening about 20 sockets several times in a few minutes. I hadn't captured Kalshi's error frames at the time, so I couldn't tell these apart. I have since added logging for `error` frames.

A run with 10,000 markets also worked fine. I haven't recorded its numbers in these notes.

## Full-scale runs (all open markets)

At this scale it does not work yet. Both runs were release builds on the same laptop, 120 seconds each. I synced the clock beforehand, so the lag numbers could be off by up to about 0.5 s, which is small next to the lags below.

| Run             | Sockets                  | Markets per socket | Coverage             | Update lag p50 / max |
| --------------- | ------------------------ | ------------------ | -------------------- | -------------------- |
| 164,640 markets | 100, no redundancy       | 1,647              | 74,304 symbols (45%) | 9.2 s / 41.0 s       |
| 169,749 markets | 100 (50 redundant pairs) | 3,395              | 64,514 symbols (38%) | 26.0 s / 107.3 s     |

Coverage is the number of symbols with at least one message in the file, divided by the markets I subscribed to. I haven't checked whether markets with empty books send a snapshot at this scale, so part of the missing share may be markets with nothing to send.

- **First run:** the updates in the file only cover about the first 30 to 40 s of exchange time, although the run lasted 120 s. Only 8 snapshots arrived beyond the first one for each symbol, so the reconnects delivered almost nothing.
- **Second run:** the backlog was still draining at the end (p50 lag about 100 s in the earliest windows, 1.7 s in the last), so 120 s was shorter than the startup drain. Every update arrives twice, so this setup carries about double the traffic.
- **Send timeouts:** the many `websocket send timed out` errors in the second run come from a 10 s send timeout I set without measuring how long a 3,395-ticker subscribe takes to send. They may be self-inflicted, so they are not evidence of network faults.
- Each result is a single run.

Possible causes, none tested:

- My process was overloaded. Some timers fired much later than they should have.
- Kalshi throttled many connections subscribing at the same moment.
- My internet connection could not carry the startup burst. The market listing itself hit timeouts and a DNS failure before any websocket was open.

I did not measure CPU or bandwidth during these runs, so I can't tell which it was.

## Reconnect test

During a 5 minute test with 200 markets (and `RECYCLE_SECS` set to 60) DNS lookups failed for a short while. Both feeds logged `no data for 60s`, then `No such host is known (os error 11001)` while retrying. The backoff (1s, 2s, 4s, 8s) worked and both feeds came back by themselves, and one of them was promoted to source. The gap in the output file is real: nothing was received, so nothing could be written. After the reconnect the books were rebuilt from fresh snapshots, so the data after the gap is still consistent.

The recycling itself worked: the standby was recycled, reconnected, warmed up for 10 s and promoted, then the old source was recycled the same way. That happened three times in the run, and the symbol count stayed at 200.

## Something my health check misses

Failover only triggers when the source feed goes silent for 15 s (`STALE_MS`). I did see it switch live, and it emitted a snapshot at each switch.

But during the slow run above, data was around 20 s late for half a minute and nothing switched, because late data is not silent data. That is the "exchange buffers messages" case from the challenge description, and my code doesn't handle it. If both sockets were delayed equally, switching wouldn't help anyway, but if only one is slow it should.

## What I would do next

1. **Measure before changing anything.** Watch CPU and network use during the first 30 s of a full-scale run, and log when the event loop runs late. That separates my process, my link and Kalshi.
2. **Run from a cloud region close to Kalshi.** I don't know where Kalshi hosts their servers, so that needs checking. This is the obvious next experiment, not something my data supports yet.
3. **Make the health check lag-aware.** Switch the source when the standby is much faster than it, not only when it goes silent. This needs a baseline per feed, so lag is measured against each feed's own minimum.
4. **Reduce the startup load.** None of these is tried yet: start the standby only after the primary has synced, subscribe in small batches and add the rest with `update_subscription`, space connections out more (I stagger them by 250 to 500 ms now), and put the standby on busy markets only.
5. **Scale the timeouts with markets per socket.** The 20 s "not synced" watchdog and the 10 s send timeout look too short at full scale, where startup took about 90 s to drain.
6. **Keep the clock synced** with NTP (chrony on Linux) and check the offset before each measurement run.
7. **Compare the two feeds of a group** (which one gets each update first, and by how much) and use that to decide which connections to keep. I didn't get to run this one.
