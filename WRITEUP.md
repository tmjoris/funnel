# What it costs to keep metrics: Funnel's numbers

Funnel rolls metrics up into 10-second summaries as they arrive and stores
only the summaries. This write-up measures what that buys: how much traffic
one CPU can take, how much disk the data needs, whether the answers stay
accurate, what a crash loses, and what the same data would cost to keep in a
hosted service or as raw points in ClickHouse.

Every number below comes from a script or example in this repository, and
the commands to rerun them are at the end. Prices were checked on the
providers' own pages in October 2026.

## Summary

| | Result |
|---|---|
| Ingest on one cloud vCPU | 1.5 to 2.0 million lines/s with no loss, depending on the CPU; under 50 MB of memory |
| Disk per raw point | 0.48 bytes, against 2.72 for raw points in ClickHouse with its best codecs (5.6x smaller) |
| Growth with traffic | 40x the traffic over the same series needs 3.3x the disk |
| Percentile accuracy, end to end | within 0.85% of the exact answer computed by ClickHouse |
| A year of data, downsampled after 7 and 30 days | about 35 GB, against 345 GB at full resolution and 1.9 TB as raw points in ClickHouse |
| Data lost to `kill -9` | 2 to 7 seconds of traffic in two runs; at most about 10 by design |
| Delay before data is queryable | 2 to 5 seconds |
| Monthly cost for the test workload | $6 to $12 on a DigitalOcean droplet, against about $1,925 on Datadog and $295 to $360 on Grafana Cloud at list prices |

The hosted services do far more than Funnel (dashboards, alerting,
replication, long retention, support), so the cost line compares only what
it costs to keep the same metrics.

## The workload

A [generator](crates/loadgen) simulates a small backend: 20 services on 5
hosts each, every host serving 50 requests a second across 10 routes. Each
request reports a request counter, its duration, a cache hit or miss and up
to three database query durations; each host reports its queue depth every
second and its memory and CPU every 10 seconds. Durations are lognormal with
a 1% slow tail.

That gives 22,600 points a second, or 1.95 billion a day, across 7,000
series (a series is one metric with one set of tag values):

| metric | kind | series |
|---|---|---|
| `http.requests` | counter | 3,000 |
| `http.duration` | distribution | 3,000 |
| `db.query.duration` | distribution | 500 |
| `cache.hits`, `cache.misses` | counter | 200 |
| `queue.depth`, `process.memory_mb`, `process.cpu_percent` | gauge | 300 |

Half the series are distributions, and that turns out to matter more than
anything else for both storage and hosted pricing.

## Ingest throughput

[`scripts/loadtest.py`](scripts/loadtest.py) starts Funnel, replays the
workload over UDP at a fixed rate with [`loadgen`](crates/loadgen) and
reads three counts: lines Funnel accepted, lines it shed on purpose because
a queue was full, and packets the kernel dropped before Funnel could read
them. The [load test workflow](.github/workflows/loadtest.yml) runs it on
GitHub's cloud VMs with Funnel pinned to one vCPU, which is what a $5 to $6
server offers.

Highest rate tested with nothing lost, Funnel pinned to one vCPU, with the
workload packed into 1,432-byte packets as StatsD clients usually send it:

| CPU | no loss up to | Funnel's CPU at that rate | at 2.5 M lines/s |
|---|---|---|---|
| Neoverse-N2 (ARM), 2 runs | 2.0 M lines/s | 0.76 to 0.77 cores | 2.15 to 2.18 M accepted |
| Intel Xeon Platinum 8573C | 1.5 M lines/s (2.0 M with 0.3% lost) | 0.66 cores | 1.99 M accepted |
| Intel Xeon Platinum 8370C | 1.5 M lines/s | 0.71 cores | 1.84 M accepted |

With every metric in its own small packet, the worst case for per-packet
costs, one vCPU took 600,000 packets a second with nothing lost on the
Neoverse-N2 and Xeon 8573C, and 400,000 on the older Xeon 8370C.

The core count is Funnel's own CPU time; the kernel's network processing on
the same vCPU takes the rest, which is why throughput stops growing before
the process reaches one full core. When the sender floods the socket with 8
to 25 million lines a second, the kernel drops most of them, Funnel keeps
accepting what it can, and memory stays around 40 MB because Funnel sheds
load instead of queueing it. The test workload's 22,600 lines/s is about 1%
of what one vCPU sustained.

On the 2015 laptop this was developed on (an Intel i5-6200U), one core
sustained 550,000 lines/s, with the sender competing for the same CPU. The
cloud numbers are what a server would see.

### What profiling changed

On the laptop, the first load test used 1.25 cores for 400,000 lines/s. A sampling
profiler built into Funnel (`--features profile`, no root needed) showed
that the work I had expected to dominate, aggregation and the sketches,
was under 2% of the time:

- 24% went to hashing series ids with xxh3's streaming API, which pays a
  setup cost on each of the dozen tiny pieces of a series key. Copying the
  pieces into a buffer and hashing once gave the same ids 17% faster.
- About 40% was parsing, done twice: the router parsed each line to choose
  a shard, and the shard parsed it again. The router now passes the series
  id and tenant along, and the shard skips the tags, which are most of each
  line. Shard work fell from 805 to 395 ns per line.

Together they cut CPU per line by 40% (1.25 to 0.75 laptop cores at
400,000 lines/s). Two more changes followed the next profile:

- The parser cut each line apart with a dozen `str::split` calls, each with
  a setup cost out of proportion to the few bytes it searched. A single pass
  over the bytes, writing into buffers reused across lines, took routing
  from 700 to about 500 ns per line and shard work from 395 to about 290.
  A property test checks that it accepts and rejects exactly what the old
  parser did, down to the error it reports, on thousands of generated and
  mutated lines.
- `recvmmsg` receives up to 32 datagrams per system call instead of one. On
  the laptop with each metric in its own packet, it raised the most Funnel
  accepted on one CPU by about 19%, from an average of 240,000 to 285,000
  packets a second over two rounds of alternating builds, and cut CPU by
  about 5% with full packets. The gain is modest because most of the
  cost of a packet is the kernel's network stack, which batching the system
  call does not change.

The in-process benchmark ([`examples/ingest.rs`](crates/funnel/examples/ingest.rs))
went from 1.8 to 0.79 microseconds per line over these four changes, or
from 0.56 to 1.27 million lines a second per laptop core.

## Storage

[`examples/storage.rs`](crates/funnel/examples/storage.rs) stores 10 minutes
of the workload (13.6 million points) several ways, and
[`scripts/clickhouse_compare.sh`](scripts/clickhouse_compare.sh) loads the
same raw points into ClickHouse 25.8 with the primary key a ClickHouse user
would pick (metric, tags, time).

| stored as | bytes per point |
|---|---|
| raw points, ClickHouse default (LZ4) | 5.67 |
| raw points, ClickHouse, Gorilla codec on values | 4.67 |
| raw points, ClickHouse, ZSTD | 2.96 |
| **raw points, ClickHouse, DoubleDelta timestamps + ZSTD** | **2.72** |
| 10 s rollups, Parquet with zstd | 0.51 |
| **10 s rollups, Funnel blocks (Gorilla columns + zstd)** | **0.48** |

The rollups replace 41 points with one summary, yet the saving over the best
ClickHouse setup is 5.6x rather than 41x. A column store compresses the
repetitive parts of raw data very well: a request counter that always sends
`1` costs it almost nothing. What it cannot compress is the random part,
latency values, and that is exactly what the sketches replace. In Funnel's
blocks, distributions are 94% of the bytes before compression; counters
cost about 2 bytes per 10-second bucket.

The saving grows with traffic, because rollup storage grows with the number
of series rather than the number of points.
[`examples/scaling.rs`](crates/funnel/examples/scaling.rs) runs the same
7,000 series at higher request rates:

| traffic | points | Funnel storage | bytes per point |
|---|---|---|---|
| 1x | 4.1 M | 2.0 MB | 0.50 |
| 10x | 40.5 M | 4.5 MB | 0.11 |
| 40x | 162 M | 6.7 MB | 0.041 |

Raw points would grow 40x. Funnel's grow 3.3x, as each sketch fills more of
its bins.

### Choices the measurements made

- Gorilla's XOR encoding (store each value as the bits that changed from
  the previous one) saves a lot on counters and gauges, whose consecutive
  values are close, and almost nothing on latency statistics, which jump
  around. Its bit packing also hides byte patterns from zstd. ClickHouse
  showed the same effect: its Gorilla codec made raw values 2x larger than
  plain ZSTD.
- Storing each bucket's sum, min and max beside its sketch bins compressed
  16% better than the textbook layout of separate columns.
- Sample rates are applied exactly. A value sent at `@0.3` counts as 3.33
  values, kept as whole numbers of 1/65536 so that adding stays an integer
  add; storing counts as floats instead cost 14% of insert speed.

## Accuracy

Rolling up only helps if the answers stay right. Distributions are kept as
[DDSketch](crates/sketch) summaries, which guarantee every percentile to
within 1% of the true value and can be merged: the p99 across 100 hosts or
across an hour comes from merging their sketches. Averaging the per-host
p99s would give a different, wrong number. [`examples/query_check.rs`](crates/funnel/examples/query_check.rs)
sends the workload through the real rollup, storage, compaction and query
path, and the ClickHouse script computes the exact answer from the raw
points:

| `http.duration`, route `/r3`, 299,060 requests | Funnel | exact | error |
|---|---|---|---|
| p50 | 67.27 ms | 67.82 ms | 0.82% |
| p99 | 354.76 ms | 352.45 ms | 0.66% |
| p99.9 | 2,571.60 ms | 2,593.62 ms | 0.85% |

The exact values are taken at DDSketch's rank, `floor(q * (n - 1))`.
ClickHouse's `quantileExact` picks the next value up, which in the tail is
already 0.24% higher.

## Durability and freshness

Funnel writes small files every 5 seconds and merges them later, the
approach VictoriaMetrics uses, instead of a write-ahead log. A per-tenant
manifest, replaced atomically, decides which files exist, so a crash leaves
either the old set of files or the new one. Open windows are flushed early
every 5 seconds as partial buckets, which queries and compaction merge;
that is safe because every aggregate here merges exactly.
[`scripts/durability_test.py`](scripts/durability_test.py) runs the real
binary, kills it with `kill -9`, restarts it on the same data and checks
compaction:

| | result |
|---|---|
| data sent but lost to `kill -9` | 7.1 and 2.1 seconds of traffic in two runs |
| time from sending a point to seeing it in a query | 2 to 5 seconds |
| compaction | 11 or 12 files merged into 3, query totals unchanged |

How much a crash loses depends on where in the 5-second checkpoint and
write cycles it lands; the worst case is about 10 seconds.

## Downsampling

Old data is rarely looked at in 10-second detail, so Funnel can rewrite it
into wider buckets as it ages: `--downsample 7d:1m,30d:10m` keeps 1-minute
buckets after 7 days and 10-minute buckets after 30, using the syntax of
VictoriaMetrics' `-downsampling.period`. Compaction does the rewriting, each
level with files 60 buckets long, and only once a whole span is older than
its level's age.

Because every bucket is a mergeable summary, downsampling is the same merge
as everything else. Counters stay exact, gauges keep their true min and
max, and distributions keep their sketches. The systems Funnel borrows from
keep less: VictoriaMetrics' downsampling keeps the last sample of each
interval, which drops spikes, and Thanos keeps count, sum, min and max,
which rules out percentiles.

[`examples/downsample.rs`](crates/funnel/examples/downsample.rs) stores 30
minutes of the workload at each resolution through a real store:

| resolution | bytes | smaller than 10 s | disk for 30 days |
|---|---|---|---|
| 10 seconds | 19.7 MB | 1.0x | 28.3 GB |
| 1 minute | 6.5 MB | 3.0x | 9.3 GB |
| 10 minutes | 1.3 MB | 14.9x | 1.9 GB |

One minute saves 3x rather than 6x because a merged sketch holds more
distinct bins than each of the six it replaced. With `7d:1m,30d:10m`, a year
of this workload is about 35 GB: 6.6 GB for the first week, 7.1 GB for the
rest of the month and 21 GB for the other eleven months.

Percentiles over a whole range do not change at all.
[`examples/query_check.rs`](crates/funnel/examples/query_check.rs) gives the
same p50, p99 and p99.9 for route `/r3` (67.27, 354.76 and 2,571.60 ms) at
10-second, 1-minute and 10-minute resolution, reading 13,554, 2,959 and 300
buckets. What is lost is the ability to look inside a bucket: last month's
data cannot show what happened in one particular 10 seconds.

A query that mixes resolutions computes a rate over each bucket's real
width: a 10-second step over 1-minute buckets divides by 60, not by 10. A
test checks this, and fails with a rate 6 times too high without it.

## Monthly cost

For the test workload, 7,000 series and 22,600 points a second:

**Funnel on one small server.** CPU is not the limit; disk is. Funnel's
blocks take about 0.94 GB a day for this workload, so 30 days need about
28 GB. A [DigitalOcean](https://www.digitalocean.com/pricing/droplets)
droplet with 1 vCPU and 1 GiB of RAM costs $6 a month with 25 GiB of disk
(about 3 weeks of data after the operating system) and $12 with 50 GiB
(30 days at full resolution, or about a year with downsampling after 7 and
30 days). Funnel used under 50 MB of memory in every test above.

**Raw points in ClickHouse.** At 2.72 bytes per point, the same traffic is
5.3 GB a day, 159 GB for 30 days and 1.9 TB for a year, before replicas.

**Datadog.** Datadog bills custom metrics at
[$5 per 100 per month](https://www.datadoghq.com/pricing/list/). A
[distribution counts as 5 custom metrics per tag combination, and 10 with
percentiles](https://docs.datadoghq.com/account_management/billing/custom_metrics/),
so this workload is 3,500 counter and gauge series plus 3,500 x 10 = 35,000
for the distributions: 38,500 custom metrics, $1,925 a month at list price.
The Pro plan's allotment of 100 custom metrics per monitored host, pooled
across 100 hosts, would bring it to $1,425, on top of the per-host fees.

**Grafana Cloud.** Grafana Cloud bills
[$6.50 per 1,000 active series, plus $19 a month for the Pro platform](https://grafana.com/pricing/).
Sent as Prometheus histograms with the client libraries' default 11 buckets,
each distribution series becomes 14 series (12 buckets, a sum and a count):
49,000 plus 3,500 others, 52,500 active series. That comes to $360 a
month ($341 for the series and the $19 fee), or $295 if the free tier's
10,000 series apply. Native histograms, where
available, would cut this sharply.

At 40 times the traffic over the same series, the hosted bills stay about
the same, since both price by series. Raw storage grows 40 times, to 212 GB
a day. Funnel's grows 3.3 times, to about 3.1 GB a day at full resolution,
and downsampling shrinks everything older than a week.

Since this project started from Azure Data Explorer: for this kind of
question (one metric, a time range, a filter, a percentile), rolling up
first is what lets a $6 to $12 server do the job. ADX remains the tool for
ad hoc analysis over raw events, which Funnel does not keep.

## What Funnel does not do

- It runs on one server, with no replication; losing the disk loses the
  data.
- Raw values are gone after rollup. Questions that need individual events
  (which request was slow) need a log or trace store.
- On UDP, tenants come from a tag with no authentication.
- One file per tenant per segment suits tens of tenants; thousands would
  want one shared write-ahead log.
- A crash loses up to about 10 seconds: open windows since the last
  checkpoint and rollups since the last file write.
- Downsampled data cannot be queried at a finer resolution than its
  buckets.

## Reproducing

```sh
cargo build --release --workspace --examples
python3 scripts/loadtest.py --cpus 0 --shards 1 --udp-threads 1   # one vCPU
python3 scripts/durability_test.py
cargo run --release -p funnel --example storage -- 10
cargo run --release -p funnel --example scaling -- 3 50 500 2000
scripts/clickhouse_compare.sh 10                                  # needs Docker
cargo run --release -p funnel --example query_check -- 10 /r3       # add 600 to downsample first
cargo run --release -p funnel --example downsample -- 30
cargo run --release -p funnel --example ingest -- 30
cargo run --release -p funnel --features profile -- --profile flame.svg
```
