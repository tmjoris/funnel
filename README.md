# Funnel

Apps report numbers about themselves all the time: how many requests they
handled, how long each one took, how full a queue is. Funnel collects those
numbers and, instead of storing every one, keeps a short summary for every
10 seconds. The goal is to run it on a $5/month server and to measure how
much storage and money that saves compared with keeping everything.

The measured answer is in [WRITEUP.md](WRITEUP.md). In short: one cloud vCPU
takes 1.5 to 2 million lines a second with no loss; the data needs 5.6x less disk
than raw points in ClickHouse with its best codecs, and the gap widens with
traffic; percentiles stay within 1% of the exact answer; and a workload of
7,000 series that would cost about $1,925 a month on Datadog or $295 to $360
on Grafana Cloud at list prices fits on a $6 to $12 server.

## The problem

Storing these numbers in a hosted service gets expensive for two reasons.

The first is volume. An app that records how long each request took, at
5,000 requests a second, produces 432 million numbers a day for that one
measurement. People almost never look at them one by one. Dashboards and
alerts ask questions like "how many requests in the last minute?" or "how
slow were the slowest 1% of requests?", so storing every number means
paying for data that is only ever read as a summary.

The second is the number of separate series. Each measurement can carry
labels such as `route:/checkout` or `region:eu`, and every different
combination of labels is stored, and billed, as its own series. A label with
endless possible values, like a user ID, can turn one measurement into
millions of series overnight.

Sentry's ingestion service, Relay, already summarizes measurements into
10-second buckets before storing them (`relay-metrics`). Until April 2026 it
also capped how many series each organization could create
(`relay-cardinality`, removed in
[getsentry/relay#5809](https://github.com/getsentry/relay/pull/5809)).
Funnel applies both ideas in one small program and measures the result:
bytes stored per number, CPU used, monthly cost, and the size of the same
data kept raw in ClickHouse.

Asking questions of the stored data stays deliberately simple: one
measurement, a time range and an optional label filter. Funnel aims to cover
what a developer typically needs from a metrics store such as Azure Data
Explorer at a fraction of the cost, by keeping far less data to search.

## Words used here

| Term | Meaning |
|---|---|
| Metric | A named measurement an app reports, such as `http.requests` or `db.query_time` |
| Tags | Labels attached to a metric, such as `route:/checkout` |
| Series | One metric with one exact set of tags: `db.query_time` with `route:/a` and with `route:/b` are two series |
| Cardinality | The number of distinct series. Hosted metrics are usually priced by it |
| Counter, gauge, distribution | The three kinds of metric. A counter is added up (requests handled), a gauge is a level read at a moment (queue length), and a distribution is a set of values whose spread matters (request durations) |
| Bucket | The summary of one series over 10 seconds |
| Rollup | Replacing raw values with buckets as they arrive |
| p99 | The 99th percentile: the value that 99% of measurements fall below |
| Sketch | A summary of a distribution, about 1 KB, that can answer "what was the p99?" to within 1%. Funnel uses [DDSketch](crates/sketch) |
| HyperLogLog | A 4 KB structure that estimates how many distinct things it has seen, to within about 2% |
| Sample rate | A client that reports 1 in 10 events marks each value `@0.1`, so each value stands for 10 |
| StatsD | A simple text format for sending metrics, one per line: `db.query_time:12\|ms\|#route:/a` |
| Tenant | A customer or team; limits and statistics are kept per tenant |
| Series id | A 64-bit fingerprint (hash) of a series' tenant, name, kind and tags, used instead of comparing strings |
| Shard | One worker thread that owns a fixed slice of the series, so threads never wait on each other |
| Load shedding | Dropping data on purpose, and counting it, when more arrives than the server can handle |

## How it works

Metrics arrive as StatsD lines over UDP or HTTP. The router parses each
line, hashes its series (tenant, name, type and sorted tags) and sends it to
one of N shard threads, so every series lives in exactly one shard and the
hot path takes no locks. When a shard's queue is full, the router drops the
batch and counts it, which keeps memory bounded under overload.

Each shard rolls points into 10-second buckets. Counters are summed, gauges
keep min, max, last, sum and count, and distributions go into a
[DDSketch](crates/sketch) of about 1 KB instead of a list of raw values.
Sample rates are applied exactly, including fractional ones such as `@0.3`.
Points up to a day late are accepted into partial buckets that merge with
the rest when read. Memory for open buckets is bounded in bytes.

A per-tenant limit caps distinct series per hour. The decision uses an
exact set of admitted series, because a HyperLogLog cannot say whether a
given series is new; a HyperLogLog does estimate how many series a tenant
tried to create, for reporting. Shards share one budget per tenant and take
its lock only for a new series. Series admitted in the previous hour keep a
reserved slot, so a tenant over its limit does not lose its long-lived
series to new ones at the boundary.

`GET /stats` accounts for every line received: accepted, a parse error,
dropped for overload, or rejected for being too old, too far in the future,
over the memory bound or over the series limit.

Every 5 seconds the shards also flush their open windows early, as partial
buckets that queries and compaction merge back together, and a writer thread
writes what it has collected as one small zstd-compressed block file per
tenant. A per-tenant manifest, replaced atomically, lists the live files, so
after a crash the set of files is either the old one or the new one.
Compaction merges each closed 10-minute span into one file, merging partial
buckets as it goes, and swaps inputs for output in a single manifest update.
This follows VictoriaMetrics (frequent small parts merged later, no
write-ahead log) and Prometheus (time-aligned compaction, deferred deletion
of compacted files).

With `--downsample 7d:1m,30d:10m`, compaction also rewrites data into
1-minute buckets after 7 days and 10-minute buckets after 30. Every bucket
is a mergeable summary, so counters stay exact, gauges keep their true min
and max, and percentiles over any range give the same answer as at full
resolution; only detail inside a bucket is lost. A year of the test
workload takes about 35 GB this way, against 345 GB at full resolution.

`GET /query` reads one metric over a time range, with optional tag filters,
grouping and a step, and returns one number per point:

```sh
# p99 latency of /checkout per host, in 1-minute points, over the last hour
curl 'localhost:8080/query?metric=http.duration&where=route:/checkout&group_by=host&step=60&agg=p99'
```

Every combination happens through the same merge: partial buckets of one
window, several hosts grouped into one line and six 10-second buckets folded
into a minute. Merging sketches is what makes a grouped p99 correct; an
average of per-host p99s is not the p99 of all requests.

## Long-term SQL with Iceberg

Built with `--features lake` and started with `--lake-dir DIR`, Funnel also publishes each closed
hour to [Apache Iceberg](https://iceberg.apache.org/) tables, stored as Apache Parquet files, and
serves SQL through [Apache DataFusion](https://datafusion.apache.org/) at `POST /sql`. The block
store above still handles recent data and `GET /query`.

Iceberg is an open table format: a set of metadata files that says which Parquet files make up a
table. It means a team can read a year of metrics from Spark or any other Iceberg engine without
exporting anything, which a private block format cannot offer.

```sh
curl localhost:8080/sql -H 'x-funnel-tenant: acme' -d "
  SELECT date_bin(INTERVAL '1 hour', ts) AS hour, quantile(sketch, 0.99) AS p99
  FROM metrics
  WHERE metric = 'http.duration'
    AND ts >= TIMESTAMP '2026-01-01T00:00:00' AND ts < TIMESTAMP '2026-01-08T00:00:00'
  GROUP BY 1 ORDER BY 1"
```

Each hour lands in four tables: `metrics` (10-second buckets), `metrics_1m`, `metrics_10m` and
`metrics_1h`. Queries name `metrics`, and a [planning rule](crates/lake/src/rule.rs) moves each one
to the coarsest table that gives the same answer. It runs inside DataFusion's optimizer and only
acts when the result is guaranteed to stay equal:

- Every aggregate has to merge: `sum(sum)`, `sum(count)`, `min(min)`, `max(max)` or
  `quantile(sketch, q)`. `count(*)` counts stored rows, which differ per table, so it stays put.
- A `date_bin` step has to be a multiple of the table's bucket width.
- Time bounds have to be `ts >= a` and `ts < b` on bucket boundaries.
- Filters can only use series columns, since value columns hold different numbers in each table.
- The table has to be published at least as far as the query reaches.

The test suite runs 200 generated queries with the rule on and off and requires equal answers.
Breaking any of three safety checks on purpose (time alignment, step divisibility, value filters)
makes that test fail.

Measured on an Intel i5-6200U laptop with 7 days of 600 series at 10-second resolution (36.3 million
rows, 200 hosts with three metrics each). Each figure is the median of 5 alternating runs, with
all planning, reading and aggregation included.

| Dashboard | Table chosen | Without the rule | With the rule | Speedup |
|---|---|---|---|---|
| p99 latency, 7 days, per hour | 1 hour | 13,524 ms | 428 ms | 31.6x |
| peak queue per host, 7 days | 1 hour | 1,381 ms | 290 ms | 4.8x |
| request total, 1 day, per 10 minutes | 10 minutes | 394 ms | 224 ms | 1.8x |
| p99 latency, 1 hour, per minute | 1 minute | 289 ms | 234 ms | 1.2x |
| request total, 7 days, end not on a boundary | 10 seconds (unchanged) | 1,132 ms | 1,112 ms | 1.0x |

The gain grows with the time range. The with-rule times have a floor of about 220 to 290 ms that
a smaller table does not remove, so short queries gain little.

| Table | Parquet size | Share of the finest |
|---|---|---|
| `metrics` | 913.5 MB | 100% |
| `metrics_1m` | 215.8 MB | 24% |
| `metrics_10m` | 34.1 MB | 3.7% |
| `metrics_1h` | 9.3 MB | 1.0% |

The coarser tables add 28% to the size of the finest one.

The tables use the standard Iceberg SQL catalog, so other engines open them directly. Spark 4.0.2
read them through its JDBC catalog, on its own JVM scan and on the native scans of Comet 1.2.0 and
Velox (Gluten 1.7.0). All three matched DataFusion exactly on two aggregate queries over 7 days
of data, one of them answered by DataFusion from the hourly rollup. [`interop/`](interop) has
that check.

Each SQL session is limited to the caller's tenant by a rule that filters every table scan, so a
query cannot read another tenant's rows by leaving out a filter. Only read-only statements run, each
with a 1 GB memory limit and a 30-second timeout.

## Storage: what each format costs

A [benchmark](crates/funnel/examples/storage.rs) generates 10 minutes of a
simulated backend with the [workload generator](crates/loadgen): 20 services
on 5 hosts each, 13.6 million points across 7,000 series. It stores the same
data in each format and reports bytes per raw point received. Measured on
an Intel i5-6200U laptop, zstd level 3:

| format | bytes per point | vs raw columns | encode | decode |
|---|---|---|---|---|
| raw StatsD text + zstd | 5.14 | 2.0x larger | | |
| raw columns + zstd (series, time, value, sorted) | 2.61 | 1.0x | 2.6 s | |
| 10 s rollups, one row per bucket + zstd | 0.76 | 3.4x smaller | 0.9 s | |
| 10 s rollups, Gorilla block, no zstd | 1.21 | 2.2x smaller | 0.9 s | 0.3 s |
| **10 s rollups, Gorilla block + zstd** | **0.48** | **5.4x smaller** | 1.1 s | 0.4 s |
| 10 s rollups, Parquet with zstd | 0.51 | 5.1x smaller | 1.5 s | 0.06 s * |

\* Parquet's decode stops at Arrow arrays and leaves sketches as bytes; the
Gorilla decode includes turning every sketch back into a sketch.

The 331k buckets replace 13.6 million points, 41 points per bucket, yet the
saving over a column store is 5.4x rather than 41x. Raw columns compress
very well where values repeat: a counter that is always `1` costs almost
nothing. What they cannot compress is the random part, latency values, and
that is the part the sketches replace. Distributions are 94% of the block
before compression: counters cost about 2 bytes per bucket, gauges 16 and
distribution sketches 93.

Two layout choices were made by measurement, after zstd, since blocks are
always stored compressed:

- Each series stores its sketch settings once instead of in every bucket.
- Each bucket's sum, min and max sit right next to its sketch bins. Moving
  them into separate columns, which is the textbook columnar choice, made
  the compressed file 16% larger, whether those columns used Gorilla's XOR
  encoding or Parquet's byte stream split. The likely reason is that a
  bucket's min and its first bin describe nearly the same number, and side
  by side they form byte patterns zstd can match.

Gorilla's XOR encoding works very well on counters and gauges, whose
consecutive values are similar, and saves little on latency statistics,
which jump around from bucket to bucket.

## Known limits

- Series ids are 64-bit hashes. Two series collide with probability around
  3e-8 at a million series, and a collision merges them silently.
- On UDP the tenant comes from a `tenant` tag with no authentication, which
  suits a server for one team, not a public multi-tenant service.
- `SO_REUSEPORT` spreads packets by source address and port, so a client
  sending from a single socket is served by one receiver thread.
- Load shedding is not fair between tenants: a noisy tenant can fill a shard
  queue and cause drops for others.
- The series limit costs about 10 bytes per admitted series per tenant:
  fine for tens of tenants, too much for tens of thousands.
- A crash loses up to about 10 seconds of data: open windows since the last
  checkpoint and rollups since the last file write. New data takes 2 to 5
  seconds to show up in queries.
- One file per tenant every 5 seconds suits tens of tenants; thousands
  would want a single write-ahead log shared by all of them.
- Downsampled data cannot be queried at a finer resolution than its
  buckets.
- The lake only receives an hour once it is compacted, so `/sql` lags `/query` by up to about 75
  minutes by default. Points that arrive after their hour was published reach `/query` only.
- The lake writes one Parquet file per table per hour and has no compaction of its own, so a
  year means about 8,760 files per table, and planning time grows with it.
- `/sql` has the same trust model as `/query`: the tenant comes from a header, with no
  authentication.

## Status

| crate | state |
|---|---|
| [`crates/sketch`](crates/sketch) | Done. DDSketch with property-tested error bounds; faster inserts and merges than `sketches-ddsketch` and under 1% error where t-digest reaches 32% |
| `crates/funnel` | Ingest, rollups, late data, memory bound, series limits, block storage with compaction, and queries. Load tested, profiled, compared with ClickHouse; results in [WRITEUP.md](WRITEUP.md) |
| [`crates/loadgen`](crates/loadgen) | Deterministic StatsD workload shaped like a small SaaS backend |
| [`crates/lake`](crates/lake) | Iceberg publishing, the `quantile` aggregate, and the DataFusion rollup and tenant rules |

## Building and testing

```sh
cargo test --workspace
cargo bench -p funnel-sketch
cargo run --release -p funnel-sketch --example accuracy
cargo run --release -p funnel --example storage -- 10   # minutes of workload

cargo run --release -p funnel -- --data-dir data   # UDP :8125, HTTP :8080

# With Iceberg publishing and SQL
cargo run --release -p funnel --features lake -- --data-dir data --lake-dir lake
cargo run --release -p funnel-lake --example bench -- /tmp/lake 7 200   # days, hosts
```

## License

MIT
