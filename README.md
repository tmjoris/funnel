# Funnel

Apps report numbers about themselves all the time: how many requests they
handled, how long each one took, how full a queue is. Funnel collects those
numbers and, instead of storing every one, keeps a short summary for every
10 seconds. The goal is to run it on a $5/month server and to measure how
much storage and money that saves compared with keeping everything.

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

Still to come: compressed storage (Gorilla-style encoding, Parquet and plain
zstd, benchmarked against each other), a small query API, a load generator
and the cost write-up.

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

## Status

| crate | state |
|---|---|
| [`crates/sketch`](crates/sketch) | Done. DDSketch with property-tested error bounds; faster inserts and merges than `sketches-ddsketch` and under 1% error where t-digest reaches 32% |
| `crates/funnel` | In progress. Ingest, rollups, late data, the memory bound and series limits work; storage, query, load testing and the write-up are next |

## Building and testing

```sh
cargo test --workspace
cargo bench -p funnel-sketch
cargo run --release -p funnel-sketch --example accuracy
```

## License

MIT
