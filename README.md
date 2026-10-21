# Funnel

Funnel is a metrics pipeline in Rust that rolls metrics up as they arrive and
stores only the rollups. The goal is a single binary that runs on a $5/month
server and a measured answer to how much that saves compared with storing
every raw point.

## The problem

Hosted metrics get expensive for two reasons.

The first is raw volume. A service that records one latency timer per request
at 5,000 requests per second produces 432 million points a day for that one
metric. Dashboards and alerts almost never read those points individually.
They ask for a sum, a rate or a p99 per 10 seconds or per minute, so storing
every point means paying to keep and scan data that is only ever read in
aggregate.

The second is cardinality. Every distinct combination of metric name and tags
is its own series, and a single tag with unbounded values (a user ID, a
request path with IDs in it) can turn one metric into millions of series. One
careless deploy is enough to multiply a customer's bill.

Sentry's ingestion service, Relay, deals with both before data reaches Kafka
or ClickHouse. `relay-metrics` aggregates metrics into time buckets, and
`relay-cardinality` caps how many distinct series an organization can create.
Funnel applies the same two ideas in one small program and measures the
result: bytes stored per point, CPU used, monthly cost, and the size of the
same data stored raw in ClickHouse.

The query side stays deliberately small: one metric, a time range and an
optional tag filter. Funnel aims to cover what a developer typically needs
from a metrics store such as Azure Data Explorer at a fraction of the cost,
by keeping far less data around to query.

## Design

The pipeline crate is under construction; this is what it is being built to do.

Metrics arrive as StatsD over UDP or as lines over HTTP. Under overload the
ingest path drops data on purpose and counts what it dropped, so memory stays
bounded. Points are grouped into 10-second buckets per series: counters are
summed, gauges keep min, max, last and sum, and distributions go into a
[DDSketch](crates/sketch) of about 1 KB instead of a list of raw values. A
per-tenant limit rejects new series past a threshold. Closed buckets are
written to disk in compressed blocks, and Gorilla-style encoding, Parquet and
plain zstd are benchmarked against each other.

## Status

| crate | state |
|---|---|
| [`crates/sketch`](crates/sketch) | Done. DDSketch with property-tested error bounds; faster inserts and merges than `sketches-ddsketch` and under 1% error where t-digest reaches 32% |
| `crates/funnel` | In progress: ingest, rollups, series limits, storage, query, cost write-up |

## Building and testing

```sh
cargo test --workspace
cargo bench -p funnel-sketch
cargo run --release -p funnel-sketch --example accuracy
```

## License

MIT
