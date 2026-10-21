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

Sentry's ingestion service, Relay, rolls metrics up before they reach Kafka
or ClickHouse: `relay-metrics` aggregates them into time buckets, with a
limit on how much memory open buckets may use. Until April 2026 Relay also
capped distinct series per organization with `relay-cardinality`; that
limiter was removed in [getsentry/relay#5809](https://github.com/getsentry/relay/pull/5809).
Funnel applies both ideas in one small program and measures the result:
bytes stored per point, CPU used, monthly cost, and the size of the same
data stored raw in ClickHouse.

The query side stays deliberately small: one metric, a time range and an
optional tag filter. Funnel aims to cover what a developer typically needs
from a metrics store such as Azure Data Explorer at a fraction of the cost,
by keeping far less data around to query.

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
