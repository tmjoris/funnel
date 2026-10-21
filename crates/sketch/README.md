# funnel-sketch

A DDSketch implementation in Rust: a mergeable quantile sketch whose answers
are within a fixed relative error (1% by default) of the exact quantile, no
matter how skewed the data is. Funnel uses it to store latency distributions
as roughly 1 KB per series per 10-second bucket instead of every raw value.

```rust
use funnel_sketch::DDSketch;

let mut s = DDSketch::default(); // alpha = 1%, cubic mapping, 2048 bins
for ms in [12.0, 15.0, 230.0, 14.0] {
    s.add(ms);
}
let p99 = s.quantile(0.99).unwrap();
let bytes = s.to_bytes(); // compact varint encoding
```

## What is guaranteed

For any quantile `q`, the result is within `alpha` relative error of the
exact value at rank `floor(q * (n - 1))`. Count, sum, min and max are exact,
and `q = 0` / `q = 1` return the exact min and max. Memory is bounded by
`max_bins` per sign; when the budget runs out, the lowest bins merge, so high
quantiles (p99 and up) keep the guarantee and only the lowest ones degrade.

These claims are checked with property tests (`tests/properties.rs`,
512 random cases each): the error bound across 21 decades of positive,
negative and zero values; merge equals a single sketch over the concatenated
data, including under tight bin budgets; encoding round-trips; decoding
arbitrary bytes never panics; and collapsed sketches keep the bound for the
quantiles near the top.

## The cubic mapping

Each insert maps a value to a bin index. The textbook mapping is
`ceil(ln(x) / ln(gamma))`, which costs one `ln` per value. The cubic mapping
instead reads the float's exponent bits and approximates `log2` of the
significand `1 + t` with `P(t) = (6/35)t³ − (3/5)t² + (10/7)t`, so
`P(0) = 0` and `P(1) = 1` and the index is continuous across powers of two.

The approximation makes some bins wider than others, so the bin width must
be chosen to keep the widest bin within `gamma`. The approximate log `f`
moves at least `min over t of P'(t)(1 + t)` per unit of `ln x`. That
minimum is `10/7` (at `t = 0`), so the multiplier becomes
`1 / (ln(gamma) · 10/7)`, costing about 1% more bins than the log mapping.

The property tests catch the obvious-but-wrong choice: using `1 / ln 2`
(the slope of the exact `log2`) instead of `10/7` makes proptest find a
value with 0.1007% error at `alpha = 0.1%`.

## Results

Intel i5-6200U (2 cores / 4 threads, 2.3 GHz laptop), rustc 1.94, release
build. Run them yourself with `cargo bench -p funnel-sketch` and
`cargo run --release -p funnel-sketch --example accuracy`.

**Insert, 100k lognormal latencies**

| | time | throughput |
|---|---|---|
| funnel-sketch, cubic | 1.68 ms | 59.5M values/s |
| funnel-sketch, log | 2.23 ms | 44.9M values/s |
| sketches-ddsketch 0.4 | 2.56 ms | 39.0M values/s |
| tdigest 1.0 (batch insert, its best case) | 5.14 ms | 19.5M values/s |

**Merge 10 / 100 sketches of 100k values total**

| | 10 | 100 |
|---|---|---|
| funnel-sketch | 1.96 µs | 16.6 µs |
| sketches-ddsketch 0.4 | 6.84 µs | 52.5 µs |
| tdigest 1.0 | 38.9 µs | 769 µs |

Merging adds the overlapping slices of two bin arrays after one resize. An
earlier version added bin by bin through the insert path and was 3.4×
slower than sketches-ddsketch.

**Relative error, 1M values** (DDSketch at alpha = 1%, t-digest at
compression 100)

| dataset | sketch | p50 | p90 | p99 | p99.9 |
|---|---|---|---|---|---|
| uniform 0..1000 | funnel-sketch | 0.48% | 0.13% | 0.49% | 0.41% |
| | sketches-ddsketch | 0.40% | 0.78% | 0.75% | 0.35% |
| | tdigest | 0.02% | 0.00% | 0.00% | 0.00% |
| lognormal latency | funnel-sketch | 0.10% | 0.47% | 0.97% | 0.96% |
| | sketches-ddsketch | 0.90% | 0.69% | 0.29% | 0.99% |
| | tdigest | 0.03% | 0.08% | 0.57% | **5.52%** |
| pareto a=1.1 | funnel-sketch | 0.63% | 0.68% | 0.42% | 0.73% |
| | sketches-ddsketch | 0.12% | 0.62% | 0.30% | 0.51% |
| | tdigest | 0.04% | 0.12% | 2.56% | **31.9%** |
| bimodal cache hit/miss | funnel-sketch | 0.42% | 0.07% | 0.41% | 0.85% |
| | sketches-ddsketch | 0.10% | 0.41% | 0.87% | 0.87% |
| | tdigest | 0.04% | **20.3%** | 0.37% | 2.57% |

t-digest is far more accurate on smooth data and near the median. Its error
is relative to rank, though, so on heavy tails and gaps between modes it can
be off by tens of percent, and those tails are what latency alerts watch.
Both DDSketch implementations stay under 1% everywhere. Encoded, the
funnel-sketch for each 1M-value dataset is 1.0–1.4 KB.
