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
and `q = 0` / `q = 1` return the exact min and max. Values can carry
fractional weights through `add_n`, so a StatsD value sampled at `@0.3`
counts as 3.33 values, and the bound holds for the weighted quantiles too.

Memory is bounded by `max_bins` per sign. When the budget runs out, the
lowest bins merge, so high quantiles (p99 and up) keep the guarantee and
only the lowest ones degrade.

These claims are checked with property tests (`tests/properties.rs`,
512 random cases each): the error bound across 21 decades of positive,
negative and zero values; merge equals a single sketch over the concatenated
data, including under tight bin budgets; encoding round-trips; decoding
arbitrary bytes never panics; collapsed sketches keep the bound for the
quantiles near the top; and weighted quantiles stay within the bound.

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

Using `1 / ln 2`, the slope of the exact `log2`, instead of `10/7` breaks
the bound, and the property tests catch it: proptest finds a value with
0.1007% error at `alpha = 0.1%`.

## Results on a laptop

Intel i5-6200U (2 cores / 4 threads, 2.3 GHz laptop), rustc 1.94, release
build. Run them yourself with `cargo bench -p funnel-sketch` and
`cargo run --release -p funnel-sketch --example accuracy`.

### Insert, 100k lognormal latencies

| | time | throughput |
|---|---|---|
| funnel-sketch, cubic | 1.68 ms | 59.5M values/s |
| funnel-sketch, log | 2.23 ms | 44.9M values/s |
| sketches-ddsketch 0.4 | 2.56 ms | 39.0M values/s |
| tdigest 1.0 (batch insert, its best case) | 5.14 ms | 19.5M values/s |

### Merge 10 or 100 sketches of 100k values in total

| | 10 | 100 |
|---|---|---|
| funnel-sketch | 1.96 µs | 16.6 µs |
| sketches-ddsketch 0.4 | 6.84 µs | 52.5 µs |
| tdigest 1.0 | 38.9 µs | 769 µs |

Merging resizes the destination once and adds the overlapping slices of the
two bin arrays.

### Relative error, 1M values

DDSketch at alpha = 1%, t-digest at compression 100. The data is seeded, so
these numbers are identical on every machine, including the CI runners below.

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

t-digest is far more accurate on smooth data and near the median. It bounds
error in rank, and in a heavy tail or in the gap between two modes a small
rank error becomes a large value error: 31.9% at p99.9 on the Pareto data. Latency alerts usually fire on exactly those quantiles. Both
DDSketch implementations stay under 1% everywhere. Encoded, the funnel-sketch
for each 1M-value dataset is 1.0 to 1.4 KB.

## Results on GitHub Actions runners

The [Benchmarks workflow](../../.github/workflows/bench.yml) runs on every
push that touches this crate, on `ubuntu-latest` (x86_64) and
`ubuntu-24.04-arm` (aarch64), both with 4 vCPUs. All runs below used
rustc 1.99.1 and the current code, with fractional counts. The x86 label is
a mixed pool: seven x86 runs of the same commit landed on three different
AMD EPYC models, so results are grouped by CPU. Repeated runs on the same
CPU agreed within 1%, except one EPYC 9V74 run, covered below.

Insert, 100k lognormal values, in millions of values per second (median):

| CPU | runs | funnel cubic | funnel log | sketches-ddsketch | tdigest batch |
|---|---|---|---|---|---|
| Neoverse-N2 | 3 | 195.7 | 123.6 | 89.8 | 52.9 |
| AMD EPYC 9V45 | 1 | 177.1 | 124.1 | 107.7 | 40.7 |
| AMD EPYC 9V74 | 1 of 2 | 110.5 | 83.7 | 72.7 | 32.8 |
| AMD EPYC 7763 | 4 | 77.6 | 60.9 | 38.3 | 28.9 |

Merge 10 sketches (median):

| CPU | funnel-sketch | sketches-ddsketch | tdigest |
|---|---|---|---|
| Neoverse-N2 | 1.09 µs | 4.07 µs | 22.7 µs |
| AMD EPYC 9V45 | 0.82 µs | 2.31 µs | 17.2 µs |
| AMD EPYC 9V74 | 1.32 µs | 3.54 µs | 27.0 µs |
| AMD EPYC 7763 | 1.21 µs | 3.73 µs | 24.3 µs |

The cubic mapping has the fastest inserts on every CPU, 1.5 to 2.2 times
sketches-ddsketch, and merging is 2.7 to 3.7 times faster. Most of the
insert speedup comes from skipping `ln`: with the log mapping, funnel-sketch
is only 1.1 to 1.6 times sketches-ddsketch, and an earlier run on an Intel
Xeon 6973P-C had it slower.

The other EPYC 9V74 run is left out of the table because it did not
reproduce. Every benchmark in it was about 30% slower than in the second
run, and the cubic insert was 2.7 times slower (41.2M values/s, below both
log mappings). A busy neighbour on the shared host fits that pattern; I
have not been able to check further without access to that CPU.

Fractional counts cost some insert speed. On the Neoverse-N2, where runs
repeat within 0.5%, the cubic insert went from 448 µs to 511 µs (+14%) and
merging from 1.06 µs to 1.09 to 1.17 µs, while sketches-ddsketch, whose code
did not change, stayed at 1.10 to 1.11 ms. A float add has several cycles of
latency against one for an integer add, and latency data keeps landing in
the same few bins, so consecutive inserts wait on each other. Keeping
integer bins until the first fractional weight arrives would win this back
if the sketch ever shows up in a profile of the pipeline.
