use std::hint::black_box;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use funnel_sketch::{Config, DDSketch, MappingKind};
use rand::{SeedableRng, rngs::StdRng};
use rand_distr::{Distribution, LogNormal};

const N: usize = 100_000;

/// Latency-like data: median ~20ms, long right tail.
fn latencies(n: usize) -> Vec<f64> {
    let mut rng = StdRng::seed_from_u64(7);
    let dist = LogNormal::new(3.0, 1.0).unwrap();
    (0..n).map(|_| dist.sample(&mut rng)).collect()
}

fn funnel(kind: MappingKind) -> DDSketch {
    DDSketch::new(Config {
        mapping: kind,
        ..Config::default()
    })
}

fn insert(c: &mut Criterion) {
    let values = latencies(N);
    let mut group = c.benchmark_group("insert");
    group.throughput(Throughput::Elements(N as u64));

    for (name, kind) in [
        ("funnel/cubic", MappingKind::Cubic),
        ("funnel/log", MappingKind::Logarithmic),
    ] {
        group.bench_function(name, |b| {
            b.iter(|| {
                let mut s = funnel(kind);
                for &v in &values {
                    s.add(v);
                }
                black_box(s.count())
            })
        });
    }

    group.bench_function("sketches-ddsketch", |b| {
        b.iter(|| {
            let mut s = sketches_ddsketch::DDSketch::new(sketches_ddsketch::Config::defaults());
            for &v in &values {
                s.add(v);
            }
            black_box(s.count())
        })
    });

    // t-digest is batch oriented: feeding it one value at a time would
    // allocate a new digest per insert, so give it its best case.
    group.bench_function("tdigest/batch", |b| {
        b.iter(|| {
            let t = tdigest::TDigest::new_with_size(100);
            black_box(t.merge_unsorted(values.clone()).count())
        })
    });
    group.finish();
}

fn merge(c: &mut Criterion) {
    let mut group = c.benchmark_group("merge");
    for parts in [10usize, 100] {
        let chunks: Vec<Vec<f64>> = latencies(N)
            .chunks(N / parts)
            .map(<[f64]>::to_vec)
            .collect();
        let ours: Vec<DDSketch> = chunks
            .iter()
            .map(|c| {
                let mut s = funnel(MappingKind::Cubic);
                c.iter().for_each(|&v| {
                    s.add(v);
                });
                s
            })
            .collect();
        let theirs: Vec<_> = chunks
            .iter()
            .map(|c| {
                let mut s = sketches_ddsketch::DDSketch::new(sketches_ddsketch::Config::defaults());
                c.iter().for_each(|&v| s.add(v));
                s
            })
            .collect();
        let digests: Vec<_> = chunks
            .iter()
            .map(|c| tdigest::TDigest::new_with_size(100).merge_unsorted(c.clone()))
            .collect();

        group.bench_with_input(BenchmarkId::new("funnel", parts), &ours, |b, ours| {
            b.iter(|| {
                let mut acc = funnel(MappingKind::Cubic);
                for s in ours {
                    acc.merge(s).unwrap();
                }
                black_box(acc.count())
            })
        });
        group.bench_with_input(
            BenchmarkId::new("sketches-ddsketch", parts),
            &theirs,
            |b, theirs| {
                b.iter(|| {
                    let mut acc =
                        sketches_ddsketch::DDSketch::new(sketches_ddsketch::Config::defaults());
                    for s in theirs {
                        acc.merge(s).unwrap();
                    }
                    black_box(acc.count())
                })
            },
        );
        group.bench_with_input(
            BenchmarkId::new("tdigest", parts),
            &digests,
            |b, digests| {
                b.iter(|| black_box(tdigest::TDigest::merge_digests(digests.clone()).count()))
            },
        );
    }
    group.finish();
}

fn encode(c: &mut Criterion) {
    let mut s = funnel(MappingKind::Cubic);
    latencies(N).into_iter().for_each(|v| {
        s.add(v);
    });
    let bytes = s.to_bytes();
    c.bench_function("encode", |b| b.iter(|| black_box(s.to_bytes().len())));
    c.bench_function("decode", |b| {
        b.iter(|| black_box(DDSketch::decode(&bytes).unwrap().1))
    });
}

criterion_group!(benches, insert, merge, encode);
criterion_main!(benches);
