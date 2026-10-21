//! Prints relative quantile error, size and memory for funnel-sketch,
//! sketches-ddsketch and t-digest on a few shapes of data.
//!
//!     cargo run --release -p funnel-sketch --example accuracy

use funnel_sketch::{Config, DDSketch, MappingKind};
use rand::{Rng, SeedableRng, rngs::StdRng};
use rand_distr::{Distribution, Exp, LogNormal, Pareto};

const N: usize = 1_000_000;
const QUANTILES: [f64; 4] = [0.5, 0.9, 0.99, 0.999];

fn datasets() -> Vec<(&'static str, Vec<f64>)> {
    let mut rng = StdRng::seed_from_u64(42);
    let mut sample =
        |d: &dyn Fn(&mut StdRng) -> f64| (0..N).map(|_| d(&mut rng)).collect::<Vec<_>>();
    vec![
        ("uniform 0..1000", sample(&|r| r.random_range(0.0..1000.0))),
        (
            "lognormal latency",
            sample(&|r| LogNormal::new(3.0, 1.0).unwrap().sample(r)),
        ),
        (
            "pareto a=1.1",
            sample(&|r| Pareto::new(1.0, 1.1).unwrap().sample(r)),
        ),
        (
            "bimodal cache hit/miss",
            sample(&|r| {
                if r.random_bool(0.9) {
                    Exp::new(1.0).unwrap().sample(r) + 0.5
                } else {
                    LogNormal::new(5.0, 0.5).unwrap().sample(r)
                }
            }),
        ),
    ]
}

fn exact(sorted: &[f64], q: f64) -> f64 {
    sorted[(q * (sorted.len() - 1) as f64).floor() as usize]
}

fn rel(got: f64, want: f64) -> String {
    format!("{:.3}%", (got - want).abs() / want.abs() * 100.0)
}

fn main() {
    println!("{N} values per dataset; relative error at each quantile\n");
    for (name, values) in datasets() {
        let mut sorted = values.clone();
        sorted.sort_by(f64::total_cmp);

        let mut ours = DDSketch::new(Config {
            mapping: MappingKind::Cubic,
            ..Config::default()
        });
        let mut theirs = sketches_ddsketch::DDSketch::new(sketches_ddsketch::Config::defaults());
        for &v in &values {
            ours.add(v);
            theirs.add(v);
        }
        let digest = tdigest::TDigest::new_with_size(100).merge_unsorted(values.clone());

        println!("{name}");
        println!(
            "  {:<20} {:>9} {:>9} {:>9} {:>9}",
            "", "p50", "p90", "p99", "p99.9"
        );
        let row = |label: &str, f: &dyn Fn(f64) -> f64| {
            let cells: Vec<_> = QUANTILES
                .iter()
                .map(|&q| rel(f(q), exact(&sorted, q)))
                .collect();
            println!(
                "  {label:<20} {:>9} {:>9} {:>9} {:>9}",
                cells[0], cells[1], cells[2], cells[3]
            );
        };
        row("funnel-sketch", &|q| ours.quantile(q).unwrap());
        row("sketches-ddsketch", &|q| {
            theirs.quantile(q).unwrap().unwrap()
        });
        row("tdigest (100)", &|q| digest.estimate_quantile(q).unwrap());
        println!(
            "  funnel-sketch: {} bytes encoded, {} bytes in memory, collapsed={}\n",
            ours.to_bytes().len(),
            ours.memory_bytes(),
            ours.collapsed()
        );
    }
}
