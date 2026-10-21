use funnel_sketch::{Config, DDSketch, Mapping, MappingKind};
use proptest::prelude::*;

fn mapping_kind() -> impl Strategy<Value = MappingKind> {
    prop_oneof![Just(MappingKind::Logarithmic), Just(MappingKind::Cubic)]
}

fn alpha() -> impl Strategy<Value = f64> {
    prop_oneof![Just(0.001), Just(0.01), Just(0.05)]
}

/// Values spread over 21 decades with both signs and some exact zeros.
fn value() -> impl Strategy<Value = f64> {
    prop_oneof![
        8 => (-9.0f64..12.0).prop_map(|e| 10f64.powf(e)),
        4 => (-9.0f64..12.0).prop_map(|e| -(10f64.powf(e))),
        1 => Just(0.0),
    ]
}

fn sketch(values: &[f64], config: Config) -> DDSketch {
    let mut s = DDSketch::new(config);
    for &v in values {
        assert!(s.add(v));
    }
    s
}

/// The value at the rank DDSketch targets for quantile `q`.
fn exact(sorted: &[f64], q: f64) -> f64 {
    sorted[(q * (sorted.len() - 1) as f64).floor() as usize]
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(512))]

    #[test]
    fn quantiles_within_relative_error(
        values in prop::collection::vec(value(), 1..2000),
        qs in prop::collection::vec(0.0f64..=1.0, 1..20),
        kind in mapping_kind(),
        alpha in alpha(),
    ) {
        // Enough bins that nothing collapses, so the guarantee is unconditional.
        let s = sketch(&values, Config { alpha, mapping: kind, max_bins: 1 << 16 });
        prop_assert!(!s.collapsed());
        let mut sorted = values.clone();
        sorted.sort_by(f64::total_cmp);
        for q in qs {
            let want = exact(&sorted, q);
            let got = s.quantile(q).unwrap();
            prop_assert!(
                (got - want).abs() <= alpha * want.abs() * (1.0 + 1e-9),
                "q={q} want={want} got={got}"
            );
        }
    }

    #[test]
    fn weighted_quantiles_within_relative_error(
        points in prop::collection::vec(
            (value(), prop_oneof![Just(1.0), Just(1.0 / 0.3), Just(2.5), Just(10.0)]),
            1..1000,
        ),
        qs in prop::collection::vec(0.0f64..=1.0, 1..20),
        kind in mapping_kind(),
    ) {
        let alpha = 0.01;
        let mut s = DDSketch::new(Config { alpha, mapping: kind, max_bins: 1 << 16 });
        for &(v, w) in &points {
            prop_assert!(s.add_n(v, w));
        }
        let mut sorted = points.clone();
        sorted.sort_by(|a, b| a.0.total_cmp(&b.0));
        let total: f64 = sorted.iter().map(|p| p.1).sum();
        prop_assert!((s.count() - total).abs() <= total * 1e-12);
        for q in qs {
            if q == 0.0 || q == 1.0 {
                continue;
            }
            // First value whose cumulative weight passes the target rank.
            // Skip ranks within float noise of a boundary between two values.
            let rank = q * (s.count() - 1.0);
            let mut seen = 0.0;
            let mut want = None;
            for &(v, w) in &sorted {
                seen += w;
                if seen > rank {
                    want = Some((v, (seen - rank).min(rank - (seen - w))));
                    break;
                }
            }
            let (want, margin) = want.unwrap_or((sorted.last().unwrap().0, 1.0));
            if margin.abs() < 1e-6 {
                continue;
            }
            let got = s.quantile(q).unwrap();
            prop_assert!(
                (got - want).abs() <= alpha * want.abs() * (1.0 + 1e-9),
                "q={q} want={want} got={got}"
            );
        }
    }

    #[test]
    fn merge_matches_single_sketch(
        a in prop::collection::vec(value(), 0..500),
        b in prop::collection::vec(value(), 0..500),
        kind in mapping_kind(),
        max_bins in prop_oneof![8u32..64, Just(1 << 16)],
    ) {
        // Collapsing the lowest bins commutes with merging: each part's cutoff
        // is at or below the merged cutoff, so tight budgets must agree too.
        let config = Config { mapping: kind, max_bins, ..Config::default() };
        let mut merged = sketch(&a, config);
        merged.merge(&sketch(&b, config)).unwrap();
        let whole = sketch(&[a, b].concat(), config);
        prop_assert_eq!(merged.count(), whole.count());
        prop_assert_eq!(merged.min(), whole.min());
        prop_assert_eq!(merged.max(), whole.max());
        for q in [0.0, 0.1, 0.25, 0.5, 0.75, 0.9, 0.99, 0.999, 1.0] {
            prop_assert_eq!(merged.quantile(q), whole.quantile(q));
        }
    }

    #[test]
    fn encoding_roundtrips(
        values in prop::collection::vec(value(), 0..500),
        kind in mapping_kind(),
        alpha in alpha(),
        max_bins in 16u32..4096,
    ) {
        let s = sketch(&values, Config { alpha, mapping: kind, max_bins });
        let bytes = s.to_bytes();
        let (d, used) = DDSketch::decode(&bytes).unwrap();
        prop_assert_eq!(used, bytes.len());
        prop_assert_eq!(d.to_bytes(), bytes);
    }

    #[test]
    fn decode_never_panics(bytes in prop::collection::vec(any::<u8>(), 0..256)) {
        let _ = DDSketch::decode(&bytes);
    }

    /// With a tight bin budget, quantiles whose bin is within `max_bins` of
    /// the top bin keep the guarantee, and lower ones are only ever pushed up.
    #[test]
    fn collapse_preserves_high_quantiles(
        exponents in prop::collection::vec(-9.0f64..12.0, 1..2000),
        q in 0.0f64..=1.0,
        kind in mapping_kind(),
        max_bins in 16u32..256,
    ) {
        let values: Vec<f64> = exponents.iter().map(|e| 10f64.powf(*e)).collect();
        let alpha = 0.01;
        let s = sketch(&values, Config { alpha, mapping: kind, max_bins });
        let mapping = Mapping::new(kind, alpha);
        let mut sorted = values.clone();
        sorted.sort_by(f64::total_cmp);
        let want = exact(&sorted, q);
        let got = s.quantile(q).unwrap();
        let top = mapping.index(*sorted.last().unwrap());
        if mapping.index(want) > top - max_bins as i32 + 1 {
            prop_assert!((got - want).abs() <= alpha * want * (1.0 + 1e-9), "want={want} got={got}");
        } else {
            prop_assert!(got >= want * (1.0 - alpha), "want={want} got={got}");
        }
    }
}
