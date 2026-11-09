//! A DataFusion planning rule that answers a query from the coarsest rollup table that gives
//! exactly the same result.
//!
//! People write queries against `metrics`, the 10-second table. A dashboard over a month would
//! read 260,000 rows per series there, and one row per hour per series in `metrics_1h`.
//!
//! A coarser table is only correct under conditions, and the rule refuses unless all of them
//! hold:
//!
//! - Every aggregate must merge: `sum(sum)`, `sum(count)`, `min(min)`, `max(max)` and
//!   `quantile(sketch, q)`. `count(*)` counts stored rows, which differ per table.
//! - Grouping may use the series columns and `date_bin` with a step that is a multiple of the
//!   table's bucket width. A step the buckets cannot divide evenly would cut a bucket in two.
//! - A time filter must be `ts >= a` and/or `ts < b` with both ends on bucket boundaries,
//!   otherwise a coarse row covers time the query excluded.
//! - Filters may not touch value columns, whose contents differ between tables.
//! - The table must be published as far as the query reaches.
//!
//! The rule runs last, after constant folding and filter pushdown, so literals are already plain
//! values and every filter on the scan is visible.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::common::{Result, ScalarValue};
use datafusion::logical_expr::expr::{AggregateFunction, ScalarFunction};
use datafusion::logical_expr::logical_plan::{Aggregate, TableScan};
use datafusion::logical_expr::utils::split_conjunction;
use datafusion::logical_expr::{BinaryExpr, Expr, LogicalPlan, Operator, TableSource};
use datafusion::optimizer::{OptimizerConfig, OptimizerRule};

const FINE_TABLE: &str = "metrics";
const SERIES_COLUMNS: [&str; 4] = ["tenant", "metric", "tags", "kind"];
const MICROS: i64 = 1_000_000;

/// A table the rule may choose.
#[derive(Clone)]
pub struct Rollup {
    pub seconds: i64,
    /// Published up to this moment for every tenant, in seconds; `None` if empty.
    pub covered_until: Option<i64>,
    pub source: Arc<dyn TableSource>,
}

#[derive(Debug, Default)]
pub struct RuleStats {
    /// How many queries moved to a coarser table.
    pub rewrites: AtomicU64,
    /// Bucket width of the table last chosen, in seconds.
    pub last_seconds: AtomicU64,
}

pub struct ChooseRollup {
    /// Finest first.
    tables: Vec<Rollup>,
    stats: Arc<RuleStats>,
}

impl std::fmt::Debug for ChooseRollup {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ChooseRollup").finish_non_exhaustive()
    }
}

/// What a query needs from a table, once its shape is known to be safe.
#[derive(Debug, Default, PartialEq)]
struct Needs {
    /// `date_bin` steps and origins, in microseconds.
    steps: Vec<(i64, i64)>,
    /// `ts >= from`, in microseconds.
    from: Option<i64>,
    /// `ts < to`, in microseconds.
    to: Option<i64>,
}

impl ChooseRollup {
    pub fn new(tables: Vec<Rollup>, stats: Arc<RuleStats>) -> Self {
        ChooseRollup { tables, stats }
    }

    /// The coarsest table that satisfies `needs`, if it is coarser than the finest.
    fn choose(&self, needs: &Needs) -> Option<&Rollup> {
        let finest_covered = self.tables.first()?.covered_until;
        self.tables.iter().skip(1).rev().find(|t| {
            let width = t.seconds * MICROS;
            let aligned = |v: i64| v.rem_euclid(width) == 0;
            let covered = match (needs.to, t.covered_until) {
                (_, None) => false,
                (Some(to), Some(until)) => to <= until * MICROS,
                // With no upper bound the query reaches the newest data anywhere.
                (None, Some(_)) => t.covered_until == finest_covered,
            };
            covered
                && needs.from.is_none_or(aligned)
                && needs.to.is_none_or(aligned)
                && needs
                    .steps
                    .iter()
                    .all(|&(step, origin)| step % width == 0 && aligned(origin))
        })
    }
}

impl OptimizerRule for ChooseRollup {
    fn name(&self) -> &str {
        "funnel_choose_rollup"
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        plan.transform_up(|node| {
            let LogicalPlan::Aggregate(aggregate) = &node else {
                return Ok(Transformed::no(node));
            };
            let Some(needs) = analyze(aggregate) else {
                return Ok(Transformed::no(node));
            };
            let Some(target) = self.choose(&needs) else {
                return Ok(Transformed::no(node));
            };
            let target = target.clone();
            let out = node.transform_up(|n| match n {
                LogicalPlan::TableScan(scan) if is_fine_table(&scan) => {
                    Ok(Transformed::yes(LogicalPlan::TableScan(TableScan {
                        source: target.source.clone(),
                        ..scan
                    })))
                }
                other => Ok(Transformed::no(other)),
            })?;
            self.stats.rewrites.fetch_add(1, Ordering::Relaxed);
            self.stats
                .last_seconds
                .store(target.seconds as u64, Ordering::Relaxed);
            Ok(out)
        })
    }
}

fn is_fine_table(scan: &TableScan) -> bool {
    scan.table_name.table() == FINE_TABLE
}

/// Returns what the query needs, or `None` if its shape cannot safely move at all.
fn analyze(aggregate: &Aggregate) -> Option<Needs> {
    let mut needs = Needs::default();
    for e in &aggregate.group_expr {
        check_group(strip(e), &mut needs)?;
    }
    for e in &aggregate.aggr_expr {
        check_aggregate(strip(e))?;
    }

    let mut found_scan = false;
    let mut safe = true;
    aggregate
        .input
        .apply(|n| {
            match n {
                LogicalPlan::Filter(f) => {
                    safe &= split_conjunction(&f.predicate)
                        .into_iter()
                        .all(|p| check_filter(p, &mut needs).is_some());
                }
                LogicalPlan::TableScan(scan) if is_fine_table(scan) && !found_scan => {
                    found_scan = true;
                    safe &= scan
                        .filters
                        .iter()
                        .flat_map(split_conjunction)
                        .all(|p| check_filter(p, &mut needs).is_some());
                }
                LogicalPlan::Projection(p) => {
                    safe &= p.expr.iter().all(|e| matches!(strip(e), Expr::Column(_)));
                }
                LogicalPlan::SubqueryAlias(_) => {}
                _ => safe = false,
            }
            Ok(if safe {
                TreeNodeRecursion::Continue
            } else {
                TreeNodeRecursion::Stop
            })
        })
        .ok()?;
    (safe && found_scan).then_some(needs)
}

fn strip(e: &Expr) -> &Expr {
    match e {
        Expr::Alias(a) => strip(&a.expr),
        other => other,
    }
}

fn column_name(e: &Expr) -> Option<&str> {
    match strip(e) {
        Expr::Column(c) => Some(c.name.as_str()),
        _ => None,
    }
}

fn check_group(e: &Expr, needs: &mut Needs) -> Option<()> {
    if column_name(e).is_some_and(|c| SERIES_COLUMNS.contains(&c)) {
        return Some(());
    }
    let Expr::ScalarFunction(ScalarFunction { func, args }) = e else {
        return None;
    };
    if func.name() != "date_bin" || column_name(args.get(1)?)? != "ts" {
        return None;
    }
    let step = interval_micros(args.first()?)?;
    let origin = match args.get(2) {
        Some(o) => timestamp_micros(o)?,
        None => 0,
    };
    needs.steps.push((step, origin));
    Some(())
}

fn check_aggregate(e: &Expr) -> Option<()> {
    let Expr::AggregateFunction(AggregateFunction { func, params }) = e else {
        return None;
    };
    if params.distinct || params.filter.is_some() || !params.order_by.is_empty() {
        return None;
    }
    let first = column_name(params.args.first()?)?;
    let allowed = match func.name() {
        "sum" => first == "sum" || first == "count",
        "min" => first == "min",
        "max" => first == "max",
        "quantile" => first == "sketch" && matches!(strip(params.args.get(1)?), Expr::Literal(..)),
        _ => false,
    };
    allowed.then_some(())
}

/// Accepts a filter that is safe on any table, recording any time bound it sets.
fn check_filter(p: &Expr, needs: &mut Needs) -> Option<()> {
    if let Expr::BinaryExpr(BinaryExpr { left, op, right }) = p {
        if column_name(left) == Some("ts") {
            let v = timestamp_micros(right)?;
            return match op {
                Operator::GtEq => {
                    needs.from = Some(needs.from.map_or(v, |o: i64| o.max(v)));
                    Some(())
                }
                Operator::Lt => {
                    needs.to = Some(needs.to.map_or(v, |o: i64| o.min(v)));
                    Some(())
                }
                _ => None,
            };
        }
        if column_name(right) == Some("ts") {
            return None;
        }
    }
    let columns = p.column_refs();
    columns
        .iter()
        .all(|c| SERIES_COLUMNS.contains(&c.name.as_str()))
        .then_some(())
}

fn timestamp_micros(e: &Expr) -> Option<i64> {
    let Expr::Literal(value, _) = strip(e) else {
        return None;
    };
    match value {
        ScalarValue::TimestampMicrosecond(Some(v), None) => Some(*v),
        ScalarValue::TimestampNanosecond(Some(v), None) if v % 1_000 == 0 => Some(v / 1_000),
        ScalarValue::TimestampMillisecond(Some(v), None) => v.checked_mul(1_000),
        ScalarValue::TimestampSecond(Some(v), None) => v.checked_mul(MICROS),
        _ => None,
    }
}

/// Interval length in microseconds. Months have no fixed length, so they are refused.
fn interval_micros(e: &Expr) -> Option<i64> {
    let Expr::Literal(value, _) = strip(e) else {
        return None;
    };
    match value {
        ScalarValue::IntervalDayTime(Some(v)) => {
            Some(i64::from(v.days) * 86_400 * MICROS + i64::from(v.milliseconds) * 1_000)
        }
        ScalarValue::IntervalMonthDayNano(Some(v))
            if v.months == 0 && v.nanoseconds % 1_000 == 0 =>
        {
            Some(i64::from(v.days) * 86_400 * MICROS + v.nanoseconds / 1_000)
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::Schema;
    use datafusion::logical_expr::builder::LogicalTableSource;

    fn rollup(seconds: i64, covered_until: Option<i64>) -> Rollup {
        Rollup {
            seconds,
            covered_until,
            source: Arc::new(LogicalTableSource::new(Arc::new(Schema::empty()))),
        }
    }

    fn rule(covered: [Option<i64>; 4]) -> ChooseRollup {
        let tables = [10, 60, 600, 3600]
            .into_iter()
            .zip(covered)
            .map(|(s, c)| rollup(s, c))
            .collect();
        ChooseRollup::new(tables, Arc::new(RuleStats::default()))
    }

    const H: i64 = 3600;
    const FULL: [Option<i64>; 4] = [Some(10 * H); 4];

    fn pick(r: &ChooseRollup, needs: Needs) -> Option<i64> {
        r.choose(&needs).map(|t| t.seconds)
    }

    fn range(from: i64, to: i64) -> Needs {
        Needs {
            steps: vec![],
            from: Some(from * MICROS),
            to: Some(to * MICROS),
        }
    }

    #[test]
    fn aligned_range_takes_the_coarsest_table() {
        assert_eq!(pick(&rule(FULL), range(H, 3 * H)), Some(3600));
    }

    #[test]
    fn an_unaligned_end_falls_to_the_table_it_aligns_with() {
        assert_eq!(pick(&rule(FULL), range(H, 3 * H + 600)), Some(600));
        assert_eq!(pick(&rule(FULL), range(H, 3 * H + 60)), Some(60));
        assert_eq!(pick(&rule(FULL), range(H, 3 * H + 10)), None);
    }

    #[test]
    fn a_step_buckets_cannot_divide_rules_out_that_table() {
        let needs = |step: i64| Needs {
            steps: vec![(step * MICROS, 0)],
            from: None,
            to: None,
        };
        assert_eq!(pick(&rule(FULL), needs(7200)), Some(3600));
        assert_eq!(pick(&rule(FULL), needs(1800)), Some(600));
        assert_eq!(pick(&rule(FULL), needs(90)), None);
    }

    #[test]
    fn a_step_origin_off_the_boundary_rules_out_that_table() {
        let needs = Needs {
            steps: vec![(3600 * MICROS, 30 * MICROS)],
            from: None,
            to: None,
        };
        assert_eq!(pick(&rule(FULL), needs), None);
    }

    #[test]
    fn a_table_is_not_used_past_what_it_has_published() {
        let lagging = [Some(10 * H), Some(10 * H), Some(10 * H), Some(5 * H)];
        assert_eq!(pick(&rule(lagging), range(0, 5 * H)), Some(3600));
        assert_eq!(pick(&rule(lagging), range(0, 6 * H)), Some(600));
    }

    #[test]
    fn an_open_ended_query_needs_a_table_as_fresh_as_the_finest() {
        let lagging = [Some(10 * H), Some(10 * H), Some(10 * H), Some(5 * H)];
        let open = || Needs {
            steps: vec![],
            from: None,
            to: None,
        };
        assert_eq!(pick(&rule(lagging), open()), Some(600));
        assert_eq!(pick(&rule(FULL), open()), Some(3600));
    }

    #[test]
    fn an_empty_table_is_never_chosen() {
        assert_eq!(pick(&rule([Some(H), None, None, None]), range(0, H)), None);
    }
}
