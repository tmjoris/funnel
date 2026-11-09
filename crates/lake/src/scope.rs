//! Keeps each SQL session inside one tenant.
//!
//! `GET /query` reads only the caller's tenant, and raw SQL has to give the same guarantee. Asking
//! users to add `WHERE tenant = ...` would not be a guarantee, so this rule adds the filter to every
//! scan before the optimizer runs, wherever the scan sits in the query.

use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{Column, Result, plan_err};
use datafusion::logical_expr::{LogicalPlan, LogicalPlanBuilder, col, lit};
use datafusion::optimizer::AnalyzerRule;

use crate::lake::RESOLUTIONS;

#[derive(Debug)]
pub struct TenantScope {
    tenant: String,
}

impl TenantScope {
    pub fn new(tenant: impl Into<String>) -> Self {
        TenantScope {
            tenant: tenant.into(),
        }
    }
}

impl AnalyzerRule for TenantScope {
    fn name(&self) -> &str {
        "funnel_tenant_scope"
    }

    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
        plan.transform_up(|node| {
            let LogicalPlan::TableScan(scan) = node else {
                return Ok(Transformed::no(node));
            };
            let name = scan.table_name.table();
            // Iceberg metadata tables such as `metrics$snapshots` have no tenant column, and their
            // properties name every tenant.
            if !RESOLUTIONS.iter().any(|r| r.table == name) {
                return plan_err!("table {name} is not available");
            }
            let tenant = Column::new(Some(scan.table_name.clone()), "tenant");
            let filtered = LogicalPlanBuilder::from(LogicalPlan::TableScan(scan))
                .filter(col(tenant).eq(lit(self.tenant.as_str())))?
                .build()?;
            Ok(Transformed::yes(filtered))
        })
        .map(|t| t.data)
    }
}
