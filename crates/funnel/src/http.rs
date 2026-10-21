//! HTTP API: StatsD lines in, counters out.

use std::collections::BTreeMap;
use std::sync::Arc;

use axum::extract::State;
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router as Http};
use serde::Serialize;

use crate::limiter::TenantReport;
use crate::protocol::valid_tenant;
use crate::router::Router;
use crate::shard::Reports;
use crate::stats::Snapshot;
use crate::unix_now;

pub const TENANT_HEADER: &str = "x-funnel-tenant";

#[derive(Clone)]
pub struct AppState {
    pub router: Arc<Router>,
    pub reports: Reports,
    pub default_tenant: Arc<str>,
}

pub fn app(state: AppState) -> Http {
    Http::new()
        .route("/ingest", post(ingest))
        .route("/stats", get(stats))
        .route("/healthz", get(|| async { "ok" }))
        .with_state(state)
}

/// `POST /ingest` with newline-separated StatsD lines. Returns 200 with
/// counts even when some lines were shed, because retrying a partly
/// accepted request would double-count its counters. Only when every line
/// was shed is the answer 503, which is safe to retry.
async fn ingest(State(state): State<AppState>, headers: HeaderMap, body: String) -> Response {
    let tenant = match headers.get(TENANT_HEADER).map(|v| v.to_str()) {
        None => state.default_tenant.clone(),
        Some(Ok(t)) if valid_tenant(t) => Arc::from(t),
        Some(_) => return (StatusCode::BAD_REQUEST, "invalid tenant").into_response(),
    };
    let routed = state.router.route(unix_now(), &tenant, &body);
    let valid = routed.lines - routed.parse_errors;
    if valid > 0 && routed.dropped == valid {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            [(header::RETRY_AFTER, "1")],
            Json(routed),
        )
            .into_response();
    }
    Json(routed).into_response()
}

#[derive(Serialize)]
struct StatsResponse {
    counters: Snapshot,
    open_buckets: usize,
    memory_bytes: usize,
    tenants: Vec<TenantReport>,
}

/// Shards see disjoint series, so per-tenant numbers add up across shards.
async fn stats(State(state): State<AppState>) -> Json<StatsResponse> {
    let mut tenants: BTreeMap<String, TenantReport> = BTreeMap::new();
    let (mut open_buckets, mut memory_bytes) = (0, 0);
    let reports = state.reports.lock().unwrap();
    for shard in reports.iter() {
        open_buckets += shard.open_buckets;
        memory_bytes += shard.memory_bytes;
    }
    for report in reports.iter().flat_map(|s| &s.tenants) {
        let t = tenants
            .entry(report.tenant.clone())
            .or_insert_with(|| TenantReport {
                tenant: report.tenant.clone(),
                allowed_series: 0,
                attempted_series_estimate: 0.0,
                rejected_points: 0,
            });
        t.allowed_series += report.allowed_series;
        t.attempted_series_estimate += report.attempted_series_estimate;
        t.rejected_points += report.rejected_points;
    }
    Json(StatsResponse {
        counters: state.router.stats().snapshot(),
        open_buckets,
        memory_bytes,
        tenants: tenants.into_values().collect(),
    })
}
