//! HTTP API: StatsD lines in, counters out.

use std::collections::BTreeMap;
use std::sync::Arc;

use axum::extract::{Query as Params, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router as Http};
use serde::{Deserialize, Serialize};

use crate::limiter::TenantReport;
use crate::protocol::valid_tenant;
use crate::query::{self, Agg, Query, QueryError};
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
    pub data_dir: Arc<std::path::PathBuf>,
}

pub fn app(state: AppState) -> Http {
    Http::new()
        .route("/ingest", post(ingest))
        .route("/stats", get(stats))
        .route("/query", get(query_handler))
        .route("/healthz", get(|| async { "ok" }))
        .with_state(state)
}

/// The tenant from the header, or the default; `None` if it is invalid.
fn tenant_of(state: &AppState, headers: &HeaderMap) -> Option<Arc<str>> {
    match headers.get(TENANT_HEADER).map(|v| v.to_str()) {
        None => Some(state.default_tenant.clone()),
        Some(Ok(t)) if valid_tenant(t) => Some(Arc::from(t)),
        Some(_) => None,
    }
}

/// `POST /ingest` with newline-separated StatsD lines. Returns 200 with
/// counts even when some lines were shed, because retrying a partly
/// accepted request would double-count its counters. Only when every line
/// was shed is the answer 503, which is safe to retry.
async fn ingest(State(state): State<AppState>, headers: HeaderMap, body: String) -> Response {
    let Some(tenant) = tenant_of(&state, &headers) else {
        return (StatusCode::BAD_REQUEST, "invalid tenant").into_response();
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

#[derive(Deserialize)]
struct QueryParams {
    metric: String,
    /// Unix seconds; defaults to one hour before `to`.
    from: Option<u64>,
    /// Unix seconds; defaults to now.
    to: Option<u64>,
    /// Tag filters, `key:value,key:value`.
    #[serde(rename = "where")]
    filters: Option<String>,
    /// Tags to group by, comma separated; empty merges every series.
    group_by: Option<String>,
    step: Option<u64>,
    agg: Option<String>,
}

/// `GET /query?metric=..&from=..&to=..&where=k:v&group_by=k&step=60&agg=p99`
async fn query_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Params(p): Params<QueryParams>,
) -> Response {
    let Some(tenant) = tenant_of(&state, &headers) else {
        return (StatusCode::BAD_REQUEST, "invalid tenant").into_response();
    };
    let bad = |m: &str| (StatusCode::BAD_REQUEST, m.to_owned()).into_response();
    let to = p.to.unwrap_or_else(unix_now);
    let filters = match p.filters.as_deref().filter(|f| !f.is_empty()) {
        None => Vec::new(),
        Some(f) => match f
            .split(',')
            .map(|kv| {
                kv.split_once(':')
                    .map(|(k, v)| (k.to_owned(), v.to_owned()))
            })
            .collect::<Option<Vec<_>>>()
        {
            Some(v) => v,
            None => return bad("where must be key:value pairs separated by commas"),
        },
    };
    let agg = match p.agg.as_deref().map(Agg::parse) {
        None => None,
        Some(Some(a)) => Some(a),
        Some(None) => return bad("unknown agg"),
    };
    let q = Query {
        tenant: tenant.to_string(),
        metric: p.metric,
        from: p.from.unwrap_or(to.saturating_sub(3600)),
        to,
        filters,
        group_by: p.group_by.map(|g| {
            g.split(',')
                .filter(|t| !t.is_empty())
                .map(str::to_owned)
                .collect()
        }),
        step: p.step.unwrap_or(10),
        agg,
    };
    let dir = state.data_dir.clone();
    match tokio::task::spawn_blocking(move || query::run(&dir, &q)).await {
        Ok(Ok(result)) => Json(result).into_response(),
        Ok(Err(QueryError::BadRequest(m))) => bad(&m),
        Ok(Err(e @ QueryError::TooLarge)) => {
            (StatusCode::PAYLOAD_TOO_LARGE, e.to_string()).into_response()
        }
        Ok(Err(e)) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}
