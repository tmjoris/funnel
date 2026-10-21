//! Per-tenant cap on distinct series per time window.
//!
//! The decision needs to know whether a given series is new, which a
//! HyperLogLog cannot answer: it only estimates how many distinct items it
//! has seen. Each tenant therefore keeps the exact set of series ids it was
//! allowed in the current window (8 bytes per series, bounded by the limit),
//! plus a 4 KiB HyperLogLog of every series it attempted, which reports how
//! far over the limit a tenant is pushing.
//!
//! Every shard has its own limiter, and the router sends each series to one
//! shard, so a tenant's distinct count is the sum of its per-shard sets.
//! Known series are checked against the local set without locking. Only a
//! new series takes the tenant's lock in the shared [`Budgets`] to claim one
//! unit of the limit; after warm-up that is rare.
//!
//! Windows are fixed (hourly by default). At a window boundary every tenant
//! starts again from an empty set, the same trade-off as a fixed-window rate
//! limiter: simple and cheap, at the cost of letting a tenant briefly
//! exceed the limit right after the boundary.

use std::collections::{HashMap, HashSet};
use std::hash::{BuildHasherDefault, Hasher};
use std::sync::{Arc, Mutex};

use crate::hll::HyperLogLog;
use crate::series::SeriesId;

#[derive(Clone, Copy, Debug)]
pub struct Config {
    pub max_series: usize,
    pub window_secs: u64,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            max_series: 10_000,
            window_secs: 3600,
        }
    }
}

/// Series ids are already xxh3 hashes, so hashing them again is waste.
#[derive(Default)]
pub struct IdHasher(u64);

impl Hasher for IdHasher {
    fn finish(&self) -> u64 {
        self.0
    }

    fn write(&mut self, _: &[u8]) {
        unreachable!("IdHasher only hashes u64 ids");
    }

    fn write_u64(&mut self, n: u64) {
        self.0 = n;
    }
}

pub type IdSet = HashSet<u64, BuildHasherDefault<IdHasher>>;

/// Admitted series per tenant, shared by every shard's limiter.
#[derive(Default)]
pub struct Budgets {
    tenants: Mutex<HashMap<String, Arc<Mutex<Budget>>>>,
}

#[derive(Default)]
struct Budget {
    window: u64,
    used: usize,
}

impl Budgets {
    fn get(&self, tenant: &str) -> Arc<Mutex<Budget>> {
        let mut tenants = self.tenants.lock().unwrap();
        match tenants.get(tenant) {
            Some(b) => b.clone(),
            None => tenants.entry(tenant.to_owned()).or_default().clone(),
        }
    }
}

struct Tenant {
    window: u64,
    allowed: IdSet,
    attempted: HyperLogLog,
    rejected_points: u64,
    budget: Arc<Mutex<Budget>>,
}

impl Tenant {
    fn new(window: u64, budget: Arc<Mutex<Budget>>) -> Self {
        Tenant {
            window,
            allowed: IdSet::default(),
            attempted: HyperLogLog::default(),
            rejected_points: 0,
            budget,
        }
    }
}

#[derive(Clone, Debug, PartialEq, serde::Serialize)]
pub struct TenantReport {
    pub tenant: String,
    pub allowed_series: usize,
    pub attempted_series_estimate: f64,
    pub rejected_points: u64,
}

pub struct Limiter {
    config: Config,
    budgets: Arc<Budgets>,
    tenants: HashMap<String, Tenant>,
}

impl Limiter {
    /// A limiter for one shard; all shards must share `budgets`.
    pub fn new(config: Config, budgets: Arc<Budgets>) -> Self {
        Limiter {
            config,
            budgets,
            tenants: HashMap::new(),
        }
    }

    /// Whether a point for series `id` of `tenant` may be stored at `now`.
    pub fn check(&mut self, now: u64, tenant: &str, id: SeriesId) -> bool {
        let window = now - now % self.config.window_secs;
        let state = match self.tenants.get_mut(tenant) {
            Some(state) => state,
            None => {
                let budget = self.budgets.get(tenant);
                self.tenants
                    .entry(tenant.to_owned())
                    .or_insert_with(|| Tenant::new(window, budget))
            }
        };
        if state.window != window {
            *state = Tenant::new(window, state.budget.clone());
        }
        state.attempted.insert(id.0);
        if state.allowed.contains(&id.0) {
            return true;
        }
        let admitted = {
            let mut budget = state.budget.lock().unwrap();
            if window > budget.window {
                budget.window = window;
                budget.used = 0;
            }
            if window < budget.window {
                // A batch stamped before a boundary another shard has
                // already crossed; the old window's budget is gone.
                true
            } else if budget.used < self.config.max_series {
                budget.used += 1;
                true
            } else {
                false
            }
        };
        if admitted {
            state.allowed.insert(id.0);
        } else {
            state.rejected_points += 1;
        }
        admitted
    }

    pub fn report(&self) -> Vec<TenantReport> {
        let mut out: Vec<_> = self
            .tenants
            .iter()
            .map(|(tenant, s)| TenantReport {
                tenant: tenant.clone(),
                allowed_series: s.allowed.len(),
                attempted_series_estimate: s.attempted.estimate(),
                rejected_points: s.rejected_points,
            })
            .collect();
        out.sort_by(|a, b| a.tenant.cmp(&b.tenant));
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(max_series: usize) -> Config {
        Config {
            max_series,
            window_secs: 3600,
        }
    }

    fn limiter(max_series: usize) -> Limiter {
        Limiter::new(config(max_series), Arc::default())
    }

    #[test]
    fn shards_share_one_exact_budget() {
        let budgets = Arc::new(Budgets::default());
        let mut a = Limiter::new(config(3), budgets.clone());
        let mut b = Limiter::new(config(3), budgets);
        assert!(a.check(0, "t", id(1)));
        assert!(a.check(0, "t", id(2)));
        assert!(b.check(0, "t", id(3)));
        assert!(!b.check(0, "t", id(4)));
        assert!(!a.check(0, "t", id(5)));
        // Known series stay allowed on their own shard.
        assert!(a.check(1, "t", id(1)));
        assert!(b.check(1, "t", id(3)));
    }

    #[test]
    fn window_boundary_across_shards() {
        let budgets = Arc::new(Budgets::default());
        let mut a = Limiter::new(config(1), budgets.clone());
        let mut b = Limiter::new(config(1), budgets);
        assert!(a.check(3599, "t", id(1)));
        assert!(!b.check(3599, "t", id(2)));
        assert!(a.check(3600, "t", id(3)));
        assert!(!b.check(3600, "t", id(4)));
        // A late batch from the old window is not held to the spent budget.
        assert!(b.check(3599, "t", id(5)));
    }

    fn id(i: u64) -> SeriesId {
        SeriesId(xxhash_rust::xxh3::xxh3_64(&i.to_le_bytes()))
    }

    #[test]
    fn existing_series_keep_flowing_past_the_limit() {
        let mut l = limiter(2);
        assert!(l.check(0, "a", id(1)));
        assert!(l.check(0, "a", id(2)));
        assert!(!l.check(0, "a", id(3)));
        assert!(l.check(1, "a", id(1)));
        assert!(l.check(2, "a", id(2)));
        assert!(!l.check(3, "a", id(3)));
    }

    #[test]
    fn tenants_are_independent() {
        let mut l = limiter(1);
        assert!(l.check(0, "a", id(1)));
        assert!(l.check(0, "b", id(2)));
        assert!(!l.check(0, "a", id(2)));
    }

    #[test]
    fn window_boundary_resets() {
        let mut l = limiter(1);
        assert!(l.check(3599, "a", id(1)));
        assert!(!l.check(3599, "a", id(2)));
        assert!(l.check(3600, "a", id(2)));
    }

    #[test]
    fn report_estimates_attempted_series() {
        let mut l = limiter(1_000);
        for i in 0..50_000 {
            l.check(0, "noisy", id(i));
        }
        let r = &l.report()[0];
        assert_eq!(r.allowed_series, 1_000);
        assert_eq!(r.rejected_points, 49_000);
        assert!((r.attempted_series_estimate - 50_000.0).abs() < 2_500.0);
    }
}
