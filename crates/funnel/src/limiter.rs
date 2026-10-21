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
//! Windows are fixed (hourly by default). A plain fixed window would let a
//! tenant that is over its limit keep whichever series happen to arrive
//! first after each boundary, so long-lived series could lose their place
//! to new ones and the allowed set would change every hour. Instead, the
//! series a shard admitted in the previous window keep a reserved slot in
//! the new one: new series only get the slots that are neither used nor
//! reserved, and a reservation lapses if its series does not return within
//! the window. Shards roll over on their flush tick, so for up to a second
//! after a boundary some shards may not have added their reservations yet.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crate::hll::HyperLogLog;
use crate::series::{IdSet, SeriesId};

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

/// Admitted series per tenant, shared by every shard's limiter.
#[derive(Default)]
pub struct Budgets {
    tenants: Mutex<HashMap<String, Arc<Mutex<Budget>>>>,
}

#[derive(Default)]
struct Budget {
    window: u64,
    used: usize,
    /// Series admitted in the previous window that have not returned yet.
    reserved: usize,
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
    /// Window of the last check, to forget tenants that stopped sending.
    seen: u64,
    allowed: IdSet,
    /// Admitted in the previous window and not yet seen in this one.
    previous: IdSet,
    attempted: HyperLogLog,
    rejected_points: u64,
    budget: Arc<Mutex<Budget>>,
}

impl Tenant {
    fn new(window: u64, budget: Arc<Mutex<Budget>>) -> Self {
        Tenant {
            window,
            seen: window,
            allowed: IdSet::default(),
            previous: IdSet::default(),
            attempted: HyperLogLog::default(),
            rejected_points: 0,
            budget,
        }
    }

    /// Moves to a later window, carrying this window's series over as
    /// reservations if the new window directly follows.
    fn roll(&mut self, window: u64, window_secs: u64) {
        let allowed = std::mem::take(&mut self.allowed);
        self.previous = if window == self.window + window_secs {
            allowed
        } else {
            IdSet::default()
        };
        self.window = window;
        self.attempted = HyperLogLog::default();
        self.rejected_points = 0;
        let mut budget = self.budget.lock().unwrap();
        if window > budget.window {
            *budget = Budget {
                window,
                used: 0,
                reserved: 0,
            };
        }
        if window == budget.window {
            budget.reserved += self.previous.len();
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
        // A batch stamped before the current window (it queued across a
        // boundary) is judged against the current window; never roll back.
        if window > state.window {
            state.roll(window, self.config.window_secs);
        }
        state.seen = state.window;
        state.attempted.insert(id.0);
        if state.allowed.contains(&id.0) {
            return true;
        }
        let admitted = {
            let mut budget = state.budget.lock().unwrap();
            if state.window < budget.window {
                // Another shard has crossed a boundary this one has not
                // reached on its tick yet; the old budget is gone.
                true
            } else if state.previous.remove(&id.0) {
                budget.reserved = budget.reserved.saturating_sub(1);
                budget.used += 1;
                true
            } else if budget.used + budget.reserved < self.config.max_series {
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

    /// Rolls every tenant into the window containing `now` and forgets
    /// tenants idle for two windows. Shards call this on every flush tick.
    pub fn tick(&mut self, now: u64) {
        let secs = self.config.window_secs;
        let window = now - now % secs;
        self.tenants.retain(|_, t| t.seen + 2 * secs > window);
        for t in self.tenants.values_mut() {
            if window > t.window {
                t.roll(window, secs);
            }
        }
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

    fn id(i: u64) -> SeriesId {
        SeriesId(xxhash_rust::xxh3::xxh3_64(&i.to_le_bytes()))
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
    fn returning_series_keep_their_place() {
        let mut l = limiter(2);
        assert!(l.check(0, "t", id(1)));
        assert!(l.check(0, "t", id(2)));
        // New hour: a newcomer arriving first cannot take a returning
        // series' slot.
        assert!(!l.check(3600, "t", id(3)));
        assert!(l.check(3601, "t", id(1)));
        assert!(l.check(3602, "t", id(2)));
        assert!(!l.check(3603, "t", id(3)));
    }

    #[test]
    fn reservations_lapse_after_a_window_without_the_series() {
        let mut l = limiter(2);
        assert!(l.check(0, "t", id(1)));
        assert!(l.check(0, "t", id(2)));
        assert!(l.check(3600, "t", id(1)));
        assert!(!l.check(3600, "t", id(3)));
        // Series 2 never came back, so the next window frees its slot.
        assert!(l.check(7200, "t", id(3)));
        assert!(l.check(7200, "t", id(1)));
        assert!(!l.check(7200, "t", id(4)));
    }

    #[test]
    fn a_gap_of_more_than_one_window_reserves_nothing() {
        let mut l = limiter(1);
        assert!(l.check(0, "t", id(1)));
        assert!(l.check(7200, "t", id(2)));
    }

    #[test]
    fn late_batch_does_not_roll_back() {
        let mut l = limiter(1);
        assert!(l.check(3600, "t", id(1)));
        assert!(!l.check(3599, "t", id(2)));
        assert!(l.check(3600, "t", id(1)));
    }

    #[test]
    fn tick_reserves_across_shards_before_traffic() {
        let budgets = Arc::new(Budgets::default());
        let mut a = Limiter::new(config(2), budgets.clone());
        let mut b = Limiter::new(config(2), budgets);
        assert!(a.check(0, "t", id(1)));
        assert!(b.check(0, "t", id(2)));
        a.tick(3600);
        b.tick(3600);
        assert!(!a.check(3600, "t", id(3)));
        assert!(!b.check(3600, "t", id(4)));
        assert!(b.check(3600, "t", id(2)));
    }

    #[test]
    fn idle_tenants_are_forgotten() {
        let mut l = limiter(1);
        assert!(l.check(0, "t", id(1)));
        l.tick(3600);
        assert_eq!(l.report().len(), 1);
        l.tick(7200);
        assert!(l.report().is_empty());
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
    fn full_tenant_keeps_old_series_across_the_boundary() {
        let mut l = limiter(1);
        assert!(l.check(3599, "a", id(1)));
        assert!(!l.check(3599, "a", id(2)));
        assert!(!l.check(3600, "a", id(2)));
        assert!(l.check(3600, "a", id(1)));
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
