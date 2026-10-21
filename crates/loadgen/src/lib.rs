//! A deterministic StatsD workload shaped like a small SaaS backend.
//!
//! Services run on several hosts and serve requests on a handful of routes.
//! Every request emits a request counter, its duration, cache hit or miss
//! counters and zero to three database query durations. Every host reports
//! its queue depth each second and memory and CPU every ten seconds. Request
//! durations are lognormal with an occasional slow outlier, so their
//! distributions have the long right tail that real latencies have.
//!
//! The same seed always produces the same lines, so storage and query
//! benchmarks can be compared run to run.

use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use rand_distr::{Distribution, Exp, LogNormal, Normal};

#[derive(Clone, Debug)]
pub struct Config {
    pub services: usize,
    pub hosts_per_service: usize,
    pub routes: usize,
    /// Mean requests per second per host.
    pub requests_per_host: f64,
    pub start: u64,
    pub seed: u64,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            services: 20,
            hosts_per_service: 5,
            routes: 10,
            requests_per_host: 50.0,
            start: 1_761_000_000,
            seed: 7,
        }
    }
}

const STATUSES: [(&str, f64); 3] = [("2xx", 0.95), ("4xx", 0.04), ("5xx", 0.01)];
const QUERIES: [&str; 5] = [
    "select_user",
    "select_items",
    "insert_event",
    "update_cart",
    "search",
];

struct Host {
    service: usize,
    host: usize,
    queue: f64,
    memory_mb: f64,
}

pub struct Workload {
    config: Config,
    rng: StdRng,
    hosts: Vec<Host>,
    second: u64,
    /// Per-route median latency in ms, so routes differ from each other.
    route_median: Vec<f64>,
}

impl Workload {
    pub fn new(config: Config) -> Self {
        let mut rng = StdRng::seed_from_u64(config.seed);
        let hosts = (0..config.services)
            .flat_map(|service| (0..config.hosts_per_service).map(move |host| (service, host)))
            .map(|(service, host)| Host {
                service,
                host,
                queue: 10.0,
                memory_mb: 512.0,
            })
            .collect();
        let route_median = (0..config.routes)
            .map(|_| rng.random_range(5.0..120.0))
            .collect();
        Workload {
            config,
            rng,
            hosts,
            second: 0,
            route_median,
        }
    }

    /// Timestamp of the next second `next_second` will produce.
    pub fn now(&self) -> u64 {
        self.config.start + self.second
    }

    /// Appends one simulated second of lines to `out` and returns its
    /// timestamp. Lines carry no `T` timestamp; senders stamp them on
    /// arrival, as StatsD clients do.
    pub fn next_second(&mut self, out: &mut Vec<String>) -> u64 {
        let now = self.now();
        let poisson_gap = Exp::new(self.config.requests_per_host).unwrap();
        for h in 0..self.hosts.len() {
            let (service, host) = (self.hosts[h].service, self.hosts[h].host);
            let base = format!("service:svc{service},host:h{service}-{host}");

            // Requests arrive as a Poisson process within the second.
            let mut t = poisson_gap.sample(&mut self.rng);
            while t < 1.0 {
                t += poisson_gap.sample(&mut self.rng);
                let route = self.rng.random_range(0..self.config.routes);
                let roll: f64 = self.rng.random();
                let status = if roll < STATUSES[0].1 {
                    STATUSES[0].0
                } else if roll < STATUSES[0].1 + STATUSES[1].1 {
                    STATUSES[1].0
                } else {
                    STATUSES[2].0
                };
                let tags = format!("{base},route:/r{route},status:{status}");
                out.push(format!("http.requests:1|c|#{tags}"));

                let median = self.route_median[route];
                let mut ms = LogNormal::new(median.ln(), 0.5)
                    .unwrap()
                    .sample(&mut self.rng);
                if self.rng.random_bool(0.01) {
                    ms *= self.rng.random_range(5.0..30.0);
                }
                out.push(format!("http.duration:{:.3}|ms|#{tags}", ms));

                let hit = self.rng.random_bool(0.8);
                let cache = if hit { "cache.hits" } else { "cache.misses" };
                out.push(format!("{cache}:1|c|#{base}"));

                for _ in 0..self.rng.random_range(0..=3) {
                    let q = QUERIES[self.rng.random_range(0..QUERIES.len())];
                    let qms = LogNormal::new(1.0, 0.8).unwrap().sample(&mut self.rng);
                    out.push(format!("db.query.duration:{qms:.3}|ms|#{base},query:{q}"));
                }
            }

            let step = Normal::new(0.0, 2.0).unwrap().sample(&mut self.rng);
            let state = &mut self.hosts[h];
            state.queue = (state.queue + step).max(0.0).round();
            out.push(format!("queue.depth:{}|g|#{base}", state.queue));
            if self.second.is_multiple_of(10) {
                state.memory_mb += self.rng.random_range(-4.0..5.0);
                let mem = state.memory_mb.round();
                let cpu: f64 = self.rng.random_range(5.0..95.0);
                out.push(format!("process.memory_mb:{mem}|g|#{base}"));
                out.push(format!("process.cpu_percent:{cpu:.1}|g|#{base}"));
            }
        }
        self.second += 1;
        now
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deterministic_for_a_seed() {
        let mut a = Workload::new(Config::default());
        let mut b = Workload::new(Config::default());
        let (mut x, mut y) = (Vec::new(), Vec::new());
        for _ in 0..3 {
            a.next_second(&mut x);
            b.next_second(&mut y);
        }
        assert_eq!(x, y);
    }

    #[test]
    fn rate_is_about_what_was_asked() {
        let config = Config {
            services: 2,
            hosts_per_service: 2,
            requests_per_host: 100.0,
            ..Config::default()
        };
        let mut w = Workload::new(config);
        let mut lines = Vec::new();
        for _ in 0..10 {
            w.next_second(&mut lines);
        }
        let requests = lines
            .iter()
            .filter(|l| l.starts_with("http.requests"))
            .count();
        // 4 hosts * 100 req/s * 10 s, give or take Poisson noise.
        assert!((3600..4400).contains(&requests), "{requests}");
    }
}
