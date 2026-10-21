//! Validates lines, picks a shard per series and hands batches to shards
//! without ever blocking: a full shard queue drops the batch and counts it.

use std::sync::Arc;

use crossbeam_channel::{Sender, TrySendError};
use serde::Serialize;

use crate::protocol::{parse_line, take_tenant};
use crate::series::{normalize_tags, series_id};
use crate::shard::Batch;
use crate::stats::Stats;

pub struct Router {
    shards: Vec<Sender<Batch>>,
    stats: Arc<Stats>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize)]
pub struct Routed {
    pub lines: u64,
    pub parse_errors: u64,
    pub dropped: u64,
}

impl Router {
    pub fn new(shards: Vec<Sender<Batch>>, stats: Arc<Stats>) -> Self {
        assert!(!shards.is_empty());
        Router { shards, stats }
    }

    pub fn stats(&self) -> &Stats {
        &self.stats
    }

    /// Shard for a series. Uses the high bits: the shard's hash tables index
    /// by the low bits of the same id, and giving every shard the same low
    /// bits would cluster its tables.
    fn shard_of(&self, id: u64) -> usize {
        (((id >> 32) * self.shards.len() as u64) >> 32) as usize
    }

    /// Routes newline-separated StatsD text received at `now`.
    pub fn route(&self, now: u64, default_tenant: &Arc<str>, text: &str) -> Routed {
        let mut pending: Vec<Option<Batch>> = (0..self.shards.len()).map(|_| None).collect();
        let mut routed = Routed::default();
        for raw in text.lines() {
            if raw.trim().is_empty() {
                continue;
            }
            routed.lines += 1;
            let Ok(mut line) = parse_line(raw) else {
                routed.parse_errors += 1;
                continue;
            };
            let tenant = match take_tenant(&mut line) {
                Ok(t) => t.unwrap_or(default_tenant),
                Err(_) => {
                    routed.parse_errors += 1;
                    continue;
                }
            };
            normalize_tags(&mut line.tags);
            let id = series_id(tenant, line.name, line.kind, &line.tags);
            let batch = pending[self.shard_of(id.0)].get_or_insert_with(|| Batch {
                now,
                default_tenant: default_tenant.clone(),
                text: String::with_capacity(text.len() / self.shards.len() + 64),
                lines: 0,
            });
            batch.text.push_str(raw);
            batch.text.push('\n');
            batch.lines += 1;
        }
        for (shard, batch) in self.shards.iter().zip(pending) {
            let Some(batch) = batch else { continue };
            let lines = batch.lines;
            match shard.try_send(batch) {
                Ok(()) => {}
                Err(TrySendError::Full(_) | TrySendError::Disconnected(_)) => {
                    routed.dropped += lines
                }
            }
        }
        Stats::add(&self.stats.lines_received, routed.lines);
        Stats::add(&self.stats.parse_errors, routed.parse_errors);
        Stats::add(&self.stats.dropped_overload, routed.dropped);
        routed
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn same_series_same_shard_and_full_queue_drops() {
        let (txs, rxs): (Vec<_>, Vec<_>) = (0..4).map(|_| crossbeam_channel::bounded(1)).unzip();
        let router = Router::new(txs, Arc::new(Stats::default()));
        let tenant: Arc<str> = "t".into();
        let r = router.route(0, &tenant, "a:1|c|#x:1\na:2|c|#x:1\nbad\n\nb:1|g\n");
        assert_eq!(
            r,
            Routed {
                lines: 4,
                parse_errors: 1,
                dropped: 0
            }
        );
        let batches: Vec<_> = rxs.iter().filter_map(|rx| rx.try_recv().ok()).collect();
        assert!(
            batches
                .iter()
                .any(|b| b.text.contains("a:1|c|#x:1\na:2|c|#x:1\n"))
        );
        assert_eq!(batches.iter().map(|b| b.lines).sum::<u64>(), 3);

        // Queues hold one batch; the second identical route overflows.
        router.route(0, &tenant, "a:1|c|#x:1\n");
        let r = router.route(0, &tenant, "a:1|c|#x:1\n");
        assert_eq!(r.dropped, 1);
        assert_eq!(router.stats().snapshot().dropped_overload, 1);
    }
}
