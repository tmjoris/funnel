pub mod aggregator;
pub mod hll;
pub mod http;
pub mod limiter;
pub mod protocol;
pub mod router;
pub mod series;
pub mod shard;
pub mod stats;
pub mod storage;
pub mod udp;

/// Wall-clock unix seconds; bucket timestamps are wall-clock by definition.
pub fn unix_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_secs())
}
