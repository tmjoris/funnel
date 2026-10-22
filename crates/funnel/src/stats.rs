//! Process-wide counters. Every line that enters Funnel ends up in exactly
//! one of: accepted, a parse error, dropped for overload, or one of the
//! rejection counters.

use std::sync::atomic::{AtomicU64, Ordering};

use serde::Serialize;

macro_rules! counters {
    ($($name:ident),* $(,)?) => {
        #[derive(Default)]
        pub struct Stats {
            $(pub $name: AtomicU64,)*
        }

        #[derive(Clone, Debug, Default, Serialize, PartialEq, Eq)]
        pub struct Snapshot {
            $(pub $name: u64,)*
        }

        impl Stats {
            pub fn snapshot(&self) -> Snapshot {
                Snapshot {
                    $($name: self.$name.load(Ordering::Relaxed),)*
                }
            }
        }
    };
}

counters!(
    packets_received,
    lines_received,
    parse_errors,
    dropped_overload,
    rejected_cardinality,
    rejected_too_old,
    rejected_future,
    rejected_overflow,
    lines_accepted,
    buckets_flushed,
    blocks_written,
    bytes_written,
    buckets_lost_write,
    files_compacted,
    files_expired,
);

impl Stats {
    #[inline]
    pub fn add(counter: &AtomicU64, n: u64) {
        if n > 0 {
            counter.fetch_add(n, Ordering::Relaxed);
        }
    }
}
