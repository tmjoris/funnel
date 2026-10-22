use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use clap::Parser;
use funnel::aggregator::Bucket;
use funnel::http::{self, AppState};
use funnel::router::Router;
use funnel::stats::Stats;
use funnel::storage::store::{self, Store};
use funnel::storage::writer;
use funnel::{aggregator, limiter, shard, udp};

#[derive(Parser, Debug)]
#[command(version, about)]
struct Args {
    #[arg(long, env = "FUNNEL_UDP", default_value = "0.0.0.0:8125")]
    udp: SocketAddr,
    #[arg(long, env = "FUNNEL_HTTP", default_value = "0.0.0.0:8080")]
    http: SocketAddr,
    /// Aggregator shards; defaults to the number of CPUs.
    #[arg(long, env = "FUNNEL_SHARDS")]
    shards: Option<usize>,
    #[arg(long, env = "FUNNEL_UDP_THREADS", default_value_t = 2)]
    udp_threads: usize,
    /// Batches each shard may queue before new ones are dropped.
    #[arg(long, env = "FUNNEL_QUEUE", default_value_t = 256)]
    queue: usize,
    #[arg(long, env = "FUNNEL_MAX_SERIES", default_value_t = 10_000)]
    max_series_per_tenant: usize,
    /// Memory for open buckets, split evenly across shards.
    #[arg(long, env = "FUNNEL_MAX_MEMORY_MB", default_value_t = 256)]
    max_memory_mb: usize,
    #[arg(long, env = "FUNNEL_TENANT", default_value = "default")]
    default_tenant: String,
    /// Directory for block files, one subdirectory per tenant.
    #[arg(long, env = "FUNNEL_DATA_DIR", default_value = "data")]
    data_dir: std::path::PathBuf,
    /// Seconds of rollups held in memory before they are written; a crash
    /// loses at most this much.
    #[arg(long, env = "FUNNEL_SEGMENT_SECS", default_value_t = 5)]
    segment_secs: u64,
    /// Seconds between early flushes of open windows as partial buckets;
    /// 0 waits for each window to close. Bounds crash loss and query lag.
    #[arg(long, env = "FUNNEL_CHECKPOINT_SECS", default_value_t = 5)]
    checkpoint_secs: u64,
    /// Seconds of data compaction merges into one file.
    #[arg(long, env = "FUNNEL_SPAN_SECS", default_value_t = 600)]
    span_secs: u64,
    /// Seconds after a span ends before it is compacted, so buckets still in
    /// flight have landed.
    #[arg(long, env = "FUNNEL_SETTLE_SECS", default_value_t = 120)]
    settle_secs: u64,
    /// Delete data older than this many days; keep everything if unset.
    #[arg(long, env = "FUNNEL_RETENTION_DAYS")]
    retention_days: Option<u64>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();
    let args = Args::parse();
    anyhow::ensure!(
        funnel::protocol::valid_tenant(&args.default_tenant),
        "invalid default tenant"
    );
    let shards = args
        .shards
        .unwrap_or_else(|| std::thread::available_parallelism().map_or(1, |n| n.get()));

    let stats = Arc::new(Stats::default());
    let reports: shard::Reports = Arc::new(Mutex::new(vec![Default::default(); shards]));
    let (bucket_tx, bucket_rx) = crossbeam_channel::bounded::<Vec<Bucket>>(64);
    let config = shard::Config {
        aggregator: aggregator::Config {
            max_bytes: (args.max_memory_mb << 20) / shards,
            ..aggregator::Config::default()
        },
        limiter: limiter::Config {
            max_series: args.max_series_per_tenant,
            ..limiter::Config::default()
        },
        flush_every: Duration::from_secs(1),
        checkpoint_every: (args.checkpoint_secs > 0)
            .then(|| Duration::from_secs(args.checkpoint_secs)),
    };
    let budgets = Arc::new(limiter::Budgets::default());
    let shards: Vec<_> = (0..shards)
        .map(|i| {
            shard::spawn(
                i,
                config,
                args.queue,
                budgets.clone(),
                bucket_tx.clone(),
                stats.clone(),
                reports.clone(),
            )
        })
        .collect();
    drop(bucket_tx);

    let store = Arc::new(Store::open(store::Config {
        dir: args.data_dir.clone(),
        span_secs: args.span_secs,
        settle_secs: args.settle_secs,
        retention_secs: args.retention_days.map(|d| d * 86_400),
        ..store::Config::default()
    })?);
    let pending = writer::Pending::default();
    let writer = writer::spawn(
        store.clone(),
        Duration::from_secs(args.segment_secs),
        bucket_rx,
        pending.clone(),
        stats.clone(),
    );

    let router = Arc::new(Router::new(
        shards.iter().map(|s| s.batches.clone()).collect(),
        stats.clone(),
    ));
    let tenant: Arc<str> = args.default_tenant.into();
    let stop = Arc::new(AtomicBool::new(false));
    let compactor = {
        let (store, stop, stats) = (store.clone(), stop.clone(), stats.clone());
        std::thread::Builder::new()
            .name("compactor".into())
            .spawn(move || {
                let mut last = std::time::Instant::now();
                while !stop.load(Ordering::Relaxed) {
                    std::thread::sleep(Duration::from_millis(500));
                    if last.elapsed() < Duration::from_secs(30) {
                        continue;
                    }
                    last = std::time::Instant::now();
                    match store.compact(funnel::unix_now()) {
                        Ok(s) => {
                            Stats::add(&stats.files_compacted, s.inputs as u64);
                            Stats::add(&stats.files_expired, s.expired as u64);
                        }
                        Err(e) => tracing::error!("compaction: {e}"),
                    }
                }
            })?
    };
    let (udp_addr, udp_threads) = udp::spawn(
        args.udp,
        args.udp_threads,
        router.clone(),
        tenant.clone(),
        stop.clone(),
    )?;

    let listener = tokio::net::TcpListener::bind(args.http).await?;
    tracing::info!(udp = %udp_addr, http = %listener.local_addr()?, shards = shards.len(), "funnel listening");
    let app = http::app(AppState {
        router: router.clone(),
        reports,
        default_tenant: tenant,
        store: store.clone(),
        pending: pending.clone(),
    });
    axum::serve(listener, app)
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await?;

    // Shutdown: stop receivers, then drop every router handle so the shards
    // see their queues close, flush everything and exit.
    tracing::info!("shutting down");
    stop.store(true, Ordering::Relaxed);
    for t in udp_threads {
        let _ = t.join();
    }
    drop(router);
    for s in shards {
        drop(s.batches);
        let _ = s.handle.join();
    }
    let _ = writer.join();
    let _ = compactor.join();
    tracing::info!(stats = ?stats.snapshot(), "stopped");
    Ok(())
}
