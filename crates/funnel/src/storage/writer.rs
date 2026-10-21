//! Writes flushed buckets to disk as zstd-compressed block files.
//!
//! Buckets are held per tenant for one segment (60 seconds by default) and
//! then written as one file per tenant:
//!
//! ```text
//! <dir>/<tenant>/<first start>-<last start>-<unix nanos>.fnlb.zst
//! ```
//!
//! The time range in the name lets a query skip files without opening
//! them. Each file is written under a temporary name, synced and then
//! renamed, so a crash leaves a whole file or none. Buckets still waiting
//! for their segment are lost in a crash; a shorter segment loses less but
//! writes more, smaller files.

use std::collections::HashMap;
use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use crossbeam_channel::{Receiver, RecvTimeoutError};

use super::block;
use crate::aggregator::Bucket;
use crate::stats::Stats;

pub const EXTENSION: &str = "fnlb.zst";

#[derive(Clone, Debug)]
pub struct Config {
    pub dir: PathBuf,
    pub segment: Duration,
    pub zstd_level: i32,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            dir: PathBuf::from("data"),
            segment: Duration::from_secs(60),
            zstd_level: 3,
        }
    }
}

pub fn spawn(config: Config, rx: Receiver<Vec<Bucket>>, stats: Arc<Stats>) -> JoinHandle<()> {
    std::thread::Builder::new()
        .name("writer".into())
        .spawn(move || run(config, rx, &stats))
        .expect("spawn writer thread")
}

fn run(config: Config, rx: Receiver<Vec<Bucket>>, stats: &Stats) {
    let mut pending: HashMap<String, Vec<Bucket>> = HashMap::new();
    let mut next_write = Instant::now() + config.segment;
    loop {
        match rx.recv_deadline(next_write) {
            Ok(buckets) => {
                Stats::add(&stats.buckets_flushed, buckets.len() as u64);
                for b in buckets {
                    pending.entry(b.series.tenant.clone()).or_default().push(b);
                }
            }
            Err(RecvTimeoutError::Timeout) => {}
            Err(RecvTimeoutError::Disconnected) => break,
        }
        if Instant::now() >= next_write {
            next_write += config.segment;
            write_all(&config, &mut pending, stats);
        }
    }
    write_all(&config, &mut pending, stats);
}

fn write_all(config: &Config, pending: &mut HashMap<String, Vec<Bucket>>, stats: &Stats) {
    for (tenant, buckets) in pending.drain() {
        if buckets.is_empty() {
            continue;
        }
        match write_block(config, &tenant, &buckets) {
            Ok(bytes) => {
                Stats::add(&stats.blocks_written, 1);
                Stats::add(&stats.bytes_written, bytes as u64);
            }
            Err(e) => {
                // Holding failed buckets would grow memory without bound
                // while the disk is unusable, so they are dropped and
                // counted.
                tracing::error!(tenant, "writing block: {e}");
                Stats::add(&stats.buckets_lost_write, buckets.len() as u64);
            }
        }
    }
}

/// Writes one block file and returns its size on disk.
pub fn write_block(config: &Config, tenant: &str, buckets: &[Bucket]) -> io::Result<usize> {
    let first = buckets.iter().map(|b| b.start).min().unwrap_or(0);
    let last = buckets.iter().map(|b| b.start).max().unwrap_or(0);
    let compressed = zstd::bulk::compress(&block::encode(tenant, buckets), config.zstd_level)?;
    let dir = config.dir.join(tenant);
    fs::create_dir_all(&dir)?;
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos());
    let name = format!("{first}-{last}-{nanos}.{EXTENSION}");
    let tmp = dir.join(format!(".{name}.tmp"));
    let mut file = fs::File::create(&tmp)?;
    file.write_all(&compressed)?;
    file.sync_all()?;
    fs::rename(&tmp, dir.join(&name))?;
    // Make the rename itself durable.
    fs::File::open(&dir)?.sync_all()?;
    Ok(compressed.len())
}

/// A block file found on disk, with the time range from its name.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlockFile {
    pub path: PathBuf,
    pub first: u64,
    pub last: u64,
}

/// Block files for `tenant` whose buckets may overlap `[from, to]`, oldest
/// first. A missing tenant directory means no files.
pub fn list(dir: &Path, tenant: &str, from: u64, to: u64) -> io::Result<Vec<BlockFile>> {
    let entries = match fs::read_dir(dir.join(tenant)) {
        Ok(e) => e,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(e),
    };
    let mut files = Vec::new();
    for entry in entries {
        let path = entry?.path();
        let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
            continue;
        };
        let Some(stem) = name.strip_suffix(&format!(".{EXTENSION}")) else {
            continue;
        };
        let mut parts = stem.splitn(3, '-').map(str::parse::<u64>);
        let (Some(Ok(first)), Some(Ok(last))) = (parts.next(), parts.next()) else {
            continue;
        };
        if last >= from && first <= to {
            files.push(BlockFile { path, first, last });
        }
    }
    // Names end in write time, so sorting by name within equal ranges keeps
    // write order; sort by first start, then by path.
    files.sort_by(|a, b| (a.first, &a.path).cmp(&(b.first, &b.path)));
    Ok(files)
}

/// Reads and decompresses a block file.
pub fn read(path: &Path) -> io::Result<Vec<u8>> {
    let compressed = fs::read(path)?;
    zstd::stream::decode_all(&compressed[..])
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::aggregator::{Aggregator, Config as AggConfig};
    use crate::protocol::parse_line;

    fn temp_dir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("funnel-test-{name}-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        dir
    }

    fn buckets(tenant: &str, start: u64) -> Vec<Bucket> {
        let mut agg = Aggregator::new(AggConfig::default());
        for i in 0..6 {
            let line = format!("req:1|c|#route:/a|T{}", start + i * 10);
            agg.insert(start + 100, tenant, &mut parse_line(&line).unwrap())
                .unwrap();
        }
        agg.flush_all()
    }

    #[test]
    fn writes_lists_and_reads_back() {
        let dir = temp_dir("write");
        let config = Config {
            dir: dir.clone(),
            ..Config::default()
        };
        write_block(&config, "acme", &buckets("acme", 1000)).unwrap();
        write_block(&config, "acme", &buckets("acme", 2000)).unwrap();

        let all = list(&dir, "acme", 0, u64::MAX).unwrap();
        assert_eq!(all.len(), 2);
        assert_eq!((all[0].first, all[0].last), (1000, 1050));
        assert_eq!(list(&dir, "acme", 1060, 1999).unwrap().len(), 0);
        assert_eq!(list(&dir, "acme", 1050, 2000).unwrap().len(), 2);
        assert!(list(&dir, "nobody", 0, u64::MAX).unwrap().is_empty());

        let bytes = read(&all[1].path).unwrap();
        let r = block::Reader::open(&bytes).unwrap();
        assert_eq!(r.tenant, "acme");
        assert_eq!(r.series(&r.index[0]).unwrap().len(), 6);
        // No temporary files are left behind.
        let names: Vec<_> = fs::read_dir(dir.join("acme")).unwrap().collect();
        assert_eq!(names.len(), 2);
        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn thread_writes_per_tenant_and_flushes_on_close() {
        let dir = temp_dir("thread");
        let (tx, rx) = crossbeam_channel::unbounded();
        let stats = Arc::new(Stats::default());
        let handle = spawn(
            Config {
                dir: dir.clone(),
                segment: Duration::from_secs(3600),
                ..Config::default()
            },
            rx,
            stats.clone(),
        );
        tx.send(buckets("acme", 1000)).unwrap();
        tx.send(buckets("globex", 1000)).unwrap();
        drop(tx);
        handle.join().unwrap();
        let s = stats.snapshot();
        assert_eq!((s.buckets_flushed, s.blocks_written), (12, 2));
        assert!(s.bytes_written > 0);
        assert_eq!(list(&dir, "acme", 0, u64::MAX).unwrap().len(), 1);
        assert_eq!(list(&dir, "globex", 0, u64::MAX).unwrap().len(), 1);
        fs::remove_dir_all(&dir).unwrap();
    }
}
