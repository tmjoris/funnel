//! Block files on disk, one directory per tenant, tracked by a manifest.
//!
//! The design follows VictoriaMetrics' parts and Prometheus' blocks:
//!
//! - New buckets are written often (every 10 seconds) as small level-0
//!   files, so a crash loses little and nothing needs replaying.
//! - A per-tenant `MANIFEST` lists the live files. A file counts only once
//!   the manifest names it, and the manifest is replaced atomically
//!   (write, fsync, rename), so after a crash the set of files is either the
//!   old one or the new one. Files the manifest does not list are leftovers
//!   and are deleted at startup.
//! - Compaction merges closed time spans (10 minutes by default) into one
//!   level-1 file per span, merging partial buckets of the same series and
//!   window as it goes. It adds its outputs and removes its inputs in a
//!   single manifest update, so a query never sees both. Inputs are deleted
//!   a minute later rather than at once, so a query still reading them is
//!   not cut off.
//! - Compaction streams one series at a time across its inputs, so memory
//!   holds the decompressed inputs and output of one span, not all of a
//!   tenant's data.

use std::collections::{BTreeSet, HashMap};
use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use super::block::{self, Builder, Reader};
use crate::aggregator::{Bucket, Value};
use crate::protocol::{Kind, valid_tenant};

pub const EXTENSION: &str = "fnlb.zst";
const MANIFEST: &str = "MANIFEST";

#[derive(Clone, Debug)]
pub struct Config {
    pub dir: PathBuf,
    pub zstd_level: i32,
    /// Length of the spans compaction merges into one file.
    pub span_secs: u64,
    /// How long after a span ends before it is compacted, so that buckets
    /// still in flight (flush delay, writer segment) have landed.
    pub settle_secs: u64,
    /// Files whose newest bucket is older than this are deleted.
    pub retention_secs: Option<u64>,
    /// Delay before files removed from the manifest are deleted.
    pub delete_after: Duration,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            dir: PathBuf::from("data"),
            zstd_level: 3,
            span_secs: 600,
            settle_secs: 120,
            retention_secs: None,
            delete_after: Duration::from_secs(60),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileMeta {
    pub name: String,
    pub first: u64,
    pub last: u64,
    pub level: u8,
}

#[derive(Serialize, Deserialize)]
struct Manifest {
    files: Vec<FileMeta>,
}

#[derive(Debug, Default, PartialEq, Eq)]
pub struct CompactStats {
    pub inputs: usize,
    pub outputs: usize,
    pub expired: usize,
}

pub struct Store {
    config: Config,
    /// Live files per tenant, sorted by first start and then name, which is
    /// write order for files covering the same span.
    tenants: Mutex<HashMap<String, Vec<FileMeta>>>,
    trash: Mutex<Vec<(PathBuf, Instant)>>,
}

impl Store {
    /// Opens the store, recovering every tenant directory: files the
    /// manifest does not list are deleted, and a directory without a
    /// manifest (written before manifests existed) adopts its files.
    pub fn open(config: Config) -> io::Result<Store> {
        fs::create_dir_all(&config.dir)?;
        let mut tenants = HashMap::new();
        for entry in fs::read_dir(&config.dir)? {
            let entry = entry?;
            let Some(tenant) = entry.file_name().to_str().map(str::to_owned) else {
                continue;
            };
            if !entry.file_type()?.is_dir() || !valid_tenant(&tenant) {
                continue;
            }
            let files = recover(&entry.path())?;
            tenants.insert(tenant, files);
        }
        Ok(Store {
            config,
            tenants: Mutex::new(tenants),
            trash: Mutex::new(Vec::new()),
        })
    }

    pub fn config(&self) -> &Config {
        &self.config
    }

    pub fn path(&self, tenant: &str, file: &FileMeta) -> PathBuf {
        self.config.dir.join(tenant).join(&file.name)
    }

    /// Writes buckets of one tenant as a new level-0 file and registers it.
    /// Returns the bytes written.
    pub fn add(&self, tenant: &str, buckets: &[Bucket]) -> io::Result<usize> {
        if buckets.is_empty() {
            return Ok(0);
        }
        let first = buckets.iter().map(|b| b.start).min().unwrap_or(0);
        let last = buckets.iter().map(|b| b.start).max().unwrap_or(0);
        let (meta, bytes) =
            self.write_file(tenant, &block::encode(tenant, buckets), first, last, 0)?;
        self.commit(tenant, vec![meta], &[])?;
        Ok(bytes)
    }

    /// Live files of `tenant` that may hold buckets in `[from, to]`.
    pub fn files(&self, tenant: &str, from: u64, to: u64) -> Vec<FileMeta> {
        let tenants = self.tenants.lock().unwrap();
        tenants
            .get(tenant)
            .map(|files| {
                files
                    .iter()
                    .filter(|f| f.last >= from && f.first <= to)
                    .cloned()
                    .collect()
            })
            .unwrap_or_default()
    }

    pub fn read(&self, tenant: &str, file: &FileMeta) -> io::Result<Vec<u8>> {
        let compressed = fs::read(self.path(tenant, file))?;
        zstd::stream::decode_all(&compressed[..])
    }

    /// Compresses and writes a block under a temporary name, syncs it and
    /// renames it into place. It is not live until committed.
    fn write_file(
        &self,
        tenant: &str,
        raw: &[u8],
        first: u64,
        last: u64,
        level: u8,
    ) -> io::Result<(FileMeta, usize)> {
        let compressed = zstd::bulk::compress(raw, self.config.zstd_level)?;
        let dir = self.config.dir.join(tenant);
        fs::create_dir_all(&dir)?;
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| d.as_nanos());
        let name = format!("{first}-{last}-{nanos:020}-l{level}.{EXTENSION}");
        let tmp = dir.join(format!(".{name}.tmp"));
        let mut file = fs::File::create(&tmp)?;
        file.write_all(&compressed)?;
        file.sync_all()?;
        fs::rename(&tmp, dir.join(&name))?;
        let meta = FileMeta {
            name,
            first,
            last,
            level,
        };
        Ok((meta, compressed.len()))
    }

    /// Adds and removes files in one atomic manifest update.
    fn commit(&self, tenant: &str, add: Vec<FileMeta>, remove: &[String]) -> io::Result<()> {
        let mut tenants = self.tenants.lock().unwrap();
        let files = tenants.entry(tenant.to_owned()).or_default();
        let mut next: Vec<FileMeta> = files
            .iter()
            .filter(|f| !remove.contains(&f.name))
            .cloned()
            .chain(add)
            .collect();
        next.sort_by(|a, b| (a.first, &a.name).cmp(&(b.first, &b.name)));
        write_manifest(&self.config.dir.join(tenant), &next)?;
        *files = next;
        Ok(())
    }

    /// Merges closed spans that have more than one file (or a file that
    /// reaches into another span) and deletes expired files.
    pub fn compact(&self, now: u64) -> io::Result<CompactStats> {
        let tenants: Vec<String> = self.tenants.lock().unwrap().keys().cloned().collect();
        let mut stats = CompactStats::default();
        for tenant in tenants {
            let s = self.compact_tenant(&tenant, now)?;
            stats.inputs += s.inputs;
            stats.outputs += s.outputs;
            stats.expired += s.expired;
        }
        self.empty_trash(false);
        Ok(stats)
    }

    fn compact_tenant(&self, tenant: &str, now: u64) -> io::Result<CompactStats> {
        let span = self.config.span_secs;
        let files = self.files(tenant, 0, u64::MAX);
        let mut stats = CompactStats::default();

        if let Some(keep) = self.config.retention_secs {
            let expired: Vec<String> = files
                .iter()
                .filter(|f| f.last.saturating_add(keep) < now)
                .map(|f| f.name.clone())
                .collect();
            if !expired.is_empty() {
                self.commit(tenant, vec![], &expired)?;
                self.trash_files(tenant, &expired);
                stats.expired = expired.len();
            }
        }
        let files = self.files(tenant, 0, u64::MAX);

        // Spans that ended at least `settle_secs` ago are closed.
        let cutoff = now.saturating_sub(self.config.settle_secs);
        let closed_end = cutoff - cutoff % span;
        let span_of = |t: u64| t - t % span;
        let mut by_span: HashMap<u64, Vec<&FileMeta>> = HashMap::new();
        for f in files.iter().filter(|f| f.last < closed_end) {
            let mut s = span_of(f.first);
            while s <= f.last {
                by_span.entry(s).or_default().push(f);
                s += span;
            }
        }
        let crosses = |f: &FileMeta| span_of(f.first) != span_of(f.last);
        let mut dirty: Vec<u64> = by_span
            .iter()
            .filter(|(_, fs)| fs.len() > 1 || fs.iter().any(|f| crosses(f)))
            .map(|(s, _)| *s)
            .collect();
        if dirty.is_empty() {
            return Ok(stats);
        }
        dirty.sort_unstable();

        let mut decompressed: HashMap<String, Vec<u8>> = HashMap::new();
        let mut inputs: BTreeSet<String> = BTreeSet::new();
        let mut outputs = Vec::new();
        for start in dirty {
            let span_files = &by_span[&start];
            for f in span_files {
                if !decompressed.contains_key(&f.name) {
                    decompressed.insert(f.name.clone(), self.read(tenant, f)?);
                }
                inputs.insert(f.name.clone());
            }
            let readers = span_files
                .iter()
                .map(|f| Reader::open(&decompressed[&f.name]))
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            let raw = merge_span(tenant, &readers, start, start + span)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            if let Some((raw, first, last)) = raw {
                let (meta, _) = self.write_file(tenant, &raw, first, last, 1)?;
                outputs.push(meta);
            }
            // Free inputs that no later dirty span needs.
            decompressed.retain(|name, _| {
                files
                    .iter()
                    .any(|f| &f.name == name && f.last >= start + span)
            });
        }
        let inputs: Vec<String> = inputs.into_iter().collect();
        stats.inputs = inputs.len();
        stats.outputs = outputs.len();
        self.commit(tenant, outputs, &inputs)?;
        self.trash_files(tenant, &inputs);
        Ok(stats)
    }

    fn trash_files(&self, tenant: &str, names: &[String]) {
        let mut trash = self.trash.lock().unwrap();
        let now = Instant::now();
        for n in names {
            trash.push((self.config.dir.join(tenant).join(n), now));
        }
    }

    /// Deletes removed files once they have waited `delete_after`, or all of
    /// them when `force` is set.
    pub fn empty_trash(&self, force: bool) {
        let delay = self.config.delete_after;
        self.trash.lock().unwrap().retain(|(path, at)| {
            if force || at.elapsed() >= delay {
                if let Err(e) = fs::remove_file(path)
                    && e.kind() != io::ErrorKind::NotFound
                {
                    tracing::warn!(path = %path.display(), "deleting compacted file: {e}");
                }
                false
            } else {
                true
            }
        });
    }
}

/// Merges every series found in `readers` (oldest file first) restricted to
/// `[from, to)`. Returns the raw block and its time range, or `None` if the
/// span holds no buckets.
fn merge_span(
    tenant: &str,
    readers: &[Reader<'_>],
    from: u64,
    to: u64,
) -> Result<Option<(Vec<u8>, u64, u64)>, block::BlockError> {
    let mut keys: BTreeSet<block::SeriesOrder> = BTreeSet::new();
    for r in readers {
        for e in r.index.iter().filter(|e| e.last >= from && e.first < to) {
            keys.insert((e.name.clone(), e.kind as u8, e.tags.clone()));
        }
    }
    let mut builder = Builder::new(tenant);
    let (mut first, mut last) = (u64::MAX, 0);
    for (name, kind, tags) in keys {
        let kind = match kind {
            0 => Kind::Counter,
            1 => Kind::Gauge,
            _ => Kind::Distribution,
        };
        let mut series: Vec<(u64, Value)> = Vec::new();
        for r in readers {
            if let Some(e) = r.find_exact(&name, kind, &tags) {
                series.extend(
                    r.series(e)?
                        .into_iter()
                        .filter(|(s, _)| *s >= from && *s < to),
                );
            }
        }
        // Stable, so equal starts stay in file order for the merge.
        series.sort_by_key(|(s, _)| *s);
        let mut merged: Vec<(u64, Value)> = Vec::with_capacity(series.len());
        for (start, value) in series {
            match merged.last_mut() {
                Some((s, acc)) if *s == start => {
                    acc.merge(&value).map_err(|_| block::BlockError::Corrupt)?
                }
                _ => merged.push((start, value)),
            }
        }
        if let (Some(f), Some(l)) = (merged.first(), merged.last()) {
            first = first.min(f.0);
            last = last.max(l.0);
        }
        let refs: Vec<(u64, &Value)> = merged.iter().map(|(s, v)| (*s, v)).collect();
        builder.add_series(&name, &tags, &refs);
    }
    if builder.is_empty() {
        return Ok(None);
    }
    Ok(Some((builder.finish(), first, last)))
}

fn write_manifest(dir: &Path, files: &[FileMeta]) -> io::Result<()> {
    fs::create_dir_all(dir)?;
    let json = serde_json::to_vec_pretty(&Manifest {
        files: files.to_vec(),
    })?;
    let tmp = dir.join(format!(".{MANIFEST}.tmp"));
    let mut f = fs::File::create(&tmp)?;
    f.write_all(&json)?;
    f.sync_all()?;
    fs::rename(&tmp, dir.join(MANIFEST))?;
    fs::File::open(dir)?.sync_all()
}

/// Loads a tenant directory's manifest, deletes unlisted files, and drops
/// listed files that are missing. Without a manifest, adopts every block
/// file whose name parses.
fn recover(dir: &Path) -> io::Result<Vec<FileMeta>> {
    let manifest_path = dir.join(MANIFEST);
    let mut files = match fs::read(&manifest_path) {
        Ok(bytes) => {
            let m: Manifest = serde_json::from_slice(&bytes)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            m.files
        }
        Err(e) if e.kind() == io::ErrorKind::NotFound => {
            let mut adopted = Vec::new();
            for entry in fs::read_dir(dir)? {
                let name = entry?.file_name().to_string_lossy().into_owned();
                if let Some(meta) = parse_name(&name) {
                    adopted.push(meta);
                }
            }
            adopted
        }
        Err(e) => return Err(e),
    };
    let listed: BTreeSet<&str> = files.iter().map(|f| f.name.as_str()).collect();
    for entry in fs::read_dir(dir)? {
        let name = entry?.file_name().to_string_lossy().into_owned();
        if name == MANIFEST || listed.contains(name.as_str()) {
            continue;
        }
        // Temporary files, uncommitted outputs and removed inputs.
        fs::remove_file(dir.join(&name))?;
    }
    files.retain(|f| {
        let exists = dir.join(&f.name).exists();
        if !exists {
            tracing::warn!(file = f.name, "manifest lists a missing file; dropping it");
        }
        exists
    });
    files.sort_by(|a, b| (a.first, &a.name).cmp(&(b.first, &b.name)));
    write_manifest(dir, &files)?;
    Ok(files)
}

/// `<first>-<last>-<nanos>[-l<level>].fnlb.zst`
fn parse_name(name: &str) -> Option<FileMeta> {
    let stem = name.strip_suffix(&format!(".{EXTENSION}"))?;
    let mut parts = stem.split('-');
    let first = parts.next()?.parse().ok()?;
    let last = parts.next()?.parse().ok()?;
    parts.next()?.parse::<u128>().ok()?;
    let level = match parts.next() {
        None => 0,
        Some(l) => l.strip_prefix('l')?.parse().ok()?,
    };
    Some(FileMeta {
        name: name.to_owned(),
        first,
        last,
        level,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::aggregator::{Aggregator, Config as AggConfig};
    use crate::protocol::parse_line;

    fn temp_dir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("funnel-store-{name}-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        dir
    }

    fn store(dir: &Path) -> Store {
        Store::open(Config {
            dir: dir.to_path_buf(),
            span_secs: 60,
            settle_secs: 0,
            delete_after: Duration::ZERO,
            ..Config::default()
        })
        .unwrap()
    }

    fn buckets(lines: &[(u64, &str)]) -> Vec<Bucket> {
        let mut agg = Aggregator::new(AggConfig::default());
        for (now, line) in lines {
            agg.insert(*now, "acme", &mut parse_line(line).unwrap())
                .unwrap();
        }
        agg.flush_all()
    }

    /// Sums one counter series over every live file.
    fn total(s: &Store, name: &str) -> f64 {
        let mut sum = 0.0;
        for f in s.files("acme", 0, u64::MAX) {
            let bytes = s.read("acme", &f).unwrap();
            let r = Reader::open(&bytes).unwrap();
            for e in r.find(name) {
                for (_, v) in r.series(e).unwrap() {
                    if let Value::Counter(c) = v {
                        sum += c;
                    }
                }
            }
        }
        sum
    }

    #[test]
    fn compaction_merges_spans_and_partial_buckets() {
        let dir = temp_dir("compact");
        let s = store(&dir);
        // Three level-0 files over two minute-long spans, one with a late
        // partial bucket for the window at 1000.
        s.add("acme", &buckets(&[(1000, "req:1|c"), (1010, "req:2|c")]))
            .unwrap();
        s.add("acme", &buckets(&[(1050, "req:3|c"), (1070, "req:4|c")]))
            .unwrap();
        s.add("acme", &buckets(&[(1090, "req:5|c|T1001")])).unwrap();
        assert_eq!(s.files("acme", 0, u64::MAX).len(), 3);
        assert_eq!(total(&s, "req"), 15.0);

        // Span [960, 1020) has two files (the late point belongs to it);
        // span [1020, 1080) has one, which is already compact.
        let stats = s.compact(1200).unwrap();
        assert_eq!((stats.inputs, stats.outputs), (2, 1));
        let files = s.files("acme", 0, u64::MAX);
        assert_eq!(files.len(), 2);
        assert_eq!((files[0].level, files[1].level), (1, 0));
        assert_eq!(total(&s, "req"), 15.0);
        // The late 5 was merged into the bucket at 1000.
        let bytes = s.read("acme", &files[0]).unwrap();
        let r = Reader::open(&bytes).unwrap();
        let first = r.series(&r.find("req")[0]).unwrap();
        assert!(matches!(first[0], (1000, Value::Counter(c)) if c == 6.0));
        // Inputs are deleted from disk.
        let on_disk = fs::read_dir(dir.join("acme")).unwrap().count();
        assert_eq!(on_disk, 2 + 1, "two files and the manifest");
        // Nothing left to do.
        assert_eq!(s.compact(1200).unwrap(), CompactStats::default());
        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn a_file_reaching_across_spans_is_split() {
        let dir = temp_dir("split");
        let s = store(&dir);
        s.add(
            "acme",
            &buckets(&[(1100, "req:1|c|T1000"), (1100, "req:2|c|T1090")]),
        )
        .unwrap();
        let stats = s.compact(1200).unwrap();
        assert_eq!((stats.inputs, stats.outputs), (1, 2));
        let files = s.files("acme", 0, u64::MAX);
        assert_eq!((files[0].first, files[0].last), (1000, 1000));
        assert_eq!((files[1].first, files[1].last), (1090, 1090));
        assert_eq!(total(&s, "req"), 3.0);
        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn open_spans_wait_and_single_files_stay() {
        let dir = temp_dir("open");
        let s = store(&dir);
        s.add("acme", &buckets(&[(1000, "req:1|c")])).unwrap();
        s.add("acme", &buckets(&[(1010, "req:1|c")])).unwrap();
        // The span [960, 1020) is still open at 1019.
        assert_eq!(s.compact(1019).unwrap().inputs, 0);
        assert_eq!(s.compact(1020).unwrap().outputs, 1);
        s.add("acme", &buckets(&[(1100, "req:1|c")])).unwrap();
        // One file alone in its span is already compact.
        assert_eq!(s.compact(5000).unwrap().inputs, 0);
        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn recovery_deletes_unlisted_files_and_keeps_listed_ones() {
        let dir = temp_dir("recover");
        let s = store(&dir);
        s.add("acme", &buckets(&[(1000, "req:1|c")])).unwrap();
        // A crash after writing an output but before committing it, and a
        // temporary file from an interrupted write.
        let (orphan, _) = s
            .write_file("acme", &block::encode("acme", &[]), 1, 2, 1)
            .unwrap();
        fs::write(dir.join("acme").join(".x.tmp"), b"partial").unwrap();
        drop(s);
        let s = store(&dir);
        assert_eq!(s.files("acme", 0, u64::MAX).len(), 1);
        assert!(!dir.join("acme").join(&orphan.name).exists());
        assert!(!dir.join("acme").join(".x.tmp").exists());
        assert_eq!(total(&s, "req"), 1.0);
        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn adopts_files_written_before_manifests() {
        let dir = temp_dir("adopt");
        fs::create_dir_all(dir.join("acme")).unwrap();
        let raw = block::encode("acme", &buckets(&[(1000, "req:7|c")]));
        let old = dir.join("acme").join(format!("1000-1000-123.{EXTENSION}"));
        fs::write(&old, zstd::bulk::compress(&raw, 3).unwrap()).unwrap();
        let s = store(&dir);
        assert_eq!(s.files("acme", 0, u64::MAX)[0].level, 0);
        assert_eq!(total(&s, "req"), 7.0);
        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn retention_expires_old_files() {
        let dir = temp_dir("retention");
        let s = Store::open(Config {
            dir: dir.clone(),
            retention_secs: Some(3600),
            delete_after: Duration::ZERO,
            ..Config::default()
        })
        .unwrap();
        s.add("acme", &buckets(&[(1000, "req:1|c")])).unwrap();
        s.add("acme", &buckets(&[(9000, "req:1|c")])).unwrap();
        assert_eq!(s.compact(9100).unwrap().expired, 1);
        assert_eq!(s.files("acme", 0, u64::MAX).len(), 1);
        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn names_parse() {
        let m = parse_name("10-20-00000000000000000005-l1.fnlb.zst").unwrap();
        assert_eq!((m.first, m.last, m.level), (10, 20, 1));
        assert_eq!(parse_name("10-20-5.fnlb.zst").unwrap().level, 0);
        assert!(parse_name("MANIFEST").is_none());
        assert!(parse_name("a-b-c.fnlb.zst").is_none());
    }
}
