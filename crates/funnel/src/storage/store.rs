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
//! - Older data can be downsampled, e.g. to 1-minute buckets after 7 days
//!   and 10-minute buckets after 30 (`7d:1m,30d:10m`, the syntax of
//!   VictoriaMetrics' `-downsampling.period`). Every bucket is a mergeable
//!   summary, so downsampling merges buckets with the same `Value::merge`
//!   as everything else: counters stay exact, gauges keep their true min
//!   and max, and distributions keep their sketches, so percentiles of old
//!   data stay within the sketch's error. Only time resolution is lost.
//!   VictoriaMetrics keeps the last sample per interval and Thanos keeps
//!   count, sum, min and max, so neither keeps percentiles.

use std::collections::{BTreeMap, BTreeSet, HashMap};
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
    /// Width of the buckets new files hold.
    pub bucket_secs: u64,
    /// Length of the spans compaction merges into one file at full
    /// resolution. Downsampled levels use 60 buckets per span.
    pub span_secs: u64,
    /// Downsampling levels, oldest last; see [`Downsample::parse_list`].
    pub downsample: Vec<Downsample>,
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
            bucket_secs: 10,
            span_secs: 600,
            downsample: Vec::new(),
            settle_secs: 120,
            retention_secs: None,
            delete_after: Duration::from_secs(60),
        }
    }
}

/// Data older than `after_secs` is kept at `resolution_secs` buckets.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Downsample {
    pub after_secs: u64,
    pub resolution_secs: u64,
}

impl Downsample {
    /// Parses `7d:1m,30d:10m` (units s, m, h, d).
    pub fn parse_list(s: &str) -> Result<Vec<Downsample>, String> {
        fn secs(v: &str) -> Result<u64, String> {
            let (n, unit) = v.split_at(v.len().saturating_sub(1));
            let mult = match unit {
                "s" => 1,
                "m" => 60,
                "h" => 3600,
                "d" => 86_400,
                _ => return Err(format!("{v:?} needs a unit: s, m, h or d")),
            };
            n.parse::<u64>()
                .ok()
                .and_then(|n| n.checked_mul(mult))
                .filter(|&x| x > 0)
                .ok_or_else(|| format!("bad duration {v:?}"))
        }
        s.split(',')
            .filter(|p| !p.is_empty())
            .map(|p| {
                let (after, res) = p
                    .split_once(':')
                    .ok_or_else(|| format!("{p:?} should be AGE:RESOLUTION, like 7d:1m"))?;
                Ok(Downsample {
                    after_secs: secs(after)?,
                    resolution_secs: secs(res)?,
                })
            })
            .collect()
    }
}

/// A resolution level: data older than `after` (once its span is closed)
/// is stored in `resolution`-wide buckets, `span` seconds per file.
#[derive(Clone, Copy, Debug)]
struct Tier {
    after: u64,
    resolution: u64,
    span: u64,
}

fn default_resolution() -> u64 {
    10
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileMeta {
    pub name: String,
    pub first: u64,
    pub last: u64,
    pub level: u8,
    /// Bucket width in seconds; manifests from before downsampling hold
    /// only 10-second buckets.
    #[serde(default = "default_resolution")]
    pub resolution: u64,
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
    tiers: Vec<Tier>,
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
        let tiers = tiers(&config).map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
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
            tiers,
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
        let resolution = self.config.bucket_secs;
        let raw = block::encode(tenant, resolution, buckets);
        let (meta, bytes) = self.write_file(tenant, &raw, first, last, 0, resolution)?;
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
        resolution: u64,
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
            resolution,
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

        // The level of time `t`: the highest one whose span around `t`
        // ended at least that level's age before `cutoff`. Spans nest (each
        // level's span is a multiple of the one below), so every time in a
        // span gets the same level.
        let cutoff = now.saturating_sub(self.config.settle_secs);
        let tiers = &self.tiers;
        let level_of = |t: u64| -> Option<usize> {
            (0..tiers.len()).rev().find(|&k| {
                let end = (t - t % tiers[k].span).saturating_add(tiers[k].span);
                end.saturating_add(tiers[k].after) <= cutoff
            })
        };
        // Only files whose whole range is closed can be inputs: an input
        // leaves the manifest, so an open part would be lost.
        let mut groups: BTreeMap<(usize, u64), Vec<&FileMeta>> = BTreeMap::new();
        for f in files.iter().filter(|f| level_of(f.last).is_some()) {
            let mut t = f.first;
            while t <= f.last {
                let k = level_of(t).expect("older than a closed time");
                let start = t - t % tiers[k].span;
                groups.entry((k, start)).or_default().push(f);
                t = start.saturating_add(tiers[k].span);
                if t == u64::MAX {
                    break;
                }
            }
        }
        // A file reaching outside its span is dirty in every span it
        // touches, so all of its parts are rewritten in the same pass.
        let dirty: Vec<(usize, u64)> = groups
            .iter()
            .filter(|((k, start), fs)| {
                let tier = tiers[*k];
                fs.len() > 1
                    || fs.iter().any(|f| {
                        f.first < *start
                            || f.last >= start + tier.span
                            || f.resolution < tier.resolution
                    })
            })
            .map(|(g, _)| *g)
            .collect();
        if dirty.is_empty() {
            return Ok(stats);
        }

        let mut decompressed: HashMap<String, Vec<u8>> = HashMap::new();
        let mut inputs: BTreeSet<String> = BTreeSet::new();
        let mut outputs = Vec::new();
        for (n, group) in dirty.iter().enumerate() {
            let (k, start) = *group;
            let tier = tiers[k];
            let group_files = &groups[group];
            for f in group_files {
                if !decompressed.contains_key(&f.name) {
                    decompressed.insert(f.name.clone(), self.read(tenant, f)?);
                }
                inputs.insert(f.name.clone());
            }
            let readers = group_files
                .iter()
                .map(|f| Reader::open(&decompressed[&f.name]))
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            let merged = merge_span(tenant, &readers, start, start + tier.span, tier.resolution)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            if let Some((raw, first, last)) = merged {
                let level = u8::try_from(k + 1).unwrap_or(u8::MAX);
                let (meta, _) =
                    self.write_file(tenant, &raw, first, last, level, tier.resolution)?;
                outputs.push(meta);
            }
            // Free inputs no later dirty span needs.
            let later = &dirty[n + 1..];
            decompressed.retain(|name, _| {
                later
                    .iter()
                    .any(|g| groups[g].iter().any(|f| &f.name == name))
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

/// The levels in use: full resolution, then each downsampling level.
fn tiers(config: &Config) -> Result<Vec<Tier>, String> {
    if config.bucket_secs == 0 || !config.span_secs.is_multiple_of(config.bucket_secs) {
        return Err("span_secs must be a multiple of bucket_secs".into());
    }
    let mut tiers = vec![Tier {
        after: 0,
        resolution: config.bucket_secs,
        span: config.span_secs,
    }];
    for d in &config.downsample {
        let prev = tiers[tiers.len() - 1];
        let tier = Tier {
            after: d.after_secs,
            resolution: d.resolution_secs,
            span: d.resolution_secs * 60,
        };
        if tier.after <= prev.after
            || tier.resolution <= prev.resolution
            || !tier.resolution.is_multiple_of(prev.resolution)
            || !tier.span.is_multiple_of(prev.span)
        {
            return Err(format!(
                "downsampling levels must grow in age and resolution, each a multiple \
                 of the one before (60x a resolution must be a multiple of {}s)",
                prev.span
            ));
        }
        tiers.push(tier);
    }
    Ok(tiers)
}

/// Merges every series found in `readers` (oldest file first) restricted to
/// `[from, to)`, into buckets `resolution` seconds wide. Returns the raw
/// block and its time range, or `None` if the span holds no buckets.
fn merge_span(
    tenant: &str,
    readers: &[Reader<'_>],
    from: u64,
    to: u64,
    resolution: u64,
) -> Result<Option<(Vec<u8>, u64, u64)>, block::BlockError> {
    let mut keys: BTreeSet<block::SeriesOrder> = BTreeSet::new();
    for r in readers {
        for e in r.index.iter().filter(|e| e.last >= from && e.first < to) {
            keys.insert((e.name.clone(), e.kind as u8, e.tags.clone()));
        }
    }
    let mut builder = Builder::new(tenant, resolution);
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
                        .filter(|(s, _)| *s >= from && *s < to)
                        .map(|(s, v)| (s - s % resolution, v)),
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
        resolution: default_resolution(),
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
            .write_file("acme", &block::encode("acme", 10, &[]), 1, 2, 1, 10)
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
        let raw = block::encode("acme", 10, &buckets(&[(1000, "req:7|c")]));
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
    fn downsample_levels_parse_and_validate() {
        let levels = Downsample::parse_list("7d:1m,30d:10m").unwrap();
        assert_eq!(
            levels,
            [
                Downsample {
                    after_secs: 7 * 86_400,
                    resolution_secs: 60
                },
                Downsample {
                    after_secs: 30 * 86_400,
                    resolution_secs: 600
                },
            ]
        );
        for bad in ["7d", "7x:1m", "0d:1m", "7d:", "d:1m"] {
            assert!(Downsample::parse_list(bad).is_err(), "{bad}");
        }
        let open = |downsample: &str| {
            Store::open(Config {
                dir: temp_dir("levels"),
                downsample: Downsample::parse_list(downsample).unwrap(),
                ..Config::default()
            })
        };
        // Ages must grow, resolutions must be multiples, spans must nest.
        assert!(open("30d:1m,7d:10m").is_err());
        assert!(open("7d:1m,30d:90s").is_err());
        assert!(open("7d:15s").is_err());
        assert!(open("7d:1m,30d:10m").is_ok());
    }

    fn downsampling_store(dir: &Path) -> Store {
        Store::open(Config {
            dir: dir.to_path_buf(),
            span_secs: 60,
            settle_secs: 0,
            delete_after: Duration::ZERO,
            // After 600 s, 60-second buckets in 3600-second files.
            downsample: vec![Downsample {
                after_secs: 600,
                resolution_secs: 60,
            }],
            ..Config::default()
        })
        .unwrap()
    }

    /// Writes 60 windows from 1000 to 1590 as three level-0 files.
    fn sixty_windows(s: &Store) {
        let lines: Vec<(u64, String)> = (0..60u64)
            .flat_map(|i| {
                let t = 1000 + i * 10;
                [
                    (t, "req:1|c".to_string()),
                    (t, format!("g:{}|g|T{}", (i * 7) % 50, t + 3)),
                    (t, format!("lat:{}|ms", 10 + i % 20)),
                ]
            })
            .collect();
        for chunk in lines.chunks(60) {
            let refs: Vec<(u64, &str)> = chunk.iter().map(|(t, l)| (*t, l.as_str())).collect();
            s.add("acme", &buckets(&refs)).unwrap();
        }
    }

    fn values(s: &Store, name: &str) -> Vec<(u64, Value)> {
        let mut out = Vec::new();
        for f in s.files("acme", 0, u64::MAX) {
            let bytes = s.read("acme", &f).unwrap();
            let r = Reader::open(&bytes).unwrap();
            for e in r.find(name) {
                out.extend(r.series(e).unwrap());
            }
        }
        out
    }

    #[test]
    fn downsampling_merges_buckets_exactly() {
        let dir = temp_dir("downsample");
        let s = downsampling_store(&dir);
        sixty_windows(&s);

        // Not yet old enough: compaction keeps 10-second buckets.
        s.compact(3600 + 599).unwrap();
        assert!(
            s.files("acme", 0, u64::MAX)
                .iter()
                .all(|f| f.resolution == 10)
        );
        assert_eq!(values(&s, "req").len(), 60);

        // Old enough: one file of 60-second buckets.
        s.compact(3600 + 600).unwrap();
        let files = s.files("acme", 0, u64::MAX);
        assert_eq!(files.len(), 1);
        assert_eq!((files[0].resolution, files[0].level), (60, 2));

        let req = values(&s, "req");
        assert!(req.iter().all(|(start, _)| start % 60 == 0));
        assert_eq!(req.len(), 11, "1000..1590 touches minutes 960 to 1560");
        let total: f64 = req
            .iter()
            .map(|(_, v)| match v {
                Value::Counter(c) => *c,
                _ => 0.0,
            })
            .sum();
        assert_eq!(total, 60.0);

        let gauges: Vec<_> = values(&s, "g")
            .into_iter()
            .map(|(_, v)| match v {
                Value::Gauge(g) => g,
                _ => panic!(),
            })
            .collect();
        let min = gauges.iter().map(|g| g.min).fold(f64::INFINITY, f64::min);
        let max = gauges
            .iter()
            .map(|g| g.max)
            .fold(f64::NEG_INFINITY, f64::max);
        let count: u64 = gauges.iter().map(|g| g.count).sum();
        assert_eq!((min, max, count), (0.0, 49.0, 60));
        // The last bucket's last value is the last one sent (i = 59).
        assert_eq!(gauges.last().unwrap().last, ((59 * 7) % 50) as f64);

        let lat: f64 = values(&s, "lat")
            .iter()
            .map(|(_, v)| match v {
                Value::Distribution(d) => d.count(),
                _ => 0.0,
            })
            .sum();
        assert_eq!(lat, 60.0);
        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn rates_over_downsampled_buckets_use_their_width() {
        use crate::query::{Agg, Query, run};
        use crate::storage::writer::Pending;

        let dir = temp_dir("rate");
        let s = downsampling_store(&dir);
        sixty_windows(&s);
        s.compact(3600 + 600).unwrap();
        let q = Query {
            tenant: "acme".into(),
            metric: "req".into(),
            from: 0,
            to: u64::MAX,
            filters: vec![],
            group_by: Some(vec![]),
            step: 10,
            agg: Some(Agg::Rate),
        };
        let r = run(&s, &Pending::default(), &q).unwrap();
        // A full minute holds 6 requests: 0.1 per second, not 0.6.
        let full_minute = r.series[0].points.iter().find(|(t, _)| *t == 1020).unwrap();
        assert!((full_minute.1 - 0.1).abs() < 1e-9, "{full_minute:?}");
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
