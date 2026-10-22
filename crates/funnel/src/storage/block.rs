//! Block files: one tenant's buckets for a span of time, grouped by series.
//!
//! ```text
//! "FNLB" version tenant
//! per series:  count, timestamps (Gorilla), value columns for its kind
//! index:       per series name, kind, tags, count, first/last start, offset
//! index offset (u64 LE) "FNLB"
//! ```
//!
//! Series are sorted by name, then kind and tags, so a reader can find a
//! metric in the index without decoding any data, and decode only the
//! series it needs. Within a series, buckets are sorted by start time;
//! buckets with the same start (partial buckets from late data) keep the
//! order they were flushed in, which is the order [`Value::merge`] expects.
//!
//! Columns per kind: counters store one XOR-compressed float column; gauges
//! store min, max, last and sum as float columns plus count and the offset
//! of `last_at` from the bucket start as varints. Distributions store the
//! sketch configuration once per series, then for each bucket its sum, min
//! and max as plain floats followed by its bins; a series whose sketches use
//! different configurations falls back to full encoded sketches.
//!
//! Blocks are meant to be zstd-compressed as a whole, so layouts were chosen
//! by size after zstd. Keeping each bucket's statistics next to its bins
//! compressed 16% better than separate sum/min/max columns, whether those
//! columns used Gorilla's XOR encoding or byte stream split. Our best guess
//! is that a bucket's min and its first bin index carry nearly the same
//! information, and side by side they form byte patterns zstd can match.

use std::collections::BTreeMap;
use std::fmt;

use funnel_sketch::{Config as SketchConfig, DDSketch, MappingKind};

use super::gorilla::{decode_floats, decode_times, encode_floats, encode_times};
use crate::aggregator::{Bucket, Gauge, Value};
use crate::protocol::Kind;

const MAGIC: &[u8; 4] = b"FNLB";
const VERSION: u8 = 2;
/// Bucket width of version-1 blocks, which did not record it.
const V1_RESOLUTION: u64 = 10;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BlockError {
    Magic,
    Version(u8),
    Corrupt,
}

impl fmt::Display for BlockError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BlockError::Magic => f.write_str("not a funnel block"),
            BlockError::Version(v) => write!(f, "unknown block version {v}"),
            BlockError::Corrupt => f.write_str("corrupt block"),
        }
    }
}

impl std::error::Error for BlockError {}

/// One series in a block's index.
#[derive(Clone, Debug, PartialEq)]
pub struct IndexEntry {
    pub name: String,
    pub kind: Kind,
    pub tags: Vec<(String, String)>,
    pub buckets: usize,
    pub first: u64,
    pub last: u64,
    offset: usize,
    len: usize,
}

impl IndexEntry {
    pub fn tag(&self, key: &str) -> Option<&str> {
        self.tags
            .binary_search_by(|(k, _)| k.as_str().cmp(key))
            .ok()
            .map(|i| self.tags[i].1.as_str())
    }
}

/// How series are ordered in a block: name, then kind, then tags.
pub type SeriesOrder = (String, u8, Vec<(String, String)>);

/// Encodes buckets that all belong to `tenant` and are `resolution`
/// seconds wide.
pub fn encode(tenant: &str, resolution: u64, buckets: &[Bucket]) -> Vec<u8> {
    let mut by_series: BTreeMap<SeriesOrder, Vec<(u64, &Value)>> = BTreeMap::new();
    for b in buckets {
        debug_assert_eq!(b.series.tenant, tenant);
        let key = (
            b.series.name.clone(),
            b.series.kind as u8,
            b.series.tags.clone(),
        );
        by_series.entry(key).or_default().push((b.start, &b.value));
    }
    let mut builder = Builder::new(tenant, resolution);
    for ((name, _, tags), mut series) in by_series {
        // Stable, so equal starts keep flush order.
        series.sort_by_key(|(start, _)| *start);
        builder.add_series(&name, &tags, &series);
    }
    builder.finish()
}

/// Writes a block one series at a time, so a block can be built without
/// holding all of its buckets in memory. Series must be added in index
/// order (name, then kind, then tags), each with its buckets sorted by
/// start.
pub struct Builder {
    out: Vec<u8>,
    index: Vec<IndexEntry>,
}

impl Builder {
    pub fn new(tenant: &str, resolution: u64) -> Self {
        let mut out = Vec::new();
        out.extend_from_slice(MAGIC);
        out.push(VERSION);
        put_str(&mut out, tenant);
        put_varint(&mut out, resolution);
        Builder {
            out,
            index: Vec::new(),
        }
    }

    pub fn add_series(&mut self, name: &str, tags: &[(String, String)], series: &[(u64, &Value)]) {
        let Some(first) = series.first() else {
            return;
        };
        let kind = first.1.kind();
        if let Some(prev) = self.index.last() {
            let order =
                |n: &str, k: Kind, t: &[(String, String)]| (n.to_owned(), k as u8, t.to_vec());
            debug_assert!(
                order(&prev.name, prev.kind, &prev.tags) < order(name, kind, tags),
                "series added out of order"
            );
        }
        let offset = self.out.len();
        encode_series(series, &mut self.out);
        self.index.push(IndexEntry {
            name: name.to_owned(),
            kind,
            tags: tags.to_vec(),
            buckets: series.len(),
            first: first.0,
            last: series[series.len() - 1].0,
            offset,
            len: self.out.len() - offset,
        });
    }

    pub fn is_empty(&self) -> bool {
        self.index.is_empty()
    }

    /// A builder that writes the version-1 header, which has no resolution.
    #[cfg(test)]
    fn new_v1(tenant: &str) -> Self {
        let mut out = Vec::new();
        out.extend_from_slice(MAGIC);
        out.push(1);
        put_str(&mut out, tenant);
        Builder {
            out,
            index: Vec::new(),
        }
    }

    pub fn finish(self) -> Vec<u8> {
        let Builder { mut out, index } = self;
        let index_offset = out.len();
        put_varint(&mut out, index.len() as u64);
        for e in &index {
            put_str(&mut out, &e.name);
            out.push(e.kind as u8);
            put_varint(&mut out, e.tags.len() as u64);
            for (k, v) in &e.tags {
                put_str(&mut out, k);
                put_str(&mut out, v);
            }
            put_varint(&mut out, e.buckets as u64);
            put_varint(&mut out, e.first);
            put_varint(&mut out, e.last - e.first);
            put_varint(&mut out, e.offset as u64);
            put_varint(&mut out, e.len as u64);
        }
        out.extend_from_slice(&(index_offset as u64).to_le_bytes());
        out.extend_from_slice(MAGIC);
        out
    }
}

fn encode_series(series: &[(u64, &Value)], out: &mut Vec<u8>) {
    let starts: Vec<u64> = series.iter().map(|b| b.0).collect();
    put_bytes(out, &encode_times(&starts));
    let column = |out: &mut Vec<u8>, f: &dyn Fn(&Value) -> f64| {
        let values: Vec<f64> = series.iter().map(|b| f(b.1)).collect();
        put_bytes(out, &encode_floats(&values));
    };
    match series[0].1.kind() {
        Kind::Counter => column(out, &|v| match v {
            Value::Counter(c) => *c,
            _ => unreachable!("one kind per series"),
        }),
        Kind::Gauge => {
            let gauges: Vec<(u64, &Gauge)> = series
                .iter()
                .map(|b| match b.1 {
                    Value::Gauge(g) => (b.0, g),
                    _ => unreachable!("one kind per series"),
                })
                .collect();
            for f in [
                |g: &Gauge| g.min,
                |g: &Gauge| g.max,
                |g: &Gauge| g.last,
                |g: &Gauge| g.sum,
            ] {
                let values: Vec<f64> = gauges.iter().map(|(_, g)| f(g)).collect();
                put_bytes(out, &encode_floats(&values));
            }
            for (start, g) in &gauges {
                put_varint(out, g.count);
                put_varint(out, zigzag(g.last_at.wrapping_sub(*start) as i64));
            }
        }
        Kind::Distribution => {
            let sketches: Vec<&DDSketch> = series
                .iter()
                .map(|b| match b.1 {
                    Value::Distribution(s) => &**s,
                    _ => unreachable!("one kind per series"),
                })
                .collect();
            let config = sketches[0].config();
            let shared = sketches.iter().all(|s| {
                let c = s.config();
                (c.alpha, c.mapping, c.max_bins) == (config.alpha, config.mapping, config.max_bins)
            });
            if !shared {
                out.push(FULL_SKETCHES);
                sketches.iter().for_each(|s| s.encode(out));
                return;
            }
            out.push(SHARED_CONFIG);
            out.push(config.mapping as u8);
            out.extend_from_slice(&config.alpha.to_le_bytes());
            put_varint(out, config.max_bins as u64);
            // Empty sketches have no min or max; store 0 and restore them as
            // empty from the bins.
            for s in &sketches {
                for v in [s.sum(), s.min().unwrap_or(0.0), s.max().unwrap_or(0.0)] {
                    out.extend_from_slice(&v.to_le_bytes());
                }
                s.encode_bins(out);
            }
        }
    }
}

const SHARED_CONFIG: u8 = 0;
const FULL_SKETCHES: u8 = 1;

/// A decoded block's tenant and index; series data is decoded on demand.
pub struct Reader<'a> {
    bytes: &'a [u8],
    pub tenant: String,
    /// Width of every bucket in this block, in seconds.
    pub resolution: u64,
    pub index: Vec<IndexEntry>,
}

impl<'a> Reader<'a> {
    pub fn open(bytes: &'a [u8]) -> Result<Self, BlockError> {
        if bytes.len() < 4 + 1 + 8 + 4 || &bytes[..4] != MAGIC || &bytes[bytes.len() - 4..] != MAGIC
        {
            return Err(BlockError::Magic);
        }
        if bytes[4] != VERSION && bytes[4] != 1 {
            return Err(BlockError::Version(bytes[4]));
        }
        let mut header = Cursor::new(bytes, 5);
        let tenant = header.str()?;
        let resolution = if bytes[4] == 1 {
            V1_RESOLUTION
        } else {
            header.varint()?
        };
        if resolution == 0 {
            return Err(BlockError::Corrupt);
        }
        let tail = bytes.len() - 12;
        let index_offset = u64::from_le_bytes(bytes[tail..tail + 8].try_into().unwrap()) as usize;
        if index_offset < header.pos || index_offset > tail {
            return Err(BlockError::Corrupt);
        }
        let mut c = Cursor::new(&bytes[..tail], index_offset);
        let n = c.varint()? as usize;
        let mut index = Vec::with_capacity(n.min(1 << 16));
        for _ in 0..n {
            let name = c.str()?;
            let kind = kind_from(c.byte()?).ok_or(BlockError::Corrupt)?;
            let tag_count = c.varint()? as usize;
            let mut tags = Vec::with_capacity(tag_count.min(64));
            for _ in 0..tag_count {
                tags.push((c.str()?, c.str()?));
            }
            let buckets = c.varint()? as usize;
            let first = c.varint()?;
            let last = first.checked_add(c.varint()?).ok_or(BlockError::Corrupt)?;
            let offset = c.varint()? as usize;
            let len = c.varint()? as usize;
            if offset.checked_add(len).is_none_or(|end| end > index_offset) || buckets == 0 {
                return Err(BlockError::Corrupt);
            }
            index.push(IndexEntry {
                name,
                kind,
                tags,
                buckets,
                first,
                last,
                offset,
                len,
            });
        }
        Ok(Reader {
            bytes,
            tenant,
            resolution,
            index,
        })
    }

    /// Bytes of data stored for one series.
    pub fn series_len(&self, entry: &IndexEntry) -> usize {
        entry.len
    }

    /// The index entry for one exact series, if this block has it.
    pub fn find_exact(
        &self,
        name: &str,
        kind: Kind,
        tags: &[(String, String)],
    ) -> Option<&IndexEntry> {
        self.find(name)
            .iter()
            .find(|e| e.kind == kind && e.tags == tags)
    }

    /// Index entries for one metric name.
    pub fn find(&self, name: &str) -> &[IndexEntry] {
        let lo = self.index.partition_point(|e| e.name.as_str() < name);
        let hi = self.index.partition_point(|e| e.name.as_str() <= name);
        &self.index[lo..hi]
    }

    /// Decodes one series' buckets as `(start, value)` in stored order.
    pub fn series(&self, entry: &IndexEntry) -> Result<Vec<(u64, Value)>, BlockError> {
        let data = &self.bytes[entry.offset..entry.offset + entry.len];
        let mut c = Cursor::new(data, 0);
        let n = entry.buckets;
        if n > data.len() * 8 {
            return Err(BlockError::Corrupt);
        }
        let starts = decode_times(c.bytes()?, n).map_err(|_| BlockError::Corrupt)?;
        let floats = |c: &mut Cursor| -> Result<Vec<f64>, BlockError> {
            decode_floats(c.bytes()?, n).map_err(|_| BlockError::Corrupt)
        };
        let values: Vec<Value> = match entry.kind {
            Kind::Counter => floats(&mut c)?.into_iter().map(Value::Counter).collect(),
            Kind::Gauge => {
                let (min, max, last, sum) = (
                    floats(&mut c)?,
                    floats(&mut c)?,
                    floats(&mut c)?,
                    floats(&mut c)?,
                );
                let mut out = Vec::with_capacity(n);
                for i in 0..n {
                    let count = c.varint()?;
                    let last_at = starts[i].wrapping_add(unzigzag(c.varint()?) as u64);
                    out.push(Value::Gauge(Gauge {
                        min: min[i],
                        max: max[i],
                        last: last[i],
                        last_at,
                        sum: sum[i],
                        count,
                    }));
                }
                out
            }
            Kind::Distribution => {
                let mut out = Vec::with_capacity(n);
                match c.byte()? {
                    FULL_SKETCHES => {
                        for _ in 0..n {
                            let (sketch, used) = DDSketch::decode(&data[c.pos..])
                                .map_err(|_| BlockError::Corrupt)?;
                            c.pos += used;
                            out.push(Value::Distribution(Box::new(sketch)));
                        }
                    }
                    SHARED_CONFIG => {
                        let mapping = match c.byte()? {
                            0 => MappingKind::Logarithmic,
                            1 => MappingKind::Cubic,
                            _ => return Err(BlockError::Corrupt),
                        };
                        let alpha = f64::from_le_bytes(c.take(8)?.try_into().unwrap());
                        let max_bins =
                            u32::try_from(c.varint()?).map_err(|_| BlockError::Corrupt)?;
                        let config = SketchConfig {
                            alpha,
                            mapping,
                            max_bins,
                        };
                        for _ in 0..n {
                            let mut stat = || -> Result<f64, BlockError> {
                                Ok(f64::from_le_bytes(c.take(8)?.try_into().unwrap()))
                            };
                            let (sum, min, max) = (stat()?, stat()?, stat()?);
                            let (sketch, used) =
                                DDSketch::decode_bins(config, sum, min, max, &data[c.pos..])
                                    .map_err(|_| BlockError::Corrupt)?;
                            c.pos += used;
                            out.push(Value::Distribution(Box::new(sketch)));
                        }
                    }
                    _ => return Err(BlockError::Corrupt),
                }
                out
            }
        };
        Ok(starts.into_iter().zip(values).collect())
    }
}

fn kind_from(b: u8) -> Option<Kind> {
    match b {
        0 => Some(Kind::Counter),
        1 => Some(Kind::Gauge),
        2 => Some(Kind::Distribution),
        _ => None,
    }
}

fn zigzag(v: i64) -> u64 {
    ((v << 1) ^ (v >> 63)) as u64
}

fn unzigzag(v: u64) -> i64 {
    (v >> 1) as i64 ^ -((v & 1) as i64)
}

fn put_varint(out: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        out.push(v as u8 | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
}

fn put_bytes(out: &mut Vec<u8>, bytes: &[u8]) {
    put_varint(out, bytes.len() as u64);
    out.extend_from_slice(bytes);
}

fn put_str(out: &mut Vec<u8>, s: &str) {
    put_bytes(out, s.as_bytes());
}

struct Cursor<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    fn new(buf: &'a [u8], pos: usize) -> Self {
        Cursor { buf, pos }
    }

    fn byte(&mut self) -> Result<u8, BlockError> {
        let b = *self.buf.get(self.pos).ok_or(BlockError::Corrupt)?;
        self.pos += 1;
        Ok(b)
    }

    fn varint(&mut self) -> Result<u64, BlockError> {
        let mut v = 0u64;
        for shift in (0..64).step_by(7) {
            let b = self.byte()?;
            v |= ((b & 0x7f) as u64) << shift;
            if b < 0x80 {
                return Ok(v);
            }
        }
        Err(BlockError::Corrupt)
    }

    fn take(&mut self, len: usize) -> Result<&'a [u8], BlockError> {
        let end = self.pos.checked_add(len).ok_or(BlockError::Corrupt)?;
        let out = self.buf.get(self.pos..end).ok_or(BlockError::Corrupt)?;
        self.pos = end;
        Ok(out)
    }

    fn bytes(&mut self) -> Result<&'a [u8], BlockError> {
        let len = self.varint()? as usize;
        let end = self.pos.checked_add(len).ok_or(BlockError::Corrupt)?;
        let out = self.buf.get(self.pos..end).ok_or(BlockError::Corrupt)?;
        self.pos = end;
        Ok(out)
    }

    fn str(&mut self) -> Result<String, BlockError> {
        String::from_utf8(self.bytes()?.to_vec()).map_err(|_| BlockError::Corrupt)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use crate::aggregator::{Aggregator, Config};
    use crate::protocol::parse_line;
    use proptest::prelude::*;

    fn buckets(lines: &[(u64, &str)]) -> Vec<Bucket> {
        let mut agg = Aggregator::new(Config::default());
        let mut out = Vec::new();
        for &(now, line) in lines {
            agg.insert(now, "acme", &mut parse_line(line).unwrap())
                .unwrap();
            out.extend(agg.flush(now));
        }
        out.extend(agg.flush_all());
        out
    }

    fn sample() -> Vec<Bucket> {
        let mut lines = Vec::new();
        for i in 0..50u64 {
            let t = 1_761_000_000 + i * 10;
            lines.push((t, format!("http.req:{}|c|#route:/a", i % 7 + 1)));
            lines.push((t, format!("http.req:{}|c|#route:/b", i % 3 + 1)));
            lines.push((t, format!("queue:{}|g", 100 - i)));
            lines.push((t + 3, format!("queue:{}|g", 90 - i)));
            lines.push((t, format!("lat:{}:{}|ms|#route:/a", i + 5, i * 2 + 7)));
        }
        let owned: Vec<(u64, String)> = lines;
        let refs: Vec<(u64, &str)> = owned.iter().map(|(t, l)| (*t, l.as_str())).collect();
        buckets(&refs)
    }

    fn same(a: &Value, b: &Value) -> bool {
        match (a, b) {
            (Value::Counter(x), Value::Counter(y)) => x.to_bits() == y.to_bits(),
            (Value::Gauge(x), Value::Gauge(y)) => x == y,
            (Value::Distribution(x), Value::Distribution(y)) => x.to_bytes() == y.to_bytes(),
            _ => false,
        }
    }

    #[test]
    fn roundtrips_every_kind() {
        let input = sample();
        let bytes = encode("acme", 10, &input);
        let r = Reader::open(&bytes).unwrap();
        assert_eq!(r.tenant, "acme");
        assert_eq!(r.index.len(), 4);
        let mut seen = 0;
        for e in &r.index {
            let decoded = r.series(e).unwrap();
            assert_eq!(decoded.len(), e.buckets);
            for (start, value) in &decoded {
                let original = input
                    .iter()
                    .find(|b| {
                        b.start == *start && b.series.name == e.name && b.series.tags == e.tags
                    })
                    .unwrap();
                assert!(same(&original.value, value), "{} at {start}", e.name);
                seen += 1;
            }
        }
        assert_eq!(seen, input.len());
    }

    #[test]
    fn find_by_name_and_tag() {
        let bytes = encode("acme", 10, &sample());
        let r = Reader::open(&bytes).unwrap();
        let req = r.find("http.req");
        assert_eq!(req.len(), 2);
        assert!(req.iter().any(|e| e.tag("route") == Some("/b")));
        assert!(r.find("missing").is_empty());
        assert_eq!(r.find("queue")[0].kind, Kind::Gauge);
    }

    #[test]
    fn partial_buckets_keep_flush_order() {
        // Two partial buckets for the window at 100: the second arrives late.
        let input = buckets(&[(100, "g:1|g"), (120, "g:2|g|T105"), (140, "g:3|g|T107")]);
        assert_eq!(input.len(), 2);
        let bytes = encode("acme", 10, &input);
        let r = Reader::open(&bytes).unwrap();
        let decoded = r.series(&r.index[0]).unwrap();
        assert_eq!(decoded.len(), 2);
        assert!(decoded.iter().all(|(s, _)| *s == 100));
        let mut merged = decoded[0].1.clone();
        merged.merge(&decoded[1].1).unwrap();
        let Value::Gauge(g) = merged else { panic!() };
        assert_eq!((g.last, g.last_at, g.count), (3.0, 107, 3));
    }

    #[test]
    fn mixed_sketch_configs_fall_back_to_full_sketches() {
        let key = Arc::new(crate::series::SeriesKey {
            tenant: "acme".into(),
            name: "lat".into(),
            kind: Kind::Distribution,
            tags: vec![],
        });
        let sketch = |alpha: f64, x: f64| {
            let mut s = DDSketch::new(SketchConfig {
                alpha,
                ..SketchConfig::default()
            });
            s.add(x);
            Value::Distribution(Box::new(s))
        };
        let input = vec![
            Bucket {
                start: 100,
                series: key.clone(),
                value: sketch(0.01, 5.0),
            },
            Bucket {
                start: 110,
                series: key,
                value: sketch(0.02, 7.0),
            },
        ];
        let bytes = encode("acme", 10, &input);
        let r = Reader::open(&bytes).unwrap();
        let decoded = r.series(&r.index[0]).unwrap();
        for (b, (start, v)) in input.iter().zip(&decoded) {
            assert_eq!(b.start, *start);
            assert!(same(&b.value, v));
        }
    }

    #[test]
    fn reads_version_1_blocks_as_10_second_buckets() {
        let input = sample();
        let mut b = Builder::new_v1("acme");
        let mut by: BTreeMap<SeriesOrder, Vec<(u64, &Value)>> = BTreeMap::new();
        for x in &input {
            let key = (
                x.series.name.clone(),
                x.series.kind as u8,
                x.series.tags.clone(),
            );
            by.entry(key).or_default().push((x.start, &x.value));
        }
        for ((name, _, tags), series) in &by {
            b.add_series(name, tags, series);
        }
        let bytes = b.finish();
        let r = Reader::open(&bytes).unwrap();
        assert_eq!((r.tenant.as_str(), r.resolution), ("acme", 10));
        let total: usize = r.index.iter().map(|e| r.series(e).unwrap().len()).sum();
        assert_eq!(total, input.len());
        let v2 = encode("acme", 60, &input);
        assert_eq!(Reader::open(&v2).unwrap().resolution, 60);
    }

    #[test]
    fn rejects_bad_headers() {
        assert_eq!(Reader::open(b"nope").err(), Some(BlockError::Magic));
        let mut bytes = encode("acme", 10, &sample());
        bytes[4] = 9;
        assert_eq!(Reader::open(&bytes).err(), Some(BlockError::Version(9)));
    }

    proptest! {
        #[test]
        fn corrupted_blocks_never_panic(flips in prop::collection::vec((any::<usize>(), any::<u8>()), 1..8)) {
            let mut bytes = encode("acme", 10, &sample());
            for (i, b) in flips {
                let i = i % bytes.len();
                bytes[i] ^= b | 1;
            }
            if let Ok(r) = Reader::open(&bytes) {
                for e in &r.index {
                    let _ = r.series(e);
                }
            }
        }
    }
}
