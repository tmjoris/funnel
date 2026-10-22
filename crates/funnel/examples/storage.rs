//! Compares ways of storing the same metrics: raw points against 10-second
//! rollups, and several encodings of the rollups.
//!
//!     cargo run --release -p funnel --example storage -- [minutes]

use std::collections::HashMap;
use std::io::Write;
use std::sync::Arc;
use std::time::Instant;

use arrow_array::builder::{
    BinaryBuilder, Float64Builder, StringBuilder, UInt8Builder, UInt64Builder,
};
use arrow_array::{ArrayRef, RecordBatch};
use funnel::aggregator::{Aggregator, Bucket, Config, Value};
use funnel::protocol::parse_line;
use funnel::series::{normalize_tags, series_id};
use funnel::storage::block;
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;

const ZSTD_LEVEL: i32 = 3;

struct Row {
    name: &'static str,
    bytes: usize,
    encode_ms: f64,
    decode_ms: Option<f64>,
}

fn main() {
    let minutes: u64 = std::env::args().nth(1).map_or(10, |m| m.parse().unwrap());
    let mut workload = loadgen::Workload::new(loadgen::Config::default());
    let mut agg = Aggregator::new(Config::default());
    let mut buckets: Vec<Bucket> = Vec::new();

    let mut raw_text = zstd::stream::Encoder::new(Vec::new(), ZSTD_LEVEL).unwrap();
    let mut raw_text_bytes = 0usize;
    let mut ids: HashMap<u64, u32> = HashMap::new();
    let mut points: Vec<(u32, u64, f64)> = Vec::new();
    let mut agg_time = 0.0;

    let mut lines = Vec::new();
    for _ in 0..minutes * 60 {
        lines.clear();
        let now = workload.next_second(&mut lines);
        for (i, line) in lines.iter().enumerate() {
            raw_text_bytes += line.len() + 1;
            raw_text.write_all(line.as_bytes()).unwrap();
            raw_text.write_all(b"\n").unwrap();
            let mut l = parse_line(line).unwrap();
            normalize_tags(&mut l.tags);
            let id = series_id("default", l.name, l.kind, &l.tags).0;
            let next = ids.len() as u32;
            let dense = *ids.entry(id).or_insert(next);
            // Millisecond timestamps, spread across the second like real
            // arrivals, as a raw store would keep them.
            let ms = now * 1000 + (i as u64 * 1000 / lines.len() as u64);
            points.push((dense, ms, l.values[0]));
        }
        let t = Instant::now();
        for line in &lines {
            agg.insert(now, "default", &mut parse_line(line).unwrap())
                .unwrap();
        }
        buckets.extend(agg.flush(now));
        agg_time += t.elapsed().as_secs_f64();
    }
    buckets.extend(agg.flush_all());
    let raw_text = raw_text.finish().unwrap();
    let n = points.len();

    println!(
        "workload: {minutes} min, {n} points, {} series, {} buckets",
        ids.len(),
        buckets.len()
    );
    println!(
        "rollup: {:.2} M lines/s on one core (parse + aggregate)",
        n as f64 / agg_time / 1e6
    );
    println!(
        "raw StatsD text: {raw_text_bytes} bytes ({:.1} per point)\n",
        raw_text_bytes as f64 / n as f64
    );

    let mut rows = vec![Row {
        name: "raw StatsD text + zstd",
        bytes: raw_text.len(),
        encode_ms: f64::NAN,
        decode_ms: None,
    }];
    rows.push(raw_columns(&mut points));
    rows.push(rollup_rows(&buckets));
    let (gorilla, gorilla_bytes) = gorilla(&buckets);
    let gorilla_ms = gorilla.encode_ms;
    rows.push(gorilla);
    rows.push(gorilla_zstd(&gorilla_bytes, gorilla_ms));
    rows.push(parquet(&buckets));

    println!(
        "{:<34} {:>12} {:>10} {:>10} {:>10} {:>10}",
        "format", "bytes", "B/point", "B/bucket", "encode ms", "decode ms"
    );
    for r in &rows {
        let per_bucket = if r.name.starts_with("raw") {
            "-".to_string()
        } else {
            format!("{:.1}", r.bytes as f64 / buckets.len() as f64)
        };
        println!(
            "{:<34} {:>12} {:>10.3} {:>10} {:>10} {:>10}",
            r.name,
            r.bytes,
            r.bytes as f64 / n as f64,
            per_bucket,
            if r.encode_ms.is_nan() {
                "-".into()
            } else {
                format!("{:.0}", r.encode_ms)
            },
            r.decode_ms.map_or("-".into(), |d| format!("{d:.0}")),
        );
    }
    println!(
        "\n* Parquet decode stops at Arrow arrays and leaves sketches as bytes; \
         the Gorilla rows decode every sketch."
    );
}

/// Every point as (series, timestamp ms, value), sorted by series then time
/// like a column store's primary key, each column compressed with zstd.
fn raw_columns(points: &mut [(u32, u64, f64)]) -> Row {
    let t = Instant::now();
    points.sort_unstable_by_key(|p| (p.0, p.1));
    let mut total = 0;
    let series: Vec<u8> = points.iter().flat_map(|p| p.0.to_le_bytes()).collect();
    let times: Vec<u8> = points.iter().flat_map(|p| p.1.to_le_bytes()).collect();
    let values: Vec<u8> = points.iter().flat_map(|p| p.2.to_le_bytes()).collect();
    for col in [series, times, values] {
        total += zstd::bulk::compress(&col, ZSTD_LEVEL).unwrap().len();
    }
    Row {
        name: "raw columns + zstd",
        bytes: total,
        encode_ms: t.elapsed().as_secs_f64() * 1e3,
        decode_ms: None,
    }
}

fn series_label(b: &Bucket) -> String {
    let tags: Vec<String> = b
        .series
        .tags
        .iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect();
    format!("{}|{}", b.series.name, tags.join(","))
}

/// Rollups serialized one bucket after another, compressed with zstd only.
fn rollup_rows(buckets: &[Bucket]) -> Row {
    let t = Instant::now();
    let mut out = Vec::new();
    for b in buckets {
        out.extend_from_slice(series_label(b).as_bytes());
        out.push(0);
        out.extend_from_slice(&b.start.to_le_bytes());
        match &b.value {
            Value::Counter(c) => out.extend_from_slice(&c.to_le_bytes()),
            Value::Gauge(g) => {
                for v in [g.min, g.max, g.last, g.sum] {
                    out.extend_from_slice(&v.to_le_bytes());
                }
                out.extend_from_slice(&g.count.to_le_bytes());
                out.extend_from_slice(&g.last_at.to_le_bytes());
            }
            Value::Distribution(s) => s.encode(&mut out),
        }
    }
    let bytes = zstd::bulk::compress(&out, ZSTD_LEVEL).unwrap().len();
    Row {
        name: "rollups, row by row + zstd",
        bytes,
        encode_ms: t.elapsed().as_secs_f64() * 1e3,
        decode_ms: None,
    }
}

fn gorilla(buckets: &[Bucket]) -> (Row, Vec<u8>) {
    let t = Instant::now();
    let bytes = block::encode("default", 10, buckets);
    let encode_ms = t.elapsed().as_secs_f64() * 1e3;
    let t = Instant::now();
    let reader = block::Reader::open(&bytes).unwrap();
    let mut decoded = 0;
    for e in &reader.index {
        decoded += reader.series(e).unwrap().len();
    }
    assert_eq!(decoded, buckets.len());
    let mut by_kind: std::collections::BTreeMap<String, (usize, usize)> = Default::default();
    for e in &reader.index {
        let k = by_kind.entry(format!("{:?}", e.kind)).or_default();
        k.0 += e.buckets;
        k.1 += reader.series_len(e);
    }
    for (kind, (n, b)) in &by_kind {
        println!(
            "  gorilla {kind:<12} {n:>8} buckets {b:>10} bytes {:>6.1} B/bucket",
            *b as f64 / *n as f64
        );
    }
    let index = bytes.len() - by_kind.values().map(|v| v.1).sum::<usize>();
    println!("  gorilla index+header  {index:>10} bytes\n");
    let row = Row {
        name: "rollups, Gorilla blocks",
        bytes: bytes.len(),
        encode_ms,
        decode_ms: Some(t.elapsed().as_secs_f64() * 1e3),
    };
    (row, bytes)
}

/// Times include building the block, so the row stands on its own.
fn gorilla_zstd(block: &[u8], block_ms: f64) -> Row {
    let t = Instant::now();
    let compressed = zstd::bulk::compress(block, ZSTD_LEVEL).unwrap();
    let encode_ms = block_ms + t.elapsed().as_secs_f64() * 1e3;
    // Decompress and decode every series, like the plain Gorilla row.
    let t = Instant::now();
    let back = zstd::bulk::decompress(&compressed, block.len()).unwrap();
    let reader = block::Reader::open(&back).unwrap();
    for e in &reader.index {
        reader.series(e).unwrap();
    }
    Row {
        name: "rollups, Gorilla blocks + zstd",
        bytes: compressed.len(),
        encode_ms,
        decode_ms: Some(t.elapsed().as_secs_f64() * 1e3),
    }
}

fn parquet(buckets: &[Bucket]) -> Row {
    let t = Instant::now();
    let mut sorted: Vec<&Bucket> = buckets.iter().collect();
    sorted.sort_by_cached_key(|b| (series_label(b), b.start));

    let mut name = StringBuilder::new();
    let mut tags = StringBuilder::new();
    let mut kind = UInt8Builder::new();
    let mut start = UInt64Builder::new();
    let mut counter = Float64Builder::new();
    let (mut gmin, mut gmax, mut glast, mut gsum) = (
        Float64Builder::new(),
        Float64Builder::new(),
        Float64Builder::new(),
        Float64Builder::new(),
    );
    let (mut gcount, mut glast_at) = (UInt64Builder::new(), UInt64Builder::new());
    let mut sketch = BinaryBuilder::new();
    for b in &sorted {
        name.append_value(&b.series.name);
        let t: Vec<String> = b
            .series
            .tags
            .iter()
            .map(|(k, v)| format!("{k}={v}"))
            .collect();
        tags.append_value(t.join(","));
        kind.append_value(b.series.kind as u8);
        start.append_value(b.start);
        let (c, g, s) = match &b.value {
            Value::Counter(c) => (Some(*c), None, None),
            Value::Gauge(g) => (None, Some(*g), None),
            Value::Distribution(s) => (None, None, Some(s.to_bytes())),
        };
        counter.append_option(c);
        gmin.append_option(g.map(|g| g.min));
        gmax.append_option(g.map(|g| g.max));
        glast.append_option(g.map(|g| g.last));
        gsum.append_option(g.map(|g| g.sum));
        gcount.append_option(g.map(|g| g.count));
        glast_at.append_option(g.map(|g| g.last_at));
        sketch.append_option(s);
    }
    let columns: Vec<(&str, ArrayRef)> = vec![
        ("name", Arc::new(name.finish())),
        ("tags", Arc::new(tags.finish())),
        ("kind", Arc::new(kind.finish())),
        ("start", Arc::new(start.finish())),
        ("counter", Arc::new(counter.finish())),
        ("gauge_min", Arc::new(gmin.finish())),
        ("gauge_max", Arc::new(gmax.finish())),
        ("gauge_last", Arc::new(glast.finish())),
        ("gauge_sum", Arc::new(gsum.finish())),
        ("gauge_count", Arc::new(gcount.finish())),
        ("gauge_last_at", Arc::new(glast_at.finish())),
        ("sketch", Arc::new(sketch.finish())),
    ];
    let batch = RecordBatch::try_from_iter(columns).unwrap();
    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(ZSTD_LEVEL).unwrap()))
        .build();
    let mut out = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut out, batch.schema(), Some(props)).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    let encode_ms = t.elapsed().as_secs_f64() * 1e3;

    let t = Instant::now();
    let reader = ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(out.clone()))
        .unwrap()
        .build()
        .unwrap();
    let rows: usize = reader.map(|b| b.unwrap().num_rows()).sum();
    assert_eq!(rows, buckets.len());
    Row {
        name: "rollups, Parquet (zstd) *",
        bytes: out.len(),
        encode_ms,
        decode_ms: Some(t.elapsed().as_secs_f64() * 1e3),
    }
}
