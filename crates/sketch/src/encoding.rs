//! Compact binary form, used when rolled-up distributions go to disk.
//!
//! Layout: version, mapping kind, alpha (f64), max_bins, a flags byte, zero
//! count, sum, min, max, then each store as `len` followed by
//! `(index delta, count)` pairs. Index deltas are zigzag varints;
//! neighbouring bins have delta 1. Counts are varints when every count in
//! the sketch is a whole number, which is the case without sampling and for
//! rates like 0.1 or 0.5, so a typical bin costs two bytes. Fractional
//! counts (rate 0.3) are stored as raw f64. The total count is not stored;
//! it is the sum of the bins.

use crate::DDSketch;
use crate::mapping::{Mapping, MappingKind};
use crate::store::Store;

const VERSION: u8 = 2;
const FLOAT_COUNTS: u8 = 1;
/// Largest f64 below which every whole number is exact.
const MAX_EXACT: f64 = (1u64 << 53) as f64;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DecodeError {
    Truncated,
    Version(u8),
    Mapping(u8),
    Invalid,
}

impl std::fmt::Display for DecodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DecodeError::Truncated => f.write_str("truncated sketch"),
            DecodeError::Version(v) => write!(f, "unknown sketch version {v}"),
            DecodeError::Mapping(m) => write!(f, "unknown mapping {m}"),
            DecodeError::Invalid => f.write_str("invalid sketch"),
        }
    }
}

impl std::error::Error for DecodeError {}

fn whole(c: f64) -> bool {
    c.fract() == 0.0 && c < MAX_EXACT
}

impl DDSketch {
    pub fn encode(&self, out: &mut Vec<u8>) {
        let float = !(whole(self.zero)
            && self.positive.iter().all(|(_, c)| whole(c))
            && self.negative.iter().all(|(_, c)| whole(c)));
        out.push(VERSION);
        out.push(self.mapping.kind() as u8);
        out.extend_from_slice(&self.mapping.alpha().to_le_bytes());
        put_varint(out, self.positive.max_bins() as u64);
        out.push(if float { FLOAT_COUNTS } else { 0 });
        put_count(out, self.zero, float);
        for v in [self.sum, self.min, self.max] {
            out.extend_from_slice(&v.to_le_bytes());
        }
        encode_store(&self.positive, float, out);
        encode_store(&self.negative, float, out);
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut out = Vec::new();
        self.encode(&mut out);
        out
    }

    /// Decodes a sketch and returns it with the number of bytes consumed.
    pub fn decode(buf: &[u8]) -> Result<(DDSketch, usize), DecodeError> {
        let mut r = Reader { buf, pos: 0 };
        let version = r.byte()?;
        if version != VERSION {
            return Err(DecodeError::Version(version));
        }
        let kind = match r.byte()? {
            0 => MappingKind::Logarithmic,
            1 => MappingKind::Cubic,
            m => return Err(DecodeError::Mapping(m)),
        };
        let alpha = r.f64()?;
        if !(alpha > 0.0 && alpha < 1.0) {
            return Err(DecodeError::Invalid);
        }
        let max_bins = r.varint()?;
        if max_bins == 0 || max_bins > u32::MAX as u64 {
            return Err(DecodeError::Invalid);
        }
        let float = match r.byte()? {
            0 => false,
            FLOAT_COUNTS => true,
            _ => return Err(DecodeError::Invalid),
        };
        let zero = r.count(float, true)?;
        let (sum, min, max) = (r.f64()?, r.f64()?, r.f64()?);
        let positive = decode_store(&mut r, max_bins as u32, float)?;
        let negative = decode_store(&mut r, max_bins as u32, float)?;
        let count = zero + positive.count() + negative.count();
        let sketch = DDSketch {
            mapping: Mapping::new(kind, alpha),
            positive,
            negative,
            zero,
            count,
            sum,
            min: if count > 0.0 { min } else { f64::INFINITY },
            max: if count > 0.0 { max } else { f64::NEG_INFINITY },
        };
        Ok((sketch, r.pos))
    }
}

fn put_count(out: &mut Vec<u8>, c: f64, float: bool) {
    if float {
        out.extend_from_slice(&c.to_le_bytes());
    } else {
        put_varint(out, c as u64);
    }
}

fn encode_store(store: &Store, float: bool, out: &mut Vec<u8>) {
    let bins: Vec<_> = store.iter().collect();
    put_varint(out, bins.len() as u64);
    let mut prev = 0i64;
    for (i, c) in bins {
        put_varint(out, zigzag(i as i64 - prev));
        put_count(out, c, float);
        prev = i as i64;
    }
}

fn decode_store(r: &mut Reader, max_bins: u32, float: bool) -> Result<Store, DecodeError> {
    let len = r.varint()?;
    let mut store = Store::new(max_bins);
    let mut index = 0i64;
    for _ in 0..len {
        index += unzigzag(r.varint()?);
        let i = i32::try_from(index).map_err(|_| DecodeError::Invalid)?;
        store.add(i, r.count(float, false)?);
    }
    Ok(store)
}

fn zigzag(v: i64) -> u64 {
    ((v << 1) ^ (v >> 63)) as u64
}

fn unzigzag(v: u64) -> i64 {
    (v >> 1) as i64 ^ -((v & 1) as i64)
}

pub(crate) fn put_varint(out: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        out.push(v as u8 | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
}

struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl Reader<'_> {
    fn byte(&mut self) -> Result<u8, DecodeError> {
        let b = *self.buf.get(self.pos).ok_or(DecodeError::Truncated)?;
        self.pos += 1;
        Ok(b)
    }

    fn f64(&mut self) -> Result<f64, DecodeError> {
        let bytes = self
            .buf
            .get(self.pos..self.pos + 8)
            .ok_or(DecodeError::Truncated)?;
        self.pos += 8;
        Ok(f64::from_le_bytes(bytes.try_into().unwrap()))
    }

    /// A bin count must be positive; the zero count may also be 0.
    fn count(&mut self, float: bool, allow_zero: bool) -> Result<f64, DecodeError> {
        let c = if float {
            self.f64()?
        } else {
            self.varint()? as f64
        };
        let ok = c.is_finite() && (c > 0.0 || (allow_zero && c == 0.0));
        if ok { Ok(c) } else { Err(DecodeError::Invalid) }
    }

    fn varint(&mut self) -> Result<u64, DecodeError> {
        let mut v = 0u64;
        for shift in (0..64).step_by(7) {
            let b = self.byte()?;
            v |= ((b & 0x7f) as u64) << shift;
            if b < 0x80 {
                return Ok(v);
            }
        }
        Err(DecodeError::Invalid)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zigzag_roundtrip() {
        for v in [0, 1, -1, 63, -64, i32::MAX as i64, i32::MIN as i64] {
            assert_eq!(unzigzag(zigzag(v)), v);
        }
    }

    #[test]
    fn roundtrip() {
        let mut s = DDSketch::default();
        for x in [-3.5, 0.0, 1.0, 2.0, 2.0, 1e6] {
            s.add(x);
        }
        let bytes = s.to_bytes();
        let (d, used) = DDSketch::decode(&bytes).unwrap();
        assert_eq!(used, bytes.len());
        assert_eq!(d.to_bytes(), bytes);
        assert_eq!(d.quantile(0.5), s.quantile(0.5));
    }

    #[test]
    fn rejects_garbage() {
        assert_eq!(DDSketch::decode(&[]).unwrap_err(), DecodeError::Truncated);
        assert_eq!(DDSketch::decode(&[9]).unwrap_err(), DecodeError::Version(9));
        let bytes = DDSketch::default().to_bytes();
        assert!(DDSketch::decode(&bytes[..bytes.len() - 1]).is_err());
    }

    #[test]
    fn whole_counts_stay_compact_and_fractional_roundtrip() {
        let mut whole = DDSketch::default();
        let mut frac = DDSketch::default();
        for x in 1..=100 {
            whole.add_n(x as f64, 10.0);
            frac.add_n(x as f64, 1.0 / 0.3);
        }
        let (w, f) = (whole.to_bytes(), frac.to_bytes());
        assert!(f.len() > w.len() * 3, "{} vs {}", f.len(), w.len());
        let (d, _) = DDSketch::decode(&f).unwrap();
        assert_eq!(d.to_bytes(), f);
        assert!((d.count() - 333.33).abs() < 0.01);
    }
}
