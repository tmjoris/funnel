//! Compact binary form, used when rolled-up distributions go to disk.
//!
//! Layout: version, mapping kind, alpha (f64), max_bins, sum, min, max, then
//! the bins: a flags byte, the zero count, and each store as `len` followed by
//! `(index delta, count)` pairs, all varints. Index deltas are zigzagged;
//! neighbouring bins have delta 1. Counts are in units of 1/65536 of a value
//! in memory. When every count is a whole number of values, which is the
//! case without sampling and for rates like 0.1 or 0.5, they are written as
//! values and a typical bin costs two bytes; otherwise (rate 0.3) they are
//! written in units, about four bytes per bin. The total count is not
//! stored; it is the sum of the bins.

use crate::mapping::{Mapping, MappingKind};
use crate::store::{Store, UNIT};
use crate::{Config, DDSketch};

const VERSION: u8 = 3;
/// Counts are written in units rather than whole values.
const UNIT_COUNTS: u8 = 1;

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

fn whole(c: u64) -> bool {
    c.is_multiple_of(UNIT)
}

impl DDSketch {
    pub fn encode(&self, out: &mut Vec<u8>) {
        out.push(VERSION);
        out.push(self.mapping.kind() as u8);
        out.extend_from_slice(&self.mapping.alpha().to_le_bytes());
        put_varint(out, self.positive.max_bins() as u64);
        for v in [self.sum, self.min, self.max] {
            out.extend_from_slice(&v.to_le_bytes());
        }
        self.encode_bins(out);
    }

    /// Writes only the bins and the zero count, for formats that store the
    /// configuration once and sum, min and max in columns of their own.
    /// Read back with [`DDSketch::decode_bins`].
    pub fn encode_bins(&self, out: &mut Vec<u8>) {
        let units = !(whole(self.zero)
            && self.positive.iter().all(|(_, c)| whole(c))
            && self.negative.iter().all(|(_, c)| whole(c)));
        out.push(if units { UNIT_COUNTS } else { 0 });
        put_count(out, self.zero, units);
        encode_store(&self.positive, units, out);
        encode_store(&self.negative, units, out);
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
        let mapping = match r.byte()? {
            0 => MappingKind::Logarithmic,
            1 => MappingKind::Cubic,
            m => return Err(DecodeError::Mapping(m)),
        };
        let alpha = r.f64()?;
        let max_bins = r.varint()?;
        if max_bins > u32::MAX as u64 {
            return Err(DecodeError::Invalid);
        }
        let config = Config {
            alpha,
            mapping,
            max_bins: max_bins as u32,
        };
        let (sum, min, max) = (r.f64()?, r.f64()?, r.f64()?);
        let (sketch, used) = Self::decode_bins(config, sum, min, max, &buf[r.pos..])?;
        Ok((sketch, r.pos + used))
    }

    /// Reads what [`DDSketch::encode_bins`] wrote, given the configuration
    /// and the sum, min and max stored elsewhere. Returns the sketch and the
    /// number of bytes consumed.
    pub fn decode_bins(
        config: Config,
        sum: f64,
        min: f64,
        max: f64,
        buf: &[u8],
    ) -> Result<(DDSketch, usize), DecodeError> {
        if !(config.alpha > 0.0 && config.alpha < 1.0) || config.max_bins == 0 {
            return Err(DecodeError::Invalid);
        }
        let mut r = Reader { buf, pos: 0 };
        let units = match r.byte()? {
            0 => false,
            UNIT_COUNTS => true,
            _ => return Err(DecodeError::Invalid),
        };
        let zero = r.count(units, true)?;
        // Bins use plain adds, so the decoder bounds the running total itself.
        let mut total = zero;
        let positive = decode_store(&mut r, config.max_bins, units, &mut total)?;
        let negative = decode_store(&mut r, config.max_bins, units, &mut total)?;
        let count = total;
        let sketch = DDSketch {
            mapping: Mapping::new(config.mapping, config.alpha),
            positive,
            negative,
            zero,
            count,
            sum,
            min: if count > 0 { min } else { f64::INFINITY },
            max: if count > 0 { max } else { f64::NEG_INFINITY },
        };
        Ok((sketch, r.pos))
    }
}

fn put_count(out: &mut Vec<u8>, c: u64, units: bool) {
    put_varint(out, if units { c } else { c / UNIT });
}

fn encode_store(store: &Store, units: bool, out: &mut Vec<u8>) {
    let bins: Vec<_> = store.iter().collect();
    put_varint(out, bins.len() as u64);
    let mut prev = 0i64;
    for (i, c) in bins {
        put_varint(out, zigzag(i as i64 - prev));
        put_count(out, c, units);
        prev = i as i64;
    }
}

fn decode_store(
    r: &mut Reader,
    max_bins: u32,
    units: bool,
    total: &mut u64,
) -> Result<Store, DecodeError> {
    let len = r.varint()?;
    let mut store = Store::new(max_bins);
    let mut index = 0i64;
    for _ in 0..len {
        index += unzigzag(r.varint()?);
        let i = i32::try_from(index).map_err(|_| DecodeError::Invalid)?;
        let c = r.count(units, false)?;
        *total = total.checked_add(c).ok_or(DecodeError::Invalid)?;
        store.add(i, c);
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

    /// A count in units. A bin count must be positive; the zero count may
    /// also be 0.
    fn count(&mut self, units: bool, allow_zero: bool) -> Result<u64, DecodeError> {
        let v = self.varint()?;
        let c = if units {
            v
        } else {
            v.checked_mul(UNIT).ok_or(DecodeError::Invalid)?
        };
        if c > 0 || allow_zero {
            Ok(c)
        } else {
            Err(DecodeError::Invalid)
        }
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
    fn bins_only_roundtrip() {
        let mut s = DDSketch::default();
        for x in [-2.0, 0.0, 3.5, 3.5, 1e4] {
            s.add_n(x, 1.0 / 0.3);
        }
        let mut bins = Vec::new();
        s.encode_bins(&mut bins);
        let (sum, min, max) = (s.sum(), s.min().unwrap(), s.max().unwrap());
        let (d, used) = DDSketch::decode_bins(s.config(), sum, min, max, &bins).unwrap();
        assert_eq!(used, bins.len());
        assert_eq!(d.to_bytes(), s.to_bytes());
        let bad = Config {
            alpha: 0.0,
            ..s.config()
        };
        assert!(DDSketch::decode_bins(bad, sum, min, max, &bins).is_err());
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
        // 100 bins: about 2 bytes each as whole values, 4 in units.
        assert!(
            w.len() < 260 && f.len() < 460,
            "{} and {}",
            w.len(),
            f.len()
        );
        let (d, _) = DDSketch::decode(&f).unwrap();
        assert_eq!(d.to_bytes(), f);
        assert!((d.count() - 333.33).abs() < 0.01);
    }
}
