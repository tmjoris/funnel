//! Bit-level codecs from Facebook's Gorilla paper (Pelkonen et al.,
//! "Gorilla: A Fast, Scalable, In-Memory Time Series Database", VLDB 2015).
//!
//! Timestamps are stored as the change in the gap between consecutive
//! timestamps ("delta of delta"). Buckets arrive at a fixed 10-second step,
//! so that change is almost always 0 and costs a single bit.
//!
//! Floats are stored as the XOR with the previous value. Similar values
//! share their sign, exponent and leading mantissa bits, so the XOR is
//! mostly zeros; only the run of bits between the leading and trailing
//! zeros is written, and when it fits inside the previous run, its position
//! is not repeated.

#[derive(Default)]
pub struct BitWriter {
    bytes: Vec<u8>,
    acc: u64,
    /// Bits held in `acc`, always below 8 between calls.
    used: u32,
}

impl BitWriter {
    /// Writes the low `n` bits of `v`, most significant first. `n <= 64`.
    pub fn write(&mut self, v: u64, n: u32) {
        debug_assert!(n <= 64);
        if n == 0 {
            return;
        }
        if n > 56 {
            self.write(v >> 32, n - 32);
            self.write(v & 0xffff_ffff, 32);
            return;
        }
        let v = if n == 64 { v } else { v & ((1 << n) - 1) };
        self.acc = (self.acc << n) | v;
        self.used += n;
        while self.used >= 8 {
            self.used -= 8;
            self.bytes.push((self.acc >> self.used) as u8);
        }
        self.acc &= (1 << self.used) - 1;
    }

    pub fn bit(&mut self, b: bool) {
        self.write(b as u64, 1);
    }

    /// Pads to a whole byte with zeros and returns the bytes.
    pub fn finish(mut self) -> Vec<u8> {
        if self.used > 0 {
            self.bytes.push((self.acc << (8 - self.used)) as u8);
        }
        self.bytes
    }
}

pub struct BitReader<'a> {
    bytes: &'a [u8],
    pos: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Truncated;

impl<'a> BitReader<'a> {
    pub fn new(bytes: &'a [u8]) -> Self {
        BitReader { bytes, pos: 0 }
    }

    pub fn read(&mut self, n: u32) -> Result<u64, Truncated> {
        debug_assert!(n <= 64);
        if self.pos + n as usize > self.bytes.len() * 8 {
            return Err(Truncated);
        }
        let mut v = 0u64;
        let mut left = n;
        while left > 0 {
            let byte = self.bytes[self.pos / 8];
            let offset = (self.pos % 8) as u32;
            let take = left.min(8 - offset);
            let bits = (byte >> (8 - offset - take)) & ((1u16 << take) - 1) as u8;
            v = (v << take) | bits as u64;
            self.pos += take as usize;
            left -= take;
        }
        Ok(v)
    }

    pub fn bit(&mut self) -> Result<bool, Truncated> {
        Ok(self.read(1)? == 1)
    }
}

/// Encodes timestamps in order (equal neighbours allowed).
pub fn encode_times(times: &[u64]) -> Vec<u8> {
    let mut w = BitWriter::default();
    let Some(&first) = times.first() else {
        return w.finish();
    };
    w.write(first, 64);
    let (mut prev, mut prev_delta) = (first, 0i64);
    for &t in &times[1..] {
        let delta = (t as i64).wrapping_sub(prev as i64);
        let dod = delta.wrapping_sub(prev_delta);
        // Control prefixes and value widths from the paper, with the last
        // case widened to 64 bits so no input is unrepresentable.
        match dod {
            0 => w.bit(false),
            -63..=64 => {
                w.write(0b10, 2);
                w.write(dod as u64, 7);
            }
            -255..=256 => {
                w.write(0b110, 3);
                w.write(dod as u64, 9);
            }
            -2047..=2048 => {
                w.write(0b1110, 4);
                w.write(dod as u64, 12);
            }
            _ => {
                w.write(0b1111, 4);
                w.write(dod as u64, 64);
            }
        }
        prev = t;
        prev_delta = delta;
    }
    w.finish()
}

fn sign_extend(v: u64, bits: u32) -> i64 {
    let shift = 64 - bits;
    ((v << shift) as i64) >> shift
}

pub fn decode_times(bytes: &[u8], n: usize) -> Result<Vec<u64>, Truncated> {
    let mut out = Vec::with_capacity(n);
    if n == 0 {
        return Ok(out);
    }
    let mut r = BitReader::new(bytes);
    let first = r.read(64)?;
    out.push(first);
    let (mut prev, mut prev_delta) = (first, 0i64);
    for _ in 1..n {
        let dod = if !r.bit()? {
            0
        } else if !r.bit()? {
            sign_extend(r.read(7)?, 7)
        } else if !r.bit()? {
            sign_extend(r.read(9)?, 9)
        } else if !r.bit()? {
            sign_extend(r.read(12)?, 12)
        } else {
            r.read(64)? as i64
        };
        let delta = prev_delta.wrapping_add(dod);
        let t = (prev as i64).wrapping_add(delta) as u64;
        out.push(t);
        prev = t;
        prev_delta = delta;
    }
    Ok(out)
}

/// Encodes floats with Gorilla's XOR scheme.
pub fn encode_floats(values: &[f64]) -> Vec<u8> {
    let mut w = BitWriter::default();
    let Some(&first) = values.first() else {
        return w.finish();
    };
    let mut prev = first.to_bits();
    w.write(prev, 64);
    // Leading and trailing zero counts of the previous stored run; 65 means
    // no run yet, so the first non-zero XOR always writes a new window.
    let (mut lead, mut trail) = (65u32, 65u32);
    for &v in &values[1..] {
        let bits = v.to_bits();
        let xor = bits ^ prev;
        prev = bits;
        if xor == 0 {
            w.bit(false);
            continue;
        }
        w.bit(true);
        // Leading zeros are stored in 5 bits, so cap them at 31.
        let l = xor.leading_zeros().min(31);
        let t = xor.trailing_zeros();
        if lead <= 64 && l >= lead && t >= trail {
            w.bit(false);
            w.write(xor >> trail, 64 - lead - trail);
        } else {
            let len = 64 - l - t;
            w.bit(true);
            w.write(l as u64, 5);
            // len is 1..=64; store len - 1 in 6 bits.
            w.write((len - 1) as u64, 6);
            w.write(xor >> t, len);
            lead = l;
            trail = t;
        }
    }
    w.finish()
}

pub fn decode_floats(bytes: &[u8], n: usize) -> Result<Vec<f64>, Truncated> {
    let mut out = Vec::with_capacity(n);
    if n == 0 {
        return Ok(out);
    }
    let mut r = BitReader::new(bytes);
    let mut prev = r.read(64)?;
    out.push(f64::from_bits(prev));
    let (mut lead, mut trail) = (0u32, 0u32);
    for _ in 1..n {
        if r.bit()? {
            if r.bit()? {
                lead = r.read(5)? as u32;
                let len = r.read(6)? as u32 + 1;
                if lead + len > 64 {
                    return Err(Truncated);
                }
                trail = 64 - lead - len;
            }
            let len = 64 - lead - trail;
            prev ^= r.read(len)? << trail;
        }
        out.push(f64::from_bits(prev));
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn bit_writer_roundtrip() {
        let mut w = BitWriter::default();
        w.write(0b101, 3);
        w.write(u64::MAX, 64);
        w.bit(true);
        w.write(0x1234, 13);
        let bytes = w.finish();
        let mut r = BitReader::new(&bytes);
        assert_eq!(r.read(3), Ok(0b101));
        assert_eq!(r.read(64), Ok(u64::MAX));
        assert_eq!(r.bit(), Ok(true));
        assert_eq!(r.read(13), Ok(0x1234 & 0x1fff));
    }

    #[test]
    fn steady_timestamps_cost_one_bit_each() {
        let times: Vec<u64> = (0..1000).map(|i| 1_761_000_000 + i * 10).collect();
        let bytes = encode_times(&times);
        // 64 bits for the first, 9 for the first delta, then 1 bit each.
        assert!(bytes.len() <= (64 + 9 + 998usize).div_ceil(8));
        assert_eq!(decode_times(&bytes, times.len()).unwrap(), times);
    }

    #[test]
    fn repeated_floats_cost_one_bit_each() {
        let values = vec![42.5; 1000];
        let bytes = encode_floats(&values);
        assert!(bytes.len() <= (64 + 999usize).div_ceil(8));
        assert_eq!(decode_floats(&bytes, values.len()).unwrap(), values);
    }

    #[test]
    fn truncated_input_errors() {
        let bytes = encode_floats(&[1.0, 2.0, 3.0]);
        assert!(decode_floats(&bytes[..4], 3).is_err());
        assert!(decode_times(&[], 1).is_err());
    }

    proptest! {
        #[test]
        fn times_roundtrip(mut times in prop::collection::vec(0u64..u64::MAX / 2, 0..300)) {
            times.sort();
            let bytes = encode_times(&times);
            prop_assert_eq!(decode_times(&bytes, times.len()).unwrap(), times);
        }

        #[test]
        fn unsorted_times_roundtrip(times in prop::collection::vec(any::<u64>(), 0..300)) {
            let bytes = encode_times(&times);
            prop_assert_eq!(decode_times(&bytes, times.len()).unwrap(), times);
        }

        #[test]
        fn floats_roundtrip_bit_exact(values in prop::collection::vec(any::<f64>(), 0..300)) {
            let bytes = encode_floats(&values);
            let back = decode_floats(&bytes, values.len()).unwrap();
            let a: Vec<u64> = values.iter().map(|v| v.to_bits()).collect();
            let b: Vec<u64> = back.iter().map(|v| v.to_bits()).collect();
            prop_assert_eq!(a, b);
        }

        #[test]
        fn random_walk_roundtrips_and_compresses(
            start in 1.0f64..1e6,
            steps in prop::collection::vec(-5i32..=5, 1..500),
        ) {
            // Values like a gauge drifting by small whole steps, which is the
            // case where the XOR window is reused.
            let mut v = start.round();
            let values: Vec<f64> = steps.iter().map(|s| { v += *s as f64; v }).collect();
            let bytes = encode_floats(&values);
            prop_assert_eq!(decode_floats(&bytes, values.len()).unwrap(), values.clone());
            prop_assert!(bytes.len() < values.len() * 8 / 2 + 16, "{} bytes", bytes.len());
        }

        #[test]
        fn decoding_garbage_never_panics(bytes in prop::collection::vec(any::<u8>(), 0..64), n in 0usize..100) {
            let _ = decode_floats(&bytes, n);
            let _ = decode_times(&bytes, n);
        }
    }
}
