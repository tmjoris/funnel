//! StatsD line format, with the DogStatsD extensions clients actually send:
//!
//! ```text
//! name:value[:value...]|type[|@rate][|#tag:value,tag][|T<unix seconds>]
//! ```
//!
//! Types are `c` (counter), `g` (gauge) and `d`, `ms` or `h` (distribution).
//! Sets (`s`) are rejected rather than silently counted as something else.

use std::fmt;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum Kind {
    Counter = 0,
    Gauge = 1,
    Distribution = 2,
}

#[derive(Debug, PartialEq)]
pub struct Line<'a> {
    pub name: &'a str,
    pub kind: Kind,
    pub values: Vec<f64>,
    /// `@rate`, in (0, 1]. Counters are scaled by `1 / rate` and
    /// distribution values are weighted by it; gauges ignore it.
    pub sample_rate: f64,
    /// In the order sent; [`crate::series`] sorts them.
    pub tags: Vec<(&'a str, &'a str)>,
    /// `T<unix seconds>`; the receive time is used when absent.
    pub timestamp: Option<u64>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ParseError {
    Empty,
    Name,
    Value,
    Type,
    UnsupportedSet,
    SampleRate,
    Tag,
    Timestamp,
    UnknownSection,
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let msg = match self {
            ParseError::Empty => "empty line",
            ParseError::Name => "invalid metric name",
            ParseError::Value => "invalid value",
            ParseError::Type => "unknown metric type",
            ParseError::UnsupportedSet => "sets are not supported",
            ParseError::SampleRate => "invalid sample rate",
            ParseError::Tag => "invalid tag",
            ParseError::Timestamp => "invalid timestamp",
            ParseError::UnknownSection => "unknown section",
        };
        f.write_str(msg)
    }
}

impl std::error::Error for ParseError {}

pub const MAX_NAME_LEN: usize = 200;
pub const MAX_TAGS: usize = 32;

impl Default for Line<'_> {
    fn default() -> Self {
        Line {
            name: "",
            kind: Kind::Counter,
            values: Vec::new(),
            sample_rate: 1.0,
            tags: Vec::new(),
            timestamp: None,
        }
    }
}

/// Parses one line. Trailing `\r` and surrounding whitespace are ignored.
pub fn parse_line(line: &str) -> Result<Line<'_>, ParseError> {
    let mut out = Line::default();
    parse_into(line, true, &mut out)?;
    Ok(out)
}

/// Parses a line that has already been validated by [`parse_line`], but
/// skips its tags, which are most of a typical line. The shards use this:
/// the router has already parsed the tags to compute the series id, and
/// the shard needs them again only for a series it has not seen.
pub fn parse_line_without_tags(line: &str) -> Result<Line<'_>, ParseError> {
    let mut out = Line::default();
    parse_into(line, false, &mut out)?;
    Ok(out)
}

/// Index of the first `byte` in `b` at or after `from`, or `b.len()`.
#[inline]
fn find(b: &[u8], from: usize, byte: u8) -> usize {
    b[from..]
        .iter()
        .position(|&c| c == byte)
        .map_or(b.len(), |p| from + p)
}

/// Parses `line` into `out`, reusing its buffers, which is what the hot
/// paths do to avoid two allocations per line.
///
/// One pass over the bytes finds every separator. The first version split
/// the line with `str::split` and `split_once` a dozen times per line, and
/// each call's setup cost on a few bytes made parsing the largest cost in
/// the ingest profile. Behavior, including which error a bad line reports,
/// is checked against that version by a property test.
pub fn parse_into<'a>(
    line: &'a str,
    with_tags: bool,
    out: &mut Line<'a>,
) -> Result<(), ParseError> {
    out.values.clear();
    out.tags.clear();
    out.sample_rate = 1.0;
    out.timestamp = None;
    let line = line.trim();
    if line.is_empty() {
        return Err(ParseError::Empty);
    }
    let b = line.as_bytes();
    let head_end = find(b, 0, b'|');
    let colon = find(&b[..head_end], 0, b':');
    if colon == head_end {
        return Err(ParseError::Value);
    }
    out.name = &line[..colon];
    if !valid_name(out.name) {
        return Err(ParseError::Name);
    }
    if head_end == b.len() {
        return Err(ParseError::Type);
    }
    let type_end = find(b, head_end + 1, b'|');
    out.kind = match &b[head_end + 1..type_end] {
        b"c" => Kind::Counter,
        b"g" => Kind::Gauge,
        b"d" | b"ms" | b"h" => Kind::Distribution,
        b"s" => return Err(ParseError::UnsupportedSet),
        _ => return Err(ParseError::Type),
    };
    let mut start = colon + 1;
    loop {
        let end = find(&b[..head_end], start, b':');
        match line[start..end].parse::<f64>() {
            Ok(x) if x.is_finite() => out.values.push(x),
            _ => return Err(ParseError::Value),
        }
        if end == head_end {
            break;
        }
        start = end + 1;
    }

    let mut bar = type_end;
    while bar < b.len() {
        let start = bar + 1;
        let end = find(b, start, b'|');
        bar = end;
        let section = &line[start..end];
        match section.as_bytes().first() {
            Some(b'@') => {
                out.sample_rate = match section[1..].parse::<f64>() {
                    Ok(r) if r > 0.0 && r <= 1.0 => r,
                    _ => return Err(ParseError::SampleRate),
                };
            }
            Some(b'#') if !with_tags => {}
            Some(b'#') => {
                let tags = &section[1..];
                let tb = tags.as_bytes();
                let mut t = 0;
                while t <= tb.len() {
                    let comma = find(tb, t, b',');
                    if comma > t {
                        let colon = find(&tb[..comma], t, b':');
                        let (k, v) = if colon == comma {
                            (&tags[t..comma], "")
                        } else {
                            (&tags[t..colon], &tags[colon + 1..comma])
                        };
                        if !valid_name(k) || !valid_tag_value(v) {
                            return Err(ParseError::Tag);
                        }
                        out.tags.push((k, v));
                    }
                    t = comma + 1;
                }
                if out.tags.len() > MAX_TAGS {
                    return Err(ParseError::Tag);
                }
            }
            Some(b'T') => {
                out.timestamp = Some(section[1..].parse().map_err(|_| ParseError::Timestamp)?);
            }
            _ => return Err(ParseError::UnknownSection),
        }
    }
    Ok(())
}

/// Tag that selects the tenant on protocols without their own way to say it
/// (UDP). It is removed from the tags so it does not count toward the series.
pub const TENANT_TAG: &str = "tenant";

/// Removes the `tenant` tag from `line` and returns its value.
pub fn take_tenant<'a>(line: &mut Line<'a>) -> Result<Option<&'a str>, ParseError> {
    let Some(i) = line.tags.iter().position(|(k, _)| *k == TENANT_TAG) else {
        return Ok(None);
    };
    let (_, tenant) = line.tags.remove(i);
    if valid_tenant(tenant) {
        Ok(Some(tenant))
    } else {
        Err(ParseError::Tag)
    }
}

/// Tenant names become directory names on disk, so they are limited to
/// ASCII letters, digits, `_` and `-`: no `/`, no `.` and so no `..`.
pub fn valid_tenant(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= 64
        && s.bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
}

fn valid_name(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= MAX_NAME_LEN
        && s.bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'.' | b'-' | b'/'))
}

fn valid_tag_value(s: &str) -> bool {
    s.len() <= MAX_NAME_LEN && s.bytes().all(|b| b.is_ascii_graphic() && b != b',')
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    /// The first, `str::split`-based parser, kept as the reference the
    /// byte-level parser must agree with.
    fn reference(line: &str, with_tags: bool) -> Result<Line<'_>, ParseError> {
        let line = line.trim();
        if line.is_empty() {
            return Err(ParseError::Empty);
        }
        let mut sections = line.split('|');
        let head = sections.next().ok_or(ParseError::Empty)?;
        let (name, raw_values) = head.split_once(':').ok_or(ParseError::Value)?;
        if !valid_name(name) {
            return Err(ParseError::Name);
        }
        let kind = match sections.next() {
            Some("c") => Kind::Counter,
            Some("g") => Kind::Gauge,
            Some("d" | "ms" | "h") => Kind::Distribution,
            Some("s") => return Err(ParseError::UnsupportedSet),
            _ => return Err(ParseError::Type),
        };
        let values = raw_values
            .split(':')
            .map(|v| match v.parse::<f64>() {
                Ok(x) if x.is_finite() => Ok(x),
                _ => Err(ParseError::Value),
            })
            .collect::<Result<Vec<_>, _>>()?;

        let mut parsed = Line {
            name,
            kind,
            values,
            sample_rate: 1.0,
            tags: Vec::new(),
            timestamp: None,
        };
        for section in sections {
            if let Some(rate) = section.strip_prefix('@') {
                parsed.sample_rate = match rate.parse::<f64>() {
                    Ok(r) if r > 0.0 && r <= 1.0 => r,
                    _ => return Err(ParseError::SampleRate),
                };
            } else if let Some(tags) = section.strip_prefix('#') {
                if !with_tags {
                    continue;
                }
                for tag in tags.split(',').filter(|t| !t.is_empty()) {
                    let (k, v) = tag.split_once(':').unwrap_or((tag, ""));
                    if !valid_name(k) || !valid_tag_value(v) {
                        return Err(ParseError::Tag);
                    }
                    parsed.tags.push((k, v));
                }
                if parsed.tags.len() > MAX_TAGS {
                    return Err(ParseError::Tag);
                }
            } else if let Some(ts) = section.strip_prefix('T') {
                parsed.timestamp = Some(ts.parse().map_err(|_| ParseError::Timestamp)?);
            } else {
                return Err(ParseError::UnknownSection);
            }
        }
        Ok(parsed)
    }

    fn agree(line: &str) -> Result<(), TestCaseError> {
        for with_tags in [true, false] {
            let mut out = Line::default();
            // Reuse a dirty buffer, as the hot paths do.
            parse_into("old:9:9|g|@0.5|#x:y|T5", true, &mut out).unwrap();
            let fast = parse_into(line, with_tags, &mut out).map(|()| out);
            prop_assert_eq!(
                fast,
                reference(line, with_tags),
                "{:?} tags={}",
                line,
                with_tags
            );
        }
        Ok(())
    }

    const PIECES: &[&str] = &[
        "a",
        "b.c",
        "-",
        "/",
        "_",
        ":",
        "::",
        "|",
        "||",
        "#",
        ",",
        ",,",
        "@",
        "T",
        "1",
        "2.5",
        "-3",
        "1e3",
        "0.5",
        "1.5",
        "0",
        "NaN",
        "inf",
        "c",
        "g",
        "ms",
        "d",
        "h",
        "s",
        "x",
        " ",
        "\r",
        "\t",
        "é",
        "\u{a0}",
        "k:v",
        "tenant",
        "1761000000",
    ];

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(4096))]

        #[test]
        fn byte_parser_matches_reference_on_arbitrary_lines(
            parts in prop::collection::vec(prop::sample::select(PIECES), 0..24),
        ) {
            agree(&parts.concat())?;
        }

        #[test]
        fn byte_parser_matches_reference_on_mutated_valid_lines(
            base in prop::sample::select(&[
                "http.req:1|c|#route:/a,status:2xx",
                "lat:12.5:30|ms|@0.25|#route:/users,env:prod,canary|T1761000000",
                "q:-3|g",
                "d:1|d|#a:b|@1|T0",
            ][..]),
            edits in prop::collection::vec((any::<prop::sample::Index>(), prop::sample::select(PIECES), any::<bool>()), 0..4),
        ) {
            let mut line = base.to_string();
            for (at, piece, insert) in edits {
                let mut i = at.index(line.len() + 1);
                while !line.is_char_boundary(i) {
                    i -= 1;
                }
                if insert {
                    line.insert_str(i, piece);
                } else if i < line.len() {
                    let mut j = i + 1;
                    while !line.is_char_boundary(j) {
                        j += 1;
                    }
                    line.replace_range(i..j, "");
                }
            }
            agree(&line)?;
        }
    }

    #[test]
    fn counter() {
        let l = parse_line("http.requests:1|c").unwrap();
        assert_eq!(l.name, "http.requests");
        assert_eq!(l.kind, Kind::Counter);
        assert_eq!(l.values, [1.0]);
        assert_eq!(l.sample_rate, 1.0);
        assert!(l.tags.is_empty());
    }

    #[test]
    fn all_sections() {
        let l =
            parse_line("db.query:12.5:30|ms|@0.25|#route:/users,env:prod,canary|T1761000000\r\n")
                .unwrap();
        assert_eq!(l.kind, Kind::Distribution);
        assert_eq!(l.values, [12.5, 30.0]);
        assert_eq!(l.sample_rate, 0.25);
        assert_eq!(
            l.tags,
            [("route", "/users"), ("env", "prod"), ("canary", "")]
        );
        assert_eq!(l.timestamp, Some(1761000000));
    }

    #[test]
    fn distribution_aliases_and_gauge() {
        assert_eq!(parse_line("a:1|h").unwrap().kind, Kind::Distribution);
        assert_eq!(parse_line("a:1|d").unwrap().kind, Kind::Distribution);
        assert_eq!(parse_line("a:-3|g").unwrap().kind, Kind::Gauge);
    }

    #[test]
    fn rejects_bad_input() {
        for (line, err) in [
            ("", ParseError::Empty),
            ("   ", ParseError::Empty),
            ("novalue|c", ParseError::Value),
            ("a:x|c", ParseError::Value),
            ("a:NaN|c", ParseError::Value),
            ("a:inf|g", ParseError::Value),
            ("a:1", ParseError::Type),
            ("a:1|q", ParseError::Type),
            ("a:1|s", ParseError::UnsupportedSet),
            ("a:1|c|@0", ParseError::SampleRate),
            ("a:1|c|@1.5", ParseError::SampleRate),
            ("a b:1|c", ParseError::Name),
            (":1|c", ParseError::Name),
            ("a:1|c|#bad key:v", ParseError::Tag),
            ("a:1|c|Tsoon", ParseError::Timestamp),
            ("a:1|c|xyz", ParseError::UnknownSection),
        ] {
            assert_eq!(parse_line(line).unwrap_err(), err, "{line:?}");
        }
    }

    #[test]
    fn tenant_tag_is_extracted() {
        let mut l = parse_line("a:1|c|#env:prod,tenant:acme").unwrap();
        assert_eq!(take_tenant(&mut l), Ok(Some("acme")));
        assert_eq!(l.tags, [("env", "prod")]);
        let mut l = parse_line("a:1|c|#tenant:").unwrap();
        assert_eq!(take_tenant(&mut l), Err(ParseError::Tag));
        let mut l = parse_line("a:1|c").unwrap();
        assert_eq!(take_tenant(&mut l), Ok(None));
    }

    #[test]
    fn tenants_cannot_name_paths() {
        for bad in ["", "..", "a/b", "../etc", ".hidden", "a.b", &"x".repeat(65)] {
            assert!(!valid_tenant(bad), "{bad:?}");
        }
        assert!(valid_tenant("acme-prod_2"));
        let mut l = parse_line("a:1|c|#tenant:../etc").unwrap();
        assert_eq!(take_tenant(&mut l), Err(ParseError::Tag));
    }

    #[test]
    fn caps_tag_count() {
        let tags: Vec<String> = (0..=MAX_TAGS).map(|i| format!("t{i}:v")).collect();
        let line = format!("a:1|c|#{}", tags.join(","));
        assert_eq!(parse_line(&line).unwrap_err(), ParseError::Tag);
    }
}
