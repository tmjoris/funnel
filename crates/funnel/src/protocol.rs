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

/// Parses one line. Trailing `\r` and surrounding whitespace are ignored.
pub fn parse_line(line: &str) -> Result<Line<'_>, ParseError> {
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

/// Tag that selects the tenant on protocols without their own way to say it
/// (UDP). It is removed from the tags so it does not count toward the series.
pub const TENANT_TAG: &str = "tenant";

/// Removes the `tenant` tag from `line` and returns its value.
pub fn take_tenant<'a>(line: &mut Line<'a>) -> Result<Option<&'a str>, ParseError> {
    let Some(i) = line.tags.iter().position(|(k, _)| *k == TENANT_TAG) else {
        return Ok(None);
    };
    let (_, tenant) = line.tags.remove(i);
    if valid_name(tenant) {
        Ok(Some(tenant))
    } else {
        Err(ParseError::Tag)
    }
}

/// Tenant names follow the metric name rules.
pub fn valid_tenant(s: &str) -> bool {
    valid_name(s)
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
    fn caps_tag_count() {
        let tags: Vec<String> = (0..=MAX_TAGS).map(|i| format!("t{i}:v")).collect();
        let line = format!("a:1|c|#{}", tags.join(","));
        assert_eq!(parse_line(&line).unwrap_err(), ParseError::Tag);
    }
}
