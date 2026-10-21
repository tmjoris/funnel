//! Series identity: tenant, metric name, kind and sorted tags.
//!
//! The 64-bit xxh3 hash of those parts is the series id used everywhere
//! after parsing. Two distinct series collide with probability about
//! n² / 2⁶⁵, around 3e-8 at a million series, which is acceptable for
//! metrics and keeps the hot path free of string comparisons.

use xxhash_rust::xxh3::Xxh3;

use crate::protocol::{Kind, Line};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SeriesId(pub u64);

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SeriesKey {
    pub tenant: String,
    pub name: String,
    pub kind: Kind,
    pub tags: Vec<(String, String)>,
}

/// Sorts tags by key and drops repeated keys, keeping the first value sent.
pub fn normalize_tags(tags: &mut Vec<(&str, &str)>) {
    tags.sort_by(|a, b| a.0.cmp(b.0));
    tags.dedup_by(|b, a| a.0 == b.0);
}

/// Hashes a series from borrowed parts. `tags` must be normalized.
pub fn series_id(tenant: &str, name: &str, kind: Kind, tags: &[(&str, &str)]) -> SeriesId {
    let mut h = Xxh3::new();
    // Field separators keep ("ab", "c") and ("a", "bc") apart; none of the
    // parts can contain \0 or \x01 after protocol validation.
    h.update(tenant.as_bytes());
    h.update(b"\0");
    h.update(name.as_bytes());
    h.update(&[0, kind as u8]);
    for (k, v) in tags {
        h.update(k.as_bytes());
        h.update(b"\x01");
        h.update(v.as_bytes());
        h.update(b"\0");
    }
    SeriesId(h.digest())
}

impl SeriesKey {
    /// Builds the owned key for a line whose tags are already normalized.
    pub fn new(tenant: &str, line: &Line<'_>) -> Self {
        SeriesKey {
            tenant: tenant.to_owned(),
            name: line.name.to_owned(),
            kind: line.kind,
            tags: line
                .tags
                .iter()
                .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
                .collect(),
        }
    }

    pub fn id(&self) -> SeriesId {
        let tags: Vec<(&str, &str)> = self
            .tags
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();
        series_id(&self.tenant, &self.name, self.kind, &tags)
    }

    pub fn tag(&self, key: &str) -> Option<&str> {
        self.tags
            .binary_search_by(|(k, _)| k.as_str().cmp(key))
            .ok()
            .map(|i| self.tags[i].1.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::parse_line;

    fn id_of(tenant: &str, line: &str) -> SeriesId {
        let mut l = parse_line(line).unwrap();
        normalize_tags(&mut l.tags);
        series_id(tenant, l.name, l.kind, &l.tags)
    }

    #[test]
    fn tag_order_does_not_matter() {
        assert_eq!(id_of("t", "a:1|c|#x:1,y:2"), id_of("t", "a:2|c|#y:2,x:1"));
    }

    #[test]
    fn identity_includes_every_part() {
        let base = id_of("t", "a:1|c|#x:1");
        assert_ne!(base, id_of("u", "a:1|c|#x:1"));
        assert_ne!(base, id_of("t", "b:1|c|#x:1"));
        assert_ne!(base, id_of("t", "a:1|g|#x:1"));
        assert_ne!(base, id_of("t", "a:1|c|#x:2"));
        assert_ne!(base, id_of("t", "a:1|c"));
        assert_ne!(id_of("t", "a:1|c|#ab:c"), id_of("t", "a:1|c|#a:bc"));
    }

    #[test]
    fn duplicate_tag_keeps_first() {
        let mut l = parse_line("a:1|c|#env:prod,env:dev").unwrap();
        normalize_tags(&mut l.tags);
        assert_eq!(l.tags, [("env", "prod")]);
    }

    #[test]
    fn owned_key_hashes_the_same() {
        let mut l = parse_line("a:1|d|#z:1,b:2").unwrap();
        normalize_tags(&mut l.tags);
        let key = SeriesKey::new("t", &l);
        assert_eq!(key.id(), series_id("t", l.name, l.kind, &l.tags));
        assert_eq!(key.tag("z"), Some("1"));
        assert_eq!(key.tag("q"), None);
    }
}
