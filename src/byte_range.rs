//! HTTP byte ranges, written and read in one place. The store door and client (`served.rs`) and the
//! sharing door (`sharing.rs`) both ask for part of an object with a `Range` header and answer with a
//! `content-range`; every such header they send or read goes through these functions, so the two
//! doors can't drift apart.
use object_store::GetRange;
use std::ops::Range;

/// The range a `Range` header asks for: `bytes=a-b`, `bytes=a-` or `bytes=-n`, as the store's ranges.
/// HTTP's `b` is inclusive, so the store's end is `b + 1`. An inverted range parses too; `is_valid`
/// refuses it, and each door says what it does with one.
pub fn parse(header: &str) -> Option<GetRange> {
    let (first, last) = header.strip_prefix("bytes=")?.split_once('-')?;
    match (first.is_empty(), last.is_empty()) {
        (true, false) => Some(GetRange::Suffix(last.parse().ok()?)),
        (false, true) => Some(GetRange::Offset(first.parse().ok()?)),
        (false, false) => {
            let (start, end): (u64, u64) = (first.parse().ok()?, last.parse().ok()?);
            Some(GetRange::Bounded(start..end.checked_add(1)?))
        }
        (true, true) => None,
    }
}

/// The `Range` header for a range, the one `parse` reads back. A range's last byte is its end less one.
pub fn header(range: &GetRange) -> String {
    match range {
        GetRange::Bounded(r) => format!("bytes={}-{}", r.start, r.end.saturating_sub(1)),
        GetRange::Offset(o) => format!("bytes={o}-"),
        GetRange::Suffix(n) => format!("bytes=-{n}"),
    }
}

/// The `content-range` of a partial answer: `bytes a-b/size`, or `bytes */size` for an empty span (every
/// store puts an empty range at the end, and HTTP has no way to name an empty span).
pub fn content_range(span: &Range<u64>, size: u64) -> String {
    if span.is_empty() { format!("bytes */{size}") } else { format!("bytes {}-{}/{size}", span.start, span.end - 1) }
}

/// The span a `content-range` says it answers, and the object's size when it says it (`bytes a-b/*`
/// leaves the size out). None for a header that isn't one.
pub fn answered(content_range: &str) -> Option<(Range<u64>, Option<u64>)> {
    let (span, size) = content_range.strip_prefix("bytes ")?.split_once('/')?;
    let size = size.parse::<u64>().ok();
    if span == "*" {
        return Some((size?..size?, size)); // (an empty span at the end: the size is all it says)
    }
    let (start, last) = span.split_once('-')?;
    Some((start.parse().ok()?..last.parse::<u64>().ok()?.checked_add(1)?, size))
}

/// The bytes of a file of `size` that a `Range` header asks for, as the sharing door answers it: a range
/// that runs past the end is cut short; one that starts exactly at the end is empty (206, no bytes);
/// one that starts past the end, or is inverted, is no range at all (the whole file, 200). None: no range
/// asked, or one we don't take.
pub fn resolve(header: &str, size: u64) -> Option<(u64, u64)> {
    let range = parse(header)?;
    match range.as_range(size) {
        Ok(span) => Some((span.start, span.end)),
        // (object_store refuses a start at or past the end; its error type is private, so the start is read here)
        Err(_) => {
            let start = match range {
                GetRange::Bounded(ref r) => r.start,
                GetRange::Offset(o) => o,
                GetRange::Suffix(_) => return None,
            };
            (start == size && range.is_valid().is_ok()).then_some((size, size))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_reads_the_three_forms_and_nothing_else() {
        assert_eq!(parse("bytes=0-9"), Some(GetRange::Bounded(0..10)), "HTTP's last byte is inclusive");
        assert_eq!(parse("bytes=5-"), Some(GetRange::Offset(5)));
        assert_eq!(parse("bytes=-4"), Some(GetRange::Suffix(4)));
        assert_eq!(parse("bytes=5-4"), Some(GetRange::Bounded(5..5)), "inverted: parsed, then refused by is_valid");
        for text in ["bytes=", "bytes=-", "bytes=x-1", "bytes=1-x", "bytes=0-1,5-6", "bytes= 0-5", "items=0-5", "0-5"] {
            assert_eq!(parse(text), None, "{text}");
        }
        assert_eq!(parse("bytes=0-18446744073709551615"), None, "its end doesn't fit a u64 once made exclusive");
    }

    #[test]
    fn header_writes_what_parse_reads() {
        for text in ["bytes=0-9", "bytes=5-", "bytes=-4"] {
            assert_eq!(header(&parse(text).unwrap()), text);
        }
        assert_eq!(header(&GetRange::Bounded(0..1)), "bytes=0-0", "a HEAD asks for the first byte");
    }

    #[test]
    fn content_range_names_the_span_and_the_size() {
        assert_eq!(content_range(&(0..10), 100), "bytes 0-9/100");
        assert_eq!(content_range(&(90..100), 100), "bytes 90-99/100");
        assert_eq!(content_range(&(100..100), 100), "bytes */100", "an empty range at the end");
    }

    #[test]
    fn answered_reads_back_what_content_range_writes() {
        for (span, size) in [(0..10u64, 100u64), (90..100, 100), (100..100, 100)] {
            assert_eq!(answered(&content_range(&span, size)), Some((span.clone(), Some(size))));
        }
        assert_eq!(answered("bytes 5-9/*"), Some((5..10, None)), "a total the server leaves out");
        assert_eq!(answered("bytes 5-9"), None, "no size at all");
        assert_eq!(answered("items 5-9/10"), None);
        assert_eq!(answered("bytes x-9/10"), None);
        assert_eq!(answered("bytes */*"), None, "an empty span with no size");
    }

    /// Each past-the-end answer of the sharing door, as `asked` gave it before these helpers, on a file of 1000
    /// bytes: the same bytes and status for each (the 206s with an empty span now say `bytes */1000`).
    #[test]
    fn resolve_answers_past_the_end_as_the_sharing_door_did() {
        assert_eq!(resolve("bytes=0-99", 1000), Some((0, 100)));
        assert_eq!(resolve("bytes=900-", 1000), Some((900, 1000)));
        assert_eq!(resolve("bytes=-8", 1000), Some((992, 1000)));
        assert_eq!(resolve("bytes=990-2000", 1000), Some((990, 1000)), "cut short at the end");
        assert_eq!(resolve("bytes=999-999", 1000), Some((999, 1000)), "the last byte");
        assert_eq!(resolve("bytes=0-0", 1000), Some((0, 1)), "the first byte");
        assert_eq!(resolve("bytes=-5000", 1000), Some((0, 1000)), "a suffix longer than the file is all of it");
        assert_eq!(resolve("bytes=-0", 1000), Some((1000, 1000)), "an empty suffix");
        assert_eq!(resolve("bytes=1000-", 1000), Some((1000, 1000)), "starting at the end: empty, 206");
        assert_eq!(resolve("bytes=1000-1500", 1000), Some((1000, 1000)), "starting at the end, ending past it: empty");
        assert_eq!(resolve("bytes=1500-", 1000), None, "starting past the end: the whole file");
        assert_eq!(resolve("bytes=1500-2000", 1000), None);
        assert_eq!(resolve("bytes=5-1", 1000), None, "inverted: the whole file");
        assert_eq!(resolve("bytes=1000-999", 1000), None, "inverted at the end: the whole file");
        assert_eq!(resolve("bytes=5-4", 1000), None, "inverted by one (sharing gave an empty answer before)");
        assert_eq!(resolve("bytes=0-18446744073709551615", 1000), None, "its end doesn't fit (sharing clamped it before)");
        assert_eq!(resolve("bytes=x-1", 1000), None);
        assert_eq!(resolve("bytes=-", 1000), None);
        assert_eq!(resolve("items=0-5", 1000), None);
    }

    /// A file of no bytes: every range starts at its end.
    #[test]
    fn resolve_answers_an_empty_file_as_it_did() {
        assert_eq!(resolve("bytes=0-0", 0), Some((0, 0)));
        assert_eq!(resolve("bytes=-5", 0), Some((0, 0)));
        assert_eq!(resolve("bytes=0-", 0), Some((0, 0)));
    }
}
