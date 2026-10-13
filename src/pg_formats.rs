//! Postgres 16 template patterns for to_char, to_timestamp and to_date.
//!
//! A template is read the way the server's formatting.c reads it: at each place the
//! longest pattern keyword wins, `"..."` is literal text, and any other character is
//! copied. Rendering lives in `render.rs`, reading input in `parse.rs`. A pattern
//! either gets the server's text or an "isn't supported" error, never a guess.

mod fields;
mod parse;
mod render;

use chrono::{NaiveDate, NaiveDateTime};

/// Pattern keywords, spelled as the server reads them: exact spellings, so `MONTH`, `Month` and
/// `month` are three keywords (the server's rule, from its own table, found by the probe in
/// tools/probe_forms.py). Where two match at a place, the longer one wins, so HH12 beats HH.
const KEYWORDS: &[&str] = &[
    "A.D.", "A.M.", "AD", "AM", "B.C.", "BC", "CC", "D", "DAY", "DD", "DDD", "DY", "Day",
    "Dy", "FF1", "FF2", "FF3", "FF4", "FF5", "FF6", "HH", "HH12", "HH24", "I", "ID", "IDDD",
    "IW", "IY", "IYY", "IYYY", "J", "MI", "MM", "MON", "MONTH", "MS", "Mon", "Month", "OF",
    "P.M.", "PM", "Q", "RM", "SS", "SSSS", "SSSSS", "TZ", "TZH", "TZM", "US", "W", "WW", "Y",
    "Y,YYY", "YY", "YYY", "YYYY", "a.d.", "a.m.", "ad", "am", "b.c.", "bc", "cc", "d", "day",
    "dd", "ddd", "dy", "ff1", "ff2", "ff3", "ff4", "ff5", "ff6", "hh", "hh12", "hh24", "i",
    "id", "iddd", "iw", "iy", "iyy", "iyyy", "j", "mi", "mm", "mon", "month", "ms", "of",
    "p.m.", "pm", "q", "rm", "ss", "ssss", "sssss", "tz", "tzh", "tzm", "us", "w", "ww", "y",
    "y,yyy", "yy", "yyy", "yyyy",
];

/// Month names, January first, as the server's to_char spells them in English.
pub(crate) const MONTHS: [&str; 12] = [
    "January", "February", "March", "April", "May", "June", "July", "August", "September",
    "October", "November", "December",
];

/// Day names, Sunday first (the order of the server's D pattern, where Sunday is 1).
pub(crate) const DAYS: [&str; 7] = [
    "Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday",
];

/// Roman numerals for the months, I to XII.
pub(crate) const ROMAN: [&str; 12] = [
    "I", "II", "III", "IV", "V", "VI", "VII", "VIII", "IX", "X", "XI", "XII",
];

/// Julian day number of 0001-01-01 (chrono counts that day as 1, the server's Julian day as 1721426).
pub(crate) const JULIAN_OFFSET: i64 = 1_721_425;

/// AM, A.M. and their PM, P.M. kin, or BC, B.C. and AD, A.D. kin, in the case of `kw`.
/// `upper` is the upper-case form ("AM", "PM", "BC" or "AD"); both to_char and the input
/// readers spell markers this way, so the two can't disagree.
pub(crate) fn marker(kw: &str, upper: &str) -> String {
    let text = if kw.contains('.') {
        format!("{}.{}.", &upper[..1], &upper[1..])
    } else {
        upper.to_string()
    };
    if kw.starts_with(|c: char| c.is_lowercase()) {
        text.to_lowercase()
    } else {
        text
    }
}

/// Is `f` a Postgres template rather than a chrono (strftime) format? True when it holds no '%'.
pub fn is_template(f: &str) -> bool {
    !f.contains('%')
}

/// to_char(timestamp, template), as Postgres 16 formats it.
pub fn to_char(t: NaiveDateTime, template: &str) -> Result<String, String> {
    let tpl = tokenize(template);
    let mut out = String::new();
    for piece in &tpl.pieces {
        match piece {
            Piece::Text(c) => out.push(*c),
            Piece::Pat(p) => out.push_str(&render::pattern(&t, p)?),
        }
    }
    Ok(out)
}

/// to_timestamp(text, template): the timestamp Postgres 16 reads. Fields missing
/// default as the server does: year 1 BC, month 1, day 1, time 00:00:00.
pub fn to_timestamp(s: &str, template: &str) -> Result<NaiveDateTime, String> {
    parse::timestamp(s, &tokenize(template))
}

/// to_date(text, template): the date Postgres 16 reads (any time fields are read and dropped).
pub fn to_date(s: &str, template: &str) -> Result<NaiveDate, String> {
    parse::date(s, &tokenize(template))
}

/// A template split into literal characters and patterns.
pub(crate) struct Template {
    /// FX at the start: punctuation and digit counts must match exactly.
    pub fx: bool,
    pub pieces: Vec<Piece>,
}

/// One piece of a template.
pub(crate) enum Piece {
    /// A literal character (copied as it is, or matched against the input).
    Text(char),
    /// A pattern keyword with its modifiers.
    Pat(Pat),
}

/// A pattern keyword as written, with FM before it and TH or th after it.
#[derive(Clone, Copy)]
pub(crate) struct Pat {
    pub kw: &'static str,
    pub fm: bool,
    pub suffix: Suffix,
}

/// The ordinal suffix a numeric pattern carries: TH (upper case) or th.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum Suffix {
    None,
    Upper,
    Lower,
}

/// Reads a template: FX, FM and TM are modifiers, `"..."` is literal text, the
/// longest keyword at each place is a pattern, and anything else is a literal.
fn tokenize(template: &str) -> Template {
    let cs: Vec<char> = template.chars().collect();
    let fx = modifier_at(&cs, 0, "FX");
    let mut i = if fx { 2 } else { 0 };
    let mut pieces = Vec::new();
    let mut fm = false;
    while i < cs.len() {
        if cs[i] == '"' {
            i = quoted(&cs, i, &mut pieces);
        } else if modifier_at(&cs, i, "FM") {
            fm = true;
            i += 2;
        } else if modifier_at(&cs, i, "TM") {
            // TM asks for translated names; the server's C locale prints the English ones.
            i += 2;
        } else if let Some(kw) = keyword_at(&cs, i) {
            i += kw.chars().count();
            let suffix = suffix_at(&cs, &mut i);
            pieces.push(Piece::Pat(Pat { kw, fm, suffix }));
            fm = false;
        } else {
            pieces.push(Piece::Text(cs[i]));
            i += 1;
        }
    }
    Template { fx, pieces }
}

/// Literal text in double quotes starting at `start`; a backslash takes the next
/// character as it is. Returns the index after the closing quote.
fn quoted(cs: &[char], start: usize, out: &mut Vec<Piece>) -> usize {
    let mut i = start + 1;
    while i < cs.len() && cs[i] != '"' {
        if cs[i] == '\\' && i + 1 < cs.len() {
            i += 1;
        }
        out.push(Piece::Text(cs[i]));
        i += 1;
    }
    i + 1
}

/// The longest keyword that matches the template at `i`, if any.
fn keyword_at(cs: &[char], i: usize) -> Option<&'static str> {
    KEYWORDS
        .iter()
        .copied()
        .filter(|kw| starts(cs, i, kw))
        .max_by_key(|kw| kw.len())
}

/// TH or th right after a pattern: moves past it and says which.
fn suffix_at(cs: &[char], i: &mut usize) -> Suffix {
    if starts(cs, *i, "TH") {
        *i += 2;
        Suffix::Upper
    } else if starts(cs, *i, "th") {
        *i += 2;
        Suffix::Lower
    } else {
        Suffix::None
    }
}

/// Whether the characters at `i` are exactly `s`.
fn starts(cs: &[char], i: usize, s: &str) -> bool {
    s.chars().enumerate().all(|(n, c)| cs.get(i + n) == Some(&c))
}

/// A modifier (FX, FM or TM) at `i`, in upper or lower case as the server reads it.
fn modifier_at(cs: &[char], i: usize, word: &str) -> bool {
    starts(cs, i, &word.to_uppercase()) || starts(cs, i, &word.to_lowercase())
}

/// Postgres 16's own answers (`pg_formats/postgres16.json`): 2,000 of the 28,890 template cases a
/// Postgres 16.15 server answered (the first case of every template, and a fixed sample of the rest,
/// years past 9999 and BC among them) and every one of the 612 cases of `pg_text`'s functions. A
/// case whose answer is null is one the server refused, and so must we.
#[cfg(test)]
mod postgres16 {
    use chrono::{Datelike, NaiveDate, NaiveDateTime, NaiveTime, Timelike};
    use serde_json::Value;

    fn cases(group: &str) -> Vec<Vec<Value>> {
        let all: Value = serde_json::from_str(include_str!("pg_formats/postgres16.json")).unwrap();
        serde_json::from_value(all[group].clone()).unwrap()
    }

    /// The year as Postgres writes it: 0 is 1 BC, -1 is 2 BC.
    fn era(y: i32) -> (i32, &'static str) { if y <= 0 { (1 - y, " BC") } else { (y, "") } }

    /// A timestamp as the server wrote it (`12345-07-04 18:30:00`, `0001-01-01 00:30:00 BC`).
    fn stamp(s: &str) -> NaiveDateTime {
        let (body, bc) = s.strip_suffix(" BC").map_or((s, false), |b| (b, true));
        let (day, time) = body.split_once(' ').unwrap();
        let mut ymd = day.rsplitn(3, '-').map(|v| v.parse::<i32>().unwrap());
        let (d, m, y) = (ymd.next().unwrap(), ymd.next().unwrap(), ymd.next().unwrap());
        let date = NaiveDate::from_ymd_opt(if bc { 1 - y } else { y }, m as u32, d as u32).unwrap();
        date.and_time(NaiveTime::parse_from_str(time, "%H:%M:%S%.f").unwrap())
    }

    fn date_text(d: NaiveDate) -> String {
        let (y, bc) = era(d.year());
        format!("{y:04}-{:02}-{:02}{bc}", d.month(), d.day())
    }

    fn stamp_text(t: NaiveDateTime) -> String {
        let (y, bc) = era(t.year());
        let micros = t.nanosecond() / 1000;
        let fraction = if micros == 0 { String::new() } else { format!(".{micros:06}").trim_end_matches('0').to_string() };
        format!("{y:04}-{:02}-{:02} {:02}:{:02}:{:02}{fraction}{bc}", t.month(), t.day(), t.hour(), t.minute(), t.second())
    }

    /// Ours against the server's: the same text, or both refused.
    fn differs(case: &[Value], want: Option<&str>, ours: Result<String, String>) -> Option<String> {
        match (want, ours) {
            (None, Err(_)) => None,
            (Some(w), Ok(o)) if w == o => None,
            (w, o) => Some(format!("{case:?}: Postgres 16 {w:?}, ours {o:?}")),
        }
    }

    fn report(name: &str, differ: Vec<String>, of: usize) {
        assert!(differ.is_empty(), "{} of {of} {name} cases differ from Postgres 16:\n{}", differ.len(), differ[..differ.len().min(20)].join("\n"));
    }

    #[test]
    fn templates_answer_as_postgres_16_does() {
        let all = cases("templates");
        assert_eq!(all.len(), 2_000);
        let differ: Vec<String> = all.iter().filter_map(|c| {
            let (input, template, want) = (c[1].as_str().unwrap(), c[2].as_str().unwrap(), c[3].as_str());
            let ours = match c[0].as_str().unwrap() {
                "to_char" => super::to_char(stamp(input), template),
                "to_timestamp" => super::to_timestamp(input, template).map(stamp_text),
                "to_date" => super::to_date(input, template).map(date_text),
                other => panic!("no such case kind: {other}"),
            };
            differs(c, want, ours)
        }).collect();
        report("template", differ, all.len());
    }

    #[test]
    fn text_functions_answer_as_postgres_16_does() {
        use crate::pg_text::{format, parse_ident, quote_ident, quote_literal};
        let all = cases("text");
        assert_eq!(all.len(), 612);
        let differ: Vec<String> = all.iter().filter_map(|c| {
            let text = |i: usize| c[i].as_str().unwrap_or("");
            match c[0].as_str().unwrap() {
                "format" => {
                    let args: Vec<Option<&str>> = c[3..].iter().map(Value::as_str).collect();
                    differs(c, c[2].as_str(), format(text(1), &args))
                }
                "quote_ident" => differs(c, c[2].as_str(), Ok(quote_ident(text(1)))),
                "quote_literal" => differs(c, c[2].as_str(), Ok(quote_literal(text(1)))),
                "parse_ident" => {
                    let want = c[3].as_array().map(|parts| parts.iter().filter_map(Value::as_str).collect::<Vec<_>>().join("\u{1}"));
                    differs(c, want.as_deref(), parse_ident(text(1), text(2) == "true").map(|p| p.join("\u{1}")))
                }
                other => panic!("no such case kind: {other}"),
            }
        }).collect();
        report("text function", differ, all.len());
    }
}
