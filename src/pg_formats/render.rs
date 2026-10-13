//! to_char: the text one template pattern gives for a timestamp, as Postgres 16 prints it.

use super::{marker, Pat, Suffix, DAYS, JULIAN_OFFSET, MONTHS, ROMAN};
use chrono::{Datelike, NaiveDate, NaiveDateTime, Timelike, Weekday};

/// What a pattern prints before its suffix is added.
enum Out {
    /// A number, zero-padded to the width (0: not padded); FM strips the padding.
    Num(i64, usize),
    /// A fraction of a second's digits, padded to the width whatever FM says.
    Fraction(i64, usize),
    /// Y,YYY: the year with its comma. Its suffix is the whole year's.
    Grouped(i64),
    /// Text printed as it is.
    Text(String),
    /// A name, blank-padded on the right to the width unless FM is given.
    Name(String, usize),
}

/// The text of one pattern for `t`.
pub(super) fn pattern(t: &NaiveDateTime, p: &Pat) -> Result<String, String> {
    Ok(match value(t, p)? {
        Out::Num(v, width) => number(v, width, p),
        Out::Fraction(v, width) => with_suffix(format!("{v:0width$}"), v, p),
        Out::Grouped(v) => with_suffix(format!("{},{:03}", v / 1000, v % 1000), v, p),
        Out::Text(s) => s,
        Out::Name(s, _) if p.fm => s,
        Out::Name(s, width) => format!("{s:<width$}"),
    })
}

/// A number with a minus sign for a BC century (the digits are the magnitude's), zero-padded
/// to its width unless FM, with its ordinal suffix if it has one.
fn number(v: i64, width: usize, p: &Pat) -> String {
    let magnitude = v.unsigned_abs();
    let digits = if p.fm || width == 0 {
        magnitude.to_string()
    } else {
        format!("{magnitude:0width$}")
    };
    let sign = if v < 0 { "-" } else { "" };
    with_suffix(format!("{sign}{digits}"), v, p)
}

/// The text with the pattern's ordinal suffix (TH or th) for the value `v`, if it has one.
fn with_suffix(text: String, v: i64, p: &Pat) -> String {
    match p.suffix {
        Suffix::None => text,
        Suffix::Upper => format!("{text}{}", ordinal(v).to_uppercase()),
        Suffix::Lower => format!("{text}{}", ordinal(v)),
    }
}

/// The English ordinal suffix for a number: st, nd, rd or th (11, 12 and 13 take th).
fn ordinal(v: i64) -> &'static str {
    let n = v.abs() % 100;
    if (11..=13).contains(&n) {
        return "th";
    }
    match n % 10 {
        1 => "st",
        2 => "nd",
        3 => "rd",
        _ => "th",
    }
}

/// The value a pattern prints for `t`, or an error when the pattern isn't supported.
fn value(t: &NaiveDateTime, p: &Pat) -> Result<Out, String> {
    let kw = p.kw;
    let d = t.date();
    let h = i64::from(t.hour());
    let mi = i64::from(t.minute());
    let s = i64::from(t.second());
    let micro = i64::from(t.nanosecond().min(999_999_999) / 1000);
    let iso = d.iso_week();
    let year = pg_year(d.year());
    let iso_year = pg_year(iso.year());
    let month = i64::from(d.month());
    let day = i64::from(d.day());
    let doy = i64::from(d.ordinal());
    let sunday_based = i64::from(d.weekday().number_from_sunday());
    let monday_based = i64::from(d.weekday().number_from_monday());
    let month_name = MONTHS[(month - 1) as usize];
    let day_name = DAYS[(sunday_based - 1) as usize];
    Ok(match kw.to_uppercase().as_str() {
        "HH" | "HH12" => Out::Num(if h % 12 == 0 { 12 } else { h % 12 }, 2),
        "HH24" => Out::Num(h, 2),
        "MI" => Out::Num(mi, 2),
        "SS" => Out::Num(s, 2),
        "SSSS" | "SSSSS" => Out::Num(h * 3600 + mi * 60 + s, 0),
        "MS" => Out::Fraction(micro / 1000, 3),
        "US" => Out::Fraction(micro, 6),
        "FF1" => Out::Fraction(micro / 100_000, 1),
        "FF2" => Out::Fraction(micro / 10_000, 2),
        "FF3" => Out::Fraction(micro / 1000, 3),
        "FF4" => Out::Fraction(micro / 100, 4),
        "FF5" => Out::Fraction(micro / 10, 5),
        "FF6" => Out::Fraction(micro, 6),
        "AM" | "A.M." | "PM" | "P.M." => {
            Out::Text(marker(kw, if h >= 12 { "PM" } else { "AM" }))
        }
        "BC" | "B.C." | "AD" | "A.D." => {
            Out::Text(marker(kw, if d.year() <= 0 { "BC" } else { "AD" }))
        }
        // FM strips the hour's padding, so the zone of a UTC value prints as +0 (TZH keeps +00).
        "OF" => Out::Text(if p.fm { "+0" } else { "+00" }.to_string()),
        "TZH" => Out::Text("+00".to_string()),
        "TZM" => Out::Text("00".to_string()),
        "TZ" => Out::Text(String::new()),
        "Y,YYY" => Out::Grouped(year),
        "YYYY" => Out::Num(year, 4),
        "YYY" => Out::Num(year % 1000, 3),
        "YY" => Out::Num(year % 100, 2),
        "Y" => Out::Num(year % 10, 1),
        "IYYY" => Out::Num(iso_year, 4),
        "IYY" => Out::Num(iso_year % 1000, 3),
        "IY" => Out::Num(iso_year % 100, 2),
        "I" => Out::Num(iso_year % 10, 1),
        "MONTH" => Out::Name(cased(kw, month_name), 9),
        "MON" => Out::Text(cased(kw, &month_name[..3])),
        "MM" => Out::Num(month, 2),
        "DAY" => Out::Name(cased(kw, day_name), 9),
        "DY" => Out::Text(cased(kw, &day_name[..3])),
        "DDD" => Out::Num(doy, 3),
        "IDDD" => Out::Num(iso_day_of_year(d, iso.year())?, 3),
        "DD" => Out::Num(day, 2),
        "D" => Out::Num(sunday_based, 1),
        "ID" => Out::Num(monday_based, 1),
        "W" => Out::Num((day - 1) / 7 + 1, 1),
        "WW" => Out::Num((doy - 1) / 7 + 1, 2),
        "IW" => Out::Num(i64::from(iso.week()), 2),
        "CC" => Out::Num(century(year, d.year() <= 0), 2),
        "J" => Out::Num(i64::from(d.num_days_from_ce()) + JULIAN_OFFSET, 0),
        "Q" => Out::Num((month - 1) / 3 + 1, 1),
        "RM" => Out::Name(cased(kw, ROMAN[(month - 1) as usize]), 4),
        _ => return Err(format!("to_char: {kw} isn't supported")),
    })
}

/// The year as the server counts it. chrono counts 1 BC as year 0, the server as 1.
fn pg_year(y: i32) -> i64 {
    if y <= 0 {
        i64::from(1 - y)
    } else {
        i64::from(y)
    }
}

/// The century: the hundreds of the year, one more when there is a remainder (2026 is 21).
/// BC years give a negative century.
fn century(year: i64, bc: bool) -> i64 {
    let c = (year + 99) / 100;
    if bc {
        -c
    } else {
        c
    }
}

/// The day of the ISO year (its week 1 starts on the Monday that is in January, or in the
/// year before). Fails only for a year chrono can't place a week 1 in.
fn iso_day_of_year(d: NaiveDate, iso_year: i32) -> Result<i64, String> {
    match NaiveDate::from_isoywd_opt(iso_year, 1, Weekday::Mon) {
        Some(start) => Ok((d - start).num_days() + 1),
        None => Err("to_char: IDDD isn't supported for this year".to_string()),
    }
}

/// A name in the case its pattern asks for: all capitals, a capital then small letters
/// (the names are stored that way), or all small letters.
fn cased(kw: &str, text: &str) -> String {
    if kw.chars().all(|c| !c.is_lowercase()) {
        text.to_uppercase()
    } else if kw.starts_with(|c: char| c.is_uppercase()) {
        text.to_string()
    } else {
        text.to_lowercase()
    }
}
