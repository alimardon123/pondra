//! The fields a template reads, and the date and time of day they make. The server's rules:
//! a missing year is 1 BC, a zero month, day, day of year or week is unset, a short year is
//! read near 2020 (or, with CC, in the century CC gives), and the ISO and Gregorian calendars
//! can't be mixed in one template.

use super::JULIAN_OFFSET;
use chrono::{Datelike, NaiveDate, TimeDelta, Weekday};

/// A year as the template read it. `short` means fewer than four digits (Y, YY, YYY and their
/// ISO kin), which the server reads near 2020 unless a century is given.
#[derive(Clone, Copy)]
pub(super) struct Year {
    /// The digits read, as a number (`negative` carries the sign).
    pub value: i64,
    pub short: bool,
    pub negative: bool,
}

/// The fields read so far. A field the template doesn't name stays None (or 0); `make`
/// gives it the server's default.
#[derive(Default)]
pub(super) struct Fields {
    pub year: Option<Year>,
    pub iso_year: Option<Year>,
    /// CC: the century, from which a short year is counted.
    pub cc: Option<i64>,
    /// BC was read (AD clears it).
    pub bc: bool,
    /// MM, MONTH, MON and RM.
    pub month: Option<i64>,
    /// DD.
    pub day: Option<i64>,
    /// DDD: the day of the year.
    pub doy: Option<i64>,
    /// W: the week of the month (day = (W - 1) * 7 + 1, which replaces DD).
    pub week_of_month: Option<i64>,
    /// WW: the week of the year (day of the year = (WW - 1) * 7 + 1).
    pub week_of_year: Option<i64>,
    pub iso_week: Option<i64>,
    /// ID: the ISO day of the week, 1 (Monday) to 7 (Sunday).
    pub iso_day: Option<i64>,
    /// IDDD: the ISO day of the year. When it is read, ID is ignored.
    pub iso_doy: Option<i64>,
    pub julian: Option<i64>,
    pub hour12: Option<i64>,
    pub hour24: Option<i64>,
    /// AM or PM was read: Some(true) for PM.
    pub meridiem: Option<bool>,
    pub minute: i64,
    pub second: i64,
    /// SSSS: seconds since midnight, which set the hour, minute and second.
    pub seconds_of_day: Option<i64>,
    /// The fraction of a second in microseconds, already rounded to the pattern's digits.
    pub micro: i64,
    /// TZH's sign (-1 for a minus; None means plus, as TZM alone is).
    pub zone_sign: Option<i64>,
    pub zone_hours: i64,
    pub zone_minutes: i64,
}

/// The date and the time of day the fields make.
pub(super) struct Stamp {
    pub date: NaiveDate,
    /// Microseconds from midnight of `date`, with the zone taken off. A fraction that rounds
    /// up may take this past a day, so the result is added to the date rather than read as a
    /// time of day.
    pub micros: i64,
}

/// The message for a field whose value can't be a date or a time.
pub(super) fn out_of_range() -> String {
    "date/time field value out of range".to_string()
}

/// Makes the date and the time of day from the fields read.
pub(super) fn make(f: &Fields) -> Result<Stamp, String> {
    if iso_fields(f) && gregorian_fields(f) {
        return Err("invalid combination of date conventions".to_string());
    }
    if needs_year(f) {
        return Err("cannot calculate day of year without year information".to_string());
    }
    let date = match set(f.julian) {
        Some(j) => julian_date(j)?,
        None if iso_mode(f) => iso_date(f)?,
        None => gregorian_date(f)?,
    };
    Ok(Stamp {
        date,
        micros: micros_of_day(f)? - zone_micros(f)?,
    })
}

/// A value that counts as read: 0 means the template left it unset.
fn set(v: Option<i64>) -> Option<i64> {
    v.filter(|&n| n != 0)
}

/// The server counts a day or week of the year only from a year it read (YYYY, YY, Y, Y,YYY or
/// CC; a year of 0 is not read): DDD and WW need one, IDDD an ISO year or CC. W, IW, ID and J
/// need none. Checked by probes of the live server (gen.py's MARKER_CASES).
fn needs_year(f: &Fields) -> bool {
    let year = f.year.is_some_and(|y| y.value != 0) || set(f.cc).is_some();
    let iso_year = f.iso_year.is_some_and(|y| y.value != 0) || set(f.cc).is_some();
    let gregorian_days = set(f.doy).is_some() || set(f.week_of_year).is_some();
    (gregorian_days && !year) || (set(f.iso_doy).is_some() && !iso_year)
}

/// An ISO year, week, day of the week or day of the year was read.
fn iso_fields(f: &Fields) -> bool {
    f.iso_year.is_some() || f.iso_week.is_some() || f.iso_doy.is_some() || f.iso_day.is_some()
}

/// A Gregorian year, month, day, day of the year or week was read.
fn gregorian_fields(f: &Fields) -> bool {
    f.year.is_some()
        || f.month.is_some()
        || f.day.is_some()
        || f.doy.is_some()
        || f.week_of_year.is_some()
        || f.week_of_month.is_some()
}

/// The ISO calendar is used when an ISO week or ISO day of the year was read. An ISO year
/// alone, or an ISO day of the week alone, doesn't change the date.
fn iso_mode(f: &Fields) -> bool {
    set(f.iso_week).is_some() || set(f.iso_doy).is_some()
}

/// The date for a Julian day number.
fn julian_date(j: i64) -> Result<NaiveDate, String> {
    let days = i32::try_from(j - JULIAN_OFFSET).map_err(|_| out_of_range())?;
    NaiveDate::from_num_days_from_ce_opt(days).ok_or_else(out_of_range)
}

/// The Gregorian date: a day of the year when the month or the day is missing (DDD, or WW),
/// else the month and day (W replaces the day). A missing month is January, a missing day 1.
fn gregorian_date(f: &Fields) -> Result<NaiveDate, String> {
    let year = i32::try_from(gregorian_year(f)).map_err(|_| out_of_range())?;
    let month = set(f.month);
    let day = set(f.day);
    let doy = set(f.doy).or_else(|| set(f.week_of_year).map(|w| (w - 1) * 7 + 1));
    if let (Some(doy), false) = (doy, month.is_some() && day.is_some()) {
        let start = NaiveDate::from_yo_opt(year, unsigned(doy)?).ok_or_else(out_of_range)?;
        return match day {
            Some(d) => NaiveDate::from_ymd_opt(year, start.month(), unsigned(d)?),
            None => Some(start),
        }
        .ok_or_else(out_of_range);
    }
    let d = match set(f.week_of_month) {
        Some(w) => (w - 1) * 7 + 1,
        None => day.unwrap_or(1),
    };
    NaiveDate::from_ymd_opt(year, unsigned(month.unwrap_or(1))?, unsigned(d)?).ok_or_else(out_of_range)
}

/// The ISO date: the Monday of ISO week 1 of the ISO year, then the week and the ISO day
/// of the week (or the ISO day of the year, which ignores ID).
fn iso_date(f: &Fields) -> Result<NaiveDate, String> {
    let year = i32::try_from(iso_year(f)).map_err(|_| out_of_range())?;
    let start = NaiveDate::from_isoywd_opt(year, 1, Weekday::Mon).ok_or_else(out_of_range)?;
    let days = match set(f.iso_doy) {
        Some(doy) => doy - 1,
        None => {
            let week = set(f.iso_week).unwrap_or(1);
            // The server takes ID 0 as Sunday and any ID past 7 as Sunday too.
            let day = match f.iso_day {
                None => 1,
                Some(0) => 7,
                Some(d) if d > 7 => 7,
                Some(d) => d,
            };
            7 * (week - 1) + (day - 1)
        }
    };
    let delta = TimeDelta::try_seconds(days.checked_mul(86_400).ok_or_else(out_of_range)?)
        .ok_or_else(out_of_range)?;
    start.checked_add_signed(delta).ok_or_else(out_of_range)
}

/// The Gregorian year, as the calendar counts it (1 BC is 0). A year of fewer than four digits
/// is read near 2020, or with CC in the century CC gives; CC alone gives the first year of its
/// century, and nothing at all gives 1 BC. A lone ISO year counts as this year.
fn gregorian_year(f: &Fields) -> i64 {
    // An ISO year with no week or ISO day is read as the Gregorian year (2026 is 2026-01-01).
    match f.year.or(f.iso_year) {
        Some(y) => read_year(y, set(f.cc), f.bc),
        None => {
            let value = set(f.cc).map_or(0, |cc| (cc - 1) * 100 + 1);
            astronomical(value, f.bc)
        }
    }
}

/// The ISO year, as the calendar counts it. Nothing read gives 1 BC.
fn iso_year(f: &Fields) -> i64 {
    match f.iso_year {
        Some(y) => read_year(y, None, f.bc),
        None => 0,
    }
}

/// The calendar year a year field reads as. A short year is counted near 2020 (or in the
/// century CC gives), with its sign counted in (-44 is 1956), and BC alone makes it BC. A full
/// year read with a minus sign is BC, as BC is.
fn read_year(y: Year, cc: Option<i64>, bc: bool) -> i64 {
    if y.short {
        let value = match cc {
            Some(c) => (c - 1) * 100 + y.value,
            None => adjust(if y.negative { -y.value } else { y.value }),
        };
        astronomical(value, bc)
    } else {
        astronomical(y.value, bc != y.negative)
    }
}

/// A year read as BC is 1 - year on the calendar (1 BC is 0). Year 0 stays 0: BC with no year
/// is 1 BC, as the default is.
fn astronomical(year: i64, bc: bool) -> i64 {
    if bc && year > 0 {
        1 - year
    } else {
        year
    }
}

/// A short year read the way the server reads it, nearest 2020: 0 to 69 are 2000 to 2069, 70
/// to 99 are 1970 to 1999, 100 to 519 are 2100 to 2519 (so 123 is 2123), 520 to 999 are 1520
/// to 1999. Years of four digits or more are left as they are.
pub(super) fn adjust(year: i64) -> i64 {
    if year < 70 {
        year + 2000
    } else if year < 100 {
        year + 1900
    } else if year < 520 {
        year + 2000
    } else if year < 1000 {
        year + 1000
    } else {
        year
    }
}

/// A count that must not be negative (a month, a day, a day of the year).
fn unsigned(v: i64) -> Result<u32, String> {
    u32::try_from(v).map_err(|_| out_of_range())
}

/// The time of day in microseconds: SSSS's seconds if read, else the hour, minute and second.
fn micros_of_day(f: &Fields) -> Result<i64, String> {
    let (hour, minute, second) = match f.seconds_of_day {
        // SSSS gives the hour, minute and second, except that an hour, minute or second read by
        // its own pattern wins: `45000 10:20` with SSSS HH24:MI is 10:20:00.
        Some(s) => {
            let hour = if set(f.hour12).is_some() || set(f.hour24).is_some() {
                hour_of(f)?
            } else {
                seconds_hour(s, f)?
            };
            let minute = if f.minute != 0 { f.minute } else { s % 3600 / 60 };
            let second = if f.second != 0 { f.second } else { s % 60 };
            (hour, minute, second)
        }
        None => (hour_of(f)?, f.minute, f.second),
    };
    Ok(((hour * 60 + minute) * 60 + second) * 1_000_000 + f.micro)
}

/// The hour SSSS gives: its whole hours. With AM or PM read, they must be on the 12-hour clock
/// (1 to 12), as an hour is.
fn seconds_hour(s: i64, f: &Fields) -> Result<i64, String> {
    let hours = s / 3600;
    match f.meridiem {
        Some(pm) if (1..=12).contains(&hours) => Ok(hours % 12 + if pm { 12 } else { 0 }),
        Some(_) => Err(out_of_range()),
        None => Ok(hours),
    }
}

/// The hour. HH and HH12 are read on the 12-hour clock (1 to 12, with AM and PM); HH24 is
/// read on the 24-hour clock, unless AM or PM was read with it (then 1 to 12 as well). With
/// AM or PM and no hour, the hour is 0, which is not on the 12-hour clock.
fn hour_of(f: &Fields) -> Result<i64, String> {
    let (hour, twelve) = match (f.hour12, f.hour24) {
        (Some(h), _) => (h, true),
        (None, Some(h)) => (h, f.meridiem.is_some()),
        (None, None) => (0, f.meridiem.is_some()),
    };
    if !twelve {
        return if (0..=23).contains(&hour) {
            Ok(hour)
        } else {
            Err(out_of_range())
        };
    }
    if !(1..=12).contains(&hour) {
        return Err(out_of_range());
    }
    let pm = f.meridiem == Some(true);
    Ok(hour % 12 + if pm { 12 } else { 0 })
}

/// The offset from UTC in microseconds, from TZH and TZM (east is positive, as TZM alone is).
/// Up to 15 hours and 59 minutes, as the server allows.
fn zone_micros(f: &Fields) -> Result<i64, String> {
    let minutes = f.zone_hours * 60 + f.zone_minutes;
    if minutes > 15 * 60 + 59 {
        return Err("time zone displacement out of range".to_string());
    }
    Ok(f.zone_sign.unwrap_or(1) * minutes * 60_000_000)
}
