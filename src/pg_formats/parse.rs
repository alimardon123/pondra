//! to_timestamp and to_date: reads a string by a template, as Postgres 16 reads it.
//!
//! The template's pieces are read in turn while input is left. Outside FX, spaces before a
//! pattern are skipped, and a digit field (a number that isn't a year or TZH) skips one
//! character that is neither a letter nor a digit before its digits, so `2026 - 03` reads with
//! YYYY MM. A number is read as many digits as there are, or as many as its pattern is wide
//! when a numeric pattern follows it or it has an ordinal suffix (FX wants exactly that many).
//! A character of the template takes one character of the input when it is a letter or digit,
//! or the input's character is not one (FX always takes one), so text never errors. Names,
//! markers and eras match ignoring case. A field read twice must read the same value.
//! `fields::make` turns what was read into a date.

use super::fields::{make, out_of_range, Fields, Stamp, Year};
use super::{marker, Pat, Piece, Suffix, Template, DAYS, MONTHS, ROMAN};
use chrono::{NaiveDate, NaiveDateTime, TimeDelta};

/// The most digits a number may have when no width is fixed.
const MAX_DIGITS: usize = 12;

/// The timestamp `s` reads by `tpl`: to_timestamp. Missing fields take the server's defaults.
pub(super) fn timestamp(s: &str, tpl: &Template) -> Result<NaiveDateTime, String> {
    let st = read(s, tpl)?;
    let midnight = st.date.and_hms_opt(0, 0, 0).ok_or_else(out_of_range)?;
    midnight
        .checked_add_signed(TimeDelta::microseconds(st.micros))
        .ok_or_else(out_of_range)
}

/// The date `s` reads by `tpl`: to_date. Its time and zone are read and dropped, so a zone
/// or a fraction doesn't move the date.
pub(super) fn date(s: &str, tpl: &Template) -> Result<NaiveDate, String> {
    Ok(read(s, tpl)?.date)
}

/// Reads the fields `s` holds by `tpl`, and makes them a date and a time of day.
fn read(s: &str, tpl: &Template) -> Result<Stamp, String> {
    let mut inp = Input {
        cs: s.chars().collect(),
        at: 0,
        fx: tpl.fx,
        seen: Vec::new(),
    };
    let mut f = Fields::default();
    // A separator in front of a number is left for the number to skip (see `field`).
    let mut after_separator = false;
    for (n, piece) in tpl.pieces.iter().enumerate() {
        // The server stops reading at the end of the input, before the next piece.
        if inp.done() {
            break;
        }
        match piece {
            Piece::Text(c) => {
                after_separator = !inp.fx && !c.is_alphanumeric() && next_is_number(&tpl.pieces, n);
                if !after_separator {
                    inp.literal(*c);
                }
            }
            Piece::Pat(p) => {
                if !inp.fx {
                    inp.skip_spaces();
                }
                let fixed = p.suffix != Suffix::None || next_is_number(&tpl.pieces, n);
                field(&mut f, p, &mut inp, fixed, std::mem::take(&mut after_separator))?;
            }
        }
    }
    make(&f)
}

/// Whether the piece after `n` is a numeric pattern, which fixes the width of the number at `n`.
fn next_is_number(pieces: &[Piece], n: usize) -> bool {
    matches!(pieces.get(n + 1), Some(Piece::Pat(p)) if is_number(p.kw))
}

/// The patterns that read a number: not names, markers, eras, zone names or TZH (which
/// reads a sign first).
fn is_number(kw: &str) -> bool {
    !matches!(
        kw.to_uppercase().as_str(),
        "MONTH" | "MON" | "DAY" | "DY" | "RM" | "AM" | "A.M." | "PM" | "P.M." | "BC" | "B.C."
            | "AD" | "A.D." | "TZ" | "TZH" | "OF"
    )
}

/// The digits a fraction pattern keeps: MS three, US six, FF1 to FF6 their number.
fn fraction_width(kw: &str) -> usize {
    match kw.to_uppercase().as_str() {
        "MS" => 3,
        "US" | "FF6" => 6,
        "FF5" => 5,
        "FF4" => 4,
        "FF3" => 3,
        "FF2" => 2,
        _ => 1,
    }
}

/// Reads one pattern's field into `f`. `fixed` gives the number its pattern's width, and
/// `after_separator` says the template had a separator (not read: `2026 -03` with YYYY-MM is
/// March, `2026-03` with YYYYMM is an error) before this number, which skips one of the input's.
fn field(f: &mut Fields, p: &Pat, inp: &mut Input, fixed: bool, after_separator: bool) -> Result<(), String> {
    let kw = p.kw;
    let width = |n: usize| fixed.then_some(n);
    if after_separator && is_number(kw) {
        inp.skip_separator();
    }
    match kw.to_uppercase().as_str() {
        "HH" | "HH12" => {
            let v = inp.number(width(2), kw)?.signed();
            f.hour12 = Some(inp.note("hour", v, kw)?);
        }
        "HH24" => {
            let v = inp.number(width(2), kw)?.signed();
            f.hour24 = Some(inp.note("hour", v, kw)?);
        }
        "MI" => f.minute = inp.ranged("minute", width(2), 0, 59, kw)?,
        "SS" => f.second = inp.ranged("second", width(2), 0, 59, kw)?,
        "SSSS" | "SSSSS" => {
            f.seconds_of_day = Some(inp.ranged("seconds", width(5), 0, 86_399, kw)?);
        }
        "MS" | "US" | "FF1" | "FF2" | "FF3" | "FF4" | "FF5" | "FF6" => {
            let micro = inp.fraction(kw, fixed)?;
            f.micro = inp.note("fraction", micro, kw)?;
        }
        // AM and AD are the zero values of their fields, so PM then AM conflicts, and AM then PM
        // does not (the server's rule for the two pairs).
        "AM" | "A.M." | "PM" | "P.M." => {
            let pm = inp.either(kw, "AM", "PM")? == 1;
            inp.note("meridiem", i64::from(pm), kw)?;
            f.meridiem = Some(pm);
        }
        "BC" | "B.C." | "AD" | "A.D." => {
            let bc = inp.either(kw, "BC", "AD")? == 0;
            inp.note("bc", i64::from(bc), kw)?;
            f.bc = bc;
        }
        "YYYY" => f.year = Some(inp.year("year", width(4), kw, false)?),
        "YYY" => f.year = Some(inp.year("year", width(3), kw, true)?),
        "YY" => f.year = Some(inp.year("year", width(2), kw, true)?),
        "Y" => f.year = Some(inp.year("year", width(1), kw, true)?),
        "Y,YYY" => f.year = Some(inp.thousands(kw)?),
        "IYYY" => f.iso_year = Some(inp.year("iso_year", width(4), kw, false)?),
        "IYY" => f.iso_year = Some(inp.year("iso_year", width(3), kw, true)?),
        "IY" => f.iso_year = Some(inp.year("iso_year", width(2), kw, true)?),
        "I" => f.iso_year = Some(inp.year("iso_year", width(1), kw, true)?),
        "MONTH" => {
            let m = inp.name(&MONTHS, false, kw)? as i64 + 1;
            f.month = Some(inp.note("month", m, kw)?);
        }
        "MON" => {
            let m = inp.name(&MONTHS, true, kw)? as i64 + 1;
            f.month = Some(inp.note("month", m, kw)?);
        }
        "RM" => {
            let m = inp.roman(kw)?;
            f.month = Some(inp.note("month", m, kw)?);
        }
        // The day of the week is read and not used: the date comes from the other fields.
        "DAY" => {
            inp.name(&DAYS, false, kw)?;
        }
        "DY" => {
            inp.name(&DAYS, true, kw)?;
        }
        "MM" => f.month = Some(inp.ranged("month", width(2), 0, 12, kw)?),
        "DD" => f.day = Some(inp.ranged("day", width(2), 0, 31, kw)?),
        "DDD" => f.doy = Some(inp.ranged("doy", width(3), 0, 366, kw)?),
        "D" | "Q" => {
            inp.number(width(1), kw)?;
        }
        "W" => f.week_of_month = Some(inp.ranged("week_of_month", width(1), 0, 5, kw)?),
        "WW" => f.week_of_year = Some(inp.ranged("week_of_year", width(2), 0, 53, kw)?),
        "IW" => f.iso_week = Some(inp.ranged("iso_week", width(2), 0, 53, kw)?),
        // Any day number is read (0 and past 7 are Sunday), and the last one read counts.
        "ID" => {
            let start = inp.at;
            let day = inp.number(width(1), kw)?.signed();
            if day < 0 {
                return Err(inp.invalid(kw, start));
            }
            f.iso_day = Some(day);
        }
        "IDDD" => f.iso_doy = Some(inp.ranged("iso_doy", width(3), 0, 366, kw)?),
        "CC" => {
            let v = inp.number(width(2), kw)?.signed();
            f.cc = Some(inp.note("cc", v, kw)?);
        }
        "J" => {
            let v = inp.number(width(7), kw)?.signed();
            f.julian = Some(inp.note("julian", v, kw)?);
        }
        "TZH" => {
            let (sign, hours) = inp.zone_hours(width(2), kw)?;
            f.zone_sign = sign;
            f.zone_hours = inp.note("tzh", hours, kw)?;
        }
        "TZM" => f.zone_minutes = inp.ranged("tzm", width(2), 0, 59, kw)?,
        // The server refuses the zone names on input (they are only for to_char).
        "TZ" | "OF" => return Err(format!("formatting field \"{kw}\" is only supported in to_char")),
        _ => return Err(format!("to_timestamp: {kw} isn't supported")),
    }
    if p.suffix != Suffix::None {
        inp.skip_suffix();
    }
    Ok(())
}

/// A number read by strtol's rules: its digits' value, the count of digits and a minus sign.
#[derive(Clone, Copy)]
struct Num {
    digits: i64,
    count: usize,
    negative: bool,
}

impl Num {
    fn signed(self) -> i64 {
        if self.negative {
            -self.digits
        } else {
            self.digits
        }
    }
}

/// The input, with a cursor. `fx` makes a fixed width exact. `seen` holds each field's value
/// as it was read, so a field read twice must read the same value.
struct Input {
    cs: Vec<char>,
    at: usize,
    fx: bool,
    seen: Vec<(&'static str, i64)>,
}

impl Input {
    fn done(&self) -> bool {
        self.at >= self.cs.len()
    }

    fn peek(&self) -> Option<char> {
        self.cs.get(self.at).copied()
    }

    fn digit(&self) -> Option<i64> {
        self.peek().and_then(|c| c.to_digit(10)).map(i64::from)
    }

    fn skip_spaces(&mut self) {
        while self.peek().is_some_and(char::is_whitespace) {
            self.at += 1;
        }
    }

    /// Outside FX, a digit field skips one character that is neither a letter nor a digit, so
    /// `2026 - 03` reads with YYYY MM.
    fn skip_separator(&mut self) {
        if self.peek().is_some_and(|c| !c.is_alphanumeric()) {
            self.at += 1;
        }
    }

    /// The two characters after a number that has TH or th, whatever they are (1st, 2nd, and
    /// the server skips a pair of any characters the same way).
    fn skip_suffix(&mut self) {
        self.at = (self.at + 2).min(self.cs.len());
    }

    /// A character of the template. It takes one character of the input when it is a letter or
    /// digit, or when the input's character isn't one; a space or a sign left in the input is
    /// then kept for the next field. FX always takes one. Nothing here is an error.
    fn literal(&mut self, want: char) {
        let Some(c) = self.peek() else {
            return;
        };
        if self.fx || want.is_alphanumeric() || !c.is_alphanumeric() {
            self.at += 1;
        }
    }

    /// Records a field's value as read, as the server keeps it: a field read again must match
    /// the value it holds, unless that is zero (not set), which the new value then replaces.
    /// So `00 05` with MM MM is May, and `05 00` is an error. The value is returned for the
    /// caller to store.
    fn note(&mut self, key: &'static str, value: i64, kw: &str) -> Result<i64, String> {
        match self.seen.iter().position(|(k, _)| *k == key) {
            Some(i) => {
                let held = self.seen[i].1;
                if held != 0 && held != value {
                    return Err(format!("conflicting values for \"{kw}\" field in formatting string"));
                }
                self.seen[i].1 = value;
            }
            None => self.seen.push((key, value)),
        }
        Ok(value)
    }

    /// A number at the cursor, after spaces, with an optional sign. A fixed width takes at
    /// most that many digits (FX wants exactly that many); otherwise up to MAX_DIGITS.
    fn number(&mut self, width: Option<usize>, kw: &str) -> Result<Num, String> {
        let start = self.at;
        while self.peek() == Some(' ') {
            self.at += 1;
        }
        let negative = self.peek() == Some('-');
        if matches!(self.peek(), Some('+' | '-')) {
            self.at += 1;
        }
        let digits_at = self.at;
        let max = width.unwrap_or(MAX_DIGITS);
        let mut digits: i64 = 0;
        while self.at - digits_at < max {
            let Some(d) = self.digit() else {
                break;
            };
            digits = digits * 10 + d;
            self.at += 1;
        }
        let count = self.at - digits_at;
        if count == 0 || (self.fx && width.is_some() && count != max) {
            return Err(self.invalid(kw, start));
        }
        Ok(Num {
            digits,
            count,
            negative,
        })
    }

    /// A number that must lie in lo..=hi, recorded as the field `key`. Zero is allowed where
    /// the server leaves a field unset (the caller decides what 0 means).
    fn ranged(
        &mut self,
        key: &'static str,
        width: Option<usize>,
        lo: i64,
        hi: i64,
        kw: &str,
    ) -> Result<i64, String> {
        let start = self.at;
        let v = self.number(width, kw)?.signed();
        if !(lo..=hi).contains(&v) {
            return Err(self.invalid(kw, start));
        }
        self.note(key, v, kw)
    }

    /// A year. A short one (Y, YY, YYY and the ISO kin) is read near 2020 later, by `fields`.
    /// Its sign is kept as it is read (no separator is skipped before a year).
    fn year(
        &mut self,
        key: &'static str,
        width: Option<usize>,
        kw: &str,
        short: bool,
    ) -> Result<Year, String> {
        let n = self.number(width, kw)?;
        self.note(key, n.signed(), kw)?;
        Ok(Year {
            value: n.digits,
            short,
            negative: n.negative,
        })
    }

    /// Y,YYY: the thousands (as many digits as there are), a comma, then three digits.
    fn thousands(&mut self, kw: &str) -> Result<Year, String> {
        let start = self.at;
        let high = self.number(None, kw)?;
        if self.peek() != Some(',') {
            return Err(self.invalid(kw, start));
        }
        self.at += 1;
        let low = self.number(Some(3), kw)?;
        // The minus sign goes with the thousands only: -2,026 is -2000 + 26, which is 1974 BC.
        let thousands = high.digits * 1000;
        let signed = if high.negative {
            -thousands + low.digits
        } else {
            thousands + low.digits
        };
        self.note("year", signed, kw)?;
        Ok(Year {
            value: signed.abs(),
            short: false,
            negative: signed < 0,
        })
    }

    /// The zone's sign and hours for TZH. Outside FX, spaces come first. A minus followed by a
    /// digit makes the zone negative; a plus, a space or nothing leaves it positive, so `- 05`
    /// is +05. The hours are the digits that follow, or 0 when there are none.
    fn zone_hours(&mut self, width: Option<usize>, kw: &str) -> Result<(Option<i64>, i64), String> {
        if !self.fx {
            self.skip_spaces();
        }
        let mut sign = None;
        if let Some(c @ ('+' | '-')) = self.peek() {
            self.at += 1;
            let negative = c == '-' && self.digit().is_some();
            sign = Some(if negative { -1 } else { 1 });
        }
        self.skip_spaces();
        let hours = if self.digit().is_some() {
            self.number(width, kw)?.digits
        } else {
            0
        };
        Ok((sign, hours))
    }

    /// A fraction of a second, in microseconds. MS and US read their digits as a number and
    /// scale it up to their width; more digits than the width is an error. FFn read their digits
    /// as six places and round them to n, a half going down, so .96 is 1.0 (the carry goes to
    /// the seconds). Negative values are errors.
    fn fraction(&mut self, kw: &str, fixed: bool) -> Result<i64, String> {
        let start = self.at;
        let w = fraction_width(kw);
        let num = self.number(fixed.then_some(w), kw)?;
        if num.negative {
            return Err(self.invalid(kw, start));
        }
        let micro = if matches!(kw.to_uppercase().as_str(), "MS" | "US") {
            let scaled = if num.count < w {
                num.digits * 10_i64.pow((w - num.count) as u32)
            } else {
                num.digits
            };
            if scaled >= 10_i64.pow(w as u32) {
                return Err(self.invalid(kw, start));
            }
            scaled * 10_i64.pow((6 - w) as u32)
        } else {
            let six = if num.count < 6 {
                num.digits * 10_i64.pow((6 - num.count) as u32)
            } else {
                num.digits
            };
            if six >= 1_000_000 {
                return Err(self.invalid(kw, start));
            }
            round_places(six, 6, w) * 10_i64.pow((6 - w) as u32)
        };
        Ok(micro)
    }

    /// A name from `names` at the cursor (its first three letters when `short`), in any case.
    /// Returns its index in `names`.
    fn name(&mut self, names: &[&str], short: bool, kw: &str) -> Result<usize, String> {
        let start = self.at;
        for (idx, &full) in names.iter().enumerate() {
            let want = if short { &full[..3] } else { full };
            if self.word_is(want) {
                self.at += want.len();
                return Ok(idx);
            }
        }
        Err(self.invalid(kw, start))
    }

    /// A month's Roman numeral at the cursor (I to XII, any case). The longest one wins.
    fn roman(&mut self, kw: &str) -> Result<i64, String> {
        let start = self.at;
        for m in (1..=12).rev() {
            let r = ROMAN[m - 1];
            if self.word_is(r) {
                self.at += r.len();
                return Ok(m as i64);
            }
        }
        Err(self.invalid(kw, start))
    }

    /// The first of two words at the cursor, in the style of `kw` (a dotted `kw` takes dotted
    /// words), in any case. Returns 0 for `first` and 1 for `second`. The server takes either
    /// word for an AM/PM or BC/AD pattern.
    fn either(&mut self, kw: &str, first: &str, second: &str) -> Result<usize, String> {
        let start = self.at;
        for (idx, word) in [first, second].into_iter().enumerate() {
            let text = marker(kw, word);
            if self.word_is(&text) {
                self.at += text.len();
                return Ok(idx);
            }
        }
        Err(self.invalid(kw, start))
    }

    /// Whether the input at the cursor reads `want`, ignoring case.
    fn word_is(&self, want: &str) -> bool {
        want.chars().enumerate().all(|(n, w)| {
            self.cs
                .get(self.at + n)
                .is_some_and(|c| c.eq_ignore_ascii_case(&w))
        })
    }

    /// The error for a field whose text (letters and digits from `start`) doesn't read.
    fn invalid(&self, kw: &str, start: usize) -> String {
        let word: String = self
            .cs
            .get(start..)
            .unwrap_or(&[])
            .iter()
            .take_while(|c| c.is_alphanumeric())
            .collect();
        format!("invalid value \"{word}\" for \"{kw}\"")
    }
}

/// The value `v` of `d` digits, as `w` places: v / 10^d rounded to w places, a half going down.
/// The result may be 10^w (a carry into the next unit).
fn round_places(v: i64, d: usize, w: usize) -> i64 {
    if d <= w {
        return v * 10_i64.pow((w - d) as u32);
    }
    let div = 10_i64.pow((d - w) as u32);
    let (q, r) = (v / div, v % div);
    q + i64::from(r > div / 2)
}
