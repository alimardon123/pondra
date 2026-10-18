//! A Postgres wire value as a SQL literal: the parameters the port substitutes into a statement
//! (`pg::bind`), and the TIME and INTERVAL text Postgres prints (`pg::PgTime`, `pg::PgInterval`).
//! Pure, with no portal and no pgwire encoder, so the unit tests at the end run alone.

use datafusion::sql::sqlparser::ast::{self, VisitMut};
use pgwire::api::Type;
use crate::pg_bytea;
use crate::pg_text::{array_literal, interval_text, time_text};
use std::ops::ControlFlow;

// A parameter comes as its bytes, text or binary as the client chose. Binary is first written as the
// text Postgres would have sent (`binary_text`); then `literal` reads that text strictly and writes
// the literal, so nothing reaches the SQL unless it parsed as its type. Dates, times and intervals
// become the forms DataFusion reads (`DATE '…'`, `TIMESTAMPTZ '…Z'`, `INTERVAL '… months … days …
// microseconds'`), numbers bare (a negative one in brackets, so `5 -$1` stays a subtraction), floats
// `CAST('…' AS DOUBLE)`, and a string, or a type with no literal of its own, a quoted string, as before.

/// Parameter `raw` (None: NULL) of type `t` as a SQL literal, read as text or, binary, by its type.
pub(crate) fn value(t: &Type, raw: Option<&[u8]>, binary: bool) -> anyhow::Result<Option<String>> {
    let Some(raw) = raw else { return Ok(None) };
    let text = match binary {
        true => binary_text(t, raw)?,
        false => utf8(raw)?,
    };
    literal(t, &text).map(Some)
}

fn utf8(raw: &[u8]) -> anyhow::Result<String> {
    std::str::from_utf8(raw).map(str::to_string).map_err(|_| crate::codes::coded("22021", "invalid byte sequence for encoding \"UTF8\""))
}

/// The SQL literal of `text`, a value of type `t` as Postgres writes it. A typed value that doesn't
/// read as its type is refused by name; anything else is a quoted string.
fn literal(t: &Type, text: &str) -> anyhow::Result<String> {
    let bad = |what: &str, code: &'static str| crate::codes::coded(code, format!("invalid input syntax for type {what}: \"{text}\""));
    Ok(match *t {
        Type::NUMERIC => sql_number(text).ok_or_else(|| bad("numeric", "22P02"))?,
        Type::UNKNOWN => sql_number(text).unwrap_or_else(|| quoted(text)), // (a number bare, anything else quoted)
        Type::BYTEA => pg_bytea::sql(&pg_bytea::parse(text).map_err(|e| crate::codes::coded(e.code, e.message))?),
        Type::DATE => format!("DATE '{}'", date_of(text).ok_or_else(|| bad("date", "22007"))?.format("%Y-%m-%d")),
        Type::TIME => format!("TIME '{}'", time_of(text).ok_or_else(|| bad("time", "22007"))?.format("%H:%M:%S%.f")),
        Type::TIMESTAMP => format!("TIMESTAMP '{}'", timestamp_of(text).ok_or_else(|| bad("timestamp", "22007"))?.format("%Y-%m-%d %H:%M:%S%.f")),
        Type::TIMESTAMPTZ => format!("TIMESTAMPTZ '{}'", instant_of(text).ok_or_else(|| bad("timestamp with time zone", "22007"))?.format("%Y-%m-%dT%H:%M:%S%.fZ")),
        Type::FLOAT4 | Type::FLOAT8 => float_literal(text, t)?,
        Type::UUID => format!("'{}'", uuid::Uuid::parse_str(text).map_err(|_| bad("uuid", "22P02"))?.hyphenated()),
        Type::INTERVAL => {
            let (months, days, micros) = interval_of(text).ok_or_else(|| bad("interval", "22007"))?;
            format!("INTERVAL '{months} months {days} days {micros} microseconds'")
        }
        _ => quoted(text),
    })
}

fn quoted(text: &str) -> String { format!("'{}'", text.replace('\'', "''")) }

/// A float as Postgres reads it (`2.5`, `-1e-3`, `NaN`, `inf`, `-Infinity`, any case), as a SQL literal
/// of Postgres's float type: `CAST('2.5' AS DOUBLE)`, or REAL for a float4. The number is quoted, so the
/// cast reads it as the float it is (a bare decimal would go through DECIMAL and round again). A value
/// past the type's range is refused (22003), as Postgres refuses it.
fn float_literal(text: &str, t: &Type) -> anyhow::Result<String> {
    let text = text.trim();
    let bad = || crate::codes::coded("22P02", format!("invalid input syntax for type {}: \"{text}\"", t.name()));
    let (sql, name, shown) = match *t {
        Type::FLOAT4 => ("REAL", "real", text.parse::<f32>().map_err(|_| bad())?.to_string()),
        _ => ("DOUBLE", "double precision", text.parse::<f64>().map_err(|_| bad())?.to_string()), // (Rust's text of the value: `inf`, `NaN`, or the digits)
    };
    let mantissa = text.split(['e', 'E']).next().unwrap_or_default();
    let named_infinity = text.trim_start_matches(['+', '-']).to_ascii_lowercase().starts_with("inf");
    let overflow = shown.contains("inf") && !named_infinity;
    let underflow = shown.bytes().all(|b| matches!(b, b'0' | b'.' | b'-')) && mantissa.bytes().any(|b| (b'1'..=b'9').contains(&b));
    if overflow || underflow {
        return Err(crate::codes::coded("22003", format!("\"{text}\" is out of range for type {name}")));
    }
    let shown = match shown.as_str() {
        "inf" => "Infinity".to_string(),
        "-inf" => "-Infinity".to_string(),
        _ => shown,
    };
    Ok(format!("CAST('{shown}' AS {sql})"))
}

/// A number as SQL reads it: plain digits and a point, the exponent worked out (`1.5E+3` is 1500),
/// a negative one in brackets. None for anything else (`NaN`, `1; DROP TABLE t`).
fn sql_number(text: &str) -> Option<String> {
    let (negative, body) = match text.strip_prefix('-') {
        Some(b) => (true, b),
        None => (false, text.strip_prefix('+').unwrap_or(text)),
    };
    let (mantissa, exponent) = match body.find(['e', 'E']) {
        Some(at) => (&body[..at], body[at + 1..].parse::<i64>().ok()?),
        None => (body, 0),
    };
    let (int, frac) = mantissa.split_once('.').unwrap_or((mantissa, ""));
    let digits = format!("{int}{frac}");
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) || exponent.abs() > 4096 {
        return None;
    }
    let point = int.len() as i64 + exponent;
    let plain = if point <= 0 {
        format!("0.{}{digits}", "0".repeat((-point) as usize))
    } else if point as usize >= digits.len() {
        format!("{digits}{}", "0".repeat(point as usize - digits.len()))
    } else {
        format!("{}.{}", &digits[..point as usize], &digits[point as usize..])
    };
    let plain = match plain.trim_start_matches('0') {
        t if t.is_empty() || t.starts_with('.') => format!("0{t}"), // (0.5 keeps its digit, 000 is 0)
        t => t.to_string(),
    };
    Some(if negative { format!("(-{plain})") } else { plain })
}

fn date_of(text: &str) -> Option<chrono::NaiveDate> { chrono::NaiveDate::parse_from_str(text, "%Y-%m-%d").ok() }

fn time_of(text: &str) -> Option<chrono::NaiveTime> { chrono::NaiveTime::parse_from_str(text, "%H:%M:%S%.f").ok() }

/// A wall-clock time, with a space or a `T` between the date and the time (psycopg sends a space).
fn timestamp_of(text: &str) -> Option<chrono::NaiveDateTime> {
    ["%Y-%m-%d %H:%M:%S%.f", "%Y-%m-%dT%H:%M:%S%.f"].iter().find_map(|f| chrono::NaiveDateTime::parse_from_str(text, f).ok())
}

/// An instant: with its zone (`+00:00`, `+01`, `Z`), or without one, which is UTC.
fn instant_of(text: &str) -> Option<chrono::DateTime<chrono::Utc>> {
    chrono::DateTime::parse_from_rfc3339(text)
        .ok()
        .or_else(|| ["%Y-%m-%d %H:%M:%S%.f%#z", "%Y-%m-%dT%H:%M:%S%.f%#z"].iter().find_map(|f| chrono::DateTime::parse_from_str(text, f).ok()))
        .map(|t| t.with_timezone(&chrono::Utc))
        .or_else(|| timestamp_of(text).map(|t| t.and_utc()))
}

/// An interval as Postgres writes it (`1 year 2 mons 3 days 04:05:06.000004`, `1 day 0:00:03`) or in
/// its unit words (`+1 day +3 second`): months, days and microseconds. None for anything else.
fn interval_of(text: &str) -> Option<(i64, i64, i64)> {
    let (mut months, mut days, mut micros) = (0i64, 0i64, 0i64);
    let mut words = text.split_whitespace().map(|w| w.trim_end_matches(','));
    while let Some(word) = words.next() {
        if word.contains(':') {
            micros = micros.checked_add(clock_micros(word)?)?;
            continue;
        }
        let n: i64 = word.parse().ok()?;
        let (m, d, us): (i64, i64, i64) = match words.next()?.to_ascii_lowercase().trim_end_matches('s') {
            "year" => (12, 0, 0),
            "mon" | "month" => (1, 0, 0),
            "week" => (0, 7, 0),
            "day" => (0, 1, 0),
            "hour" => (0, 0, 3_600_000_000),
            "min" | "minute" => (0, 0, 60_000_000),
            "sec" | "second" => (0, 0, 1_000_000),
            "microsecond" => (0, 0, 1),
            _ => return None,
        };
        months = months.checked_add(n.checked_mul(m)?)?;
        days = days.checked_add(n.checked_mul(d)?)?;
        micros = micros.checked_add(n.checked_mul(us)?)?;
    }
    Some((months, days, micros))
}

/// `[-]h:mm[:ss[.ffffff]]` as microseconds (a sign before it too).
fn clock_micros(word: &str) -> Option<i64> {
    let (negative, body) = match word.strip_prefix('-') {
        Some(b) => (true, b),
        None => (false, word.strip_prefix('+').unwrap_or(word)),
    };
    let mut parts = body.split(':');
    let hours: i64 = parts.next()?.parse().ok()?;
    let minutes: i64 = parts.next()?.parse().ok()?;
    let (seconds, fraction) = match parts.next() {
        None => (0, 0),
        Some(s) => {
            let (whole, frac) = s.split_once('.').unwrap_or((s, ""));
            if frac.len() > 6 || !frac.bytes().all(|b| b.is_ascii_digit()) {
                return None;
            }
            (whole.parse::<i64>().ok()?, if frac.is_empty() { 0 } else { format!("{frac:0<6}").parse::<i64>().ok()? })
        }
    };
    if parts.next().is_some() {
        return None;
    }
    let total = ((hours * 60 + minutes) * 60 + seconds) * 1_000_000 + fraction;
    Some(if negative { -total } else { total })
}

/// A timestamp as Postgres writes it in ISO style: `2026-01-01 00:00:00`, `…12:34:56.5`, and `+00` after
/// an instant (always in UTC here).
pub(crate) fn stamp_text(at: chrono::NaiveDateTime, zoned: bool) -> String {
    let micros = at.time().signed_duration_since(chrono::NaiveTime::MIN).num_microseconds().unwrap_or(0);
    format!("{} {}{}", at.format("%Y-%m-%d"), time_text(micros), if zoned { "+00" } else { "" })
}

/// Postgres's binary NUMERIC as its decimal text: base-10000 digits from weight, a sign and the
/// display scale. NaN and the infinities are refused by name.
fn numeric_text(raw: &[u8]) -> anyhow::Result<String> {
    let bad = || crate::codes::coded("22P03", "the binary value of a numeric parameter is malformed");
    let word = |at: usize| -> anyhow::Result<i64> {
        let bytes: [u8; 2] = raw.get(at..at + 2).ok_or_else(bad)?.try_into().map_err(|_| bad())?;
        Ok(i16::from_be_bytes(bytes).into())
    };
    let (count, weight, sign, scale) = (word(0)?, word(2)?, word(4)? as u16, word(6)?);
    if count < 0 || scale < 0 || raw.len() as i64 != 8 + 2 * count {
        return Err(bad());
    }
    let special = match sign {
        0xC000 => Some("NaN"),
        0xD000 => Some("Infinity"),
        0xF000 => Some("-Infinity"),
        _ => None,
    };
    if let Some(s) = special {
        return Err(crate::codes::coded("22P02", format!("invalid input syntax for type numeric: \"{s}\"")));
    }
    // the base-10000 digit whose place is 10000^e
    let digit = |e: i64| match weight - e {
        k if (0..count).contains(&k) => word(8 + 2 * k as usize).unwrap_or(0),
        _ => 0,
    };
    let mut int = String::new();
    for e in (0..=weight).rev() {
        int.push_str(&format!("{:04}", digit(e)));
    }
    let int = match int.trim_start_matches('0') {
        "" => "0".to_string(),
        s => s.to_string(),
    };
    let last = (weight - count + 1).min(-((scale + 3) / 4)); // (as many fraction digits as the scale needs, and every digit it has)
    let mut frac = String::new();
    for e in (last..0).rev() {
        frac.push_str(&format!("{:04}", digit(e)));
    }
    frac.truncate(scale as usize);
    while (frac.len() as i64) < scale {
        frac.push('0');
    }
    let mut text = int;
    if scale > 0 {
        text.push('.');
        text.push_str(&frac);
    }
    if sign == 0x4000 && text.bytes().any(|b| (b'1'..=b'9').contains(&b)) {
        text.insert(0, '-');
    }
    Ok(text)
}

/// Microseconds since midnight as a time; None past a day.
fn micros_time(us: i64) -> Option<chrono::NaiveTime> {
    if !(0..86_400_000_000).contains(&us) {
        return None;
    }
    chrono::NaiveTime::from_num_seconds_from_midnight_opt((us / 1_000_000) as u32, (us % 1_000_000 * 1000) as u32)
}

/// Postgres's own epoch for dates and times in binary: 2000-01-01.
fn pg_epoch() -> chrono::NaiveDate { chrono::NaiveDate::from_ymd_opt(2000, 1, 1).expect("2000-01-01 is a date") }

/// Parameter `raw` of type `t` in binary, written as the text Postgres would have sent.
fn binary_text(t: &Type, raw: &[u8]) -> anyhow::Result<String> {
    let short = || crate::codes::coded("22P03", format!("the binary value of a {} parameter has the wrong length", t.name()));
    let out_of_range = || crate::codes::coded("22008", format!("the binary value of a {} parameter is out of range", t.name()));
    let eight = || -> anyhow::Result<i64> { Ok(i64::from_be_bytes(raw.try_into().map_err(|_| short())?)) };
    let four = || -> anyhow::Result<i64> { Ok(i32::from_be_bytes(raw.try_into().map_err(|_| short())?).into()) };
    Ok(match *t {
        // bytea's bytes as Postgres writes them, which the literal reads back
        Type::BYTEA => pg_bytea::text(raw),
        Type::NUMERIC => numeric_text(raw)?,
        // a float's bits, read as the text Rust writes it (which reads back to the same value)
        Type::FLOAT8 => f64::from_bits(eight()? as u64).to_string(),
        Type::FLOAT4 => f32::from_bits(four()? as u32).to_string(),
        Type::DATE => pg_epoch().checked_add_signed(chrono::TimeDelta::days(four()?)).ok_or_else(out_of_range)?.format("%Y-%m-%d").to_string(),
        Type::TIME => micros_time(eight()?).ok_or_else(out_of_range)?.format("%H:%M:%S%.f").to_string(),
        Type::TIMESTAMP | Type::TIMESTAMPTZ => {
            // microseconds since 2000-01-01; a zone-less one is the instant in UTC
            let us = eight()?;
            let at = pg_epoch().and_hms_opt(0, 0, 0).and_then(|t| t.checked_add_signed(chrono::TimeDelta::microseconds(us))).ok_or_else(out_of_range)?;
            match *t {
                Type::TIMESTAMP => at.format("%Y-%m-%d %H:%M:%S%.f").to_string(),
                _ => at.and_utc().format("%Y-%m-%dT%H:%M:%S%.fZ").to_string(),
            }
        }
        Type::UUID => uuid::Uuid::from_slice(raw).map_err(|_| short())?.to_string(),
        // a JSONB's first byte is its version: 1, then the text
        Type::JSONB => match raw.split_first() {
            Some((&1, text)) => utf8(text)?,
            _ => return Err(crate::codes::coded("22P03", "the binary value of a jsonb parameter has no version 1 first")),
        },
        Type::JSON | Type::TEXT | Type::VARCHAR | Type::BPCHAR | Type::NAME | Type::UNKNOWN => utf8(raw)?,
        // time (microseconds), days, months: Postgres's order
        Type::INTERVAL => {
            let b: [u8; 16] = raw.try_into().map_err(|_| short())?;
            let micros = i64::from_be_bytes(b[0..8].try_into().map_err(|_| short())?);
            let days = i32::from_be_bytes(b[8..12].try_into().map_err(|_| short())?);
            let months = i32::from_be_bytes(b[12..16].try_into().map_err(|_| short())?);
            interval_text(months.into(), days.into(), micros)
        }
        _ => return Err(crate::codes::coded("0A000", format!("a {} parameter in binary isn't taken: send it as text", t.name()))),
    })
}

/// `sql` with `$1`, `$2`… replaced by `values` (their literals, in order), each replaced once: a
/// value that contains `$1` stays as written. A `$n` with no value is left as it is.
pub(crate) fn substitute(sql: &str, values: &[String]) -> String {
    let mut out = String::with_capacity(sql.len());
    let mut rest = sql;
    while let Some(at) = rest.find('$') {
        out.push_str(&rest[..at]);
        let after = &rest[at + 1..];
        let digits = after.bytes().take_while(u8::is_ascii_digit).count();
        match after[..digits].parse::<usize>().ok().and_then(|n| values.get(n.checked_sub(1)?)) {
            Some(v) => out.push_str(v),
            None => out.push_str(&rest[at..at + 1 + digits]),
        }
        rest = &after[digits..];
    }
    out.push_str(rest);
    out
}

// ---- Array literals: a string cast to an array type is read as Postgres reads it (`'{a,b}'::text[]`) ----

/// Each string literal cast to an array type, read as Postgres reads an array literal and written as the array it
/// names: `'{a,b}'::text[]`, `CAST('{1,2}' AS INT[])` and `TEXT[] '{a,b}'` become `CAST(make_array('a', 'b') AS TEXT[])`,
/// a NULL element as NULL. Nothing else is changed. Before this a text array literal was a list of one string, so
/// `doc ?| '{a,b}'::text[]` asked for the key `{a,b}`. Returns whether one was found; a malformed one is 22P02.
pub(crate) fn array_literals(stmt: &mut ast::Statement) -> anyhow::Result<bool> {
    let mut found = ArrayLiterals { found: false };
    match stmt.visit(&mut found) {
        ControlFlow::Break(e) => Err(e),
        ControlFlow::Continue(()) => Ok(found.found),
    }
}

/// The rewrite `array_literals` makes, in one walk over the statement.
struct ArrayLiterals {
    found: bool,
}

impl ast::VisitorMut for ArrayLiterals {
    type Break = anyhow::Error;

    fn post_visit_expr(&mut self, e: &mut ast::Expr) -> ControlFlow<anyhow::Error> {
        let Some((text, data_type)) = array_text(e) else { return ControlFlow::Continue(()) };
        match array_of(&text, &data_type) {
            Ok(made) => {
                *e = made;
                self.found = true;
                ControlFlow::Continue(())
            }
            Err(err) => ControlFlow::Break(err),
        }
    }
}

/// The text of a string literal cast to an array type, and that type (`'…'::t[]`, `CAST('…' AS t[])`, `t[] '…'`).
fn array_text(e: &ast::Expr) -> Option<(String, ast::DataType)> {
    match e {
        ast::Expr::Cast { kind: ast::CastKind::Cast | ast::CastKind::DoubleColon, expr, data_type, .. } if is_array(data_type) => Some((string_of(expr)?, data_type.clone())),
        ast::Expr::TypedString(typed) if is_array(&typed.data_type) => Some((string_value(&typed.value.value)?, typed.data_type.clone())),
        _ => None,
    }
}

fn is_array(t: &ast::DataType) -> bool { matches!(t, ast::DataType::Array(_)) }

fn string_of(e: &ast::Expr) -> Option<String> {
    match e {
        ast::Expr::Value(v) => string_value(&v.value),
        _ => None,
    }
}

fn string_value(v: &ast::Value) -> Option<String> {
    match v {
        ast::Value::SingleQuotedString(s) => Some(s.clone()),
        _ => None,
    }
}

/// `CAST(make_array(…) AS t)` for an array literal's elements: each a string, NULL where the literal has NULL.
fn array_of(text: &str, data_type: &ast::DataType) -> anyhow::Result<ast::Expr> {
    let items = array_literal(text)?.into_iter().map(|item| ast::Expr::value(item.map_or(ast::Value::Null, ast::Value::SingleQuotedString))).collect();
    let made = crate::text::function("make_array", items);
    Ok(ast::Expr::Cast { kind: ast::CastKind::Cast, expr: Box::new(made), data_type: data_type.clone(), array: false, format: None })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The statement `sql` reads as after `array_literals`: whether it had a cast, and its text (or the refusal's SQLSTATE
    /// and words).
    fn rewritten(sql: &str) -> Result<(bool, String), String> {
        use datafusion::sql::sqlparser::{dialect::{GenericDialect, PostgreSqlDialect}, parser::Parser};
        // (the generic dialect reads most of these; one that it refuses is read as Postgres reads it)
        let parsed = Parser::parse_sql(&GenericDialect {}, sql).or_else(|_| Parser::parse_sql(&PostgreSqlDialect {}, sql));
        let mut stmt = parsed.unwrap().remove(0);
        match array_literals(&mut stmt) {
            Ok(found) => Ok((found, crate::text::sql(&stmt))),
            Err(e) => Err(format!("{} {e}", crate::codes::of(&e))),
        }
    }

    #[test]
    fn array_literals_read_as_postgres_reads_them() {
        assert_eq!(rewritten("SELECT '{a,b}'::text[]"), Ok((true, "SELECT CAST(make_array('a', 'b') AS TEXT[])".to_string())));
        assert_eq!(rewritten("SELECT CAST('{1,2}' AS INT[])"), Ok((true, "SELECT CAST(make_array('1', '2') AS INT[])".to_string())));
        assert_eq!(rewritten(r#"SELECT TEXT[] '{a,"b,c",NULL}'"#), Ok((true, "SELECT CAST(make_array('a', 'b,c', NULL) AS TEXT[])".to_string())));
        assert_eq!(rewritten("SELECT '{}'::text[]"), Ok((true, "SELECT CAST(make_array() AS TEXT[])".to_string())), "an empty one");
        assert_eq!(rewritten("SELECT '{NULL}'::text[]"), Ok((true, "SELECT CAST(make_array(NULL) AS TEXT[])".to_string())));
        assert_eq!(rewritten("SELECT '{it''s,\"x\\\"y\"}'::text[]"), Ok((true, "SELECT CAST(make_array('it''s', 'x\"y') AS TEXT[])".to_string())), "quotes and escapes");
    }

    #[test]
    fn array_literals_leave_other_statements_as_they_are() {
        let (found, _) = rewritten("SELECT 'a'::text, ARRAY[1, 2]::int[], x FROM t WHERE y = '{a}'").unwrap();
        assert!(!found, "a string cast to text, an array built by ARRAY, and a literal not cast to an array: none");
        let (found, _) = rewritten("SELECT doc ?| ARRAY['a', 'b'] FROM t").unwrap();
        assert!(!found);
    }

    #[test]
    fn array_literals_refuse_nested_and_malformed_literals() {
        let nested = rewritten("SELECT '{{1,2},{3,4}}'::int[]").unwrap_err();
        assert!(nested.starts_with("0A000"), "{nested}");
        let malformed = rewritten("SELECT '{a,b'::text[]").unwrap_err();
        assert_eq!(malformed, r#"22P02 malformed array literal: "{a,b""#);
    }

    fn lit(t: Type, text: &str) -> String { literal(&t, text).unwrap_or_else(|e| format!("ERR {}: {e}", crate::codes::of(&e))) }

    fn bin(t: Type, raw: &[u8]) -> String { value(&t, Some(raw), true).map(|v| v.unwrap_or_default()).unwrap_or_else(|e| format!("ERR {}: {e}", crate::codes::of(&e))) }

    #[test]
    fn numeric_is_a_bare_number_and_refuses_what_is_not_one() {
        assert_eq!(lit(Type::NUMERIC, "15.7563"), "15.7563");
        assert_eq!(lit(Type::NUMERIC, "-4.24"), "(-4.24)");
        assert_eq!(lit(Type::NUMERIC, "1.5E+3"), "1500");
        assert_eq!(lit(Type::NUMERIC, "1E-3"), "0.001");
        assert_eq!(lit(Type::NUMERIC, ".5"), "0.5");
        assert_eq!(lit(Type::NUMERIC, "000.50"), "0.50");
        assert_eq!(lit(Type::NUMERIC, "1; DROP TABLE t"), "ERR 22P02: invalid input syntax for type numeric: \"1; DROP TABLE t\"");
        assert!(lit(Type::NUMERIC, "NaN").starts_with("ERR 22P02"));
        assert!(lit(Type::NUMERIC, "Infinity").starts_with("ERR 22P02"));
        assert!(lit(Type::NUMERIC, "1.2.3").starts_with("ERR 22P02"));
    }

    #[test]
    fn bytea_reads_both_of_postgres_forms() {
        assert_eq!(lit(Type::BYTEA, "\\xff0001"), "X'ff0001'");
        assert_eq!(lit(Type::BYTEA, "\\x"), "X''");
        assert_eq!(lit(Type::BYTEA, "\\xFF"), "X'ff'");
        // (no `\x`: the escape form, so the text's own bytes)
        assert_eq!(lit(Type::BYTEA, "ff00"), "X'66663030'");
        assert!(lit(Type::BYTEA, "\\x0").starts_with("ERR 22023"));
        assert!(lit(Type::BYTEA, "a\\b").starts_with("ERR 22P02"));
    }

    #[test]
    fn dates_and_times_as_psycopg_and_postgres_write_them() {
        assert_eq!(lit(Type::DATE, "2020-01-01"), "DATE '2020-01-01'");
        assert!(lit(Type::DATE, "2020-13-01").starts_with("ERR 22007"));
        assert_eq!(lit(Type::TIME, "12:34:56.789000"), "TIME '12:34:56.789'");
        assert_eq!(lit(Type::TIME, "12:34:56"), "TIME '12:34:56'");
        assert_eq!(lit(Type::TIMESTAMP, "2020-01-01 12:00:00"), "TIMESTAMP '2020-01-01 12:00:00'");
        assert_eq!(lit(Type::TIMESTAMP, "2020-01-01T12:00:00.5"), "TIMESTAMP '2020-01-01 12:00:00.500'");
        assert_eq!(lit(Type::TIMESTAMPTZ, "2020-01-01 12:00:00+00:00"), "TIMESTAMPTZ '2020-01-01T12:00:00Z'");
        assert_eq!(lit(Type::TIMESTAMPTZ, "2020-01-01 13:00:00+01"), "TIMESTAMPTZ '2020-01-01T12:00:00Z'");
        assert_eq!(lit(Type::TIMESTAMPTZ, "2020-01-01T12:00:00.123456Z"), "TIMESTAMPTZ '2020-01-01T12:00:00.123456Z'");
        assert_eq!(lit(Type::TIMESTAMPTZ, "2020-01-01 12:00:00"), "TIMESTAMPTZ '2020-01-01T12:00:00Z'");
    }

    #[test]
    fn uuids_and_text_are_quoted_and_checked() {
        assert_eq!(lit(Type::UUID, "12345678-1234-5678-1234-567812345678"), "'12345678-1234-5678-1234-567812345678'");
        assert!(lit(Type::UUID, "not a uuid").starts_with("ERR 22P02"));
        assert_eq!(lit(Type::JSON, "{\"a\": \"it's\"}"), "'{\"a\": \"it''s\"}'");
        assert_eq!(lit(Type::TEXT, "it's"), "'it''s'");
        assert_eq!(lit(Type::UNKNOWN, "5"), "5");
        assert_eq!(lit(Type::UNKNOWN, "-5"), "(-5)");
        assert_eq!(lit(Type::UNKNOWN, "NaN"), "'NaN'");
    }

    #[test]
    fn intervals_read_postgres_text_and_units() {
        assert_eq!(lit(Type::INTERVAL, "1 day 0:00:03.000004"), "INTERVAL '0 months 1 days 3000004 microseconds'");
        assert_eq!(lit(Type::INTERVAL, "1 year 2 mons 3 days 04:05:06.000004"), "INTERVAL '14 months 3 days 14706000004 microseconds'");
        assert_eq!(lit(Type::INTERVAL, "-1 day 23:59:56.999996"), "INTERVAL '0 months -1 days 86396999996 microseconds'");
        assert_eq!(lit(Type::INTERVAL, "+1 day +3 second +4 microsecond"), "INTERVAL '0 months 1 days 3000004 microseconds'");
        assert_eq!(lit(Type::INTERVAL, "00:00:00"), "INTERVAL '0 months 0 days 0 microseconds'");
        assert!(lit(Type::INTERVAL, "1 fortnight").starts_with("ERR 22007"));
    }

    #[test]
    fn binary_values_are_decoded_by_their_type() {
        assert_eq!(bin(Type::BYTEA, &[0xff, 0x00, 0x01]), "X'ff0001'");
        // 15.7563: two base-10000 digits (15, 7563), weight 0, positive, scale 4
        assert_eq!(bin(Type::NUMERIC, &[0, 2, 0, 0, 0, 0, 0, 4, 0, 15, 0x1d, 0x8b]), "15.7563");
        // -4.24 and 0.0001
        assert_eq!(bin(Type::NUMERIC, &[0, 2, 0, 0, 0x40, 0, 0, 2, 0, 4, 0x09, 0x60]), "(-4.24)");
        assert_eq!(bin(Type::NUMERIC, &[0, 1, 0xff, 0xff, 0, 0, 0, 4, 0, 1]), "0.0001");
        assert!(bin(Type::NUMERIC, &[0, 0, 0, 0, 0xc0, 0, 0, 0]).starts_with("ERR 22P02"));
        // 2020-01-01 is 7305 days after 2000-01-01
        assert_eq!(bin(Type::DATE, &7305i32.to_be_bytes()), "DATE '2020-01-01'");
        // 12:34:56.789, and 2020-01-01 12:00 as a timestamp (631195200 seconds after the epoch)
        assert_eq!(bin(Type::TIME, &45_296_789_000i64.to_be_bytes()), "TIME '12:34:56.789'");
        let noon = 631_195_200_000_000i64;
        assert_eq!(bin(Type::TIMESTAMP, &noon.to_be_bytes()), "TIMESTAMP '2020-01-01 12:00:00'");
        assert_eq!(bin(Type::TIMESTAMPTZ, &noon.to_be_bytes()), "TIMESTAMPTZ '2020-01-01T12:00:00Z'");
        assert!(bin(Type::DATE, &[0, 0]).starts_with("ERR 22P03"));
        assert_eq!(bin(Type::UUID, &[0x12, 0x34, 0x56, 0x78, 0x12, 0x34, 0x56, 0x78, 0x12, 0x34, 0x56, 0x78, 0x12, 0x34, 0x56, 0x78]), "'12345678-1234-5678-1234-567812345678'");
        let mut jsonb = vec![1u8];
        jsonb.extend_from_slice(b"{}");
        assert_eq!(bin(Type::JSONB, &jsonb), "'{}'");
        assert!(bin(Type::JSONB, b"{}").starts_with("ERR 22P03"));
        let mut interval = 3_000_004i64.to_be_bytes().to_vec();
        interval.extend_from_slice(&1i32.to_be_bytes());
        interval.extend_from_slice(&0i32.to_be_bytes());
        assert_eq!(bin(Type::INTERVAL, &interval), "INTERVAL '0 months 1 days 3000004 microseconds'");
        assert_eq!(bin(Type::TEXT, b"abc"), "'abc'");
    }

    #[test]
    fn a_binary_type_without_a_decoder_is_refused_by_name_never_read_as_text() {
        let e = value(&Type::INET, Some(&[10, 0, 0, 1]), true).unwrap_err();
        assert_eq!(crate::codes::of(&e), "0A000");
        assert!(e.to_string().contains("a inet parameter in binary isn't taken: send it as text"));
        assert_eq!(value(&Type::INET, Some(&b"10.0.0.1"[..]), false).unwrap(), Some("'10.0.0.1'".to_string()));
        assert_eq!(value(&Type::TEXT, None, true).unwrap(), None);
    }

    #[test]
    fn stamp_text_cuts_the_fraction_and_marks_a_zone_as_postgres_does() {
        let at = |s: &str| chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f").unwrap();
        assert_eq!(stamp_text(at("2026-01-01 00:00:00"), false), "2026-01-01 00:00:00");
        assert_eq!(stamp_text(at("2026-01-07 13:05:09.5"), false), "2026-01-07 13:05:09.5");
        assert_eq!(stamp_text(at("2026-01-07 13:05:09.123456"), true), "2026-01-07 13:05:09.123456+00");
        assert_eq!(stamp_text(at("2026-01-01 00:00:00"), true), "2026-01-01 00:00:00+00");
    }

    #[test]
    fn floats_are_cast_from_quoted_text_as_rust_reads_them() {
        assert_eq!(lit(Type::FLOAT8, "2.5"), "CAST('2.5' AS DOUBLE)");
        assert_eq!(lit(Type::FLOAT8, "-2.5"), "CAST('-2.5' AS DOUBLE)");
        assert_eq!(lit(Type::FLOAT8, "1e-3"), "CAST('0.001' AS DOUBLE)");
        assert_eq!(lit(Type::FLOAT8, "0.0"), "CAST('0' AS DOUBLE)");
        assert_eq!(lit(Type::FLOAT8, "NaN"), "CAST('NaN' AS DOUBLE)");
        assert_eq!(lit(Type::FLOAT8, "Infinity"), "CAST('Infinity' AS DOUBLE)");
        assert_eq!(lit(Type::FLOAT8, "-inf"), "CAST('-Infinity' AS DOUBLE)");
        assert_eq!(lit(Type::FLOAT4, "0.1"), "CAST('0.1' AS REAL)");
        assert!(lit(Type::FLOAT8, "1e400").starts_with("ERR 22003"));
        assert!(lit(Type::FLOAT8, "1e-400").starts_with("ERR 22003"));
        assert!(lit(Type::FLOAT8, "2.5x").starts_with("ERR 22P02"));
        // a negative float after a minus stays a subtraction: the number is quoted
        assert_eq!(substitute("SELECT 5 - $1", &[lit(Type::FLOAT8, "-2.5")]), "SELECT 5 - CAST('-2.5' AS DOUBLE)");
        assert_eq!(bin(Type::FLOAT8, &(-2.5f64).to_be_bytes()), "CAST('-2.5' AS DOUBLE)");
        assert_eq!(bin(Type::FLOAT8, &f64::INFINITY.to_be_bytes()), "CAST('Infinity' AS DOUBLE)");
        assert_eq!(bin(Type::FLOAT8, &f64::NAN.to_be_bytes()), "CAST('NaN' AS DOUBLE)");
        assert_eq!(bin(Type::FLOAT4, &1.5f32.to_be_bytes()), "CAST('1.5' AS REAL)");
        assert_eq!(bin(Type::FLOAT4, &f32::NEG_INFINITY.to_be_bytes()), "CAST('-Infinity' AS REAL)");
    }

    #[test]
    fn substitution_is_one_pass_and_reads_whole_numbers() {
        assert_eq!(substitute("SELECT $1, $2", &["'$2'".into(), "'z'".into()]), "SELECT '$2', 'z'");
        let ten: Vec<String> = (1..=10).map(|i| format!("{i}")).collect();
        assert_eq!(substitute("SELECT $10 + $1", &ten), "SELECT 10 + 1");
        assert_eq!(substitute("SELECT $3, $0, $x, $", &ten[..2]), "SELECT $3, $0, $x, $");
    }
}
