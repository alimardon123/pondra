//! Other databases' names for what Pondra's functions do, each written as the SQL it means (`call`, from
//! `friendly::expression`, so every door and every form reads them alike):
//! - Snowflake's `IFF`, `ZEROIFNULL`, `DECODE(x, a, ra, b, rb, …, else)`, `ARRAY_CONSTRUCT`, `ARRAY_SIZE`, `OBJECT_CONSTRUCT`;
//! - SQL Server's `IIF`, `ISNULL(a, b)`, `CHARINDEX(sub, s)`, `TRY_CONVERT(type, 'text')`, `GETDATE()`, `EOMONTH(d)`, `DATEPART(part, d)`;
//! - MySQL's `CURDATE()`, `UNIX_TIMESTAMP([ts])`, `STR_TO_DATE(s, '%m/%d/%Y')`, `DATE_FORMAT(ts, '%Y-%m-%d %H:%i')` (its
//!   `FROM_UNIXTIME(n)` is Pondra's own);
//! - Spark's `date_format(ts, 'yyyy-MM-dd HH:mm')`, Java's letters (a `%` in the format is MySQL's, as above);
//! - BigQuery's `COUNTIF`, `DATE(ts)`, `DATE_TRUNC(d, MONTH)`, `TIMESTAMP_TRUNC(ts, HOUR)`,
//!   `TIMESTAMP_DIFF(a, b, HOUR)`, `FORMAT_DATE(fmt, d)`, `PARSE_DATE(fmt, s)`, `UNIX_SECONDS(ts)`, `TIMESTAMP_SECONDS(n)`.
//!
//! BigQuery's `SAFE_DIVIDE(a, b)` answers in a type its arguments decide, so it is a function of its own (`register`).
//!
//! A name Pondra already has keeps its own meaning where its arguments say so: `decode(bytes, 'hex')`, `date_trunc('month',
//! ts)`. MySQL's `TIMESTAMPDIFF` counts whole units and Snowflake's counts part boundaries, so it is refused by name with
//! `date_diff`, which counts boundaries as DuckDB does.
//!
//! Another database's name for a function Pondra doesn't have is refused by name, with what to write (`OURS`). So is SQL
//! Server's `FORMAT(value, 'pattern')`, which Postgres's `format` takes the other way round.
use crate::friendly::{part_word, quote, string_of as string}; // (`string`: a string literal's text)
use anyhow::Result;
use std::sync::LazyLock;
use datafusion::sql::sqlparser::ast::{Expr, ValueWithSpan};

/// Other databases' functions Pondra doesn't have, as (name, whose, what to write). `call` refuses each by name, and
/// `friendly::wanted` looks for them, so the list is kept in one place.
pub(crate) const OURS: &[(&str, &str, &str)] = &[
    ("parse_json", "Snowflake", "JSON is text here: payload->>'a', json_extract(payload, '$.a.b')"),
    ("try_parse_json", "Snowflake", "JSON is text here: payload->>'a', json_extract(payload, '$.a.b')"),
    ("json_unquote", "MySQL", "payload->>'$.a' or json_extract_string(payload, '$.a')"),
    ("try_to_number", "Snowflake", "TRY_CAST(s AS DECIMAL(38, 0))"),
    ("try_to_decimal", "Snowflake", "TRY_CAST(s AS DECIMAL(38, 0))"),
    ("try_to_numeric", "Snowflake", "TRY_CAST(s AS DECIMAL(38, 0))"),
    ("try_to_double", "Snowflake", "TRY_CAST(s AS DOUBLE)"),
    ("try_to_date", "Snowflake", "TRY_CAST(s AS DATE)"),
    ("try_to_timestamp", "Snowflake", "TRY_CAST(s AS TIMESTAMP)"),
    ("regexp_contains", "BigQuery", "regexp_like(s, re)"),
    ("timestamp_sub", "BigQuery", "ts - INTERVAL 1 HOUR"),
    ("timestamp_add", "BigQuery", "ts + INTERVAL 1 HOUR"),
    ("percentile_approx", "Spark", "approx_percentile_cont(x, p)"),
    ("months_between", "Spark", "date_diff('month', a, b) (whole month boundaries; Spark's is fractional)"),
];

/// The text `name(args)` is, or None: not one of these, or not with these arguments.
pub fn call(name: &str, a: &[&Expr]) -> Result<Option<String>> {
    // (a plain call of any arity: the name is refused, with what to write)
    if let Some((_, whose, ours)) = OURS.iter().find(|(n, _, _)| *n == name) {
        return Err(crate::codes::coded("42883", format!("{} is {whose}'s: write {ours}", name.to_uppercase())));
    }
    let epoch = |x: &Expr| format!("date_part('epoch', {x})");
    // (a moment as whole microseconds since 1970, exactly: its minute's epoch is a whole number a Float64 holds, and
    // the microseconds within the minute an integer; `epoch(x) * 1000000` is a Float64 one step of which is 0.24 µs)
    let micros = |x: &str| format!("(CAST(date_part('epoch', date_trunc('minute', {x})) AS BIGINT) * 1000000 + CAST(date_part('microsecond', {x}) AS BIGINT))");
    // (BigQuery's WEEK is WEEK(SUNDAY): the Sunday on or before, at midnight)
    let sunday = |x: &Expr| format!("date_trunc('day', {x}) - CAST(date_part('dow', {x}) AS BIGINT) * INTERVAL '1 day'");
    Ok(Some(match (name, a.len()) {
        ("iff" | "iif", 3) => format!("CASE WHEN {} THEN {} ELSE {} END", a[0], a[1], a[2]),
        ("zeroifnull", 1) => format!("coalesce({}, 0)", a[0]),
        // (NULL matches NULL in DECODE, as Snowflake and Oracle have it)
        ("decode", n) if n >= 3 => {
            let pairs: Vec<String> = a[1..].chunks(2).filter(|c| c.len() == 2).map(|c| format!("WHEN {} IS NOT DISTINCT FROM {} THEN {}", a[0], c[0], c[1])).collect();
            let other = if n % 2 == 0 { format!(" ELSE {}", a[n - 1]) } else { String::new() };
            format!("CASE {}{other} END", pairs.join(" "))
        }
        ("array_construct", _) => format!("make_array({})", list(a)),
        ("array_size", 1) => format!("cardinality({})", a[0]),
        // (a pair whose value is NULL is left out, as Snowflake leaves it)
        ("object_construct", n) if n % 2 == 0 => format!("json_strip_nulls(json_build_object({}))", list(a)),
        ("isnull", 2) => format!("coalesce({}, {})", a[0], a[1]),
        ("charindex", 2) => format!("strpos({}, {})", a[1], a[0]),
        ("try_convert", 2) => format!("TRY_CAST({} AS {})", a[1], a[0]), // (a literal's: `TRY_CONVERT(INT, x)` is `friendly`'s refusal)
        ("getdate" | "sysdatetime", 0) => "CAST(now() AS TIMESTAMP)".into(),
        ("eomonth", 1) => format!("CAST(date_trunc('month', {}) + INTERVAL '1 month' - INTERVAL '1 day' AS DATE)", a[0]),
        // (SQL Server's, the part a bare word; `datepart('month', d)` is Pondra's own)
        ("datepart", 2) => match word(a[0]).as_deref() {
            None => return Ok(None),
            Some("weekday" | "dw") => format!("CAST(date_part('dow', {}) + 1 AS INT)", a[1]), // (1 is Sunday, as SQL Server's default)
            Some("dayofyear" | "dy") => format!("CAST(date_part('doy', {}) AS INT)", a[1]),
            Some("isowk" | "isoww" | "iso_week") => format!("CAST(date_part('week', {}) AS INT)", a[1]),
            Some(_) => match part_word(a[0], name)? {
                // (SQL Server's weeks start on Sunday, 1 January in week 1; ISO's, `date_part('week')`, on Monday)
                Some("week") => format!("CAST((CAST(date_part('doy', {0}) AS BIGINT) + CAST(date_part('dow', date_trunc('year', {0})) AS BIGINT) + 6) / 7 AS INT)", a[1]),
                // (the milliseconds or microseconds within the second; `date_part`'s count the seconds too)
                Some("millisecond") => format!("CAST(CAST(date_part('millisecond', {}) AS BIGINT) % 1000 AS INT)", a[1]),
                Some("microsecond") => format!("CAST(CAST(date_part('microsecond', {}) AS BIGINT) % 1000000 AS INT)", a[1]),
                Some(p) => format!("CAST(date_part('{p}', {}) AS INT)", a[1]),
                None => return Ok(None),
            },
        },
        ("curdate", 0) => "current_date".into(),
        ("unix_timestamp", 0) => "CAST(floor(date_part('epoch', now())) AS BIGINT)".into(),
        ("unix_timestamp" | "unix_seconds", 1) => format!("CAST(floor({}) AS BIGINT)", epoch(a[0])),
        ("timestamp_seconds", 1) => format!("from_unixtime({})", a[0]), // (MySQL's FROM_UNIXTIME is Pondra's own)
        ("str_to_date", 2) => {
            let Some(f) = string(a[1]) else { return Ok(None) };
            let (chrono, time) = mysql_format(f)?;
            format!("{}({}, {})", if time { "to_timestamp" } else { "to_date" }, a[0], quote(&chrono))
        }
        // (`date_format` is DataFusion's own name for `to_char`, with chrono's codes: a format with `%` is MySQL's only when
        // it holds MySQL's `%i` (minutes), which chrono hasn't; one with a code the two read apart is refused, and one with
        // only codes they share is DataFusion's, as written. With no `%` it is Spark's; a column of formats is DataFusion's.
        // A format with no code in it (only literal text, as `'''Q'''`) is that text, not Pondra's `to_char` template, where
        // Q is a quarter)
        ("date_format", 2) => {
            let Some(f) = string(a[1]) else { return Ok(None) };
            if f.contains('%') && !mysqls(f) {
                return match apart(f) {
                    Some(k) => Err(crate::codes::coded("22023", format!("date_format(ts, '{f}'): %{k} is one thing in MySQL's DATE_FORMAT and another in DataFusion's date_format (chrono's codes), so it isn't guessed: write strftime(ts, '…') with DuckDB's codes (%M the minutes, %B a month's name, %S the seconds, %A a weekday's name)"))),
                    None => Ok(None),
                };
            }
            let chrono = if f.contains('%') { mysql_format(f)?.0 } else { java_format(f)? };
            if chrono.contains('%') {
                format!("to_char({}, {})", a[0], quote(&chrono))
            } else {
                format!("CASE WHEN {0} IS NULL THEN NULL ELSE {1} END", a[0], quote(&chrono))
            }
        }
        // (SQL Server's FORMAT(value, pattern): a value first, a pattern second, where Postgres's format reads the pattern first)
        // (only a pattern as SQL Server writes one, `'yyyy-MM-dd'` or `'N2'`, after a value that is no text of any kind:
        // `format(E'%s\n', 'x')` and `format(pattern_col, 'x')` are Postgres's)
        ("format", 2) => match (texts(a[0]), string(a[1])) {
            (false, Some(p)) if dotnet(p) => return Err(crate::codes::coded("0A000", format!("FORMAT(value, '{p}') is SQL Server's (Postgres's format takes its pattern first): write strftime(ts, '%Y-%m-%d') or to_char(ts, 'YYYY-MM-DD') for a date, printf('%.2f', x) for a number"))),
            _ => return Ok(None),
        },
        ("timestampdiff", 3) => {
            return Err(crate::codes::coded("42883", format!("TIMESTAMPDIFF counts whole units in MySQL and part boundaries in Snowflake, so it isn't taken: date_diff('{}', {}, {}) counts the boundaries, as DuckDB does", a[0].to_string().to_lowercase(), a[1], a[2])));
        }
        ("countif", 1) => format!("count(CASE WHEN {} THEN 1 END)", a[0]),
        ("date", 1) => format!("CAST({} AS DATE)", a[0]),
        ("date", 3) => format!("make_date({}, {}, {})", a[0], a[1], a[2]),
        // (BigQuery's order, the part a word after the value; `date_trunc('month', d)` is Pondra's own)
        ("date_trunc", 2) if string(a[0]).is_none() => match bare_part(a[1]) {
            Some("week") => format!("CAST({} AS DATE)", sunday(a[0])),
            Some(p) => format!("CAST(date_trunc('{}', {}) AS DATE)", iso(p), a[0]),
            None => return Ok(None),
        },
        ("timestamp_trunc" | "datetime_trunc", 2) => match bare_part(a[1]) {
            Some("week") => sunday(a[0]),
            Some(p) => format!("date_trunc('{}', {})", iso(p), a[0]),
            None => return Ok(None),
        },
        // (BigQuery's, a day or shorter: TIMESTAMP_DIFF the whole parts from `b` to `a` (a DAY is 24 hours),
        // DATETIME_DIFF the part's boundaries crossed (559 days from 2008-12-25 15:30 to 2010-07-07 10:20), each in
        // whole microseconds)
        ("timestamp_diff" | "datetime_diff", 3) => {
            let Some(p) = bare_part(a[2]) else { return Ok(None) };
            let scale = match p {
                "day" => " / 86400000000",
                "hour" => " / 3600000000",
                "minute" => " / 60000000",
                "second" => " / 1000000",
                "millisecond" => " / 1000",
                "microsecond" => "",
                _ => return Ok(None),
            };
            let at = |x: &Expr| match name {
                "datetime_diff" => micros(&format!("date_trunc('{p}', {x})")),
                _ => micros(&x.to_string()),
            };
            format!("(({} - {}){scale})", at(a[0]), at(a[1]))
        }
        ("format_date" | "format_timestamp" | "format_datetime", 2) => format!("to_char({}, {})", a[1], a[0]),
        ("parse_date", 2) => format!("to_date({}, {})", a[1], a[0]),
        ("parse_timestamp" | "parse_datetime", 2) => format!("to_timestamp({}, {})", a[1], a[0]),
        _ => return Ok(None),
    }))
}

fn list(a: &[&Expr]) -> String { a.iter().map(|e| e.to_string()).collect::<Vec<_>>().join(", ") }


/// A bare word's text, in lower case (`weekday`, `MONTH`).
fn word(e: &Expr) -> Option<String> {
    match e {
        Expr::Identifier(i) if i.quote_style.is_none() => Some(i.value.to_lowercase()),
        _ => None,
    }
}

/// Is `e` text written in any of SQL's ways (`'…'`, `E'…'`, `$$…$$`, `N'…'`)?
fn texts(e: &Expr) -> bool { matches!(e, Expr::Value(ValueWithSpan { value: v, .. }) if v.clone().into_string().is_some()) }

/// Does `p` look like a pattern of .NET's, as SQL Server's FORMAT takes: a standard one (`N2`, `C`, `X`; a lower-case
/// letter alone, as `'x'`, is a value as likely), or one made of a date's or a number's placeholders (`yyyy-MM-dd`,
/// `#,##0.00`)?
fn dotnet(p: &str) -> bool {
    static STANDARD: LazyLock<regex::Regex> = LazyLock::new(|| regex::Regex::new(r"^(?:[CDEFGNPRX]\d{0,2}|[cdefgnprx]\d{1,2})$").expect("a regex"));
    static CUSTOM: LazyLock<regex::Regex> = LazyLock::new(|| regex::Regex::new(r"yy|MM|dd|HH|hh|mm|ss|#|0[.,]0").expect("a regex"));
    !p.contains('%') && (STANDARD.is_match(p) || CUSTOM.is_match(p))
}

/// The codes of a `%` format (`%%` is a `%`).
fn codes(f: &str) -> impl Iterator<Item = char> + '_ {
    let mut chars = f.chars();
    std::iter::from_fn(move || loop {
        match chars.next()? {
            '%' => return chars.next(),
            _ => continue,
        }
    })
}

/// Does a `%` format hold MySQL's own `%i` (its minutes), which chrono hasn't?
fn mysqls(f: &str) -> bool { codes(f).any(|k| k == 'i') }

/// A code a `%` format holds that MySQL's DATE_FORMAT and chrono read apart (`%M` a month's name or the minutes, `%s`
/// the seconds or the epoch, …), if any.
fn apart(f: &str) -> Option<char> { codes(f).find(|k| "MshWceklxvfDVXu".contains(*k)) }

/// The part `date_trunc` takes for one of BigQuery's: ISOWEEK is ISO's week, from Monday.
fn iso(p: &str) -> &str { if p == "isoweek" { "week" } else { p } }

/// A part written as BigQuery writes it, a whole bare word (`MONTH`; not `m` or `d`, which may be a column's name).
fn bare_part(e: &Expr) -> Option<&'static str> {
    Some(match word(e)?.as_str() {
        "year" => "year",
        "quarter" => "quarter",
        "month" => "month",
        "week" => "week",
        "isoweek" => "isoweek",
        "day" => "day",
        "hour" => "hour",
        "minute" => "minute",
        "second" => "second",
        "millisecond" => "millisecond",
        "microsecond" => "microsecond",
        _ => return None,
    })
}

/// MySQL's date format as chrono's (`%i` minutes, `%s` seconds, `%M` a month's name, …), and whether it reads a time. A
/// code Pondra has none for (`%D`, `%V`, `%X`) is refused by name; a letter MySQL doesn't list is written as itself.
fn mysql_format(f: &str) -> Result<(String, bool)> {
    let (mut out, mut time, mut chars) = (String::new(), false, f.chars());
    while let Some(c) = chars.next() {
        if c != '%' {
            out.push(c);
            continue;
        }
        let Some(k) = chars.next() else {
            out.push_str("%%");
            break;
        };
        let (s, t) = match k {
            'i' => ("%M", true),
            's' | 'S' => ("%S", true),
            'M' => ("%B", false),
            'W' => ("%A", false),
            'c' => ("%-m", false),
            'e' => ("%-d", false),
            'h' | 'I' => ("%I", true),
            'k' => ("%-H", true),
            'l' => ("%-I", true),
            'H' => ("%H", true),
            'p' => ("%p", true),
            'f' => ("%6f", true),
            'r' => ("%I:%M:%S %p", true),
            'T' => ("%H:%M:%S", true),
            'Y' => ("%Y", false),
            'y' => ("%y", false),
            'm' => ("%m", false),
            'd' => ("%d", false),
            'b' => ("%b", false),
            'a' => ("%a", false),
            'j' => ("%j", false),
            'U' => ("%U", false),
            'u' => return Err(crate::codes::coded("22023", "MySQL's %u (weeks from Monday, week 1 the first with four days of the year, days before it week 0) isn't taken: write %v for ISO's week, with %x its year")),
            'v' => ("%V", false),
            'x' => ("%G", false),
            'w' => ("%w", false),
            'D' | 'V' | 'X' => return Err(crate::codes::coded("22023", format!("MySQL's %{k} isn't taken: Pondra has no code for it (write it apart with CASE)"))),
            '%' => ("%%", false),
            _ => {
                out.push(k);
                continue;
            }
        };
        out.push_str(s);
        time |= t;
    }
    Ok((out, time))
}

/// Spark's date pattern (Java's letters) as chrono's, which `to_char` reads: `yyyy-MM-dd HH:mm:ss.SSS` is
/// `%Y-%m-%d %H:%M:%S.%3f`. A run of one letter is one field; `'text'` is literal text and `''` one quote. Another
/// letter or count is refused by name.
fn java_format(f: &str) -> Result<String> {
    let (mut out, mut quoted, mut chars) = (String::new(), false, f.chars().peekable());
    while let Some(c) = chars.next() {
        if c == '\'' {
            if chars.peek() == Some(&'\'') {
                chars.next();
                push_text(&mut out, '\'');
            } else {
                quoted = !quoted;
            }
        } else if quoted || !c.is_ascii_alphabetic() {
            push_text(&mut out, c);
        } else {
            let mut n = 1;
            while chars.peek() == Some(&c) {
                chars.next();
                n += 1;
            }
            out.push_str(java_field(c, n)?);
        }
    }
    Ok(out)
}

/// A literal character in chrono's format: `%` is `%%`.
fn push_text(out: &mut String, c: char) {
    if c == '%' {
        out.push_str("%%");
    } else {
        out.push(c);
    }
}

/// One of Java's fields, a letter `c` written `n` times, as chrono's code.
fn java_field(c: char, n: usize) -> Result<&'static str> {
    Ok(match (c, n) {
        ('y', 2) => "%y",
        ('y', _) => "%Y",
        ('M', 1) => "%-m",
        ('M', 2) => "%m",
        ('M', 3) => "%b",
        ('M', 4) => "%B",
        ('d', 1) => "%-d",
        ('d', 2) => "%d",
        ('H', 1) => "%-H",
        ('H', 2) => "%H",
        ('h', 1) => "%-I",
        ('h', 2) => "%I",
        ('m', 1) => "%-M",
        ('m', 2) => "%M",
        ('s', 1) => "%-S",
        ('s', 2) => "%S",
        ('S', 3) => "%3f",
        ('S', 6) => "%6f",
        ('S', 9) => "%9f",
        ('a', _) => "%p",
        ('E', 1..=3) => "%a",
        ('E', 4) => "%A",
        ('D', 1) => "%-j",
        ('D', 3) => "%j",
        _ => return Err(crate::codes::coded("22023", format!("date_format's pattern letter {c} isn't taken: yyyy MM dd HH mm ss SSS a EEE are, or write strftime(ts, '%Y-%m-%d')"))),
    })
}

/// BigQuery's `SAFE_DIVIDE(a, b)`, a function in every session: `a / b`, NULL where `b` is 0 (either zero). It answers
/// in the type BigQuery does, so it is typed by its arguments, which text alone doesn't know: two integers divide into a
/// DOUBLE (BigQuery's FLOAT64, where Postgres's `/` keeps whole numbers), and anything else as `/` does, so a NUMERIC
/// stays a NUMERIC (11-03 audit L8: it went through DOUBLE). The same cast, zero check and division in every plan
/// and every expression planned alone (a CHECK, a DEFAULT).
pub fn register(ctx: &datafusion::prelude::SessionContext) {
    ctx.register_udf(divide::safe_divide());
}

mod divide {
    use datafusion::arrow::array::{new_null_array, AsArray};
    use datafusion::arrow::compute::{cast_with_options, concat, kernels::numeric::div, nullif, CastOptions};
    use datafusion::arrow::datatypes::DataType;
    use datafusion::arrow::error::ArrowError;
    use datafusion::common::{exec_err, DataFusionError, Result, ScalarValue};
    use datafusion::logical_expr::type_coercion::binary::BinaryTypeCoercer;
    use datafusion::logical_expr::{ColumnarValue, Operator, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility};
    use datafusion::physical_expr_common::datum::{apply, apply_cmp};

    pub(super) fn safe_divide() -> ScalarUDF { ScalarUDF::new_from_impl(SafeDivide(Signature::any(2, Volatility::Immutable))) }

    #[derive(Debug, PartialEq, Eq, Hash)]
    struct SafeDivide(Signature);

    /// What `a` and `b` are cast to before dividing: `/`'s own types, but two integers (or NULLs) as DOUBLEs.
    fn halves(a: &DataType, b: &DataType) -> Result<(DataType, DataType)> {
        let whole = |t: &DataType| t.is_integer() || t.is_null();
        let a = if whole(a) && whole(b) { &DataType::Float64 } else { a };
        BinaryTypeCoercer::new(a, &Operator::Divide, b).get_input_types()
    }

    impl ScalarUDFImpl for SafeDivide {
        fn name(&self) -> &str { "safe_divide" }
        fn signature(&self) -> &Signature { &self.0 }
        fn return_type(&self, args: &[DataType]) -> Result<DataType> {
            let [a, b] = args else { return exec_err!("safe_divide takes two values") };
            let (a, b) = halves(a, b)?;
            BinaryTypeCoercer::new(&a, &Operator::Divide, &b).get_result_type()
        }
        fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
            let [a, b] = &args.args[..] else { return exec_err!("safe_divide takes two values") };
            let (to_a, to_b) = halves(&a.data_type(), &b.data_type())?;
            let (a, b) = (a.cast_to(&to_a, None)?, b.cast_to(&to_b, None)?);
            // (`=`'s comparison, which takes -0.0 as 0 too, where arrow's kernels compare bits)
            let zero = apply_cmp(Operator::Eq, &b, &ColumnarValue::Scalar(ScalarValue::new_zero(&to_b)?))?;
            let b = match (b, zero) {
                (ColumnarValue::Array(b), ColumnarValue::Array(zero)) => ColumnarValue::Array(nullif(&b, zero.as_boolean())?),
                (ColumnarValue::Scalar(_), ColumnarValue::Scalar(ScalarValue::Boolean(Some(true)))) => ColumnarValue::Scalar(ScalarValue::try_from(&to_b)?),
                (b, _) => b,
            };
            let to = args.return_field.data_type();
            match apply(&a, &b, div) {
                Ok(q) => q.cast_to(to, None), // (the type `/` gives: no change)
                Err(DataFusionError::ArrowError(e, _)) if matches!(*e, ArrowError::ArithmeticOverflow(_)) => by_row(&a, &b, args.number_rows, to),
                Err(e) => Err(e),
            }
        }
    }

    /// Each row's quotient on its own, NULL where it overflows: BigQuery's SAFE_ answers NULL for a row's error, where
    /// `/` fails the statement (11-06 audit L2: a NUMERIC whose quotient is past its precision failed every row).
    /// Taken only once the whole division has overflowed.
    fn by_row(a: &ColumnarValue, b: &ColumnarValue, rows: usize, to: &DataType) -> Result<ColumnarValue> {
        let one_row = matches!((a, b), (ColumnarValue::Scalar(_), ColumnarValue::Scalar(_)));
        let (a, b) = (a.to_array(rows)?, b.to_array(rows)?);
        let safe = CastOptions { safe: true, ..Default::default() };
        let parts = (0..a.len())
            .map(|i| match div(&a.slice(i, 1), &b.slice(i, 1)) {
                Ok(q) => Ok(cast_with_options(&q, to, &safe)?),
                Err(ArrowError::ArithmeticOverflow(_)) => Ok(new_null_array(to, 1)),
                Err(e) => Err(e.into()),
            })
            .collect::<Result<Vec<_>>>()?;
        let all = concat(&parts.iter().map(|p| p.as_ref()).collect::<Vec<_>>())?;
        Ok(match one_row {
            true => ColumnarValue::Scalar(ScalarValue::try_from_array(&all, 0)?),
            false => ColumnarValue::Array(all),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::sql::sqlparser::{dialect::GenericDialect, parser::Parser};

    fn said(sql: &str) -> Option<String> { try_said(sql).unwrap() }

    /// The call's text, or its refusal.
    fn try_said(sql: &str) -> Result<Option<String>> {
        let e = Parser::new(&GenericDialect {}).try_with_sql(sql).unwrap().parse_expr().unwrap();
        let Expr::Function(f) = &e else { panic!("{sql}") };
        let name = f.name.to_string().to_lowercase();
        let args: Vec<&Expr> = match &f.args {
            datafusion::sql::sqlparser::ast::FunctionArguments::List(l) => l.args.iter().filter_map(|a| match a {
                datafusion::sql::sqlparser::ast::FunctionArg::Unnamed(datafusion::sql::sqlparser::ast::FunctionArgExpr::Expr(e)) => Some(e),
                _ => None,
            }).collect(),
            _ => vec![],
        };
        call(&name, &args)
    }

    #[test]
    fn date_format_is_mysqls_or_sparks() {
        assert_eq!(said("DATE_FORMAT(ts, '%Y-%m-%d %H:%i:%s %M %W')").unwrap(), "to_char(ts, '%Y-%m-%d %H:%M:%S %B %A')");
        assert_eq!(said("date_format(ts, 'yyyy-MM-dd HH:mm:ss.SSS')").unwrap(), "to_char(ts, '%Y-%m-%d %H:%M:%S.%3f')");
        assert_eq!(said("date_format(ts, 'dd MMM yyyy ''at'' h a')").unwrap(), "to_char(ts, '%d %b %Y at %-I %p')");
        assert_eq!(said("date_format(ts, '''Q''')").unwrap(), "CASE WHEN ts IS NULL THEN NULL ELSE 'Q' END"); // (Q is literal here, not a quarter)
        assert_eq!(said("date_format(ts, fmt)"), None); // (a column of formats is DataFusion's)
        let refusal = try_said("date_format(ts, 'G')").unwrap_err();
        assert!(crate::codes::of(&refusal) == "22023" && refusal.to_string().contains("pattern letter G"), "{refusal:#}");
        let refusal = try_said("DATE_FORMAT(ts, '%D %i')").unwrap_err();
        assert!(crate::codes::of(&refusal) == "22023" && refusal.to_string().contains("MySQL's %D"), "{refusal:#}");
        // (without MySQL's %i: a code the two read apart is refused, codes they share are DataFusion's as written)
        let refusal = try_said("date_format(ts, '%Y %M')").unwrap_err();
        assert!(crate::codes::of(&refusal) == "22023" && refusal.to_string().contains("%M is one thing"), "{refusal:#}");
        assert_eq!(said("date_format(ts, '%Y-%m-%d %H:%S')"), None);
    }

    #[test]
    fn sql_server_format_and_other_databases_names_are_refused() {
        // (FORMAT(value, pattern) is refused; Postgres's format, its pattern first, is not)
        let refusal = try_said("format(ts, 'yyyy')").unwrap_err();
        assert!(crate::codes::of(&refusal) == "0A000" && refusal.to_string().contains("FORMAT(value, 'yyyy') is SQL Server's"), "{refusal:#}");
        assert_eq!(said("format('%s', x)"), None);
        assert_eq!(said("format(E'%s\\n', 'x')"), None); // (Postgres's: its pattern an escaped string, or a column of them)
        assert_eq!(said("format(pattern, 'x')"), None);
        assert!(try_said("FORMAT(amount, 'N2')").is_err() && try_said("FORMAT(d, 'dd/MM/yyyy')").is_err());
        // (a name of OURS is refused by name, with what to write, whatever its arguments)
        let refusal = try_said("PARSE_JSON(p)").unwrap_err();
        assert!(crate::codes::of(&refusal) == "42883" && refusal.to_string().contains("PARSE_JSON is Snowflake's") && refusal.to_string().contains("payload->>'a'"), "{refusal:#}");
        let refusal = try_said("MONTHS_BETWEEN(a, b)").unwrap_err();
        assert!(crate::codes::of(&refusal) == "42883" && refusal.to_string().contains("date_diff('month', a, b)"), "{refusal:#}");
        assert_eq!(said("approx_percentile_cont(x, 0.5)"), None); // (Pondra's own name is not refused)
    }

    #[test]
    fn other_databases_names_as_pondras() {
        assert_eq!(said("IFF(a > 1, a, 0)").unwrap(), "CASE WHEN a > 1 THEN a ELSE 0 END");
        assert_eq!(said("DECODE(c, 'US', 'America', 'UK', 'Britain', 'other')").unwrap(), "CASE WHEN c IS NOT DISTINCT FROM 'US' THEN 'America' WHEN c IS NOT DISTINCT FROM 'UK' THEN 'Britain' ELSE 'other' END");
        assert_eq!(said("DECODE(c, 1, 'one')").unwrap(), "CASE WHEN c IS NOT DISTINCT FROM 1 THEN 'one' END");
        assert_eq!(said("decode(b, 'hex')"), None); // (Pondra's own)
        assert_eq!(said("CHARINDEX('@', email)").unwrap(), "strpos(email, '@')");
        assert_eq!(said("TRY_CONVERT(date, '2026-03-01')").unwrap(), "TRY_CAST('2026-03-01' AS date)");
        assert_eq!(said("DATEPART(weekday, d)").unwrap(), "CAST(date_part('dow', d) + 1 AS INT)");
        assert_eq!(said("DATEPART(month, d)").unwrap(), "CAST(date_part('month', d) AS INT)");
        assert_eq!(said("STR_TO_DATE('03/01/2026', '%m/%d/%Y')").unwrap(), "to_date('03/01/2026', '%m/%d/%Y')");
        assert_eq!(said("STR_TO_DATE(s, '%d.%m.%Y %H:%i:%s')").unwrap(), "to_timestamp(s, '%d.%m.%Y %H:%M:%S')");
        assert_eq!(said("UNIX_TIMESTAMP()").unwrap(), "CAST(floor(date_part('epoch', now())) AS BIGINT)");
        assert_eq!(said("DATE_TRUNC(d, MONTH)").unwrap(), "CAST(date_trunc('month', d) AS DATE)");
        assert_eq!(said("date_trunc('month', d)"), None); // (Pondra's own)
        assert_eq!(said("date_trunc(unit, d)"), None); // (a column of parts, and `d` a column)
        assert_eq!(said("datepart('month', d)"), None); // (Pondra's own)
        assert_eq!(said("FROM_UNIXTIME(n)"), None); // (Pondra's own)
        let us = |x: &str| format!("(CAST(date_part('epoch', date_trunc('minute', {x})) AS BIGINT) * 1000000 + CAST(date_part('microsecond', {x}) AS BIGINT))");
        assert_eq!(said("TIMESTAMP_DIFF(a, b, HOUR)").unwrap(), format!("(({} - {}) / 3600000000)", us("a"), us("b")));
        assert_eq!(said("TIMESTAMP_DIFF(a, b, MICROSECOND)").unwrap(), format!("(({} - {}))", us("a"), us("b")));
        assert_eq!(said("DATETIME_DIFF(a, b, DAY)").unwrap(), format!("(({} - {}) / 86400000000)", us("date_trunc('day', a)"), us("date_trunc('day', b)")));
        assert_eq!(said("TIMESTAMP_DIFF(a, b, MONTH)"), None);
        assert_eq!(said("SAFE_DIVIDE(clicks, views)"), None); // (a function of its own, typed by its arguments: `register`)
        assert_eq!(said("DATEPART(week, d)").unwrap(), "CAST((CAST(date_part('doy', d) AS BIGINT) + CAST(date_part('dow', date_trunc('year', d)) AS BIGINT) + 6) / 7 AS INT)");
        assert_eq!(said("DATEPART(isowk, d)").unwrap(), "CAST(date_part('week', d) AS INT)");
        assert_eq!(said("DATEPART(ms, d)").unwrap(), "CAST(CAST(date_part('millisecond', d) AS BIGINT) % 1000 AS INT)");
        assert_eq!(said("TIMESTAMP_TRUNC(ts, WEEK)").unwrap(), "date_trunc('day', ts) - CAST(date_part('dow', ts) AS BIGINT) * INTERVAL '1 day'");
        assert_eq!(said("DATE_TRUNC(d, ISOWEEK)").unwrap(), "CAST(date_trunc('week', d) AS DATE)");
        assert_eq!(crate::codes::of(&try_said("DATE_FORMAT(d, '%u')").unwrap_err()), "22023");
        assert_eq!(said("COUNTIF(s = 'paid')").unwrap(), "count(CASE WHEN s = 'paid' THEN 1 END)");
        assert!(call("timestampdiff", &[&Expr::Identifier("hour".into()), &Expr::Identifier("a".into()), &Expr::Identifier("b".into())]).unwrap_err().to_string().contains("date_diff('hour', a, b)"));
    }

    #[tokio::test]
    async fn safe_divide_answers_in_the_type_its_arguments_say() {
        // (11-03 audit L8: a NUMERIC went through DOUBLE. Each row: what is divided, the type its answer must be, and
        // whether two integers' DOUBLE is meant; the answer's type and text are `/`'s, NULL where `b` is either zero;
        // once over literals, folded, and once over a table's columns)
        let ctx = datafusion::prelude::SessionContext::new();
        register(&ctx);
        let rows = [
            ("7, 2", "Float64", true),
            ("CAST(7 AS SMALLINT), CAST(0 AS BIGINT)", "Float64", true),
            ("NULL, 2", "Float64", true),
            ("CAST(7 AS DECIMAL(10, 2)), 2", "Decimal128(", false),
            ("CAST(1 AS DECIMAL(10, 2)), CAST(3 AS DECIMAL(10, 2))", "Decimal128(", false),
            ("CAST(1 AS DECIMAL(10, 2)), CAST(0 AS DECIMAL(10, 2))", "Decimal128(", false),
            ("CAST(1.5 AS DOUBLE), CAST('-0.0' AS DOUBLE)", "Float64", false),
            ("CAST(1.5 AS DOUBLE), 3", "Float64", false),
        ];
        let answer = |sql: String| {
            let ctx = ctx.clone();
            async move {
                let batches = ctx.sql(&sql).await.unwrap().collect().await.unwrap();
                let column = batches[0].column(0).clone();
                let text = datafusion::arrow::util::display::ArrayFormatter::try_new(column.as_ref(), &Default::default()).unwrap().value(0).to_string();
                (column.data_type().to_string(), text)
            }
        };
        for (args, typed, whole) in rows {
            let divided = if whole { "CAST(a AS DOUBLE) / b" } else { "a / b" };
            for from in [format!("(SELECT {args}) AS t(a, b)"), format!("(VALUES ({args})) AS t(a, b)")] {
                let ours = answer(format!("SELECT safe_divide(a, b) AS r FROM {from}")).await;
                let theirs = answer(format!("SELECT CASE WHEN b = 0 THEN NULL ELSE {divided} END AS r FROM {from}")).await;
                assert_eq!(ours, theirs, "{args}");
                assert!(ours.0.starts_with(typed), "{args}: {}", ours.0);
            }
        }
        // (11-06 audit L2: a NUMERIC quotient past its precision is NULL for its row, as BigQuery's SAFE_ answers an
        // error, and the other rows keep theirs; `/` fails, and so did this, for every row)
        let big = "(VALUES (CAST(1e27 AS DECIMAL(38, 10)), CAST(0.001 AS DECIMAL(38, 10))), (CAST(5 AS DECIMAL(38, 10)), CAST(2 AS DECIMAL(38, 10))), (CAST(5 AS DECIMAL(38, 10)), CAST(0 AS DECIMAL(38, 10)))) AS t(a, b)";
        assert!(ctx.sql(&format!("SELECT a / b AS r FROM {big}")).await.unwrap().collect().await.is_err(), "`/` overflows");
        let batches = ctx.sql(&format!("SELECT safe_divide(a, b) AS r FROM {big}")).await.unwrap().collect().await.unwrap();
        let rows = datafusion::arrow::util::pretty::pretty_format_batches(&batches).unwrap().to_string();
        let r = batches[0].column(0);
        assert!(r.data_type().to_string().starts_with("Decimal128("), "{rows}");
        assert!(r.is_null(0) && !r.is_null(1) && r.is_null(2) && rows.contains("2.5"), "{rows}");
        let one = ctx.sql("SELECT safe_divide(CAST(1e27 AS DECIMAL(38, 10)), CAST(0.001 AS DECIMAL(38, 10))) AS r").await.unwrap().collect().await.unwrap();
        assert!(one[0].column(0).is_null(0), "a folded overflow is NULL too");
    }
}
