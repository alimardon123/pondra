//! Other databases' names for what Pondra's functions do, each written as the SQL it means (`call`, from
//! `friendly::expression`, so every door and every form reads them alike):
//! - Snowflake's `IFF`, `ZEROIFNULL`, `DECODE(x, a, ra, b, rb, …, else)`, `ARRAY_CONSTRUCT`, `ARRAY_SIZE`, `OBJECT_CONSTRUCT`;
//! - SQL Server's `IIF`, `ISNULL(a, b)`, `CHARINDEX(sub, s)`, `TRY_CONVERT(type, 'text')`, `GETDATE()`, `EOMONTH(d)`, `DATEPART(part, d)`;
//! - MySQL's `CURDATE()`, `UNIX_TIMESTAMP([ts])`, `STR_TO_DATE(s, '%m/%d/%Y')`, `DATE_FORMAT(ts, '%Y-%m-%d %H:%i')` (its
//!   `FROM_UNIXTIME(n)` is Pondra's own);
//! - Spark's `date_format(ts, 'yyyy-MM-dd HH:mm')`, Java's letters (a `%` in the format is MySQL's, as above);
//! - BigQuery's `COUNTIF`, `SAFE_DIVIDE`, `DATE(ts)`, `DATE_TRUNC(d, MONTH)`, `TIMESTAMP_TRUNC(ts, HOUR)`,
//!   `TIMESTAMP_DIFF(a, b, HOUR)`, `FORMAT_DATE(fmt, d)`, `PARSE_DATE(fmt, s)`, `UNIX_SECONDS(ts)`, `TIMESTAMP_SECONDS(n)`.
//!
//! A name Pondra already has keeps its own meaning where its arguments say so: `decode(bytes, 'hex')`, `date_trunc('month',
//! ts)`. MySQL's `TIMESTAMPDIFF` counts whole units and Snowflake's counts part boundaries, so it is refused by name with
//! `date_diff`, which counts boundaries as DuckDB does.
//!
//! Another database's name for a function Pondra doesn't have is refused by name, with what to write (`OURS`). So is SQL
//! Server's `FORMAT(value, 'pattern')`, which Postgres's `format` takes the other way round.
use crate::friendly::{part_word, quote};
use anyhow::Result;
use datafusion::sql::sqlparser::ast::{Expr, Value, ValueWithSpan};

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
            Some(_) => match part_word(a[0], name)? {
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
        // (a format with a `%` is MySQL's, else Spark's; a column of formats is DataFusion's. A format with no code in it
        // (only literal text, as `'''Q'''`) is that text, not Pondra's `to_char` template, where Q is a quarter)
        ("date_format", 2) => {
            let Some(f) = string(a[1]) else { return Ok(None) };
            let chrono = if f.contains('%') { mysql_format(f)?.0 } else { java_format(f)? };
            if chrono.contains('%') {
                format!("to_char({}, {})", a[0], quote(&chrono))
            } else {
                format!("CASE WHEN {0} IS NULL THEN NULL ELSE {1} END", a[0], quote(&chrono))
            }
        }
        // (SQL Server's FORMAT(value, pattern): a value first, a pattern second, where Postgres's format reads the pattern first)
        ("format", 2) => match (string(a[0]), string(a[1])) {
            (None, Some(p)) => return Err(crate::codes::coded("0A000", format!("FORMAT(value, '{p}') is SQL Server's (Postgres's format takes its pattern first): write strftime(ts, '%Y-%m-%d') or to_char(ts, 'YYYY-MM-DD') for a date, printf('%.2f', x) for a number"))),
            _ => return Ok(None),
        },
        ("timestampdiff", 3) => {
            return Err(crate::codes::coded("42883", format!("TIMESTAMPDIFF counts whole units in MySQL and part boundaries in Snowflake, so it isn't taken: date_diff('{}', {}, {}) counts the boundaries, as DuckDB does", a[0].to_string().to_lowercase(), a[1], a[2])));
        }
        ("countif", 1) => format!("count(CASE WHEN {} THEN 1 END)", a[0]),
        ("safe_divide", 2) => format!("({}) / nullif({}, 0)", a[0], a[1]),
        ("date", 1) => format!("CAST({} AS DATE)", a[0]),
        ("date", 3) => format!("make_date({}, {}, {})", a[0], a[1], a[2]),
        // (BigQuery's order, the part a word after the value; `date_trunc('month', d)` is Pondra's own)
        ("date_trunc", 2) if string(a[0]).is_none() => match bare_part(a[1]) {
            Some(p) => format!("CAST(date_trunc('{p}', {}) AS DATE)", a[0]),
            None => return Ok(None),
        },
        ("timestamp_trunc" | "datetime_trunc", 2) => match bare_part(a[1]) {
            Some(p) => format!("date_trunc('{p}', {})", a[0]),
            None => return Ok(None),
        },
        // (BigQuery's: the whole parts from `b` to `a`, a day or shorter)
        ("timestamp_diff" | "datetime_diff", 3) => {
            let Some(p) = bare_part(a[2]) else { return Ok(None) };
            let scale = match p {
                "day" => " / 86400",
                "hour" => " / 3600",
                "minute" => " / 60",
                "second" => "",
                "millisecond" => " * 1000",
                "microsecond" => " * 1000000",
                _ => return Ok(None),
            };
            format!("CAST(trunc(({} - {}){scale}) AS BIGINT)", epoch(a[0]), epoch(a[1]))
        }
        ("format_date" | "format_timestamp" | "format_datetime", 2) => format!("to_char({}, {})", a[1], a[0]),
        ("parse_date", 2) => format!("to_date({}, {})", a[1], a[0]),
        ("parse_timestamp" | "parse_datetime", 2) => format!("to_timestamp({}, {})", a[1], a[0]),
        _ => return Ok(None),
    }))
}

fn list(a: &[&Expr]) -> String { a.iter().map(|e| e.to_string()).collect::<Vec<_>>().join(", ") }

/// A string literal's text.
fn string(e: &Expr) -> Option<&str> {
    match e {
        Expr::Value(ValueWithSpan { value: Value::SingleQuotedString(s), .. }) => Some(s),
        _ => None,
    }
}

/// A bare word's text, in lower case (`weekday`, `MONTH`).
fn word(e: &Expr) -> Option<String> {
    match e {
        Expr::Identifier(i) if i.quote_style.is_none() => Some(i.value.to_lowercase()),
        _ => None,
    }
}

/// A part written as BigQuery writes it, a whole bare word (`MONTH`; not `m` or `d`, which may be a column's name).
fn bare_part(e: &Expr) -> Option<&'static str> {
    Some(match word(e)?.as_str() {
        "year" => "year",
        "quarter" => "quarter",
        "month" => "month",
        "week" => "week",
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
            'u' => ("%W", false),
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
        let refusal = try_said("DATE_FORMAT(ts, '%D')").unwrap_err();
        assert!(crate::codes::of(&refusal) == "22023" && refusal.to_string().contains("MySQL's %D"), "{refusal:#}");
    }

    #[test]
    fn sql_server_format_and_other_databases_names_are_refused() {
        // (FORMAT(value, pattern) is refused; Postgres's format, its pattern first, is not)
        let refusal = try_said("format(ts, 'yyyy')").unwrap_err();
        assert!(crate::codes::of(&refusal) == "0A000" && refusal.to_string().contains("FORMAT(value, 'yyyy') is SQL Server's"), "{refusal:#}");
        assert_eq!(said("format('%s', x)"), None);
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
        assert_eq!(said("TIMESTAMP_DIFF(a, b, HOUR)").unwrap(), "CAST(trunc((date_part('epoch', a) - date_part('epoch', b)) / 3600) AS BIGINT)");
        assert_eq!(said("TIMESTAMP_DIFF(a, b, MONTH)"), None);
        assert_eq!(said("COUNTIF(s = 'paid')").unwrap(), "count(CASE WHEN s = 'paid' THEN 1 END)");
        assert!(call("timestampdiff", &[&Expr::Identifier("hour".into()), &Expr::Identifier("a".into()), &Expr::Identifier("b".into())]).unwrap_err().to_string().contains("date_diff('hour', a, b)"));
    }
}
