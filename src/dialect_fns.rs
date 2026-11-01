//! Other databases' names for what Pondra's functions do, each written as the SQL it means (`call`, from
//! `friendly::expression`, so every door and every form reads them alike):
//! - Snowflake's `IFF`, `ZEROIFNULL`, `DECODE(x, a, ra, b, rb, …, else)`, `ARRAY_CONSTRUCT`, `ARRAY_SIZE`, `OBJECT_CONSTRUCT`;
//! - SQL Server's `IIF`, `ISNULL(a, b)`, `CHARINDEX(sub, s)`, `TRY_CONVERT(type, 'text')`, `GETDATE()`, `EOMONTH(d)`, `DATEPART(part, d)`;
//! - MySQL's `CURDATE()`, `UNIX_TIMESTAMP([ts])`, `STR_TO_DATE(s, '%m/%d/%Y')` (its `FROM_UNIXTIME(n)` is Pondra's own);
//! - BigQuery's `COUNTIF`, `SAFE_DIVIDE`, `DATE(ts)`, `DATE_TRUNC(d, MONTH)`, `TIMESTAMP_TRUNC(ts, HOUR)`,
//!   `TIMESTAMP_DIFF(a, b, HOUR)`, `FORMAT_DATE(fmt, d)`, `PARSE_DATE(fmt, s)`, `UNIX_SECONDS(ts)`, `TIMESTAMP_SECONDS(n)`.
//!
//! A name Pondra already has keeps its own meaning where its arguments say so: `decode(bytes, 'hex')`, `date_trunc('month',
//! ts)`. MySQL's `TIMESTAMPDIFF` counts whole units and Snowflake's counts part boundaries, so it is refused by name with
//! `date_diff`, which counts boundaries as DuckDB does.
use crate::friendly::part_word;
use anyhow::Result;
use datafusion::sql::sqlparser::ast::{Expr, Value, ValueWithSpan};

/// The text `name(args)` is, or None: not one of these, or not with these arguments.
pub fn call(name: &str, a: &[&Expr]) -> Result<Option<String>> {
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
            let (chrono, time) = mysql_format(f);
            format!("{}({}, '{}')", if time { "to_timestamp" } else { "to_date" }, a[0], chrono.replace('\'', "''"))
        }
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

/// MySQL's date format as chrono's (`%i` minutes, `%s` seconds, `%M` a month's name, …), and whether it reads a time.
fn mysql_format(f: &str) -> (String, bool) {
    let (mut out, mut time, mut chars) = (String::new(), false, f.chars());
    while let Some(c) = chars.next() {
        if c != '%' {
            out.push(c);
            continue;
        }
        let Some(k) = chars.next() else {
            out.push('%');
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
            '%' => ("%%", false),
            _ => {
                out.push('%');
                out.push(k);
                continue;
            }
        };
        out.push_str(s);
        time |= t;
    }
    (out, time)
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::sql::sqlparser::{dialect::GenericDialect, parser::Parser};

    fn said(sql: &str) -> Option<String> {
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
        call(&name, &args).unwrap()
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
