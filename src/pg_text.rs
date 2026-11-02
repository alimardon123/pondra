//! Postgres 16 text functions: format, quote_ident, quote_literal, parse_ident.
//!
//! Each one follows the server's own rules (utils/adt/quote.c, varlena.c)
//! so the same text comes out for the same input. tests/text_cases.json holds
//! 612 cases written by gen.py from Postgres 16.15, and every one matches (see NOTES.md).

use datafusion::sql::sqlparser::ast;
use std::ops::ControlFlow;

/// Every keyword that quote_ident must quote: the words pg_get_keywords() gives a category
/// other than unreserved (column name, type or function name, and reserved), as PG 16 reads them.
/// Read from the server (gen.py checks each one), so `json` (unreserved) stays bare.
const QUOTED_KEYWORDS: &[&str] = &[
    "all", "analyse", "analyze", "and", "any", "array", "as", "asc", "asymmetric",
    "authorization", "between", "bigint", "binary", "bit", "boolean", "both", "case", "cast",
    "char", "character", "check", "coalesce", "collate", "collation", "column",
    "concurrently", "constraint", "create", "cross", "current_catalog", "current_date",
    "current_role", "current_schema", "current_time", "current_timestamp", "current_user",
    "dec", "decimal", "default", "deferrable", "desc", "distinct", "do", "else", "end",
    "except", "exists", "extract", "false", "fetch", "float", "for", "foreign", "freeze",
    "from", "full", "grant", "greatest", "group", "grouping", "having", "ilike", "in",
    "initially", "inner", "inout", "int", "integer", "intersect", "interval", "into", "is",
    "isnull", "join", "json_array", "json_arrayagg", "json_object", "json_objectagg",
    "lateral", "leading", "least", "left", "like", "limit", "localtime", "localtimestamp",
    "national", "natural", "nchar", "none", "normalize", "not", "notnull", "null", "nullif",
    "numeric", "offset", "on", "only", "or", "order", "out", "outer", "overlaps", "overlay",
    "placing", "position", "precision", "primary", "real", "references", "returning", "right",
    "row", "select", "session_user", "setof", "similar", "smallint", "some", "substring",
    "symmetric", "system_user", "table", "tablesample", "then", "time", "timestamp", "to",
    "trailing", "treat", "trim", "true", "union", "unique", "user", "using", "values",
    "varchar", "variadic", "verbose", "when", "where", "window", "with", "xmlattributes",
    "xmlconcat", "xmlelement", "xmlexists", "xmlforest", "xmlnamespaces", "xmlparse", "xmlpi",
    "xmlroot", "xmlserialize", "xmltable",
];

/// quote_ident: the identifier as is when a bare name would read back the same,
/// else in double quotes with each `"` doubled.
pub fn quote_ident(s: &str) -> String {
    if plain_ident(s) {
        s.to_string()
    } else {
        format!("\"{}\"", s.replace('"', "\"\""))
    }
}

/// A bare identifier: starts with a lower-case letter or `_`, continues with
/// lower-case letters, digits or `_`, and isn't a keyword the grammar needs quoted.
/// The empty string is not bare (the server quotes it as "").
fn plain_ident(s: &str) -> bool {
    let first_ok = matches!(s.chars().next(), Some(c) if c.is_ascii_lowercase() || c == '_');
    first_ok
        && s.chars().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
        && !QUOTED_KEYWORDS.contains(&s)
}

/// quote_literal: single quotes with `'` doubled. A backslash anywhere makes it
/// the E'...' form, with each backslash doubled too.
pub fn quote_literal(s: &str) -> String {
    let doubled = s.replace('\'', "''");
    if s.contains('\\') {
        format!("E'{}'", doubled.replace('\\', "\\\\"))
    } else {
        format!("'{doubled}'")
    }
}

/// format(formatstr, args...): %s, %I and %L, %% for a percent sign, `N$` for an
/// argument's position, `-` and a width (a number or `*` / `*N$` from an argument).
/// `args[i]` is the i-th argument after the format string, None for NULL.
pub fn format(f: &str, args: &[Option<&str>]) -> Result<String, String> {
    let cs: Vec<char> = f.chars().collect();
    let mut out = String::new();
    let mut seq = 0; // the next argument for a directive that names none
    let mut i = 0;
    while i < cs.len() {
        if cs[i] != '%' {
            out.push(cs[i]);
            i += 1;
            continue;
        }
        i += 1;
        if cs.get(i) == Some(&'%') {
            out.push('%');
            i += 1;
            continue;
        }
        let spec = read_spec(&cs, &mut i)?;
        let (width, left) = width_of(&spec, args, &mut seq)?;
        let value = arg(args, spec.pos, &mut seq)?;
        let text = directive_text(spec.conv, value)?;
        pad(&mut out, &text, width, left);
    }
    Ok(out)
}

/// One directive after its `%`: its position, flag, width and conversion letter.
struct Spec {
    pos: Option<usize>,
    left: bool,
    width: Width,
    conv: char,
}

/// A width: none, a number in the directive, or `*` (taken from an argument, with
/// its own position when given as `*N$`).
enum Width {
    None,
    Num(usize),
    Star(Option<usize>),
}

/// Reads a directive from `i` on, moving `i` past its conversion letter.
fn read_spec(cs: &[char], i: &mut usize) -> Result<Spec, String> {
    let pos = position(cs, i)?;
    let left = take(cs, i, '-');
    let width = if take(cs, i, '*') {
        Width::Star(position(cs, i)?)
    } else {
        match number(cs, i) {
            Some(n) => Width::Num(n),
            None => Width::None,
        }
    };
    let conv = match cs.get(*i) {
        Some(&c) => c,
        None => return Err("unterminated format() type specifier".to_string()),
    };
    *i += 1;
    Ok(Spec { pos, left, width, conv })
}

/// `N$` at `i`: returns N (1 or more) and moves past the `$`. Digits not followed
/// by `$` are left for the width.
fn position(cs: &[char], i: &mut usize) -> Result<Option<usize>, String> {
    let mut j = *i;
    while cs.get(j).is_some_and(|c| c.is_ascii_digit()) {
        j += 1;
    }
    if j == *i || cs.get(j) != Some(&'$') {
        return Ok(None);
    }
    let digits: String = cs[*i..j].iter().collect();
    *i = j + 1;
    match digits.parse::<usize>() {
        Ok(0) => Err("format specifies argument 0, but arguments are numbered from 1".to_string()),
        Ok(n) => Ok(Some(n)),
        Err(_) => Err(format!("argument number is out of range: {digits}")),
    }
}

/// Reads a run of digits at `i` as a number, if there is one.
fn number(cs: &[char], i: &mut usize) -> Option<usize> {
    let start = *i;
    while cs.get(*i).is_some_and(|c| c.is_ascii_digit()) {
        *i += 1;
    }
    if *i == start {
        return None;
    }
    cs[start..*i].iter().collect::<String>().parse().ok()
}

/// Moves past `want` at `i` when it is there.
fn take(cs: &[char], i: &mut usize, want: char) -> bool {
    if cs.get(*i) == Some(&want) {
        *i += 1;
        true
    } else {
        false
    }
}

/// The widest a directive may pad to. A wider one is refused rather than allocated: a
/// width of billions would take gigabytes for one value.
const MAX_WIDTH: usize = 1 << 20;

/// The width and the justification of a directive. A `*` width comes from an argument
/// (taken before the value); a negative one means left-justified. A `*N$` width names its
/// argument, but still takes the next one in turn, so the server gives the value after it
/// (`%*1$s` with 5, ab prints ab, padded to 5). A NULL width is 0.
fn width_of(spec: &Spec, args: &[Option<&str>], seq: &mut usize) -> Result<(usize, bool), String> {
    let (width, left) = match spec.width {
        Width::None => (0, spec.left),
        Width::Num(n) => (n, spec.left),
        Width::Star(pos) => {
            let turn = arg(args, None, seq)?;
            let text = match pos {
                Some(_) => arg(args, pos, &mut 0)?,
                None => turn,
            };
            let Some(text) = text else {
                return Ok((0, spec.left));
            };
            let w: i64 = text
                .trim()
                .parse()
                .map_err(|_| format!("invalid input syntax for type integer: \"{text}\""))?;
            (w.unsigned_abs() as usize, spec.left || w < 0)
        }
    };
    if width > MAX_WIDTH {
        return Err(format!("format width {width} is too large"));
    }
    Ok((width, left))
}

/// The argument for a directive: the one it names, else the next in turn.
/// None (NULL) is a valid value, so the result is an Option of an Option.
fn arg<'a>(args: &[Option<&'a str>], pos: Option<usize>, seq: &mut usize) -> Result<Option<&'a str>, String> {
    let idx = match pos {
        Some(n) => n - 1,
        None => {
            let k = *seq;
            *seq += 1;
            k
        }
    };
    args.get(idx)
        .copied()
        .ok_or_else(|| "too few arguments for format()".to_string())
}

/// The text one conversion letter gives for a value.
fn directive_text(conv: char, value: Option<&str>) -> Result<String, String> {
    match conv {
        's' => Ok(value.unwrap_or("").to_string()),
        'I' => match value {
            Some(v) => Ok(quote_ident(v)),
            None => Err("null values cannot be formatted as an SQL identifier".to_string()),
        },
        'L' => Ok(match value {
            Some(v) => quote_literal(v),
            None => "NULL".to_string(),
        }),
        other => Err(format!("unrecognized format() type specifier \"{other}\"")),
    }
}

/// Appends `text` padded to `width` characters (not bytes), on the right when `left`.
fn pad(out: &mut String, text: &str, width: usize, left: bool) {
    let n = text.chars().count();
    let fill = width.saturating_sub(n);
    if left {
        out.push_str(text);
        out.extend(std::iter::repeat_n(' ', fill));
    } else {
        out.extend(std::iter::repeat_n(' ', fill));
        out.push_str(text);
    }
}

/// parse_ident(qualified, strict): the parts of a dotted name. Bare parts are
/// lower-cased, quoted parts keep their case with `""` read as one quote.
/// Strict: text after the last part is an error; otherwise it is ignored.
pub fn parse_ident(s: &str, strict: bool) -> Result<Vec<String>, String> {
    let cs: Vec<char> = s.chars().collect();
    let mut parts = Vec::new();
    let mut i = skip_space(&cs, 0);
    loop {
        let (part, next) = if cs.get(i) == Some(&'"') {
            quoted_part(&cs, i, s)?
        } else {
            bare_part(&cs, i, s)?
        };
        parts.push(part);
        i = skip_space(&cs, next);
        match cs.get(i) {
            None => return Ok(parts),
            Some('.') => i = skip_space(&cs, i + 1),
            Some(_) if strict => return Err(not_ident(s, "trailing characters after the last identifier")),
            Some(_) => return Ok(parts),
        }
    }
}

fn skip_space(cs: &[char], mut i: usize) -> usize {
    while cs.get(i).is_some_and(|c| c.is_whitespace()) {
        i += 1;
    }
    i
}

fn not_ident(s: &str, why: &str) -> String {
    format!("string is not a valid identifier: \"{s}\": {why}")
}

/// An unquoted part: letters, digits, `_` and `$`, not starting with a digit or `$`.
/// Returns the part lower-cased and the index after it.
fn bare_part(cs: &[char], start: usize, s: &str) -> Result<(String, usize), String> {
    let is_start = |c: char| c.is_alphabetic() || c == '_' || !c.is_ascii();
    let is_inner = |c: char| is_start(c) || c.is_ascii_digit() || c == '$';
    let first = match cs.get(start) {
        Some(&c) if is_start(c) => c,
        _ => return Err(not_ident(s, "an unquoted identifier must start with a letter or underscore")),
    };
    let mut part = String::new();
    part.push(first.to_ascii_lowercase());
    let mut i = start + 1;
    while let Some(&c) = cs.get(i) {
        if !is_inner(c) {
            break;
        }
        part.push(c.to_ascii_lowercase());
        i += 1;
    }
    Ok((part, i))
}

/// A part in double quotes: `""` inside is one `"`. The closing quote must come.
/// Returns the part as written and the index after the closing quote.
fn quoted_part(cs: &[char], start: usize, s: &str) -> Result<(String, usize), String> {
    let mut part = String::new();
    let mut i = start + 1;
    loop {
        match cs.get(i) {
            None => return Err(not_ident(s, "String has unclosed double quotes")),
            Some('"') if cs.get(i + 1) == Some(&'"') => {
                part.push('"');
                i += 2;
            }
            Some('"') => break,
            Some(&c) => {
                part.push(c);
                i += 1;
            }
        }
    }
    if part.is_empty() {
        return Err(not_ident(s, "quoted identifier must not be empty"));
    }
    Ok((part, i + 1))
}

// ---- Intervals, times and array literals: Postgres's text for them, as the port writes and reads them (`pg.rs`,
// the JSON functions, `pg_values.rs`'s parameters and casts) ----

/// An interval in Postgres's own text (`postgres` style): `1 year 2 mons 3 days 04:05:06.000004`.
/// A negative clock after days shows its `+`, as Postgres writes it (`-1 days +00:00:03`).
pub(crate) fn interval_text(months: i64, days: i64, micros: i64) -> String {
    let unit = |n: i64, word: &str| format!("{n} {word}{}", if n == 1 { "" } else { "s" });
    let mut parts = vec![];
    if months / 12 != 0 {
        parts.push(unit(months / 12, "year"));
    }
    if months % 12 != 0 {
        parts.push(unit(months % 12, "mon"));
    }
    if days != 0 {
        parts.push(unit(days, "day"));
    }
    if micros != 0 || parts.is_empty() {
        let sign = if micros < 0 { "-" } else if days < 0 { "+" } else { "" };
        parts.push(format!("{sign}{}", clock(micros.unsigned_abs())));
    }
    parts.join(" ")
}

/// A time of day as Postgres writes it: `12:34:56.789`, with no fraction for a whole second.
pub(crate) fn time_text(us: i64) -> String { clock(us.unsigned_abs()) }

/// `hh:mm:ss` and the fraction of a second that is left (`.789`, `.000004`; none for a whole second).
fn clock(us: u64) -> String {
    let fraction = match us % 1_000_000 {
        0 => String::new(),
        f => format!(".{f:06}").trim_end_matches('0').to_string(),
    };
    format!("{:02}:{:02}:{:02}{fraction}", us / 3_600_000_000, us / 60_000_000 % 60, us / 1_000_000 % 60)
}

/// Postgres's text array literal (`{a,"b,c",NULL}`): a bare NULL is a NULL element, a quoted one is text, a bare
/// element is trimmed of its spaces, and `{}` has no elements. Nested braces are refused by name (0A000) for now.
pub(crate) fn array_literal(s: &str) -> anyhow::Result<Vec<Option<String>>> {
    let malformed = || crate::codes::coded("22P02", format!("malformed array literal: \"{s}\""));
    let body = s.trim().strip_prefix('{').and_then(|b| b.strip_suffix('}')).ok_or_else(malformed)?;
    let mut rest = body.trim_start();
    let mut items = Vec::new();
    if rest.is_empty() {
        return Ok(items);
    }
    loop {
        if rest.starts_with('{') {
            return Err(crate::codes::coded("0A000", format!("nested array literals are not supported yet: \"{s}\"")));
        }
        let (item, after) = if rest.starts_with('"') { quoted_item(rest) } else { bare_item(rest) }.ok_or_else(malformed)?;
        items.push(item);
        let after = after.trim_start();
        if after.is_empty() {
            return Ok(items);
        }
        rest = after.strip_prefix(',').ok_or_else(malformed)?.trim_start();
    }
}

/// A quoted element at the start of `s`, its escapes undone, and the text after its closing quote.
fn quoted_item(s: &str) -> Option<(Option<String>, &str)> {
    let mut out = String::new();
    let mut chars = s[1..].char_indices();
    while let Some((i, c)) = chars.next() {
        match c {
            '"' => return Some((Some(out), &s[2 + i..])),
            '\\' => out.push(chars.next()?.1),
            c => out.push(c),
        }
    }
    None
}

/// A bare element at the start of `s`: up to the next comma, trimmed, with `\x` read as `x`. A bare NULL is NULL,
/// and an empty element (`{a,,b}`) is malformed.
fn bare_item(s: &str) -> Option<(Option<String>, &str)> {
    let mut end = s.len();
    let mut chars = s.char_indices();
    while let Some((i, c)) = chars.next() {
        match c {
            '\\' => {
                chars.next();
            }
            ',' => {
                end = i;
                break;
            }
            _ => {}
        }
    }
    let raw = s[..end].trim();
    if raw.is_empty() {
        return None;
    }
    let item = if raw.eq_ignore_ascii_case("NULL") { None } else { Some(unescaped(raw)) };
    Some((item, &s[end..]))
}

/// `s` with each `\x` read as `x`.
fn unescaped(s: &str) -> String {
    let mut out = String::new();
    let mut escaped = false;
    for c in s.chars() {
        if c == '\\' && !escaped {
            escaped = true;
        } else {
            out.push(c);
            escaped = false;
        }
    }
    out
}

// ---- Array literals: a string cast to an array type is read as Postgres reads it (`'{a,b}'::text[]`) ----

/// Each string literal cast to an array type, read as Postgres reads an array literal and written as the array it
/// names: `'{a,b}'::text[]`, `CAST('{1,2}' AS INT[])` and `TEXT[] '{a,b}'` become `CAST(make_array('a', 'b') AS TEXT[])`,
/// a NULL element as NULL. Nothing else is changed. Before this a text array literal was a list of one string, so
/// `doc ?| '{a,b}'::text[]` asked for the key `{a,b}`. Returns whether one was found; a malformed one is 22P02.
pub(crate) fn array_literals(stmt: &mut ast::Statement) -> anyhow::Result<bool> {
    let mut found = ArrayLiterals { found: false };
    match crate::walk::visit_mut(stmt, &mut found) {
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

    #[test]
    fn interval_text_is_postgres_style() {
        assert_eq!(interval_text(14, 3, 14_706_000_004), "1 year 2 mons 3 days 04:05:06.000004");
        assert_eq!(interval_text(0, 0, 0), "00:00:00");
        assert_eq!(interval_text(0, -1, 3_000_004), "-1 days +00:00:03.000004");
        assert_eq!(interval_text(0, 1, -1_500_000), "1 day -00:00:01.5");
    }

    #[test]
    fn time_text_cuts_the_fraction_as_postgres_does() {
        assert_eq!(time_text(45_296_789_000), "12:34:56.789");
        assert_eq!(time_text(45_296_000_000), "12:34:56");
        assert_eq!(time_text(0), "00:00:00");
        assert_eq!(time_text(86_399_999_999), "23:59:59.999999");
    }

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
}
