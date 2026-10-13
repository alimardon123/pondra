//! Postgres 16 text functions: format, quote_ident, quote_literal, parse_ident.
//!
//! Each one follows the server's own rules (utils/adt/quote.c, varlena.c)
//! so the same text comes out for the same input. tests/text_cases.json holds
//! 612 cases written by gen.py from Postgres 16.15, and every one matches (see NOTES.md).

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
