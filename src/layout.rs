//! A table's layout as clauses (the SQL review's decision 1A, after BigQuery's, Snowflake's,
//! Databricks' and ClickHouse's words):
//!
//! ```sql
//! CREATE TABLE sessions (user_id BIGINT PRIMARY KEY, seen TIMESTAMP, page VARCHAR)
//! PARTITION BY day(seen) CLUSTER BY (page) SEQUENCE BY seen TTL seen + INTERVAL '1 hour'
//! WITH (publish = (delta, iceberg));
//! CREATE TABLE totals (region VARCHAR PRIMARY KEY, amount DOUBLE MERGE sum, n BIGINT MERGE count);
//! ALTER TABLE sessions TTL seen + INTERVAL '2 hours';
//! ```
//!
//! Where SQL comes in (`write::parse`), the clauses, in any order and beside a `WITH (…)`, become
//! that list's options (`partition_by`, `cluster_by`, `order_by`, `ttl`, `merge`), so everything
//! after the parser knows one form, and the options as strings keep working.

use crate::write::{cluster_names, ident, partition_text};
use anyhow::{bail, ensure, Result};
use datafusion::sql::sqlparser::{ast, dialect::GenericDialect, parser::Parser, tokenizer::{Location, Token, TokenWithSpan, Tokenizer}};
use std::borrow::Cow;
use std::sync::LazyLock;

static CREATE: LazyLock<regex::Regex> = LazyLock::new(|| regex::Regex::new(r"(?is)^\s*CREATE\s+(OR\s+REPLACE\s+)?((GLOBAL|LOCAL)\s+)?((TEMP|TEMPORARY|UNLOGGED|TRANSIENT)\s+)?TABLE\b").expect("a regex"));
static WORDS: LazyLock<regex::Regex> = LazyLock::new(|| regex::Regex::new(r"(?i)\b(PARTITION|PARTITIONED|CLUSTER|SEQUENCE|TTL|MERGE)\b").expect("a regex"));
static ALTER: LazyLock<regex::Regex> = LazyLock::new(|| {
    regex::Regex::new(r#"(?is)^\s*ALTER\s+TABLE\s+(IF\s+EXISTS\s+)?([\w."-]+)\s+(?:(?:MODIFY|SET)\s+)?(TTL|SEQUENCE\s+BY)\s+(.+?)\s*;?\s*$"#).expect("a regex")
});

/// The statement with its layout clauses as `WITH (…)` options; as it was when it has none.
pub fn clauses(sql: &str) -> Result<Cow<'_, str>> {
    if let Some(c) = ALTER.captures(sql) {
        let (option, value) = match c[3].to_uppercase().as_str() {
            "TTL" => ("ttl", ttl(&expr(&c[4])?)?),
            _ => ("order_by", column(&expr(&c[4])?, "SEQUENCE BY")?),
        };
        return Ok(Cow::Owned(format!("ALTER TABLE {}{} SET ({option} = {})", c.get(1).map_or("", |m| m.as_str()), &c[2], quoted(&value))));
    }
    if !CREATE.is_match(sql) || !WORDS.is_match(sql) {
        return Ok(Cow::Borrowed(sql));
    }
    let Ok(tokens) = Tokenizer::new(&GenericDialect {}, sql).tokenize_with_location() else { return Ok(Cow::Borrowed(sql)) };
    let solid: Vec<&TokenWithSpan> = tokens.iter().filter(|t| !matches!(t.token, Token::Whitespace(_))).collect();
    let text = Text::new(sql);
    let word = |k: usize| match solid.get(k).map(|t| &t.token) {
        Some(Token::Word(w)) if w.quote_style.is_none() => Some(w.value.to_uppercase()),
        _ => None,
    };
    let is = |k: usize, w: &str| word(k).as_deref() == Some(w);
    // The table's name: after TABLE and IF NOT EXISTS, parts joined by dots.
    let Some(mut at) = (0..solid.len()).find(|&k| is(k, "TABLE")).map(|k| k + 1) else { return Ok(Cow::Borrowed(sql)) };
    if is(at, "IF") && is(at + 1, "NOT") && is(at + 2, "EXISTS") {
        at += 3;
    }
    while matches!(solid.get(at + 1).map(|t| &t.token), Some(Token::Period)) {
        at += 2;
    }
    // Its columns, if listed: `MERGE f` after a column's type is that column's merge function.
    let mut cut: Vec<(usize, usize)> = vec![]; // (byte ranges taken out)
    let mut merge: Vec<String> = vec![];
    let mut head = at; // (the last token before the clauses)
    if matches!(solid.get(at + 1).map(|t| &t.token), Some(Token::LParen)) {
        let close = closing(&solid, at + 1).ok_or_else(|| anyhow::anyhow!("CREATE TABLE: a column list without its )"))?;
        let (mut depth, mut start) = (0, at + 2);
        for k in at + 1..close {
            match solid[k].token {
                Token::LParen => depth += 1,
                Token::RParen => depth -= 1,
                Token::Comma if depth == 1 => start = k + 1,
                _ => {}
            }
            if depth == 1 && k > start && is(k, "MERGE") && matches!(solid.get(k + 1).map(|t| &t.token), Some(Token::Word(_))) {
                let Some(Token::Word(c)) = solid.get(start).map(|t| &t.token) else { continue };
                let Some(Token::Word(f)) = solid.get(k + 1).map(|t| &t.token) else { continue };
                let c = if c.quote_style.is_some() { c.value.clone() } else { c.value.to_lowercase() };
                merge.push(format!("{c}:{}", f.value.to_lowercase()));
                cut.push((text.end(solid[k - 1]), text.end(solid[k + 1]))); // (` MERGE sum`)
            }
        }
        head = close;
    }
    // After them, up to the query (`AS …`) or the end: each clause runs to the next one.
    const STARTS: [(&str, Option<&str>); 6] = [("PARTITION", Some("BY")), ("PARTITIONED", Some("BY")), ("CLUSTER", Some("BY")), ("SEQUENCE", Some("BY")), ("TTL", None), ("WITH", None)];
    let starts = |k: usize| STARTS.iter().find(|(a, b)| is(k, a) && b.is_none_or(|b| is(k + 1, b)) && (*a != "WITH" || matches!(solid.get(k + 1).map(|t| &t.token), Some(Token::LParen)))).map(|(a, _)| *a);
    let (mut depth, mut found): (i32, Vec<(&str, usize)>) = (0, vec![]);
    let mut end = solid.len();
    for k in head + 1..solid.len() {
        match solid[k].token {
            Token::LParen => depth += 1,
            Token::RParen => depth -= 1,
            Token::SemiColon if depth == 0 => {
                end = k;
                break;
            }
            _ => {}
        }
        if depth != 0 || matches!(solid[k].token, Token::LParen) {
            continue;
        }
        if is(k, "AS") {
            end = k;
            break;
        }
        if let Some(s) = starts(k) {
            found.push((s, k));
        }
    }
    let mut options: Vec<(String, String)> = vec![]; // (name, as written after `=`)
    let mut kept_with: Vec<String> = vec![];
    let mut add = |name: &str, value: String| -> Result<()> {
        ensure!(!options.iter().any(|(n, _)| n == name), "CREATE TABLE: {name} is given twice");
        options.push((name.into(), quoted(&value)));
        Ok(())
    };
    for (i, (clause, k)) in found.iter().enumerate() {
        let stop = found.get(i + 1).map_or(end, |(_, n)| *n);
        let body = |skip: usize| if k + skip < stop { text.between(solid[k + skip], solid[stop - 1]) } else { "" };
        match *clause {
            "PARTITION" => add("partition_by", partition_text(&expr(body(2))?))?,
            "PARTITIONED" => {
                let parts = exprs(body(2).trim().trim_start_matches('(').trim_end_matches(')'))?;
                ensure!(parts.len() == 1, "PARTITIONED BY (…): one column, or day(ts): each file holds one partition");
                add("partition_by", partition_text(&parts[0]))?;
            }
            "CLUSTER" => add("cluster_by", exprs(body(2))?.iter().flat_map(cluster_names).collect::<Vec<_>>().join(", "))?,
            "SEQUENCE" => add("order_by", column(&expr(body(2))?, "SEQUENCE BY")?)?,
            "TTL" => add("ttl", ttl(&expr(body(1))?)?)?,
            _ => kept_with.extend(entries(&solid, *k + 1, &text)),
        }
        cut.push((text.start(solid[*k]), if stop > *k { text.end(solid[stop - 1]) } else { text.start(solid[*k]) }));
    }
    if !merge.is_empty() {
        add("merge", merge.join(", "))?;
    }
    if options.is_empty() {
        return Ok(Cow::Borrowed(sql));
    }
    for w in &kept_with {
        let name = w.split('=').next().unwrap_or_default().trim().to_lowercase();
        ensure!(!options.iter().any(|(n, _)| *n == name), "CREATE TABLE: {name} is given twice, as a clause and in WITH (…)");
    }
    // Put together: the head with MERGE taken out, one WITH, then what's left (the query).
    let after = text.end(solid[head]);
    let keep = |from: usize, to: usize| -> String {
        let mut s = String::new();
        let mut at = from;
        for &(a, b) in cut.iter().filter(|(a, _)| *a >= from && *a < to) {
            s.push_str(&sql[at..a]);
            at = b;
        }
        s.push_str(&sql[at.min(to)..to]);
        s
    };
    let with = kept_with.into_iter().chain(options.into_iter().map(|(n, v)| format!("{n} = {v}"))).collect::<Vec<_>>().join(", ");
    let rest = keep(after, sql.len());
    Ok(Cow::Owned(format!("{} WITH ({with}){}", keep(0, after), if rest.trim().is_empty() { String::new() } else { format!(" {}", rest.trim()) })))
}

/// `TTL seen + INTERVAL '1 hour'`: the `ttl` option, `seen:3600`.
fn ttl(e: &ast::Expr) -> Result<String> {
    let ast::Expr::BinaryOp { left, op: ast::BinaryOperator::Plus, right } = e else { bail!("TTL: a column plus how long its rows live, as TTL seen + INTERVAL '1 hour'") };
    let ast::Expr::Interval(i) = right.as_ref() else { bail!("TTL {e}: how long is an INTERVAL, as TTL seen + INTERVAL '1 hour'") };
    let amount = match i.value.as_ref() {
        ast::Expr::Value(v) => match &v.value {
            ast::Value::SingleQuotedString(s) => s.clone(),
            other => other.to_string(),
        },
        other => other.to_string(),
    };
    let text = match &i.leading_field {
        Some(f) => format!("{amount} {f}"),
        None => amount,
    };
    Ok(format!("{}:{}", column(left, "TTL")?, seconds(&text)?))
}

/// '1 hour', '90 minutes', '1 day 12 hours', '2 weeks': in seconds. Months and years vary in
/// length, so they are refused.
fn seconds(text: &str) -> Result<u64> {
    let words: Vec<String> = text.to_lowercase().split_whitespace().map(String::from).collect();
    ensure!(!words.is_empty() && words.len() % 2 == 0, "an interval such as '1 hour', '30 minutes' or '7 days', not {text:?}");
    let mut total = 0u64;
    for pair in words.chunks(2) {
        let n: u64 = pair[0].parse().map_err(|_| anyhow::anyhow!("an interval such as '1 hour', not {text:?}"))?;
        let unit = match pair[1].trim_end_matches('s') {
            "second" | "sec" => 1,
            "minute" | "min" => 60,
            "hour" => 3600,
            "day" => 86_400,
            "week" => 604_800,
            u => bail!("an interval in seconds, minutes, hours, days or weeks, not {u:?} ({text:?})"),
        };
        total += n * unit;
    }
    ensure!(total > 0, "an interval of at least a second, not {text:?}");
    Ok(total)
}

/// A plain column, as SQL names it.
fn column(e: &ast::Expr, clause: &str) -> Result<String> {
    match e {
        ast::Expr::Identifier(i) => Ok(ident(i)),
        ast::Expr::Nested(e) => column(e, clause),
        _ => bail!("{clause} {e}: a column of the table"),
    }
}

fn expr(text: &str) -> Result<ast::Expr> {
    let mut all = exprs(text)?;
    ensure!(all.len() == 1, "{text:?}: one expression");
    Ok(all.remove(0))
}

/// Expressions separated by commas, and nothing after them.
fn exprs(text: &str) -> Result<Vec<ast::Expr>> {
    let mut p = Parser::new(&GenericDialect {}).try_with_sql(text)?;
    let all = p.parse_comma_separated(Parser::parse_expr)?;
    ensure!(p.peek_token().token == Token::EOF, "{text:?}: {} isn't taken here", p.peek_token().token);
    Ok(all)
}

fn quoted(s: &str) -> String { format!("'{}'", s.replace('\'', "''")) }

/// A `WITH (a = 1, b = '…')` list's entries, as written.
fn entries(solid: &[&TokenWithSpan], open: usize, text: &Text) -> Vec<String> {
    let Some(close) = closing(solid, open) else { return vec![] };
    let (mut depth, mut first, mut out) = (0, open + 1, vec![]);
    for k in open + 1..=close {
        match solid[k].token {
            Token::LParen => depth += 1,
            Token::RParen if depth > 0 => depth -= 1,
            Token::Comma | Token::RParen if depth == 0 => {
                if k > first {
                    out.push(text.between(solid[first], solid[k - 1]).to_string());
                }
                first = k + 1;
            }
            _ => {}
        }
    }
    out
}

/// The `)` that closes the `(` at `open`.
fn closing(solid: &[&TokenWithSpan], open: usize) -> Option<usize> {
    let mut depth = 0;
    (open..solid.len()).find(|&k| {
        depth += match solid[k].token {
            Token::LParen => 1,
            Token::RParen => -1,
            _ => 0,
        };
        depth == 0
    })
}

/// Byte offsets of tokens in the text they came from.
struct Text<'a> {
    sql: &'a str,
    lines: Vec<usize>,
}

impl<'a> Text<'a> {
    fn new(sql: &'a str) -> Self { Text { sql, lines: std::iter::once(0).chain(sql.match_indices('\n').map(|(i, _)| i + 1)).collect() } }

    fn byte(&self, l: &Location) -> usize {
        let start = self.lines.get((l.line.max(1) - 1) as usize).copied().unwrap_or(self.sql.len());
        self.sql[start..].char_indices().nth((l.column.max(1) - 1) as usize).map_or(self.sql.len(), |(i, _)| start + i)
    }

    fn start(&self, t: &TokenWithSpan) -> usize { self.byte(&t.span.start) }

    fn end(&self, t: &TokenWithSpan) -> usize { self.byte(&t.span.end) }

    fn between(&self, a: &TokenWithSpan, b: &TokenWithSpan) -> &'a str { &self.sql[self.start(a)..self.end(b).max(self.start(a))] }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn with(sql: &str) -> String { clauses(sql).expect("taken").into_owned() }

    #[test]
    fn clauses_become_options() {
        assert_eq!(
            with("CREATE TABLE s (id BIGINT PRIMARY KEY, seen TIMESTAMP, page VARCHAR) PARTITION BY day(seen) CLUSTER BY (page) SEQUENCE BY seen TTL seen + INTERVAL '1 hour' WITH (publish = (delta, iceberg))"),
            "CREATE TABLE s (id BIGINT PRIMARY KEY, seen TIMESTAMP, page VARCHAR) WITH (publish = (delta, iceberg), partition_by = 'day(seen)', cluster_by = 'page', order_by = 'seen', ttl = 'seen:3600')"
        );
        assert_eq!(
            with("CREATE TABLE t (region VARCHAR PRIMARY KEY, amount DOUBLE MERGE sum, n BIGINT MERGE count)"),
            "CREATE TABLE t (region VARCHAR PRIMARY KEY, amount DOUBLE, n BIGINT) WITH (merge = 'amount:sum, n:count')"
        );
        assert_eq!(with("CREATE TABLE t CLUSTER BY a, b AS SELECT 1 AS a, 2 AS b"), "CREATE TABLE t WITH (cluster_by = 'a, b') AS SELECT 1 AS a, 2 AS b");
        assert_eq!(with("ALTER TABLE s TTL seen + INTERVAL 2 HOUR"), "ALTER TABLE s SET (ttl = 'seen:7200')");
        assert_eq!(with("ALTER TABLE s SEQUENCE BY seen;"), "ALTER TABLE s SET (order_by = 'seen')");
        // (a column named merge, and a window's PARTITION BY in the query, are not clauses)
        assert_eq!(with("CREATE TABLE t (merge INT, x INT)"), "CREATE TABLE t (merge INT, x INT)");
        assert_eq!(with("CREATE TABLE t AS SELECT row_number() OVER (PARTITION BY a) AS r FROM u"), "CREATE TABLE t AS SELECT row_number() OVER (PARTITION BY a) AS r FROM u");
    }

    #[test]
    fn refused_by_name() {
        assert!(clauses("CREATE TABLE t (a INT, ts TIMESTAMP) PARTITION BY a WITH (partition_by = 'a')").is_err());
        assert!(clauses("CREATE TABLE t (id INT PRIMARY KEY, ts TIMESTAMP) TTL ts").is_err());
        assert!(clauses("CREATE TABLE t (id INT PRIMARY KEY, ts TIMESTAMP) TTL ts + INTERVAL '1 month'").is_err());
    }
}
