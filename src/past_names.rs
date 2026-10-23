//! `t AT (…)` as text (ADR-043): its syntax, the table name `"at:…"` it becomes, reading that name back, and
//! `RESTORE`'s words. Reading the table as it was is the node's (`past.rs`).

use anyhow::{anyhow, bail, ensure, Result};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD as B64, Engine};
use datafusion::sql::sqlparser::{ast, dialect::GenericDialect, keywords::{Keyword, RESERVED_FOR_TABLE_ALIAS}, tokenizer::{Location, Token, Tokenizer}};
use serde::{Deserialize, Serialize};
use std::borrow::Cow;

/// What a query asked for: a table, and when.
#[derive(Serialize, Deserialize)]
pub(crate) struct Spec {
    pub(crate) table: String,
    pub(crate) at: String,   // version, timestamp or offset
    pub(crate) expr: String, // as written: worked out when the query runs (`now()` is then)
}

impl Spec {
    /// The spec a table's name stands for, with or without its `at:`: base64url of its JSON, as `table_factor` wrote it.
    pub(crate) fn of(name: &str) -> Result<Spec> {
        Ok(serde_json::from_slice(&B64.decode(name.trim_start_matches("at:"))?)?)
    }
}

/// `t AT (VERSION => …)` → `pondra_at(t, version => …) AS t`, outside strings and comments: the
/// parser takes no `AT` after a table (only Snowflake's and Databricks' dialects do).
pub fn syntax(sql: &str) -> Cow<'_, str> {
    if let Cow::Owned(at) = as_of(sql) {
        return Cow::Owned(syntax(&at).into_owned());
    }
    static AT: std::sync::LazyLock<regex::Regex> = std::sync::LazyLock::new(|| regex::Regex::new(r"(?i)\bAT\s*\(\s*(VERSION|TIMESTAMP|OFFSET)\s*=>").expect("a regex"));
    if !AT.is_match(sql) {
        return Cow::Borrowed(sql);
    }
    let Ok(tokens) = Tokenizer::new(&GenericDialect {}, sql).tokenize_with_location() else { return Cow::Borrowed(sql) };
    let lines: Vec<usize> = std::iter::once(0).chain(sql.match_indices('\n').map(|(i, _)| i + 1)).collect();
    let byte = |l: &Location| -> usize {
        let start = lines.get((l.line.max(1) - 1) as usize).copied().unwrap_or(sql.len());
        sql[start..].char_indices().nth((l.column.max(1) - 1) as usize).map_or(sql.len(), |(i, _)| start + i)
    };
    let solid: Vec<&datafusion::sql::sqlparser::tokenizer::TokenWithSpan> = tokens.iter().filter(|t| !matches!(t.token, Token::Whitespace(_))).collect();
    let word = |k: usize| match solid.get(k).map(|t| &t.token) {
        Some(Token::Word(w)) => Some(w),
        _ => None,
    };
    let is = |k: usize, kw: Keyword| word(k).is_some_and(|w| w.quote_style.is_none() && w.keyword == kw);
    let (mut out, mut copied, mut i) = (String::new(), 0, 1);
    while i < solid.len() {
        let when = [Keyword::VERSION, Keyword::TIMESTAMP, Keyword::OFFSET].into_iter().find(|k| is(i + 2, *k));
        let shaped = is(i, Keyword::AT) && matches!(solid.get(i + 1).map(|t| &t.token), Some(Token::LParen)) && matches!(solid.get(i + 3).map(|t| &t.token), Some(Token::RArrow));
        let (Some(when), true, Some(_)) = (when, shaped, word(i - 1)) else {
            i += 1;
            continue;
        };
        let mut first = i - 1; // (the table's name: `t`, `s.t`, `lake.s.t`, quoted or not)
        while first >= 2 && matches!(solid[first - 1].token, Token::Period) && word(first - 2).is_some() {
            first -= 2;
        }
        let mut depth = 0;
        let Some(close) = (i + 1..solid.len()).find(|&k| {
            depth += match solid[k].token {
                Token::LParen => 1,
                Token::RParen => -1,
                _ => 0,
            };
            depth == 0
        }) else { break };
        let (start, end) = (byte(&solid[first].span.start), byte(&solid[close].span.end));
        let name = &sql[start..byte(&solid[i - 1].span.end)];
        let expr = &sql[byte(&solid[i + 3].span.end)..byte(&solid[close].span.start)];
        let aliased = word(close + 1).is_some_and(|w| w.keyword == Keyword::AS || w.quote_style.is_some() || !RESERVED_FOR_TABLE_ALIAS.contains(&w.keyword));
        out.push_str(&sql[copied..start]);
        out.push_str(&format!("pondra_at({name}, {} => {expr})", format!("{when:?}").to_lowercase()));
        if !aliased {
            out.push_str(&format!(" AS {}", word(i - 1).expect("a name")));
        }
        (copied, i) = (end, close + 1);
    }
    if copied == 0 {
        return Cow::Borrowed(sql);
    }
    out.push_str(&sql[copied..]);
    Cow::Owned(out)
}

/// Other engines' words for `AT (…)`: Delta's and Spark's `t VERSION AS OF n` and `t TIMESTAMP AS
/// OF '…'`, and SQL:2011's and BigQuery's `t FOR SYSTEM_TIME AS OF '…'`, after a table in a FROM or
/// a JOIN. What follows is a number, a string, `TIMESTAMP '…'`, a `$variable` or `(an expression)`.
fn as_of(sql: &str) -> Cow<'_, str> {
    static AS_OF: std::sync::LazyLock<regex::Regex> = std::sync::LazyLock::new(|| regex::Regex::new(r"(?i)\b(VERSION|TIMESTAMP|SYSTEM_TIME)\s+AS\s+OF\b").expect("a regex"));
    if !AS_OF.is_match(sql) {
        return Cow::Borrowed(sql);
    }
    let Ok(tokens) = Tokenizer::new(&GenericDialect {}, sql).tokenize_with_location() else { return Cow::Borrowed(sql) };
    let lines: Vec<usize> = std::iter::once(0).chain(sql.match_indices('\n').map(|(i, _)| i + 1)).collect();
    let byte = |l: &Location| -> usize {
        let start = lines.get((l.line.max(1) - 1) as usize).copied().unwrap_or(sql.len());
        sql[start..].char_indices().nth((l.column.max(1) - 1) as usize).map_or(sql.len(), |(i, _)| start + i)
    };
    let solid: Vec<_> = tokens.iter().filter(|t| !matches!(t.token, Token::Whitespace(_))).collect();
    let is = |k: usize, kw: Keyword| matches!(solid.get(k).map(|t| &t.token), Some(Token::Word(w)) if w.quote_style.is_none() && w.keyword == kw);
    let named = |k: usize| matches!(solid.get(k).map(|t| &t.token), Some(Token::Word(_)));
    let (mut out, mut copied, mut next) = (String::new(), 0, 1);
    for i in 1..solid.len() {
        if i < next {
            continue;
        }
        let (when, at) = match () {
            _ if is(i, Keyword::VERSION) && is(i + 1, Keyword::AS) && is(i + 2, Keyword::OF) => ("VERSION", i + 3),
            _ if is(i, Keyword::TIMESTAMP) && is(i + 1, Keyword::AS) && is(i + 2, Keyword::OF) => ("TIMESTAMP", i + 3),
            _ if is(i, Keyword::FOR) && is(i + 1, Keyword::SYSTEM_TIME) && is(i + 2, Keyword::AS) && is(i + 3, Keyword::OF) => ("TIMESTAMP", i + 4),
            _ => continue,
        };
        let mut first = i - 1; // (the table's name, after FROM, JOIN or a comma)
        while first >= 2 && matches!(solid[first - 1].token, Token::Period) && named(first - 2) {
            first -= 2;
        }
        let after = first.checked_sub(1).is_some_and(|k| is(k, Keyword::FROM) || is(k, Keyword::JOIN) || matches!(solid[k].token, Token::Comma));
        if !named(i - 1) || !after {
            continue;
        }
        let end = match solid.get(at).map(|t| &t.token) {
            Some(Token::Number(..) | Token::SingleQuotedString(_) | Token::Placeholder(_)) => at,
            Some(Token::Word(_)) if matches!(solid.get(at + 1).map(|t| &t.token), Some(Token::SingleQuotedString(_))) => at + 1, // (TIMESTAMP '…')
            Some(Token::LParen) => {
                let mut depth = 0;
                match (at..solid.len()).find(|&k| {
                    depth += match solid[k].token {
                        Token::LParen => 1,
                        Token::RParen => -1,
                        _ => 0,
                    };
                    depth == 0
                }) {
                    Some(k) => k,
                    None => continue,
                }
            }
            _ => continue,
        };
        let (start, stop) = (byte(&solid[i].span.start), byte(&solid[end].span.end));
        let expr = &sql[byte(&solid[at].span.start)..stop];
        out.push_str(&sql[copied..start]);
        out.push_str(&format!("AT ({when} => {expr})"));
        (copied, next) = (stop, end + 1);
    }
    match copied {
        0 => Cow::Borrowed(sql),
        _ => Cow::Owned(out + &sql[copied..]),
    }
}

/// `pondra_at(t, version => …)` in a FROM: the table of `t` as it was, by its own name. Ok(false):
/// not one.
pub fn table_factor(lake: &str, t: &mut ast::TableFactor) -> Result<bool> {
    let ast::TableFactor::Table { name, args: Some(args), .. } = t else { return Ok(false) };
    if crate::text::object(name) != "pondra_at" {
        return Ok(false);
    }
    let shape = "t AT (VERSION => n), AT (TIMESTAMP => t) or AT (OFFSET => -seconds)";
    let a = crate::text::args("AT", &args.args).map_err(|_| anyhow!("{shape}"))?;
    let table = match a.given.first() {
        Some(ast::Expr::Identifier(i)) => crate::text::ident(i),
        Some(ast::Expr::CompoundIdentifier(parts)) => parts.iter().map(crate::text::ident).collect::<Vec<_>>().join("."),
        _ => bail!("{shape}: after a table's name"),
    };
    let table = crate::text::local_in(lake, &table).ok_or_else(|| anyhow!("{table} AT (…): only this lake's tables are read as they were (ask a node of that lake)"))?;
    ensure!(a.given.len() == 1 && a.named.len() == 1 && ["version", "timestamp", "offset"].contains(&a.named[0].0.as_str()), "{shape}");
    let (at, expr) = (a.named[0].0.clone(), a.named[0].1.to_string());
    let spec = B64.encode(serde_json::to_vec(&Spec { table, at, expr })?);
    let ast::TableFactor::Table { name, args, .. } = t else { unreachable!() };
    (*name, *args) = (ast::ObjectName::from(vec![ast::Ident::with_quote('"', format!("at:{spec}"))]), None);
    Ok(true)
}

/// Does this (expanded) SQL read a table as it was? Such a query runs on its node alone.
pub fn mentioned(sql: &str) -> bool { sql.contains("\"at:") }

/// The `t AT (… => …)` that each table as it was in this (expanded) SQL stands for, as written: its
/// moment is worked out as the query runs, so `server::query` looks there for one that moves.
pub fn spelled(sql: &str) -> String {
    names(sql).iter().filter_map(|n| Spec::of(n).ok()).map(|s| format!("{} AT ({} => {})", s.table, s.at.to_uppercase(), s.expr)).collect::<Vec<_>>().join(" ")
}

/// The tables as they were that `sql` reads, by their names in it.
pub fn names(sql: &str) -> Vec<String> {
    static NAMES: std::sync::LazyLock<regex::Regex> = std::sync::LazyLock::new(|| regex::Regex::new(r#""(at:[A-Za-z0-9_-]+)""#).expect("a regex"));
    let mut out: Vec<String> = NAMES.captures_iter(sql).map(|c| c[1].to_string()).collect();
    out.sort();
    out.dedup();
    out
}

#[cfg_attr(not(feature = "server"), allow(dead_code))] // (a node's RESTORE reads it; a page has no past)
pub(crate) static RESTORE: std::sync::LazyLock<regex::Regex> = std::sync::LazyLock::new(|| {
    regex::Regex::new(r#"(?is)^\s*RESTORE\s+(?:TABLE\s+)?([\w."-]+)\s+(?:TO\s+)?(VERSION|TIMESTAMP)\s+AS\s+OF\s+(.+?)\s*;?\s*$"#).expect("a regex")
});

/// A RESTORE's text before its moment, and the moment (`$day` in it is bound by `routines::bind`:
/// the parser doesn't read a RESTORE). None: not a RESTORE.
#[cfg_attr(not(feature = "server"), allow(dead_code))] // (a node's RESTORE reads it; a page has no past)
pub fn restore_moment(sql: &str) -> Option<(&str, &str)> {
    if crate::text::head(sql) != "restore" {
        return None;
    }
    let moment = RESTORE.captures(sql)?.get(3)?;
    Some((&sql[..moment.start()], moment.as_str()))
}
