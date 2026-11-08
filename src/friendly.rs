//! Friendly SQL, where SQL comes in (round 34; the parity list in
//! `designs/scripting-design-review.md`): what users of DuckDB, Snowflake and Postgres type and
//! DataFusion doesn't plan, rewritten into SQL it does before anything else sees it
//! (`routines::expand`), as `FROM`-first and `ASOF JOIN` already are. A view, a frame, a spread
//! query and every door then get plain SQL.
//!
//! - In the text, before it is parsed (`text`): DuckDB's `PIVOT` and `UNPIVOT` statements as the standard's; DuckDB's
//!   `ASOF JOIN … ON a.t >= b.t` as Snowflake's; `USING SAMPLE`.
//! - In the parse, hooks of the fallback dialect (`dialect.rs`): list comprehensions (`[x * 2 FOR x IN l IF x > 0]`) as
//!   `array_transform(…, x -> …)`, and `LAMBDA x: …` as `x -> …`.
//! - In the syntax tree (`rewrite`): `FETCH FIRST`, `ORDER BY ALL`, `({…}).a`, `max_by` and
//!   `arg_max`, `string_split`, `::json`, `json_extract`, `payload->>'$.a'`, `PIVOT`, `UNPIVOT`, `TABLESAMPLE` (which
//!   DataFusion ignored), `COLUMNS(…)`, `* RENAME`, a select's alias in its WHERE.
//! - Whole statements: `SUMMARIZE` (`summarize`).
//! - Where a query is planned (`lambdas`): a higher-order function's `x -> …` as a lambda.
//!
//! What needs the data (a PIVOT's values, a FROM's columns) is asked with a query of its own, as
//! the caller, before the tree is rewritten: a first pass collects the questions, a second takes
//! the answers in the same order (`Asks`).
use crate::expander::grouped; // (a call written as an expression: in parentheses inside an operator)
use anyhow::{bail, ensure, Context, Result};
use datafusion::sql::sqlparser::ast::*;
use datafusion::sql::sqlparser::{dialect::GenericDialect, keywords::Keyword, parser::Parser, tokenizer::Token};
use regex::Regex;
use std::borrow::Cow;
use std::collections::{BTreeSet, HashMap, HashSet, VecDeque};
use std::ops::ControlFlow;
use std::sync::LazyLock;

fn expr(sql: &str) -> Result<Expr> { Ok(Parser::new(&GenericDialect {}).try_with_sql(sql)?.parse_expr()?) }
pub(crate) fn query(sql: &str) -> Result<Query> { Ok(*Parser::new(&GenericDialect {}).try_with_sql(sql)?.parse_query()?) }
pub(crate) fn quote(s: &str) -> String { format!("'{}'", s.replace('\'', "''")) }
fn name(s: &str) -> String { format!("\"{}\"", s.replace('"', "\"\"")) }

/// Whether `sql` may hold any of them: a look before anything is parsed. The front asks its three parts (`passes`), each
/// only where its trigger is in the text; this module's tests ask the three at once.
#[cfg(test)]
fn wanted(sql: &str) -> bool {
    wants_forms(sql) || wants_marked(sql) || wants_aliases(sql)
}

/// Postgres's bytea-only functions (`pg_bytea`), whose string literal reads as a bytea: a look's and a trigger's.
const TAKES_BYTEA: &[&str] = crate::pg_bytea::TAKES_BYTEA;

/// The front's passes of this module (`passes.rs`): the text's rewrite, and the three looks `wanted` makes, each with a
/// trigger that holds every text its look finds.
pub(crate) fn passes() -> Vec<crate::passes::Pass> {
    use crate::passes::{Hook, Parse, Pass, Run};
    let tables = DUCKDB_TYPES.iter().map(|(n, _)| *n).chain(OTHER_TYPES.iter().map(|(n, _, _)| *n)).chain(crate::dialect_fns::OURS.iter().map(|(n, _, _)| *n)).chain(TAKES_BYTEA.iter().copied());
    let mut forms = vec![
        "fetch", "all", "max_by", "min_by", "arg_max", "arg_min", "string_split", "str_split", "json", "jsonb", "json_value", "get_json_object", "tablesample", "sample", "pondra_sample",
        "pivot", "unpivot", "rename", "apply", "list_apply", "array_apply", "columns", "filter", "list", "split", "lateral", "strftime", "strptime", "epoch", "regexp_extract", "datediff",
        "date_diff", "dateadd", "date_add", "date_sub", "timestampadd", "timeadd", "group_concat", "array_agg", "iff", "iif", "zeroifnull", "decode", "array_construct", "array_size",
        "object_construct", "isnull", "charindex", "convert", "try_convert", "getdate", "sysdatetime", "eomonth", "datepart", "curdate", "unix_timestamp", "unix_seconds", "timestamp_seconds",
        "str_to_date", "date_format", "timestampdiff", "countif", "date", "date_trunc", "timestamp_trunc", "datetime_trunc", "timestamp_diff", "datetime_diff", "format_date", "format_timestamp",
        "format_datetime", "parse_date", "parse_timestamp", "parse_datetime", "transform", "format", "top", "int64", "float64", "number", "byteint", "within", "safe", "materialized",
        "timestamp_ntz", "time",
    ];
    forms.extend(tables);
    vec![
        Pass { name: "friendly.text", order: 40, words: vec!["for", "pivot", "unpivot", "asof", "sample", "tblproperties", "top"], marks: vec![&["@@"]], run: Run::Text(text) },
        // (`json_extract…` is a word's start, so a mark; `).a`, Snowflake's `payload:page`)
        Pass { name: "friendly.forms", order: 10, words: forms, marks: vec![&["json_extract"], &[")", "."], &[":"]], run: Run::Wants(wants_forms) },
        Pass {
            name: "friendly.marked",
            order: 20,
            words: vec!["bytea", "character"],
            // (`(a, b) IN (SELECT …)` and its NOT IN, `CHAR VARYING`, `DEFAULT VALUES`, `ESCAPE '…'`, an array literal or
            // `ARRAY[`, a JSONPath after `->` or `->>`, a national string `N'…'`)
            marks: vec![&["+0'"], &["+1'"], &["+2'"], &["+3'"], &["+4'"], &["+5'"], &["+6'"], &["+7'"], &["+8'"], &["+9'"], &["-0'"], &["-1'"], &["-2'"], &["-3'"], &["-4'"], &["-5'"], &["-6'"], &["-7'"], &["-8'"], &["-9'"],
                &["0e"], &["1e"], &["2e"], &["3e"], &["4e"], &["5e"], &["6e"], &["7e"], &["8e"], &["9e"], &[")", "in"], &[")", "not"], &["char", "varying"], &["default", "values"], &["escape", "'"],
                &["["], &["->", "'$"], &["->>", "'$"], &["n'"]],
            run: Run::Wants(wants_marked),
        },
        Pass { name: "friendly.aliases", order: 30, words: vec!["where", "delete", "update"], marks: vec![], run: Run::Wants(wants_aliases) },
        Pass {
            name: "friendly.comprehension",
            order: 10,
            words: vec!["for"],
            marks: vec![],
            run: Run::Parse(Parse {
                claims: "[",
                reads: &["SELECT [x * 2 FOR x IN [1, 2, 3]] AS v", "SELECT [x FOR x IN l IF x > 1] FROM t", "SELECT [[y FOR y IN x] FOR x IN [[1, 2], [3]]] AS v"],
                hook: Hook::Prefix(comprehension),
            }),
        },
        Pass {
            name: "friendly.lambda",
            order: 20,
            words: vec!["lambda"],
            marks: vec![],
            run: Run::Parse(Parse {
                claims: "lambda",
                reads: &["SELECT list_transform(l, LAMBDA x: x + 1) FROM t", "SELECT list_reduce(l, LAMBDA a, b: a + b) FROM t"],
                hook: Hook::Prefix(lambda_words),
            }),
        },
    ]
}

/// Another database's word, function or type, a dbt view in parentheses, or a call Pondra refuses by name.
fn wants_forms(sql: &str) -> bool {
    // (the last alternatives: BigQuery's `SAFE.f(`, and Snowflake's `payload:page` (a colon after a word or a closing
    // bracket, then a letter, `_`, a quote or `[`: not `x::INT`, nor `a = :p`, a parameter))
    static WORDS: LazyLock<Regex> = LazyLock::new(|| {
        Regex::new(r#"(?i)\b(?:fetch|order\s+by\s+all|max_by|min_by|arg_max|arg_min|string_split|str_split|jsonb?|json_extract\w*|json_value|get_json_object|tablesample|sample|pondra_sample|pivot|unpivot|rename|apply|list_apply|array_apply)\b|\b(?:columns|filter|list|split|lateral|strftime|strptime|epoch|regexp_extract|date_?diff|date_?add|date_sub|timestampadd|timeadd|group_concat|array_agg|iff|iif|zeroifnull|decode|array_construct|array_size|object_construct|isnull|charindex|convert|try_convert|getdate|sysdatetime|eomonth|datepart|curdate|unix_timestamp|unix_seconds|timestamp_seconds|str_to_date|date_format|timestampdiff|countif|date|date_trunc|timestamp_trunc|datetime_trunc|timestamp_diff|datetime_diff|format_date|format_timestamp|format_datetime|parse_date|parse_timestamp|parse_datetime|transform|format)\s*\(|\bselect\s+(?:all\s+|distinct\s+)?top\b|\b(?:int64|float64|number|byteint)\b|\bwithin\s+group\b|\)\s*\.\s*[a-z_]|\bsafe\s*\.\s*\w+\s*\(|(?:[\w)\]"])\s*:(?:[A-Za-z_"\[])"#).expect("a regex")
    });
    // (a type DuckDB names otherwise, or one Pondra refuses by name: `DUCKDB_TYPES`, `OTHER_TYPES`, and the ones sqlparser
    // reads as its own; only where a type stands, after AS or `::`, before a literal's text, or in a CREATE, ALTER or
    // DECLARE: `year`, `long` or `money` as a column's name sends a query through no parse)
    static TYPES: LazyLock<Regex> = LazyLock::new(|| {
        let names: Vec<&str> = DUCKDB_TYPES.iter().map(|(n, _)| *n).chain(OTHER_TYPES.iter().map(|(n, _, _)| *n)).collect();
        let t = format!(r"(?:{}|timestamp_ntz|time\s+with\s+time\s+zone)", names.join("|"));
        Regex::new(&format!(r"(?i)(?:\bas\s+|::\s*){t}\b|\b{t}\s*'|(?:^|;)\s*(?:--[^\n]*\n\s*)*(?:create|alter|declare)\b[^;]*\b{t}\b")).expect("a regex")
    });
    // (another database's function Pondra refuses by name: the names `dialect_fns::OURS` lists, as calls)
    static OURS_CALL: LazyLock<Regex> = LazyLock::new(|| Regex::new(&format!(r"(?i)\b(?:{})\s*\(", crate::dialect_fns::OURS.iter().map(|(n, _, _)| *n).collect::<Vec<_>>().join("|"))).expect("a regex"));
    // (a materialized view whose query is in parentheses, dbt's `AS (SELECT …)`: nothing else in its
    // text is a friendly form, so `unparen` is what it needs)
    static PAREN_VIEW: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"(?is)^\s*create\s+(?:or\s+replace\s+)?materialized\s+view\b.*?\bas\s*\(").expect("a regex"));
    // (a call of one of Postgres's bytea-only functions, whose string literal `expression` reads as a bytea)
    static BYTEA_CALL: LazyLock<Regex> = LazyLock::new(|| Regex::new(&format!(r"(?i)\b(?:{})\s*\(", TAKES_BYTEA.join("|"))).expect("a regex"));
    WORDS.is_match(sql) || PAREN_VIEW.is_match(sql) || BYTEA_CALL.is_match(sql) || OURS_CALL.is_match(sql) || TYPES.is_match(sql)
}

/// A form a rewrite takes that a symbol marks: a zone, an exponent, a bytea, an array literal, a JSONPath.
fn wants_marked(sql: &str) -> bool {
    // (the forms the rewrites below take: a timestamp's one-digit zone, a number with an exponent, a bytea
    // literal in either of its forms (matched by the word, since the escape form has no `\x` to match),
    // `(a, b) IN (SELECT …)`, a character type's Postgres name, DEFAULT VALUES, LIKE … ESCAPE, an array literal
    // `[…]` or `ARRAY[…]` (a `[` straight after a word, `)`, `]` or `"` is a subscript, `arr[1]`), and a JSONPath
    // after `->` or `->>` (`payload->>'$.a'`, DuckDB's and MySQL's), and a national string `N'…'`)
    static MORE: LazyLock<Regex> = LazyLock::new(|| {
        Regex::new(r#"(?i)[+-]\d'|\d[e][+-]?\d|\bbytea\b|\)\s*(?:not\s+)?in\s*\(\s*(?:select|with)\b|\bcharacter\b|\bchar\s+varying\b|\bdefault\s+values\b|\bescape\s+'|\barray\s*\[|(?:^|[^\w)\]"])\[|->>?\s*'\$|(?:^|[^\w'])[nN]'"#).expect("a regex")
    });
    MORE.is_match(sql)
}

/// A select's alias read in its WHERE, or a DELETE's or UPDATE's table with one.
fn wants_aliases(sql: &str) -> bool {
    alias_in_where(sql) || alias_after_table(sql)
}

/// Whether a name given with `AS` comes up again after a WHERE (a select's alias it may use).
fn alias_in_where(sql: &str) -> bool {
    static WHERE: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"(?i)\bwhere\b").expect("a regex"));
    static AS: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"(?i)\bas\s+([a-z_][a-z0-9_]*)").expect("a regex"));
    static WORD: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"[A-Za-z_][A-Za-z0-9_]*").expect("a regex"));
    let Some(w) = WHERE.find(sql) else { return false };
    let named: Vec<&str> = AS.captures_iter(&sql[..w.start()]).filter_map(|c| c.get(1)).map(|m| m.as_str()).collect();
    !named.is_empty() && WORD.find_iter(&sql[w.end()..]).any(|m| named.iter().any(|n| n.eq_ignore_ascii_case(m.as_str())))
}

/// Whether a DELETE or UPDATE's table has an alias (`UPDATE t AS u`, `UPDATE t u`, `DELETE FROM t AS d`).
fn alias_after_table(sql: &str) -> bool {
    static TABLE: LazyLock<Regex> = LazyLock::new(|| Regex::new(r#"(?i)^\s*(?:delete\s+from|update)\s+[\w."]+\s+(as\s+)?(\w+)\b"#).expect("a regex"));
    let Some(c) = TABLE.captures(sql) else { return false };
    let word = c[2].to_lowercase();
    c.get(1).is_some() || !["set", "where", "using", "from", "returning"].contains(&word.as_str())
}

// ---------------------------------------------------------------- the text

/// `sql` with what the parser can't read as it is written turned into what it can.
pub fn text(sql: &str) -> Result<Cow<'_, str>> {
    static LOOK: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"(?i)^\s*(?:--[^\n]*\n\s*)*(?:pivot|unpivot)\b|\basof\b|\busing\s+sample\b|\bfor\s+(?:no\s+key\s+update|key\s+share)\b|\btblproperties\b|@@\w|\btop\b").expect("a regex"));
    if !LOOK.is_match(sql) {
        return Ok(Cow::Borrowed(sql));
    }
    let mut s = statements(sql)?.unwrap_or_else(|| sql.to_string());
    // (each pass reads the text as the passes before it left it, tokenized again only after one of them changed it: a
    // `top` that is only a word in a string costs one tokenize, audit 11-02 L13)
    let passes = [asof_on, using_sample, lock_words, at_at_words, property_keys, top_alias];
    let mut next = 0;
    while next < passes.len() {
        let Some(t) = Tokens::of(&s) else { break };
        let mut edited = None;
        while edited.is_none() && next < passes.len() {
            let edits = passes[next](&t)?;
            next += 1;
            edited = (!edits.is_empty()).then(|| t.edit(edits));
        }
        match edited {
            Some(e) => s = e,
            None => break,
        }
    }
    Ok(Cow::Owned(s))
}

/// A text and its tokens (`text::tokens`, shared with every other look at the statement), so edits keep the rest of it
/// as written.
struct Tokens<'a> {
    sql: &'a str,
    t: std::sync::Arc<crate::text::Tokens>,
}

impl std::ops::Deref for Tokens<'_> {
    type Target = crate::text::Tokens; // (`toks`, `start`, `next`, `word`)
    fn deref(&self) -> &Self::Target { &self.t }
}

impl<'a> Tokens<'a> {
    fn of(sql: &'a str) -> Option<Self> { Some(Tokens { sql, t: crate::text::tokens(sql)? }) }
    fn end(&self, i: usize) -> usize { self.t.end.get(i).copied().unwrap_or(self.sql.len()) }
    fn span(&self, from: usize, to: usize) -> &'a str { &self.sql[self.start.get(from).copied().unwrap_or(self.sql.len())..self.start.get(to).copied().unwrap_or(self.sql.len())] }
    fn edit(&self, mut edits: Vec<(usize, usize, String)>) -> String {
        let mut out = self.sql.to_string();
        edits.sort_by_key(|e| std::cmp::Reverse(e.0));
        for (from, to, with) in edits {
            out.replace_range(from..to, &with);
        }
        out
    }
}

/// DuckDB's `LAMBDA x, y: body`, read by the fallback dialect (`dialect.rs`) as `(x, y) -> body`: the tree GenericDialect
/// reads the old text pass's `(x, y) -> body` to, JSON's arrow at its precedence, which `lambdas` takes as a lambda where
/// the query is planned (`lambda` gives the body what follows an AND or OR, as it does for `x -> …`). None when the word is
/// not one: the dialect reads it as a name. (At the start of an expression the arrow takes what follows it there: `a +
/// LAMBDA x: x` is `a + (x -> x)`, where the text read `(a + x) -> x`; no function takes either.) `LAMBDA (x): …` is not
/// one: DuckDB refuses it, and GenericDialect reads it as a call of `lambda`, so no hook is asked.
fn lambda_words(p: &mut Parser) -> Option<Result<Expr, datafusion::sql::sqlparser::parser::ParserError>> {
    use datafusion::sql::sqlparser::dialect::{Dialect, Precedence};
    if !matches!(&p.peek_token_ref().token, Token::Word(w) if w.keyword == Keyword::LAMBDA) {
        return None;
    }
    let mut params = match p.maybe_parse(|p| {
        p.expect_keyword_is(Keyword::LAMBDA)?;
        let params = p.parse_comma_separated(|p| p.parse_identifier())?;
        p.expect_token(&Token::Colon)?;
        Ok(params)
    }) {
        Ok(Some(params)) => params,
        Ok(None) => return None,
        Err(e) => return Some(Err(e)),
    };
    let left = match params.len() {
        1 => Expr::Identifier(params.remove(0)),
        _ => Expr::Tuple(params.into_iter().map(Expr::Identifier).collect()),
    };
    let arrow = GenericDialect {}.prec_value(Precedence::PgOther);
    Some(p.parse_subexpr(arrow).map(|body| Expr::BinaryOp { left: Box::new(left), op: BinaryOperator::Arrow, right: Box::new(body) }))
}

/// A list comprehension, `[expr FOR x IN list IF cond]`, read by the fallback dialect (`dialect.rs`) as
/// `array_transform(array_filter(list, x -> (cond)), x -> (expr))` (`array_transform((list), x -> (expr))` without IF):
/// the tree GenericDialect reads that text to, so the tree passes and the engine take it as they took the text.
/// None when the bracket is not one: the dialect reads it as an array.
fn comprehension(p: &mut Parser) -> Option<Result<Expr, datafusion::sql::sqlparser::parser::ParserError>> {
    use datafusion::sql::sqlparser::parser::ParserError;
    if p.peek_token_ref().token != Token::LBracket {
        return None;
    }
    // (a comprehension from its FOR on: up to it, an array's first element as well)
    let body = match p.maybe_parse(|p| {
        p.expect_token(&Token::LBracket)?;
        let body = p.parse_expr()?;
        p.expect_keyword_is(Keyword::FOR)?;
        Ok(body)
    }) {
        Ok(Some(body)) => body,
        Ok(None) => return None,
        Err(e) => return Some(Err(e)),
    };
    // (said where the name should be, as sqlparser says where it stopped)
    let one_name = |at| ParserError::ParserError(format!("a list comprehension takes one name: [… FOR x IN list]{at}"));
    let read = |p: &mut Parser| -> Result<Expr, ParserError> {
        let at = p.peek_token_ref().span.start;
        let var = match p.parse_identifier() {
            Ok(v) if v.quote_style.is_none() && v.value.chars().next().is_some_and(|c| c.is_ascii_alphabetic() || c == '_') && v.value.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') => v,
            _ => return Err(one_name(at)),
        };
        if !p.parse_keyword(Keyword::IN) {
            return Err(one_name(p.peek_token_ref().span.start));
        }
        let list = p.parse_expr()?;
        let cond = match p.parse_keyword(Keyword::IF) {
            true => Some(p.parse_expr()?),
            false => None,
        };
        p.expect_token(&Token::RBracket)?;
        let var = Expr::Identifier(var);
        Ok(match cond {
            Some(cond) => templated("array_transform(array_filter($_1, $_2 -> ($_3)), $_2 -> ($_4))", &[list, var, cond, body.clone()]),
            None => templated("array_transform(($_1), $_2 -> ($_3))", &[list, var, body.clone()]),
        })
    };
    Some(read(p))
}

/// `template`, read by GenericDialect, with each `$_n` the nth of `parts`, as it stands (`expander::around` groups them).
fn templated(template: &str, parts: &[Expr]) -> Expr {
    let mut e = crate::expander::parse_expr(template).expect("a template that parses");
    let _ = crate::walk::visit_expressions_mut(&mut e, |x| {
        if let Expr::Value(ValueWithSpan { value: Value::Placeholder(p), .. }) = x {
            if let Some(part) = p.strip_prefix("$_").and_then(|n| n.parse::<usize>().ok()).and_then(|n| parts.get(n.wrapping_sub(1))) {
                *x = part.clone();
            }
        }
        ControlFlow::<()>::Continue(())
    });
    e
}

/// DuckDB's `a ASOF [LEFT] JOIN b ON a.k = b.k AND a.t >= b.t`, marked for `asof::as_of`, which
/// takes the inequality for Snowflake's MATCH_CONDITION (and keeps a row with no match only for
/// LEFT, as DuckDB does).
fn asof_on(t: &Tokens) -> Result<Vec<(usize, usize, String)>> {
    let mut edits = Vec::new();
    for i in (0..t.toks.len()).filter(|&i| t.word(i, Keyword::ASOF)) {
        let mut j = t.next(i);
        let left = t.word(j, Keyword::LEFT);
        if left {
            j = t.next(j);
            if t.word(j, Keyword::OUTER) {
                j = t.next(j);
            }
        }
        if !t.word(j, Keyword::JOIN) {
            continue;
        }
        let mut depth = 0;
        let on = (j + 1..t.toks.len()).find(|&k| {
            match &t.toks[k] {
                Token::LParen => depth += 1,
                Token::RParen => depth -= 1,
                Token::Word(_) if depth == 0 => return t.word(k, Keyword::ON) || t.word(k, Keyword::MATCH_CONDITION) || t.word(k, Keyword::USING),
                _ => {}
            }
            false
        });
        let Some(on) = on.filter(|&k| t.word(k, Keyword::ON)) else { continue };
        if left {
            edits.push((t.end(i), t.start[j], " ".into()));
        }
        edits.push((t.start[on], t.start[on], format!("MATCH_CONDITION ({}) ", if left { "pondra_duckdb_asof_left" } else { "pondra_duckdb_asof" })));
    }
    Ok(edits)
}

/// DuckDB's `FROM … USING SAMPLE 10%` (or `10 ROWS`) as a marker in the WHERE that `sample`
/// takes out again: `WHERE pondra_sample('10%') [AND …]`.
fn using_sample(t: &Tokens) -> Result<Vec<(usize, usize, String)>> {
    let mut edits = Vec::new();
    for i in (0..t.toks.len()).filter(|&i| t.word(i, Keyword::USING) && t.word(t.next(i), Keyword::SAMPLE)) {
        let from = t.next(t.next(i));
        let mut depth = 0;
        let ends = [Keyword::WHERE, Keyword::GROUP, Keyword::HAVING, Keyword::QUALIFY, Keyword::WINDOW, Keyword::ORDER, Keyword::LIMIT, Keyword::OFFSET, Keyword::FETCH, Keyword::UNION, Keyword::EXCEPT, Keyword::INTERSECT];
        let end = (from..t.toks.len()).find(|&k| {
            match &t.toks[k] {
                Token::LParen => depth += 1,
                Token::RParen if depth == 0 => return true,
                Token::RParen => depth -= 1,
                Token::SemiColon | Token::EOF => return true,
                _ if depth == 0 => return ends.iter().any(|&e| t.word(k, e)),
                _ => {}
            }
            false
        });
        let end = end.unwrap_or(t.toks.len());
        let spec = quote(t.span(from, end).trim());
        match t.word(end, Keyword::WHERE) {
            true => edits.push((t.start[i], t.start.get(t.next(end)).copied().unwrap_or(t.sql.len()), format!("WHERE pondra_sample({spec}) AND "))),
            false => edits.push((t.start[i], t.start.get(end).copied().unwrap_or(t.sql.len()), format!("WHERE pondra_sample({spec}) "))),
        }
    }
    Ok(edits)
}

/// Postgres's weaker row locks, `FOR NO KEY UPDATE` and `FOR KEY SHARE`, as the `FOR UPDATE` and `FOR SHARE`
/// the parser reads: they take the same locks here (ADR-067 §3).
fn lock_words(t: &Tokens) -> Result<Vec<(usize, usize, String)>> {
    let mut edits = Vec::new();
    for i in (0..t.toks.len()).filter(|&i| t.word(i, Keyword::FOR)) {
        let (mut at, mut ids, mut words) = (i, Vec::new(), Vec::new());
        for _ in 0..3 {
            let j = t.next(at);
            let Some(Token::Word(w)) = t.toks.get(j) else { break };
            words.push(w.value.to_uppercase());
            ids.push(j);
            at = j;
        }
        let words: Vec<&str> = words.iter().map(String::as_str).collect();
        let (len, with) = if words.starts_with(&["NO", "KEY", "UPDATE"]) {
            (3, "FOR UPDATE")
        } else if words.starts_with(&["KEY", "SHARE"]) {
            (2, "FOR SHARE")
        } else {
            continue;
        };
        edits.push((t.start[i], t.end(ids[len - 1]), with.to_string()));
    }
    Ok(edits)
}

/// SQL Server's system variables: `@@version` (any case) at a code position (a word the tokens read: not in a string or
/// a comment) is `version()`. Any other `@@x` is refused by name, with the way Pondra gives a setting; a call `@@x(…)` is
/// a function's name and is left as it is.
fn at_at_words(t: &Tokens) -> Result<Vec<(usize, usize, String)>> {
    let mut edits = Vec::new();
    for i in 0..t.toks.len() {
        let Token::Word(w) = &t.toks[i] else { continue };
        let Some(name) = w.value.strip_prefix("@@") else { continue };
        if w.quote_style.is_some() || t.toks.get(t.next(i)) == Some(&Token::LParen) {
            continue;
        }
        if name.eq_ignore_ascii_case("version") {
            edits.push((t.start[i], t.end(i), "version()".to_string()));
        } else {
            let lower = name.to_lowercase();
            return Err(crate::codes::coded("0A000", format!("@@{name} is SQL Server's: Pondra's settings are SHOW {lower} / current_setting('{lower}')")));
        }
    }
    Ok(edits)
}

/// Spark's `TBLPROPERTIES (write.delete.mode = 'merge-on-read')`: a dotted key written bare, which the
/// parser reads only quoted, quoted (`'write.delete.mode' = …`), so the property is refused or kept by
/// its name as the quoted one is, never as a statement that doesn't parse.
fn property_keys(t: &Tokens) -> Result<Vec<(usize, usize, String)>> {
    let mut edits = Vec::new();
    for i in (0..t.toks.len()).filter(|&i| t.word(i, Keyword::TBLPROPERTIES)) {
        let mut at = t.next(i);
        if t.toks.get(at) != Some(&Token::LParen) {
            continue;
        }
        loop {
            // (one entry: `word(.word)* =`, then its value up to a comma or the closing parenthesis)
            let first = t.next(at);
            let mut last = first;
            let mut dotted = false;
            while matches!(t.toks.get(last), Some(Token::Word(w)) if w.quote_style.is_none()) && t.toks.get(last + 1) == Some(&Token::Period) {
                dotted = true;
                last += 2;
            }
            if dotted && matches!(t.toks.get(last), Some(Token::Word(w)) if w.quote_style.is_none()) && t.toks.get(t.next(last)) == Some(&Token::Eq) {
                edits.push((t.start[first], t.end(last), format!("'{}'", t.span(first, last + 1))));
            }
            let mut depth = 0;
            at = last;
            while let Some(tok) = t.toks.get(at) {
                match tok {
                    Token::LParen => depth += 1,
                    Token::RParen if depth == 0 => break,
                    Token::RParen => depth -= 1,
                    Token::Comma if depth == 0 => break,
                    _ => {}
                }
                at += 1;
            }
            if t.toks.get(at) != Some(&Token::Comma) {
                break;
            }
        }
    }
    Ok(edits)
}

/// SQL Server's `TOP` where it is a name (`top.amount`, `) top ON true`, `AS top`, `SELECT top FROM t`): quoted, so the
/// parser reads an alias, a column or a table. After SELECT, DISTINCT, ALL, UPDATE, DELETE or INSERT, a `TOP` before a
/// number or a parenthesis is SQL Server's own (`SELECT TOP 3`, `TOP (3)`, `TOP 10 PERCENT`), and is left as it is.
fn top_alias(t: &Tokens) -> Result<Vec<(usize, usize, String)>> {
    let mut edits = Vec::new();
    for i in 0..t.toks.len() {
        let Token::Word(w) = &t.toks[i] else { continue };
        if w.quote_style.is_some() || !w.value.eq_ignore_ascii_case("top") {
            continue;
        }
        let before = (0..i).rev().find(|&j| !matches!(t.toks[j], Token::Whitespace(_)));
        let after = t.toks.get(t.next(i));
        let after_select = before.is_some_and(|j| {
            matches!(&t.toks[j], Token::Word(p) if p.quote_style.is_none() && matches!(p.keyword, Keyword::SELECT | Keyword::DISTINCT | Keyword::ALL | Keyword::UPDATE | Keyword::DELETE | Keyword::INSERT))
        });
        let sql_server = after_select && matches!(after, Some(Token::Number(..)) | Some(Token::LParen));
        if !sql_server {
            edits.push((t.start[i], t.end(i), "\"top\"".to_string()));
        }
    }
    Ok(edits)
}

/// DuckDB's `PIVOT t ON g [IN (…)] [USING agg] [GROUP BY …]` and `UNPIVOT t ON a, b [INTO NAME n
/// VALUE v]` statements, as the standard's `SELECT * FROM t PIVOT (…)` and `UNPIVOT (…)`.
fn statements(sql: &str) -> Result<Option<String>> {
    let Some(t) = Tokens::of(sql) else { return Ok(None) };
    let first = (0..t.toks.len()).find(|&i| !matches!(t.toks[i], Token::Whitespace(_))).unwrap_or(t.toks.len());
    let pivot = t.word(first, Keyword::PIVOT);
    if !pivot && !t.word(first, Keyword::UNPIVOT) {
        return Ok(None);
    }
    // the statement's clauses, each where its first word is at the top level
    let words: &[&[Keyword]] = match pivot {
        true => &[&[Keyword::ON], &[Keyword::USING], &[Keyword::GROUP, Keyword::BY], &[Keyword::ORDER, Keyword::BY], &[Keyword::LIMIT]],
        false => &[&[Keyword::ON], &[Keyword::INTO], &[Keyword::ORDER, Keyword::BY], &[Keyword::LIMIT]],
    };
    let mut found: Vec<Option<(usize, usize)>> = vec![None; words.len()];
    let (mut depth, mut k) = (0i32, t.next(first));
    while k < t.toks.len() {
        match &t.toks[k] {
            Token::LParen => depth += 1,
            Token::RParen => depth -= 1,
            _ if depth == 0 => {
                for (w, ws) in words.iter().enumerate() {
                    let mut at = k;
                    let whole = ws.iter().enumerate().all(|(n, &kw)| {
                        if n > 0 {
                            at = t.next(at);
                        }
                        t.word(at, kw)
                    });
                    if whole && found[w].is_none() && found[w..].iter().all(Option::is_none) {
                        found[w] = Some((k, t.next(at)));
                    }
                }
            }
            _ => {}
        }
        k += 1;
    }
    let ends: Vec<usize> = (0..words.len()).map(|w| found[w + 1..].iter().flatten().map(|f| f.0).next().unwrap_or(t.toks.len())).collect();
    let part = |w: usize| found[w].map(|(_, from)| t.span(from, ends[w]).trim().trim_end_matches(';').trim().to_string());
    let Some((on_at, _)) = found[0] else { bail!("{}: ON names the column ({})", if pivot { "PIVOT" } else { "UNPIVOT" }, if pivot { "PIVOT orders ON item USING sum(amount)" } else { "UNPIVOT orders ON qty, amount INTO NAME field VALUE value" }) };
    let rel = t.span(t.next(first), on_at).trim().to_string();
    let tail = |from: usize| (from..words.len()).filter_map(|w| found[w].map(|(at, _)| t.span(at, ends[w]).trim().trim_end_matches(';').trim().to_string())).collect::<Vec<_>>().join(" ");
    if !pivot {
        let on = part(0).unwrap_or_default();
        let (mut n, mut v) = ("name".to_string(), "value".to_string());
        if let Some(into) = part(1) {
            static INTO: LazyLock<Regex> = LazyLock::new(|| Regex::new(r#"(?is)^name\s+("[^"]+"|\w+)\s+value\s+("[^"]+"|\w+)$"#).expect("a regex"));
            let m = INTO.captures(&into).context("UNPIVOT … INTO NAME n VALUE v")?;
            (n, v) = (m[1].to_string(), m[2].to_string());
        }
        return Ok(Some(format!("SELECT * FROM {rel} UNPIVOT ({v} FOR {n} IN ({on})) {}", tail(2))));
    }
    let on = expr(&part(0).unwrap_or_default()).context("PIVOT … ON a column")?;
    let (col, values) = match on {
        Expr::InList { expr, list, negated: false } => (*expr, list.iter().map(|e| e.to_string()).collect::<Vec<_>>().join(", ")),
        Expr::Identifier(_) | Expr::CompoundIdentifier(_) => (on, "ANY".to_string()),
        _ => bail!("PIVOT … ON one column (and IN its values); a pivot on several columns isn't taken yet"),
    };
    let using = part(1).unwrap_or_else(|| "count(*)".into());
    let source = match part(2) {
        Some(group) => {
            let aggs = query(&format!("SELECT {using}"))?;
            let mut cols: Vec<String> = Vec::new();
            for e in group.split(',').map(str::trim).filter(|g| !g.is_empty()).map(String::from).chain([col.to_string()]) {
                if !cols.contains(&e) {
                    cols.push(e);
                }
            }
            let _ = crate::walk::visit_expressions(&aggs, |e| {
                if let Expr::Identifier(_) | Expr::CompoundIdentifier(_) = e {
                    let e = e.to_string();
                    if !cols.contains(&e) {
                        cols.push(e);
                    }
                }
                ControlFlow::<()>::Continue(())
            });
            format!("(SELECT {} FROM {rel}) AS __pivot", cols.join(", "))
        }
        None => rel,
    };
    Ok(Some(format!("SELECT * FROM {source} PIVOT ({using} FOR {col} IN ({values})) {}", tail(3))))
}

// ---------------------------------------------------------------- the tree

/// What a rewrite asks the data: a query's distinct values (a PIVOT's), or a FROM's columns.
enum Ask {
    Values(String),
    Columns(String),
}

#[derive(Clone, Debug)]
struct Column {
    table: Option<String>,
    name: String,
    numeric: bool,
    sql_type: String,
}

enum Asks {
    Collect(Vec<Ask>),
    Apply(VecDeque<Vec<Column>>),
}

struct Friendly {
    asks: Asks,
    with: Vec<Level>,
    orders: Vec<Option<usize>>,
    selects: Vec<Option<Vec<Column>>>,
    names: Vec<Vec<Option<String>>>,
}

/// A query being visited: its WITH, which of its parent's CTEs it defines (if it is one), and how
/// many of its own CTEs have been entered (they are visited first, in order).
struct Level {
    with: Option<With>,
    defines: Option<usize>,
    entered: usize,
}

/// `stmts` with every friendly form in them rewritten, asking the host what they need to know;
/// whether any was.
pub async fn rewrite<H: crate::front::Host>(host: &H, stmts: &mut [Statement]) -> Result<bool> {
    let before = stmts.to_vec();
    for s in stmts.iter_mut() {
        default_values(host, s).await?;
    }
    // (dbt's `AS (SELECT …)`: a materialized view's query is read as `first_table` reads it, a SELECT
    // of its own; done here, not in the visitor, which doesn't reach a view's query)
    for s in stmts.iter_mut() {
        if let Statement::CreateView(CreateView { materialized: true, query, .. }) = s {
            unparen(query);
        }
        unalias(s)?; // (`DELETE FROM t AS d`: `d.col` is `t.col` in the rows' query, which names the table)
    }
    let mut f = Friendly { asks: Asks::Collect(Vec::new()), with: Vec::new(), orders: Vec::new(), selects: Vec::new(), names: Vec::new() };
    // (a stored view keeps its text: it is expanded as it is read, as its functions are)
    let kept = |s: &Statement| matches!(s, Statement::CreateMacro { .. } | Statement::CreateView(CreateView { materialized: false, .. }));
    for s in stmts.iter_mut().filter(|s| !kept(s)) {
        if let ControlFlow::Break(e) = crate::walk::visit_mut(s, &mut f) {
            return Err(e);
        }
    }
    let Asks::Collect(asks) = std::mem::replace(&mut f.asks, Asks::Apply(VecDeque::new())) else { unreachable!() };
    let mut answers = VecDeque::new();
    for a in asks {
        answers.push_back(Box::pin(answer(host, a)).await?); // (its query is expanded too)
    }
    f.asks = Asks::Apply(answers);
    for s in stmts.iter_mut().filter(|s| !kept(s)) {
        if let ControlFlow::Break(e) = crate::walk::visit_mut(s, &mut f) {
            return Err(e);
        }
    }
    // (after the loop above: TOP and FETCH FIRST are LIMIT by now, and `*` and ORDER BY ALL are expanded)
    for s in stmts.iter_mut().filter(|s| !kept(s)) {
        crate::lateral_join::rewrite(s)?;
    }
    Ok(before != stmts) // (a LATERAL join rewritten changes the statement, so it counts here too)
}

tokio::task_local! {
    /// How many asks this one is inside (an ask's query is expanded, and may ask in turn).
    static ASKING: usize;
}

/// The answer to an ask, from a query run as the caller (itself expanded: it may hold friendly
/// SQL too). Asks inside asks stop 8 deep: a probe that asked itself again overflowed the stack
/// and took the node down.
async fn answer<H: crate::front::Host>(host: &H, ask: Ask) -> Result<Vec<Column>> {
    let depth = ASKING.try_with(|d| *d).unwrap_or(0);
    ensure!(depth < 8, "a query that needs its own columns to know its own columns (asked 8 deep)");
    ASKING.scope(depth + 1, answered(host, ask)).await
}

async fn answered<H: crate::front::Host>(host: &H, ask: Ask) -> Result<Vec<Column>> {
    let (sql, values) = match ask {
        Ask::Values(q) => (q, true),
        Ask::Columns(q) => (q, false),
    };
    let df = host.ask(&sql).await?;
    if !values {
        return Ok(df.schema().iter().map(|(q, f)| Column { table: q.map(|q| q.table().to_string()), name: f.name().clone(), numeric: f.data_type().is_numeric(), sql_type: sql_type(f.data_type()) }).collect());
    }
    let mut out = Vec::new();
    for b in host.collect(df).await? {
        let col = datafusion::arrow::compute::cast(b.column(0), &datafusion::arrow::datatypes::DataType::Utf8)?;
        let col = col.as_any().downcast_ref::<datafusion::arrow::array::StringArray>().context("a pivot's values as text")?;
        out.extend(col.iter().flatten().map(|v| Column { table: None, name: v.to_string(), numeric: false, sql_type: String::new() }));
        ensure!(out.len() <= 1000, "PIVOT: more than 1,000 values; name them with IN (…)");
    }
    Ok(out)
}

/// A type as SQL names it.
fn sql_type(t: &datafusion::arrow::datatypes::DataType) -> String {
    use datafusion::arrow::datatypes::DataType as D;
    match t {
        D::Int8 => "TINYINT".into(),
        D::Int16 => "SMALLINT".into(),
        D::Int32 => "INTEGER".into(),
        D::Int64 => "BIGINT".into(),
        D::UInt8 => "UTINYINT".into(),
        D::UInt16 => "USMALLINT".into(),
        D::UInt32 => "UINTEGER".into(),
        D::UInt64 => "UBIGINT".into(),
        D::Float32 => "FLOAT".into(),
        D::Float64 => "DOUBLE".into(),
        D::Utf8 | D::LargeUtf8 | D::Utf8View => "VARCHAR".into(),
        D::Boolean => "BOOLEAN".into(),
        D::Date32 | D::Date64 => "DATE".into(),
        D::Timestamp(_, None) => "TIMESTAMP".into(),
        D::Timestamp(_, Some(_)) => "TIMESTAMP WITH TIME ZONE".into(),
        D::Decimal128(p, s) | D::Decimal256(p, s) => format!("DECIMAL({p},{s})"),
        D::Binary | D::LargeBinary | D::BinaryView => "BLOB".into(),
        D::List(f) | D::LargeList(f) | D::ListView(f) => format!("{}[]", sql_type(f.data_type())),
        t => t.to_string(),
    }
}

impl Friendly {
    fn collecting(&self) -> bool { matches!(self.asks, Asks::Collect(_)) }
    /// Asked in the first pass, answered in the second.
    fn ask(&mut self, a: impl FnOnce() -> Ask) -> Option<Vec<Column>> {
        match &mut self.asks {
            Asks::Collect(v) => {
                v.push(a());
                None
            }
            Asks::Apply(q) => q.pop_front(),
        }
    }
    /// A query over `from`, under every WITH it is inside: inside a CTE, only the CTEs before it,
    /// and itself as its first term when the WITH is recursive (its columns are that term's). Its
    /// own definition in a probe would ask the same probe again, without end.
    fn over(&self, sql: String) -> String {
        let mut ctes = vec![];
        for (k, level) in self.with.iter().enumerate() {
            let Some(w) = &level.with else { continue };
            match self.with.get(k + 1).and_then(|l| l.defines) {
                None => ctes.extend(w.cte_tables.iter().map(|c| c.to_string())),
                Some(i) => {
                    ctes.extend(w.cte_tables[..i].iter().map(|c| c.to_string()));
                    if w.recursive {
                        let mut first = &*w.cte_tables[i].query.body;
                        while let SetExpr::SetOperation { left, .. } = first {
                            first = left;
                        }
                        ctes.push(format!("{} AS ({first})", w.cte_tables[i].alias));
                    }
                }
            }
        }
        let recursive = self.with.iter().filter_map(|l| l.with.as_ref()).any(|w| w.recursive);
        match ctes.is_empty() {
            true => sql,
            false => format!("WITH {}{} {sql}", if recursive { "RECURSIVE " } else { "" }, ctes.join(", ")),
        }
    }
}

impl VisitorMut for Friendly {
    type Break = anyhow::Error;

    fn pre_visit_query(&mut self, q: &mut Query) -> ControlFlow<anyhow::Error> {
        let defines = self.with.last_mut().and_then(|l| {
            let w = l.with.as_ref()?;
            let i = l.entered;
            (i < w.cte_tables.len() && *w.cte_tables[i].query == *q).then(|| {
                l.entered += 1;
                i
            })
        });
        self.with.push(Level { with: q.with.clone(), defines, entered: 0 });
        // ORDER BY ALL over `*`: how many columns the answer has
        let n = match all_of(q) == Some(true) {
            true => {
                let probe = self.over(format!("SELECT * FROM ({}) AS __all LIMIT 0", q.body));
                self.ask(|| Ask::Columns(probe)).map(|c| c.len())
            }
            false => None,
        };
        self.orders.push(n);
        ControlFlow::Continue(())
    }

    fn post_visit_query(&mut self, q: &mut Query) -> ControlFlow<anyhow::Error> {
        self.with.pop();
        let n = self.orders.pop().flatten();
        if self.collecting() {
            return ControlFlow::Continue(());
        }
        or_break(top(q).and_then(|_| fetch(q)).and_then(|_| order_by_all(q, n)))
    }

    fn pre_visit_select(&mut self, s: &mut Select) -> ControlFlow<anyhow::Error> {
        self.names.push(s.projection.iter().map(|i| match i {
            SelectItem::UnnamedExpr(e) => Some(e.to_string()),
            _ => None,
        }).collect());
        let wanted = renames(s) || has_columns(s) || !aliases(s).is_empty();
        let from = s.from.iter().map(|t| t.to_string()).collect::<Vec<_>>().join(", ");
        let cols = match wanted && !from.is_empty() {
            true => {
                let probe = self.over(format!("SELECT * FROM {from} LIMIT 0"));
                self.ask(|| Ask::Columns(probe)).or_else(|| (!self.collecting()).then(Vec::new))
            }
            false => wanted.then(Vec::new),
        };
        self.selects.push(cols);
        ControlFlow::Continue(())
    }

    fn post_visit_select(&mut self, s: &mut Select) -> ControlFlow<anyhow::Error> {
        let (cols, names) = (self.selects.pop().flatten(), self.names.pop().unwrap_or_default());
        if self.collecting() {
            return ControlFlow::Continue(());
        }
        // a column of the answer keeps the name it was written as
        for (item, was) in s.projection.iter_mut().zip(names) {
            if let (SelectItem::UnnamedExpr(e), Some(was)) = (&*item, was) {
                if e.to_string() != was {
                    *item = SelectItem::ExprWithAlias { expr: e.clone(), alias: Ident::with_quote('"', was) };
                }
            }
        }
        or_break((|| {
            if let Some(cols) = cols {
                with_aliases(s, &cols)?;
                expand_columns(s, &cols)?;
                rename(s, &cols)?;
            }
            sample(s)
        })())
    }

    fn post_visit_table_factor(&mut self, t: &mut TableFactor) -> ControlFlow<anyhow::Error> {
        match t {
            TableFactor::Pivot { value_source, value_column, table, .. } => {
                let dynamic = match value_source {
                    PivotValueSource::List(_) => None,
                    PivotValueSource::Any(order) => {
                        let col = value_column.iter().map(|c| c.to_string()).collect::<Vec<_>>().join(", ");
                        let order = match order.is_empty() {
                            true => "1".to_string(),
                            false => order.iter().map(|o| o.to_string()).collect::<Vec<_>>().join(", "),
                        };
                        Some(self.over(format!("SELECT DISTINCT {col} FROM {table} ORDER BY {order}")))
                    }
                    PivotValueSource::Subquery(q) => Some(self.over(q.to_string())),
                };
                let values = match dynamic {
                    Some(q) => self.ask(|| Ask::Values(q)),
                    None => None,
                };
                if self.collecting() {
                    return ControlFlow::Continue(());
                }
                or_break(pivot(t, values).map(|p| *t = p))
            }
            TableFactor::Unpivot { columns, table, .. } => {
                let probe = columns.iter().any(|c| columns_call(&c.expr).is_some()).then(|| self.over(format!("SELECT * FROM {table} LIMIT 0")));
                let cols = probe.and_then(|p| self.ask(|| Ask::Columns(p)));
                if self.collecting() {
                    return ControlFlow::Continue(());
                }
                or_break(unpivot(t, cols).map(|u| *t = u))
            }
            TableFactor::Table { sample: Some(_), .. } if !self.collecting() => or_break(table_sample(t)),
            _ => ControlFlow::Continue(()),
        }
    }

    fn post_visit_expr(&mut self, e: &mut Expr) -> ControlFlow<anyhow::Error> {
        if self.collecting() {
            return ControlFlow::Continue(());
        }
        or_break(expression(e))
    }

    fn post_visit_statement(&mut self, s: &mut Statement) -> ControlFlow<anyhow::Error> {
        if self.collecting() {
            return ControlFlow::Continue(());
        }
        arrays(s); // (after the select's COLUMNS are expanded: `COLUMNS([…])` reads its list as written)
        match s {
            Statement::CreateTable(c) => {
                for col in c.columns.iter_mut() {
                    // (a JSON column keeps its type: `pg_casts::rewrite` holds it as text, checked)
                    if let Err(e) = spell(&mut col.data_type) {
                        return ControlFlow::Break(e);
                    }
                }
            }
            Statement::AlterTable(a) => {
                for op in a.operations.iter_mut() {
                    let spelled = match op {
                        AlterTableOperation::AddColumn { column_def, .. } => spell(&mut column_def.data_type),
                        AlterTableOperation::AlterColumn { op: AlterColumnOperation::SetDataType { data_type, .. }, .. } => spell(data_type),
                        _ => Ok(()),
                    };
                    if let Err(e) = spelled {
                        return ControlFlow::Break(e);
                    }
                }
            }
            _ => {}
        }
        ControlFlow::Continue(())
    }
}

fn or_break(r: Result<()>) -> ControlFlow<anyhow::Error> { r.map_or_else(ControlFlow::Break, ControlFlow::Continue) }

/// `(SELECT …)` as a query is the query itself: Postgres takes both, and a view's query must be a
/// SELECT. Only a query with nothing of its own around the parentheses (WITH, ORDER BY, LIMIT,
/// OFFSET, FETCH, locks) is taken out of them; repeated while it is still one.
fn unparen(q: &mut Query) {
    while let SetExpr::Query(inner) = q.body.as_ref() {
        if q.with.is_some() || q.order_by.is_some() || q.limit_clause.is_some() || q.fetch.is_some() || !q.locks.is_empty() {
            return;
        }
        *q = (**inner).clone();
    }
}

/// `DELETE FROM t AS d …` and `UPDATE t AS u …`: the target's alias comes off, and the references to
/// it are written as the table's own name, which is what change.rs builds its query over (`d.id` had
/// no `d` there, so a correlated EXISTS failed). Refused when another relation of the statement is
/// visibly named the alias or the table: the rename would be caught by it.
fn unalias(s: &mut Statement) -> Result<()> {
    let target = match s {
        Statement::Delete(d) => match &mut d.from {
            FromTable::WithFromKeyword(t) | FromTable::WithoutKeyword(t) => t.first_mut(),
        },
        Statement::Update(u) => Some(&mut u.table),
        _ => None,
    };
    let Some(TableWithJoins { relation: TableFactor::Table { name, alias, .. }, .. }) = target else { return Ok(()) };
    let Some(alias) = alias.take() else { return Ok(()) };
    let table = name.0.last().and_then(|p| p.as_ident()).map(|i| i.value.clone()).unwrap_or_default();
    let alias = alias.name.value;
    if alias.eq_ignore_ascii_case(&table) {
        return Ok(());
    }
    // (the target itself is not visited: its name is the table's now)
    let mut r = Renamed { alias: alias.clone(), table: table.clone(), shadow: None };
    match s {
        Statement::Delete(d) => {
            let _ = crate::walk::visit_mut(&mut d.selection, &mut r);
            let _ = crate::walk::visit_mut(&mut d.using, &mut r);
            returning(&mut d.returning, &mut r);
        }
        Statement::Update(u) => {
            let _ = crate::walk::visit_mut(&mut u.assignments, &mut r);
            let _ = crate::walk::visit_mut(&mut u.from, &mut r);
            let _ = crate::walk::visit_mut(&mut u.selection, &mut r);
            returning(&mut u.returning, &mut r);
        }
        _ => {}
    }
    match r.shadow {
        Some(n) => Err(crate::codes::coded("0A000", format!("{alias} is the alias of {table} here, and another relation of this statement is named {n}: give that one another name"))),
        None => Ok(()),
    }
}

/// A RETURNING list's references to the alias, `t.*` too, written as the table's name (`returning.rs` reads them so).
fn returning(items: &mut Option<Vec<SelectItem>>, r: &mut Renamed) {
    let _ = crate::walk::visit_mut(items, r);
    for item in items.iter_mut().flatten() {
        if let SelectItem::QualifiedWildcard(SelectItemQualifiedWildcardKind::ObjectName(n), _) = item {
            if n.0.len() == 1 && n.0[0].as_ident().is_some_and(|i| i.value.eq_ignore_ascii_case(&r.alias)) {
                *n = ObjectName::from(vec![Ident::new(&r.table)]);
            }
        }
    }
}

/// Renames one alias's references to a table's name, and notes a relation of the statement that is
/// visibly named the alias or the table (the rename would be caught by it).
struct Renamed {
    alias: String,
    table: String,
    shadow: Option<String>,
}

impl VisitorMut for Renamed {
    type Break = ();

    fn pre_visit_table_factor(&mut self, t: &mut TableFactor) -> ControlFlow<()> {
        match visible(t) {
            Some(n) if n.eq_ignore_ascii_case(&self.alias) || n.eq_ignore_ascii_case(&self.table) => {
                self.shadow = Some(n);
                ControlFlow::Break(())
            }
            _ => ControlFlow::Continue(()),
        }
    }

    fn post_visit_expr(&mut self, e: &mut Expr) -> ControlFlow<()> {
        if let Expr::CompoundIdentifier(parts) = e {
            if parts.len() == 2 && parts[0].value.eq_ignore_ascii_case(&self.alias) {
                parts[0] = Ident::new(&self.table);
            }
        }
        ControlFlow::Continue(())
    }
}

/// The name a relation is visible by in its statement: its alias, else its table's last name part.
fn visible(t: &TableFactor) -> Option<String> {
    match t {
        TableFactor::Table { alias: Some(a), .. } | TableFactor::Derived { alias: Some(a), .. } => Some(a.name.value.clone()),
        TableFactor::Table { name, .. } => name.0.last().and_then(|p| p.as_ident()).map(|i| i.value.clone()),
        _ => None,
    }
}

/// `(a, b) IN (SELECT …)` as `EXISTS (SELECT 1 FROM (SELECT …) AS __in(__c1, __c2) WHERE __in.__c1 =
/// a AND __in.__c2 = b)`. A NULL comparison drops the row in a WHERE, as the IN's NULL did; elsewhere
/// the EXISTS is false where the IN would be NULL. NOT IN is refused: a NULL in the subquery changes
/// its answer, which NOT EXISTS doesn't keep.
fn row_in(e: &Expr) -> Result<Option<String>> {
    let Expr::InSubquery { expr: left, subquery, negated } = e else { return Ok(None) };
    let Expr::Tuple(items) = left.as_ref() else { return Ok(None) };
    if *negated {
        return Err(crate::codes::coded("0A000", format!("({}) NOT IN (subquery): a NULL in the subquery changes NOT IN's answer, and NOT EXISTS doesn't keep that: write NOT EXISTS (SELECT 1 … WHERE …) with what a NULL means for you", items.iter().map(|i| i.to_string()).collect::<Vec<_>>().join(", "))));
    }
    let cols: Vec<String> = (1..=items.len()).map(|i| format!("__c{i}")).collect();
    let same: Vec<String> = items.iter().zip(&cols).map(|(a, c)| format!("__in.{c} = {a}")).collect();
    Ok(Some(format!("EXISTS (SELECT 1 FROM ({subquery}) AS __in({}) WHERE {})", cols.join(", "), same.join(" AND "))))
}

/// DuckDB's names for types Pondra has under another name (each DuckDB's own alias: DATETIME is TIMESTAMP, …), by their
/// lower-cased name as sqlparser leaves it (a type it doesn't know is `Custom`).
const DUCKDB_TYPES: &[(&str, &str)] = &[("datetime", "TIMESTAMP"), ("bpchar", "VARCHAR"), ("logical", "BOOLEAN"), ("int1", "TINYINT"), ("short", "SMALLINT"),
    ("long", "BIGINT"), ("utinyint", "TINYINT UNSIGNED"), ("usmallint", "SMALLINT UNSIGNED"), ("uinteger", "INT UNSIGNED"), ("ubigint", "BIGINT UNSIGNED"),
    ("timestamp_s", "TIMESTAMP(0)"), ("timestamp_ms", "TIMESTAMP(3)"), ("timestamp_ns", "TIMESTAMP(9)"), ("blob", "BYTEA"),
    ("varbinary", "BYTEA"), ("binary", "BYTEA")];

/// Types Pondra doesn't have, refused by name (0A000) with what to write: (name, whose, what Pondra has).
const OTHER_TYPES: &[(&str, &str, &str)] = &[("hugeint", "DuckDB's 128-bit integer", "DECIMAL(38, 0)"), ("uhugeint", "DuckDB's 128-bit integer", "DECIMAL(38, 0)"),
    ("varint", "DuckDB's integer of any size", "DECIMAL(38, 0)"), ("bit", "a bit string in Postgres and DuckDB, and SQL Server's boolean", "BOOLEAN, or BYTEA for bits"),
    ("bitstring", "DuckDB's bit string", "BYTEA"), ("varbit", "Postgres's bit string", "BYTEA"), ("timetz", "a time of day with a zone", "TIME, or TIMESTAMPTZ for an instant"),
    ("datetime2", "SQL Server's", "TIMESTAMP"), ("smalldatetime", "SQL Server's", "TIMESTAMP"), ("datetimeoffset", "SQL Server's", "TIMESTAMPTZ"),
    ("uniqueidentifier", "SQL Server's", "UUID"), ("money", "SQL Server's and Postgres's", "DECIMAL(19, 4)"), ("smallmoney", "SQL Server's", "DECIMAL(10, 4)"),
    ("tinytext", "MySQL's", "TEXT"), ("mediumtext", "MySQL's", "TEXT"), ("longtext", "MySQL's", "TEXT"), ("tinyblob", "MySQL's", "BYTEA"),
    ("mediumblob", "MySQL's", "BYTEA"), ("longblob", "MySQL's", "BYTEA"), ("year", "MySQL's", "SMALLINT")];

/// Postgres's names for types DataFusion doesn't read as written, as the names it reads (and a few of BigQuery's and
/// Snowflake's, and DuckDB's own aliases, `DUCKDB_TYPES`); a type Pondra doesn't have is refused by name (`OTHER_TYPES`).
fn spell(t: &mut DataType) -> Result<()> {
    let named = |name: &str| -> Result<Option<DataType>> {
        let lower = name.to_lowercase();
        if let Some((_, ours)) = DUCKDB_TYPES.iter().find(|(n, _)| *n == lower) {
            return Ok(Some(datatype(ours)?));
        }
        if let Some((n, whose, ours)) = OTHER_TYPES.iter().find(|(n, _, _)| *n == lower) {
            return Err(crate::codes::coded("0A000", format!("{} is {whose}: write {ours}", n.to_uppercase())));
        }
        Ok(None)
    };
    let spelled = match t {
        DataType::CharacterVarying(n) | DataType::CharVarying(n) => Some(DataType::Varchar(n.take())),
        DataType::Character(n) => Some(DataType::Char(n.take())),
        // (BigQuery's INT64 and FLOAT64, Snowflake's NUMBER(p, s) (38, 0 when not given) and BYTEINT)
        DataType::Int64 => Some(DataType::BigInt(None)),
        DataType::Float64 => Some(DataType::Double(ExactNumberInfo::None)),
        DataType::Custom(name, args) if name.to_string().eq_ignore_ascii_case("number") => {
            let at = |i: usize| args.get(i).and_then(|a| a.trim().parse::<u64>().ok());
            Some(DataType::Decimal(ExactNumberInfo::PrecisionAndScale(at(0).unwrap_or(38), at(1).unwrap_or(0) as i64)))
        }
        DataType::Custom(name, args) if args.is_empty() && name.to_string().eq_ignore_ascii_case("byteint") => Some(DataType::TinyInt(None)),
        // (DuckDB's names sqlparser reads as its own types, and the ones it leaves as written; Spark's and Snowflake's
        // TIMESTAMP_NTZ is TIMESTAMP too, nearly free and common in their DDL)
        DataType::Datetime(p) | DataType::TimestampNtz(p) => Some(DataType::Timestamp(*p, TimezoneInfo::None)),
        DataType::Blob(_) | DataType::Binary(_) | DataType::Varbinary(_) => Some(DataType::Bytea),
        DataType::UTinyInt => Some(DataType::TinyIntUnsigned(None)),
        DataType::USmallInt => Some(DataType::SmallIntUnsigned(None)),
        DataType::UBigInt => Some(DataType::BigIntUnsigned(None)),
        DataType::HugeInt => named("hugeint")?,
        DataType::UHugeInt => named("uhugeint")?,
        DataType::Bit(_) => named("bit")?,
        DataType::BitVarying(_) => named("varbit")?,
        DataType::Time(_, TimezoneInfo::WithTimeZone | TimezoneInfo::Tz) => named("timetz")?,
        DataType::Custom(name, args) if args.is_empty() => named(&name.to_string())?,
        _ => None,
    };
    if let Some(s) = spelled {
        *t = s;
    }
    Ok(())
}

/// A type as a routine's parameter or its RETURNS writes it (routines.rs keeps types as text), spelled as a column's
/// type is: DuckDB's DATETIME is TIMESTAMP, and a type Pondra doesn't have is refused by name. A type `spell` leaves
/// is kept as written.
pub(crate) fn spelled(t: &str) -> Result<String> {
    let Ok(mut ty) = datatype(t) else { return Ok(t.to_string()) };
    let was = ty.clone();
    spell(&mut ty)?;
    Ok(if ty == was { t.to_string() } else { ty.to_string() })
}

/// The type `sql` names (`TIMESTAMP(3)`, `INT UNSIGNED`), as sqlparser reads it.
fn datatype(sql: &str) -> Result<DataType> { Ok(Parser::new(&GenericDialect {}).try_with_sql(sql)?.parse_data_type()?) }

/// A string literal written the way its type reads it, where DataFusion doesn't read Postgres's
/// spelling: a timestamp's zone of one hour digit (`-0`, `+5`) gets two (`-00:00`, `+05:00`), since
/// Arrow reads `±HH`, `±HHMM` and `±HH:MM` only; a NUMERIC's exponent is written out (`1E-7` is
/// `0.0000001`); a bytea literal in either of Postgres's forms (`\xff00`, or `a\\b` with its escapes) is
/// its `X'…'` (`pg_bytea`). Some(text): the whole literal is `text` instead.
fn literal(t: &DataType, s: &mut String) -> Result<Option<String>> {
    match t {
        DataType::Timestamp(..) => {
            static ZONE: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"^(.*\d:\d{2}(?::\d{2}(?:\.\d+)?)?)([+-])(\d)$").expect("a regex"));
            let fixed = ZONE.captures(s).map(|c| format!("{}{}0{}:00", &c[1], &c[2], &c[3]));
            if let Some(f) = fixed {
                *s = f;
            }
        }
        DataType::Bytea => {
            return crate::pg_bytea::parse(s).map(|b| Some(crate::pg_bytea::sql(&b))).map_err(|e| crate::codes::coded(e.code, e.message));
        }
        _ if numeric(t) => {
            if let Some(plain) = plain_decimal(s)? {
                *s = plain;
            }
        }
        _ => {}
    }
    Ok(None)
}

/// NUMERIC or DECIMAL, with or without its precision and scale.
fn numeric(t: &DataType) -> bool { matches!(t, DataType::Numeric(_) | DataType::Decimal(_) | DataType::Dec(_)) }

/// A typed literal or cast in Postgres's spelling (`literal`, `spell`), fixed in place; Some(text)
/// when the whole expression is `text` instead.
fn typed_literal(e: &mut Expr) -> Result<Option<String>> {
    match e {
        Expr::Cast { data_type, expr: inner, .. } => {
            spell(data_type)?;
            match inner.as_mut() {
                Expr::Value(ValueWithSpan { value: Value::SingleQuotedString(s), .. }) => literal(data_type, s),
                // (`1E-7::numeric`: the number itself, as its plain digits)
                Expr::Value(ValueWithSpan { value: Value::Number(n, _), .. }) if numeric(data_type) => {
                    if let Some(plain) = plain_decimal(n)? {
                        *n = plain;
                    }
                    Ok(None)
                }
                _ => Ok(None),
            }
        }
        Expr::TypedString(TypedString { data_type, value, .. }) => {
            spell(data_type)?;
            let Value::SingleQuotedString(s) = &mut value.value else { return Ok(None) };
            literal(data_type, s)
        }
        _ => Ok(None),
    }
}

/// A number written with an exponent, as plain digits (`1E-7` is `0.0000001`, `-2.5e3` is `-2500`):
/// the digits move, no float, so it is exact. None when `s` isn't such a number. Past 10,000 digits
/// of shift it is refused (22003), as Postgres's own limit is far beyond what this would write out.
fn plain_decimal(s: &str) -> Result<Option<String>> {
    let t = s.trim();
    let (sign, body) = match t.strip_prefix('-') {
        Some(b) => ("-", b),
        None => ("", t.strip_prefix('+').unwrap_or(t)),
    };
    let Some((mant, exp)) = body.split_once(['e', 'E']) else { return Ok(None) };
    let Ok(exp) = exp.parse::<i64>() else { return Ok(None) };
    let (int, frac) = mant.split_once('.').unwrap_or((mant, ""));
    if (int.is_empty() && frac.is_empty()) || !int.bytes().chain(frac.bytes()).all(|b| b.is_ascii_digit()) {
        return Ok(None);
    }
    if exp.abs() > 10_000 {
        return Err(crate::codes::coded("22003", "numeric field overflow: an exponent past 10,000 digits"));
    }
    let digits = format!("{int}{frac}");
    let point = int.len() as i64 + exp; // (where the decimal point falls among `digits`)
    let out = if point <= 0 {
        format!("0.{}{digits}", "0".repeat((-point) as usize))
    } else if point as usize >= digits.len() {
        format!("{digits}{}", "0".repeat(point as usize - digits.len()))
    } else {
        format!("{}.{}", &digits[..point as usize], &digits[point as usize..])
    };
    // (no leading zeros left over from the digits' own: `0001E2` is 100)
    let trimmed = out.trim_start_matches('0');
    let out = if trimmed.is_empty() || trimmed.starts_with('.') { format!("0{trimmed}") } else { trimmed.to_string() };
    Ok(Some(format!("{sign}{out}")))
}

/// `x LIKE 'p' ESCAPE '/'` (ILIKE and NOT LIKE alike), rewritten to LIKE's own backslash escape: the
/// pattern's `/%` becomes `\%`, and the ESCAPE goes. A pattern of literals joined by `||` is one
/// literal (SQLAlchemy's `startswith` sends `$1::VARCHAR || '%'`, its value bound in place); any other
/// pattern can't be rewritten, so it is refused. ESCAPE '\' is already LIKE's; ESCAPE '' turns escapes off.
fn like_escape(pattern: &mut Box<Expr>, escape: &mut Option<ValueWithSpan>) -> Result<()> {
    let esc = match escape.as_ref().map(|e| &e.value) {
        Some(Value::SingleQuotedString(c)) if c.as_str() != "\\" => c.clone(),
        _ => return Ok(()),
    };
    let esc_char = match (esc.chars().next(), esc.chars().nth(1)) {
        (None, _) => None,
        (Some(c), None) => Some(c),
        _ => return Err(crate::codes::coded("22025", "invalid escape string: an ESCAPE is one character, or '' for none")),
    };
    if let Some(p) = written(pattern) {
        **pattern = Expr::Value(Value::SingleQuotedString(p).into());
    }
    let Expr::Value(ValueWithSpan { value: Value::SingleQuotedString(p), .. }) = pattern.as_mut() else {
        return Err(crate::codes::coded("0A000", "LIKE … ESCAPE '/' with a pattern that isn't written out: use \\ as the escape character"));
    };
    *p = backslash_pattern(p, esc_char)?;
    *escape = None;
    Ok(())
}

/// A pattern's text when it is string literals joined by `||`, each perhaps cast to a text type.
fn written(e: &Expr) -> Option<String> {
    match e {
        Expr::Value(ValueWithSpan { value: Value::SingleQuotedString(s) | Value::EscapedStringLiteral(s), .. }) => Some(s.clone()),
        Expr::BinaryOp { left, op: BinaryOperator::StringConcat, right } => Some(written(left)? + &written(right)?),
        Expr::Nested(e) => written(e),
        Expr::Cast { expr, data_type: DataType::Varchar(_) | DataType::Text | DataType::String(_) | DataType::CharacterVarying(_), .. } => written(expr),
        _ => None,
    }
}

/// A LIKE pattern written with `esc` as its escape, as one with backslash: `esc%` is a literal `%`,
/// a literal `\` is `\\`, the rest as written.
fn backslash_pattern(pattern: &str, esc: Option<char>) -> Result<String> {
    let mut out = String::new();
    let mut chars = pattern.chars();
    while let Some(c) = chars.next() {
        if Some(c) == esc {
            let Some(next) = chars.next() else {
                return Err(crate::codes::coded("22025", "LIKE pattern must not end with escape character"));
            };
            if matches!(next, '%' | '_' | '\\') {
                out.push('\\'); // (the next one is a literal here, not a wildcard)
            }
            out.push(next);
        } else if c == '\\' {
            out.push_str("\\\\");
        } else {
            out.push(c);
        }
    }
    Ok(out)
}

/// `INSERT INTO t DEFAULT VALUES` as `INSERT INTO t VALUES (DEFAULT, …)`, one DEFAULT per column, so
/// defaults and identity columns fill the row as they do for any INSERT. Only a table the host has; a
/// temporary table's INSERT is its own path (`temp.rs`), so it is left as it is.
async fn default_values<H: crate::front::Host>(host: &H, s: &mut Statement) -> Result<bool> {
    let Statement::Insert(ins) = s else { return Ok(false) };
    if ins.source.is_some() || !ins.assignments.is_empty() {
        return Ok(false);
    }
    let TableObject::TableName(name) = &ins.table else { return Ok(false) };
    let table = crate::text::object(name);
    let Some(names) = host.columns(BTreeSet::from([table.clone()])).await?.remove(&table) else { return Ok(false) };
    let columns = names.iter().filter(|c| !crate::text::NAMES.contains(&c.as_str()) && c.as_str() != "_deleted").count();
    let Statement::Query(query) = Parser::parse_sql(&GenericDialect {}, &format!("VALUES ({})", vec!["DEFAULT"; columns].join(", ")))?.remove(0) else { return Ok(false) };
    ins.source = Some(query);
    Ok(true)
}

/// SQL Server's `SELECT TOP n …` (or `TOP (n)`) as `LIMIT n`, where the query is that select. A TOP in a UNION's branch
/// is left to DataFusion's refusal.
fn top(q: &mut Query) -> Result<()> {
    let SetExpr::Select(s) = &mut *q.body else { return Ok(()) };
    let Some(t) = s.top.take() else { return Ok(()) };
    if t.with_ties || t.percent {
        return Err(crate::codes::coded("0A000", format!("SELECT TOP … {}: not taken; LIMIT n", if t.percent { "PERCENT" } else { "WITH TIES" })));
    }
    let n = match t.quantity {
        Some(TopQuantity::Expr(e)) => e,
        Some(TopQuantity::Constant(c)) => expr(&c.to_string())?,
        None => return Err(crate::codes::coded("42601", "SELECT TOP without a number: LIMIT n")),
    };
    match &mut q.limit_clause {
        None => q.limit_clause = Some(LimitClause::LimitOffset { limit: Some(n), offset: None, limit_by: vec![] }),
        Some(LimitClause::LimitOffset { limit: l @ None, .. }) => *l = Some(n),
        _ => return Err(crate::codes::coded("42601", "SELECT TOP and LIMIT together: write one of them (LIMIT n)")),
    }
    Ok(())
}

/// `FETCH FIRST n ROWS ONLY` as `LIMIT n`.
fn fetch(q: &mut Query) -> Result<()> {
    let Some(f) = q.fetch.take() else { return Ok(()) };
    if f.with_ties || f.percent {
        return Err(crate::codes::coded("0A000", format!("FETCH FIRST … {}: not taken; LIMIT n", if f.percent { "PERCENT" } else { "WITH TIES" })));
    }
    let n = f.quantity.unwrap_or_else(|| expr("1").expect("a literal"));
    match &mut q.limit_clause {
        None => q.limit_clause = Some(LimitClause::LimitOffset { limit: Some(n), offset: None, limit_by: vec![] }),
        Some(LimitClause::LimitOffset { limit: l @ None, .. }) => *l = Some(n),
        _ => return Err(crate::codes::coded("42601", "FETCH FIRST and LIMIT together: write one of them (LIMIT n, or FETCH FIRST n ROWS ONLY)")),
    }
    Ok(())
}

/// Whether `q` is ordered `BY ALL`, and if so whether its answer's columns are a `*`'s (whose
/// number only a query of it tells).
fn all_of(q: &Query) -> Option<bool> {
    let Some(OrderBy { kind: OrderByKind::Expressions(es), .. }) = &q.order_by else { return None };
    let [OrderByExpr { expr: Expr::Identifier(i), .. }] = es.as_slice() else { return None };
    if i.quote_style.is_some() || !i.value.eq_ignore_ascii_case("all") {
        return None;
    }
    let mut body = &*q.body;
    while let SetExpr::SetOperation { left, .. } = body {
        body = left;
    }
    Some(!matches!(body, SetExpr::Select(s) if !s.projection.iter().any(|p| matches!(p, SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(..)))))
}

/// `ORDER BY ALL`: by every column of the answer, left to right (`n` of them, for a `*`).
fn order_by_all(q: &mut Query, n: Option<usize>) -> Result<()> {
    let Some(wild) = all_of(q) else { return Ok(()) };
    let items = match (wild, &*q.body) {
        (false, SetExpr::Select(s)) => s.projection.len(),
        (false, SetExpr::SetOperation { .. }) => {
            let mut body = &*q.body;
            while let SetExpr::SetOperation { left, .. } = body {
                body = left;
            }
            let SetExpr::Select(s) = body else { unreachable!() };
            s.projection.len()
        }
        _ => n.context("ORDER BY ALL: the answer's columns weren't found")?,
    };
    let Some(OrderBy { kind: OrderByKind::Expressions(es), .. }) = &mut q.order_by else { unreachable!() };
    let options = es[0].options;
    *es = (1..=items).map(|n| OrderByExpr { expr: expr(&n.to_string()).expect("a number"), options, with_fill: None }).collect();
    Ok(())
}

pub(crate) fn function_name(f: &Function) -> String { f.name.0.last().and_then(|p| p.as_ident()).map(|i| i.value.to_lowercase()).unwrap_or_default() }

fn args(f: &Function) -> Vec<&Expr> {
    match &f.args {
        FunctionArguments::List(l) => l.args.iter().filter_map(|a| match a {
            FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => Some(e),
            _ => None,
        }).collect(),
        _ => vec![],
    }
}

/// SQL Server's and Snowflake's `string_agg(x, sep) WITHIN GROUP (ORDER BY y)` (`listagg` too, which is `string_agg` here):
/// the ORDER BY is the aggregate's own, as `string_agg(x, sep ORDER BY y)` takes it. Other aggregates keep their WITHIN GROUP.
fn within_group(f: &mut Function) -> Result<()> {
    let n = function_name(f);
    if f.within_group.is_empty() || !matches!(n.as_str(), "string_agg" | "listagg") {
        return Ok(());
    }
    let FunctionArguments::List(list) = &mut f.args else { return Err(crate::codes::coded("42601", format!("{n}(x, sep) WITHIN GROUP (ORDER BY y): the arguments come first"))) };
    if list.clauses.iter().any(|c| matches!(c, FunctionArgumentClause::OrderBy(_))) {
        return Err(crate::codes::coded("42601", format!("{n}: an ORDER BY among its arguments and one in WITHIN GROUP: write one of them")));
    }
    list.clauses.push(FunctionArgumentClause::OrderBy(std::mem::take(&mut f.within_group)));
    f.name = ObjectName::from(vec![Ident::new("string_agg")]);
    Ok(())
}

/// MySQL's and DuckDB's `group_concat(x [ORDER BY …] [SEPARATOR ', '])`: `string_agg(x, ', ' [ORDER BY …])`, its
/// separator `,` when none is given.
fn group_concat(f: &mut Function) {
    if function_name(f) != "group_concat" {
        return;
    }
    let FunctionArguments::List(l) = &mut f.args else { return };
    if l.args.len() != 1 {
        return;
    }
    let sep = l.clauses.iter().position(|c| matches!(c, FunctionArgumentClause::Separator(_))).map(|i| l.clauses.remove(i));
    let sep = match sep {
        Some(FunctionArgumentClause::Separator(v)) => Expr::Value(v),
        _ => expr("','").expect("a literal"),
    };
    l.args.push(FunctionArg::Unnamed(FunctionArgExpr::Expr(sep)));
    f.name = ObjectName::from(vec![Ident::new("string_agg")]);
}

/// BigQuery's `array_agg(x ORDER BY y LIMIT n)`: the first `n` of the list, `array_slice(array_agg(x ORDER BY y), 1, n)`.
fn array_agg_limit(f: &mut Function) -> Option<String> {
    if function_name(f) != "array_agg" {
        return None;
    }
    let FunctionArguments::List(l) = &mut f.args else { return None };
    let i = l.clauses.iter().position(|c| matches!(c, FunctionArgumentClause::Limit(_)))?;
    let FunctionArgumentClause::Limit(n) = l.clauses.remove(i) else { return None };
    Some(format!("array_slice({f}, 1, {n})"))
}

/// A string literal's text.
pub(crate) fn string_of(e: &Expr) -> Option<&str> {
    match e {
        Expr::Value(ValueWithSpan { value: Value::SingleQuotedString(s), .. }) => Some(s),
        _ => None,
    }
}

/// A date part as DuckDB, Snowflake and SQL Server spell it (`'day'`, `dd`, `HOURS`): its name in `date_trunc`, or
/// None for any other expression (a column holding the part is DuckDB's, not taken). A string that isn't a part is
/// refused by name, and so are the letters the databases read apart: `m` (DuckDB's minute, SQL Server's month), `y`
/// (DuckDB's year, SQL Server's day of the year) and `w` (DuckDB's week, SQL Server's weekday).
pub(crate) fn part_word(e: &Expr, call: &str) -> Result<Option<&'static str>> {
    let (w, word) = match e {
        Expr::Identifier(i) if i.quote_style.is_none() => (i.value.to_lowercase(), true),
        _ => match string_of(e) {
            Some(s) => (s.trim().to_lowercase(), false),
            None => return Ok(None),
        },
    };
    let apart = |means: &str| crate::codes::coded("22023", format!("{call}: '{w}' is {means} in other databases: write the part's name (year, month, week, day, minute, …)"));
    Ok(Some(match w.as_str() {
        "m" => return Err(apart("DuckDB's minute and SQL Server's month")),
        "y" => return Err(apart("DuckDB's year and SQL Server's day of the year")),
        "w" => return Err(apart("DuckDB's week and SQL Server's weekday")),
        "year" | "years" | "yr" | "yrs" | "yy" | "yyyy" => "year",
        "quarter" | "quarters" | "q" | "qq" | "qtr" => "quarter",
        "month" | "months" | "mon" | "mons" | "mm" => "month",
        "week" | "weeks" | "wk" | "wks" | "ww" => "week",
        "day" | "days" | "d" | "dd" | "dayofmonth" => "day",
        "hour" | "hours" | "h" | "hh" | "hr" | "hrs" => "hour",
        "minute" | "minutes" | "mi" | "min" | "mins" | "n" => "minute",
        "second" | "seconds" | "s" | "ss" | "sec" | "secs" => "second",
        "millisecond" | "milliseconds" | "ms" | "msec" | "msecs" => "millisecond",
        "microsecond" | "microseconds" | "us" | "usec" | "usecs" | "mcs" => "microsecond",
        _ if word => return Ok(None),
        _ => return Err(crate::codes::coded("22023", format!("{call}: '{w}' isn't a date part here: year, quarter, month, week, day, hour, minute, second, millisecond or microsecond"))),
    }))
}

/// Whose format codes a `strftime` or `strptime` takes.
#[derive(Clone, Copy, PartialEq)]
enum Codes {
    DuckDb,
    Sqlite,
}
use Codes::{DuckDb, Sqlite};

/// A format in DuckDB's codes (or SQLite's) as chrono's, which `to_char` and `to_timestamp` read. Most are the same;
/// DuckDB's `%f` is microseconds, `%g` milliseconds and `%n` nanoseconds (chrono's `%6f`, `%3f`, `%9f`), its `%c` and
/// `%x` ISO's date and time; SQLite's `%f` is the seconds with their milliseconds. A code the other database hasn't is
/// refused by name (22023), as DuckDB refuses it.
fn strftime_codes(f: &str, from: Codes, call: &str) -> Result<String> {
    const SAME: &str = "aAwdbhBmyYHIpMSjUWXGVuzZ";
    const SQLITE_TOO: &str = "eFkPlRsTg";
    let (mut out, mut chars) = (String::new(), f.chars().peekable());
    while let Some(c) = chars.next() {
        if c != '%' {
            out.push(c);
            continue;
        }
        let dash = chars.next_if_eq(&'-').is_some();
        let k = chars.next().unwrap_or('%');
        let code = match (from, k) {
            (_, '%') => "%%".to_string(),
            (DuckDb, 'f') => "%6f".into(),
            (DuckDb, 'g') => "%3f".into(),
            (DuckDb, 'n') => "%9f".into(),
            (DuckDb, 'c') => "%Y-%m-%d %H:%M:%S".into(),
            (DuckDb, 'x') => "%Y-%m-%d".into(),
            (Sqlite, 'f') => "%S%.3f".into(),
            (_, k) if SAME.contains(k) || (from == Sqlite && SQLITE_TOO.contains(k)) => format!("%{}{k}", if dash { "-" } else { "" }),
            (_, k) => {
                let whose = if from == DuckDb { "DuckDB's" } else { "SQLite's" };
                return Err(crate::codes::coded("22023", format!("{call}: %{}{k} isn't one of {whose} format codes (%Y %m %d %H %M %S %f, …)", if dash { "-" } else { "" })));
            }
        };
        out += &code;
    }
    Ok(out)
}

/// DuckDB's `strftime(ts, '%Y-%m')` (SQLite's `strftime('%Y', ts)` too), `strptime(text, format)`, `epoch(ts)` and
/// `regexp_extract(s, pattern[, group[, flags]])` (the group's text, '' with no match); `date_diff(part, a, b)` (also
/// `datediff`: Snowflake's and SQL Server's), the part boundaries from `a` to `b` as DuckDB counts them (a week is 7 of
/// the days between, whole ones); `dateadd(part, n, ts)` (Snowflake's and SQL Server's, and `timestampadd`); `date_add(ts, INTERVAL …)` and
/// `date_sub` (DuckDB's and MySQL's). The text each is, or None: Spark's own `date_add(d, n)` and `datediff(end, start)`
/// stay as they are, `date_add`'s days as an INT (a literal is a BIGINT, which Spark's doesn't take).
fn dates(name: &str, a: &[&Expr]) -> Result<Option<String>> {
    Ok(Some(match (name, a.len()) {
        // (each format's codes as chrono's: DuckDB's `%f` is microseconds, SQLite's seconds with milliseconds)
        ("strftime", 2) => match (string_of(a[0]), string_of(a[1])) {
            (Some(f), None) if f.contains('%') => format!("to_char({}, {})", a[1], quote(&strftime_codes(f, Sqlite, name)?)),
            (_, Some(f)) => format!("to_char({}, {})", a[0], quote(&strftime_codes(f, DuckDb, name)?)),
            _ => format!("to_char({}, {})", a[0], a[1]),
        },
        ("strptime", 2) => match string_of(a[1]) {
            Some(f) => format!("to_timestamp({}, {})", a[0], quote(&strftime_codes(f, DuckDb, name)?)),
            None => format!("to_timestamp({}, {})", a[0], a[1]),
        },
        ("epoch", 1) => format!("date_part('epoch', {})", a[0]),
        ("regexp_extract", 2..=4) => {
            let group = match a.get(2) {
                None => 1,
                Some(Expr::Value(ValueWithSpan { value: Value::Number(n, _), .. })) => match n.parse::<u64>() {
                    Ok(g) => g + 1,
                    Err(_) => return Ok(None),
                },
                Some(_) => return Ok(None), // (DuckDB's list of names for a struct: not taken)
            };
            let pattern = match string_of(a[1]) {
                Some(p) => quote(&format!("({p})")), // (the whole match is the first group: the pattern's own come after)
                None => format!("concat('(', {}, ')')", a[1]),
            };
            let flags = a.get(3).map(|f| format!(", {f}")).unwrap_or_default();
            format!("coalesce(regexp_match({s}, {pattern}{flags})[{group}], CASE WHEN ({s}) IS NOT NULL THEN '' END)", s = a[0])
        }
        ("date_diff" | "datediff", 3) => {
            let Some(part) = part_word(a[0], name)? else { return Ok(None) };
            // (the part a bare word: Snowflake's and SQL Server's DATEDIFF, which count week boundaries, from Monday in
            // Snowflake and from Sunday in SQL Server; DuckDB's `date_diff('week', …)` counts whole weeks)
            if part == "week" && matches!(a[0], Expr::Identifier(i) if i.quote_style.is_none()) {
                return Err(crate::codes::coded("22023", format!("{name}(week, a, b) counts Sundays crossed in SQL Server and Mondays in Snowflake, so it isn't guessed: date_diff('week', a, b) counts whole weeks, as DuckDB does, and date_diff('day', date_trunc('week', a), date_trunc('week', b)) / 7 the Mondays crossed")));
            }
            let (from, to) = (a[1], a[2]);
            let at = |p: &str, x: &Expr| format!("date_part('{p}', {x})");
            let n = match part {
                "year" => format!("{} - {}", at("year", to), at("year", from)),
                "quarter" => format!("({} * 4 + {}) - ({} * 4 + {})", at("year", to), at("quarter", to), at("year", from), at("quarter", from)),
                "month" => format!("({} * 12 + {}) - ({} * 12 + {})", at("year", to), at("month", to), at("year", from), at("month", from)),
                _ => {
                    let scale = match part {
                        "day" | "week" => " / 86400",
                        "hour" => " / 3600",
                        "minute" => " / 60",
                        "millisecond" => " * 1000",
                        "microsecond" => " * 1000000",
                        _ => "",
                    };
                    let unit = if part == "week" { "day" } else { part };
                    let n = format!("round((date_part('epoch', date_trunc('{unit}', {to})) - date_part('epoch', date_trunc('{unit}', {from}))){scale})");
                    match part {
                        "week" => format!("trunc({n} / 7)"), // (whole weeks of the days between, as DuckDB counts them)
                        _ => n,
                    }
                }
            };
            format!("CAST({n} AS BIGINT)")
        }
        ("dateadd" | "date_add" | "timestampadd" | "timeadd", 3) => {
            let Some(part) = part_word(a[0], name)? else { return Ok(None) };
            let (n, one) = match part {
                "quarter" => (format!("CAST({} AS BIGINT) * 3", a[1]), "month"),
                "week" => (format!("CAST({} AS BIGINT) * 7", a[1]), "day"),
                p => (format!("CAST({} AS BIGINT)", a[1]), p),
            };
            format!("({}) + {n} * INTERVAL '1 {one}'", a[2])
        }
        ("date_add" | "date_sub", 2) if matches!(a[1], Expr::Interval(_)) => format!("({}) {} {}", a[0], if name == "date_add" { "+" } else { "-" }, a[1]),
        ("date_add" | "date_sub", 2) if !matches!(a[1], Expr::Cast { .. }) => format!("{name}({}, CAST({} AS INT))", a[0], a[1]),
        _ => return Ok(None),
    }))
}

/// One expression's own forms.
fn expression(e: &mut Expr) -> Result<()> {
    if let Expr::Like { pattern, escape_char, .. } | Expr::ILike { pattern, escape_char, .. } = e {
        return like_escape(pattern, escape_char);
    }
    if let Some(text) = typed_literal(e)? {
        *e = grouped(expr(&text)?); // (a literal in Postgres's spelling, as the expression DataFusion reads)
        return Ok(());
    }
    if let Some(text) = row_in(e)? {
        *e = grouped(expr(&text)?);
        return Ok(());
    }
    if let Some(text) = json_arrow(e)? {
        *e = grouped(expr(&text)?);
        return Ok(());
    }
    match e {
        // (Postgres's `N'…'`, the standard's national character string: a string, as Postgres reads it)
        Expr::Value(ValueWithSpan { value: v @ Value::NationalStringLiteral(_), .. }) => {
            let Value::NationalStringLiteral(s) = std::mem::replace(v, Value::Null) else { unreachable!() };
            *v = Value::SingleQuotedString(s);
        }
        // (`LAMBDA (x): …`, which GenericDialect reads as a call of lambda and a path, so no hook is asked for it: DuckDB
        // refuses it too, as its LAMBDA's names have no parentheses)
        Expr::JsonAccess { value, .. } if matches!(&**value, Expr::Function(f) if f.name.to_string().eq_ignore_ascii_case("lambda")) => {
            let Expr::Function(f) = &**value else { unreachable!() };
            let names = match &f.args {
                FunctionArguments::List(l) => l.args.iter().map(|a| a.to_string()).collect::<Vec<_>>().join(", "),
                _ => "x".to_string(),
            };
            return Err(crate::codes::coded("42601", format!("a LAMBDA's names take no parentheses: LAMBDA {names}: …")));
        }
        // (Snowflake's `payload:page::STRING`: DataFusion can't plan it, and Pondra's JSON is text, so it is refused with
        // the forms that read it: `->>` for a key, json_extract_string for a path. The path is written as it was)
        Expr::JsonAccess { value, path } => {
            let steps: String = path.path.iter().map(json_step).collect();
            let first = match path.path.first() {
                Some(JsonPathElem::Dot { key, .. }) => key.clone(),
                Some(elem) => format!("${}", json_step(elem)),
                None => String::new(),
            };
            return Err(crate::codes::coded("0A000", format!("{value}{path} is Snowflake's path: Pondra's JSON is text, read with {value}->>'{first}' or json_extract_string({value}, '${steps}')")));
        }
        // `({'a': 1}).a`, `f(x).a`: a field of a struct that isn't a column, as `…['a']`
        Expr::CompoundFieldAccess { root, access_chain } if !matches!(**root, Expr::Identifier(_) | Expr::CompoundIdentifier(_)) => {
            for a in access_chain.iter_mut() {
                if let AccessExpr::Dot(Expr::Identifier(i)) = a {
                    *a = AccessExpr::Subscript(Subscript::Index { index: expr(&quote(&i.value))? });
                }
            }
        }
        // (SQL Server's `CONVERT(INT, x)` and MySQL's `CONVERT(x, SIGNED)` put the type on either side, so neither is
        // guessed: CAST names it one way. A `TRY_CONVERT(type, 'literal')` the parser reads as a call is `dialect_fns`')
        Expr::Convert { is_try, .. } => {
            let (name, cast) = if *is_try { ("TRY_CONVERT", "TRY_CAST") } else { ("CONVERT", "CAST") };
            return Err(crate::codes::coded("0A000", format!("{name}(…) puts the type first in SQL Server and last in MySQL, so it isn't guessed: write {cast}(x AS type)")));
        }
        Expr::Function(f) => {
            // (BigQuery's `SAFE.f(…)`, its NULL-on-error prefix, which Pondra doesn't have: a name of two parts, the first `safe`)
            if let [first, last] = f.name.0.as_slice() {
                if first.as_ident().is_some_and(|i| i.value.eq_ignore_ascii_case("safe")) {
                    let name = last.as_ident().map(|i| i.value.to_uppercase()).unwrap_or_default();
                    return Err(crate::codes::coded("0A000", format!("SAFE.{name}(…) is BigQuery's NULL-on-error prefix, which Pondra doesn't have: TRY_CAST(x AS type) is NULL where a cast fails, and SAFE_DIVIDE(a, b) where b is 0")));
                }
            }
            within_group(f)?;
            group_concat(f);
            if let Some(text) = array_agg_limit(f) {
                *e = grouped(expr(&text)?);
                return Ok(());
            }
            let n = function_name(f);
            let a = args(f);
            // (other databases' names for Pondra's functions, a plain call: unqualified (BigQuery's `SAFE.` and a schema's own
            // function are left as written), no OVER, FILTER, DISTINCT or named argument)
            let plain = f.name.0.len() == 1 && f.over.is_none() && f.filter.is_none() && match &f.args {
                FunctionArguments::List(l) => l.args.len() == a.len() && l.duplicate_treatment.is_none() && l.clauses.is_empty(),
                FunctionArguments::None => true,
                FunctionArguments::Subquery(_) => false,
            };
            if plain {
                if let Some(text) = crate::dialect_fns::call(&n, &a)? {
                    *e = grouped(expr(&text)?);
                    return Ok(());
                }
            }
            match (n.as_str(), a.len()) {
                ("max_by" | "arg_max" | "min_by" | "arg_min", 2) => {
                    ensure!(f.over.is_none(), "{n}(…) OVER (…): first_value(a) OVER (… ORDER BY b DESC) is the window's form");
                    let (desc, (x, by)) = (n.starts_with("max") || n == "arg_max", (a[0].to_string(), a[1].to_string()));
                    let only = match &f.filter {
                        Some(w) => format!("({w}) AND ({by}) IS NOT NULL"),
                        None => format!("({by}) IS NOT NULL"),
                    };
                    *e = grouped(expr(&format!("first_value({x} ORDER BY {by} {} NULLS LAST) FILTER (WHERE {only})", if desc { "DESC" } else { "ASC" }))?);
                }
                ("string_split" | "str_split" | "split", 2) => f.name = ObjectName::from(vec![Ident::new("string_to_array")]),
                // (DuckDB's dates and patterns, and the part-first DATEDIFF and DATEADD of Snowflake and SQL Server: `dates`)
                ("strftime" | "strptime" | "epoch" | "regexp_extract" | "date_diff" | "datediff" | "dateadd" | "date_add" | "date_sub" | "timestampadd" | "timeadd", _) => {
                    if let Some(text) = dates(&n, &a)? {
                        *e = grouped(expr(&text)?);
                    }
                }
                ("list", 1) => f.name = ObjectName::from(vec![Ident::new("array_agg")]), // (DuckDB's name for it)
                ("apply" | "list_apply" | "array_apply", 2) => f.name = ObjectName::from(vec![Ident::new("array_transform")]),
                ("filter", 2) if matches!(a[1], Expr::BinaryOp { op: BinaryOperator::Arrow, .. }) => f.name = ObjectName::from(vec![Ident::new("array_filter")]),
                ("transform", 2) if matches!(a[1], Expr::BinaryOp { op: BinaryOperator::Arrow, .. }) => f.name = ObjectName::from(vec![Ident::new("array_transform")]), // (Spark's `transform(arr, x -> …)`)
                // (Spark's get_json_object and BigQuery's json_extract_scalar give a string, as json_as_text does)
                ("json_extract" | "json_extract_string" | "json_extract_path_text" | "json_value" | "get_json_object" | "json_extract_scalar", 2) => {
                    let Expr::Value(ValueWithSpan { value: Value::SingleQuotedString(path), .. }) = a[1] else { bail!("{n}(json, path): the path is a literal, such as '$.a.b'; json_get(json, key, …) takes expressions") };
                    let keys = json_path(path)?.join(", ");
                    let call = if n == "json_extract" { "json_get_json" } else { "json_as_text" };
                    *e = grouped(expr(&match keys.is_empty() {
                        true => a[0].to_string(),
                        false => format!("{call}({}, {keys})", a[0]),
                    })?);
                }
                // (Postgres's bytea-only functions read a string literal as their first argument as a bytea, of any
                // arity: `sha256('\x01')` hashes the byte 0x01, and `encode('\x01', 'hex')` is `01`, as Postgres does;
                // a column or any other expression is left as it is)
                (fname, _) if TAKES_BYTEA.contains(&fname) => {
                    if let Some(Expr::Value(ValueWithSpan { value: Value::SingleQuotedString(s), .. })) = a.first() {
                        let bytes = crate::pg_bytea::parse(s).map_err(|e| crate::codes::coded(e.code, e.message))?;
                        let lit = expr(&crate::pg_bytea::sql(&bytes))?;
                        if let FunctionArguments::List(l) = &mut f.args {
                            let first = l.args.iter_mut().find_map(|x| match x {
                                FunctionArg::Unnamed(FunctionArgExpr::Expr(arg)) => Some(arg),
                                _ => None,
                            });
                            if let Some(arg) = first {
                                *arg = lit;
                            }
                        }
                    }
                }
                _ => {}
            }
        }
        _ => {}
    }
    Ok(())
}

/// Every array literal in `s` as the call it stands for: `[…]` and `ARRAY[…]` are `make_array(…)`. Run
/// once the statement's COLUMNS lists are expanded (`expand_columns` reads a list as it is written, and
/// a select's names keep its text). DataFusion plans a literal's elements with no outer columns, CTEs or
/// parameter types (`sql_array_literal`); a call with them is planned with the query's own, as any call is.
fn arrays(s: &mut Statement) {
    let _ = crate::walk::visit_expressions_mut(s, |e| {
        if let Expr::Array(Array { elem, .. }) = e {
            *e = make_array(std::mem::take(elem)); // (post-order: a nested literal is already a call)
        }
        ControlFlow::<()>::Continue(())
    });
}

/// `make_array(elem, …)`: the call an array literal stands for (`make_array()` when it is empty).
fn make_array(elem: Vec<Expr>) -> Expr {
    Expr::Function(Function {
        name: ObjectName::from(vec![Ident::new("make_array")]),
        uses_odbc_syntax: false,
        parameters: FunctionArguments::None,
        args: FunctionArguments::List(FunctionArgumentList {
            duplicate_treatment: None,
            args: elem.into_iter().map(|e| FunctionArg::Unnamed(FunctionArgExpr::Expr(e))).collect(),
            clauses: vec![],
        }),
        filter: None,
        null_treatment: None,
        over: None,
        within_group: vec![],
    })
}

/// A JSON path (`$.a.b[0]`, `$."a b"`, or a pointer `/a/0`) as json_get's keys.
fn json_path(path: &str) -> Result<Vec<String>> {
    let mut keys = Vec::new();
    if let Some(p) = path.strip_prefix('/') {
        for k in p.split('/') {
            keys.push(k.parse::<u64>().map_or_else(|_| quote(&k.replace("~1", "/").replace("~0", "~")), |n| n.to_string()));
        }
        return Ok(keys);
    }
    let Some(mut p) = path.strip_prefix('$') else { return Ok(vec![quote(path)]) };
    static PART: LazyLock<Regex> = LazyLock::new(|| Regex::new(r#"^(?:\.([A-Za-z_][A-Za-z0-9_]*)|\."((?:[^"\\]|\\.)*)"|\[(\d+)\]|\['((?:[^'\\]|\\.)*)'\]|\["((?:[^"\\]|\\.)*)"\])"#).expect("a regex"));
    while !p.is_empty() {
        let m = PART.captures(p).with_context(|| format!("a JSON path the json functions take: $.a.b[0] (not {path})"))?;
        keys.push(match (m.get(1).or(m.get(2)).or(m.get(4)).or(m.get(5)), m.get(3)) {
            (Some(k), _) => quote(k.as_str()),
            (None, Some(n)) => n.as_str().to_string(),
            _ => unreachable!(),
        });
        p = &p[m.get(0).expect("a match").end()..];
    }
    Ok(keys)
}

/// `payload->>'$.a.b'` (DuckDB's and MySQL's JSONPath after `->>`) as `json_as_text(payload, 'a', 'b')`; after `->` it is
/// `json_get_json`, which keeps the JSON, and `'$'` alone is the value itself. A key that isn't a path (Postgres's
/// `->> 'a'`, or a key such as MongoDB's `'$oid'` or `'$ref'`: `$` then neither `.` nor `[`) is None, as it is.
fn json_arrow(e: &Expr) -> Result<Option<String>> {
    let Expr::BinaryOp { left, op: op @ (BinaryOperator::Arrow | BinaryOperator::LongArrow), right } = e else { return Ok(None) };
    let Expr::Value(ValueWithSpan { value: Value::SingleQuotedString(path), .. }) = &**right else { return Ok(None) };
    if !(path == "$" || path.starts_with("$.") || path.starts_with("$[")) {
        return Ok(None);
    }
    let keys = json_path(path)?.join(", ");
    let text = match (keys.is_empty(), matches!(op, BinaryOperator::LongArrow)) {
        (true, false) => left.to_string(),
        (true, true) => format!("json_as_text({left})"),
        (false, false) => format!("json_get_json({left}, {keys})"),
        (false, true) => format!("json_as_text({left}, {keys})"),
    };
    Ok(Some(text))
}

/// One step of a Snowflake path as a JSON path writes it: `.key`, `[n]`, `['key']`.
fn json_step(elem: &JsonPathElem) -> String {
    match elem {
        JsonPathElem::Dot { key, .. } => format!(".{key}"),
        JsonPathElem::Bracket { key } | JsonPathElem::ColonBracket { key } => match key {
            Expr::Value(ValueWithSpan { value: Value::Number(n, _), .. }) => format!("[{n}]"),
            _ => format!("['{}']", key.to_string().trim_matches('\'')),
        },
    }
}

/// A table factor's `TABLESAMPLE` / `SAMPLE`, which DataFusion ignores, as a subquery that samples.
fn table_sample(t: &mut TableFactor) -> Result<()> {
    let TableFactor::Table { sample, alias, name, .. } = t else { return Ok(()) };
    let s = match sample.take() {
        Some(TableSampleKind::BeforeTableAlias(s) | TableSampleKind::AfterTableAlias(s)) => s,
        None => return Ok(()),
    };
    if s.seed.is_some() || s.bucket.is_some() || s.offset.is_some() {
        return Err(crate::codes::coded("0A000", "a sample with a seed, a bucket or an offset isn't taken"));
    }
    let q = s.quantity.context("TABLESAMPLE (n PERCENT) or (n ROWS)")?;
    let rows = matches!(q.unit, Some(TableSampleUnit::Rows));
    let named = alias.take().unwrap_or_else(|| TableAlias { explicit: true, name: name.0.last().and_then(|p| p.as_ident()).cloned().unwrap_or_else(|| Ident::new("t")), columns: vec![], at: None });
    *t = sampled(&t.to_string(), &q.value.to_string(), rows, named)?;
    Ok(())
}

/// `from` sampled: a share of its rows (Bernoulli: each row on its own), or n of them.
fn sampled(from: &str, n: &str, rows: bool, alias: TableAlias) -> Result<TableFactor> {
    let q = match rows {
        true => format!("SELECT * FROM {from} ORDER BY random() LIMIT {n}"),
        false => format!("SELECT * FROM {from} WHERE random() < ({n}) / 100.0"),
    };
    Ok(TableFactor::Derived { lateral: false, subquery: Box::new(query(&q)?), alias: Some(alias), sample: None })
}

/// `USING SAMPLE …`'s marker (`using_sample`) taken out of a select's WHERE and done.
fn sample(s: &mut Select) -> Result<()> {
    fn marker(e: &Expr) -> Option<String> {
        match e {
            Expr::Function(f) if function_name(f) == "pondra_sample" => match args(f).as_slice() {
                [Expr::Value(ValueWithSpan { value: Value::SingleQuotedString(s), .. })] => Some(s.clone()),
                _ => None,
            },
            _ => None,
        }
    }
    // the marker is the WHERE's first term: `marker AND rest` parses with it leftmost under ANDs
    fn take(e: &mut Expr) -> Option<(String, Option<Expr>)> {
        if let Some(spec) = marker(e) {
            return Some((spec, None));
        }
        let Expr::BinaryOp { left, op, right } = e else { return None };
        if *op == BinaryOperator::And {
            if let Some(spec) = marker(left) {
                return Some((spec, Some((**right).clone())));
            }
        }
        if matches!(op, BinaryOperator::And | BinaryOperator::Or) {
            let (spec, rest) = take(left)?;
            *left = Box::new(rest?);
            return Some((spec, Some(e.clone())));
        }
        None
    }
    let Some(w) = &mut s.selection else { return Ok(()) };
    let Some((spec, rest)) = take(w) else { return Ok(()) };
    s.selection = rest;
    static SPEC: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"(?i)^(?:(?:reservoir|bernoulli|system)\s*\(\s*)?(\d+(?:\.\d+)?)\s*(%|percent|rows)?\s*\)?(?:\s*\(\s*(?:reservoir|bernoulli|system)\s*\))?$").expect("a regex"));
    let m = SPEC.captures(spec.trim()).with_context(|| format!("USING SAMPLE {spec}: n% or n ROWS (a seed isn't taken)"))?;
    let n = m[1].to_string();
    match m.get(2).map(|u| u.as_str().to_lowercase()).as_deref() {
        Some("%" | "percent") => {
            let keep = expr(&format!("random() < ({n}) / 100.0"))?;
            s.selection = Some(match s.selection.take() {
                Some(w) => Expr::BinaryOp { left: Box::new(keep), op: BinaryOperator::And, right: Box::new(Expr::Nested(Box::new(w))) },
                None => keep,
            });
        }
        _ => {
            ensure!(s.from.len() == 1 && s.from[0].joins.is_empty(), "USING SAMPLE {n} ROWS over a join: sample a subquery of it, or a share (n%)");
            let r = &mut s.from[0].relation;
            let alias = match r {
                TableFactor::Table { alias: a, name, .. } => a.take().unwrap_or_else(|| TableAlias { explicit: true, name: name.0.last().and_then(|p| p.as_ident()).cloned().unwrap_or_else(|| Ident::new("t")), columns: vec![], at: None }),
                TableFactor::Derived { alias: Some(a), .. } => a.clone(),
                _ => TableAlias { explicit: true, name: Ident::new("__sample"), columns: vec![], at: None },
            };
            *r = sampled(&r.to_string(), &n, true, alias)?;
        }
    }
    Ok(())
}

/// The standard's PIVOT as an aggregation, a FILTER per value: `(SELECT * EXCLUDE (g, x), sum(x)
/// FILTER (WHERE g = 'a') AS a, … FROM t GROUP BY ALL)`. `found` are the values a dynamic one's
/// query found.
fn pivot(t: &TableFactor, found: Option<Vec<Column>>) -> Result<TableFactor> {
    let TableFactor::Pivot { table, aggregate_functions, value_column, value_source, default_on_null, alias } = t else { unreachable!() };
    let [col] = value_column.as_slice() else { bail!("PIVOT … FOR one column IN (…); several aren't taken yet") };
    let values: Vec<(String, String)> = match (value_source, found) {
        (PivotValueSource::List(l), _) => l.iter().map(|v| {
            let label = match (&v.alias, &v.expr) {
                (Some(a), _) => a.value.clone(),
                (None, Expr::Value(ValueWithSpan { value: Value::SingleQuotedString(s), .. })) => s.clone(),
                (None, e) => e.to_string(),
            };
            (v.expr.to_string(), label)
        }).collect(),
        (_, Some(found)) => found.into_iter().map(|c| (quote(&c.name), c.name)).collect(),
        (_, None) => bail!("PIVOT: no values found"),
    };
    let mut used: Vec<String> = vec![col.to_string()];
    for a in aggregate_functions {
        let _ = crate::walk::visit_expressions(&a.expr, |e| {
            if let Expr::Identifier(_) | Expr::CompoundIdentifier(_) = e {
                let e = e.to_string();
                if !used.contains(&e) {
                    used.push(e);
                }
            }
            ControlFlow::<()>::Continue(())
        });
    }
    let mut items = Vec::new();
    for (v, label) in &values {
        for a in aggregate_functions {
            let Expr::Function(f) = &a.expr else { bail!("PIVOT's aggregates are calls: sum(x), count(*)") };
            let mut f = f.clone();
            let only = format!("{col} = {v}");
            f.filter = Some(Box::new(expr(&match &f.filter {
                Some(w) => format!("({w}) AND {only}"),
                None => only,
            })?));
            let agg = match default_on_null {
                Some(d) => format!("COALESCE({f}, {d})"),
                None => f.to_string(),
            };
            let named = match (&a.alias, aggregate_functions.len()) {
                (Some(n), _) => format!("{label}_{}", n.value),
                (None, 1) => label.clone(),
                (None, _) => format!("{label}_{}", a.expr),
            };
            items.push(format!("{agg} AS {}", name(&named)));
        }
    }
    ensure!(!items.is_empty(), "PIVOT: no values to pivot on");
    let q = query(&format!("SELECT * EXCLUDE ({}), {} FROM {table} GROUP BY ALL", used.join(", "), items.join(", ")))?;
    Ok(TableFactor::Derived { lateral: false, subquery: Box::new(q), alias: alias.clone(), sample: None })
}

/// The standard's UNPIVOT as two lists unnested side by side: `(SELECT * EXCLUDE (a, b),
/// unnest(['a', 'b']) AS name, unnest([a, b]) AS value FROM t)`, rows whose value is NULL left out
/// unless `INCLUDE NULLS`. `cols` are the table's, for `COLUMNS(…)` in its list.
fn unpivot(t: &TableFactor, cols: Option<Vec<Column>>) -> Result<TableFactor> {
    let TableFactor::Unpivot { table, value, name: n, columns, null_inclusion, alias } = t else { unreachable!() };
    ensure!(matches!(value, Expr::Identifier(_)), "UNPIVOT (value FOR name IN (…)): one value column");
    let mut list: Vec<(String, String)> = Vec::new();
    for c in columns {
        match (columns_call(&c.expr), &cols) {
            (Some(pattern), Some(cols)) => list.extend(matching(&pattern, cols)?.into_iter().map(|c| (name(&c.name), c.name.clone()))),
            _ => {
                let label = match (&c.alias, &c.expr) {
                    (Some(a), _) => a.value.clone(),
                    (None, Expr::Identifier(i)) => i.value.clone(),
                    (None, e) => e.to_string(),
                };
                list.push((c.expr.to_string(), label));
            }
        }
    }
    ensure!(!list.is_empty(), "UNPIVOT: no columns to unpivot");
    let names = list.iter().map(|(c, _)| c.as_str()).collect::<Vec<_>>().join(", ");
    let labels = list.iter().map(|(_, l)| quote(l)).collect::<Vec<_>>().join(", ");
    let inner = format!("SELECT * EXCLUDE ({names}), unnest([{labels}]) AS {n}, unnest([{names}]) AS {value} FROM {table}");
    let q = match null_inclusion {
        Some(NullInclusion::IncludeNulls) => inner,
        _ => format!("SELECT * FROM ({inner}) AS __unpivot WHERE {value} IS NOT NULL"),
    };
    Ok(TableFactor::Derived { lateral: false, subquery: Box::new(query(&q)?), alias: alias.clone(), sample: None })
}

/// What a `COLUMNS(…)` call picks: a regular expression over the names, or every column.
enum Pattern {
    All,
    Like(Regex),
    Names(Vec<String>),
}

fn columns_call(e: &Expr) -> Option<Pattern> {
    let Expr::Function(f) = e else { return None };
    if function_name(f) != "columns" {
        return None;
    }
    let FunctionArguments::List(l) = &f.args else { return None };
    match l.args.as_slice() {
        [FunctionArg::Unnamed(FunctionArgExpr::Wildcard)] => Some(Pattern::All),
        [FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(ValueWithSpan { value: Value::SingleQuotedString(re), .. })))] => Regex::new(re).ok().map(Pattern::Like),
        [FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Array(Array { elem, .. })))] => Some(Pattern::Names(elem.iter().map(|e| match e {
            Expr::Value(ValueWithSpan { value: Value::SingleQuotedString(s), .. }) => s.clone(),
            e => e.to_string(),
        }).collect())),
        _ => None,
    }
}

fn matching<'c>(p: &Pattern, cols: &'c [Column]) -> Result<Vec<&'c Column>> {
    let picked: Vec<&Column> = cols.iter().filter(|c| match p {
        Pattern::All => true,
        Pattern::Like(re) => re.is_match(&c.name),
        Pattern::Names(n) => n.iter().any(|n| n.eq_ignore_ascii_case(&c.name)),
    }).collect();
    ensure!(!picked.is_empty(), "COLUMNS(…) matches no column");
    Ok(picked)
}

/// A column as the query should name it: by its table too where two have its name.
fn reference(c: &Column, cols: &[Column]) -> String {
    match (&c.table, cols.iter().filter(|o| o.name == c.name).count() > 1) {
        (Some(t), true) => format!("{}.{}", name(t), name(&c.name)),
        _ => name(&c.name),
    }
}

fn has_columns(s: &Select) -> bool {
    let found = |e: &Expr| crate::walk::visit_expressions(e, |e| match columns_call(e) {
        Some(_) => ControlFlow::Break(()),
        None => ControlFlow::Continue(()),
    }).is_break();
    s.projection.iter().any(|i| match i {
        SelectItem::UnnamedExpr(e) | SelectItem::ExprWithAlias { expr: e, .. } => found(e),
        _ => false,
    }) || s.selection.as_ref().is_some_and(found)
}

/// Every `COLUMNS(…)` in `e` replaced by `col`.
fn substituted(e: &Expr, col: &str) -> Result<Expr> {
    let mut e = e.clone();
    let col = expr(col)?;
    let _ = crate::walk::visit_expressions_mut(&mut e, |x| {
        if columns_call(x).is_some() {
            *x = col.clone();
        }
        ControlFlow::<()>::Continue(())
    });
    Ok(e)
}

/// `COLUMNS('re')`, `COLUMNS(*)` and `COLUMNS(['a', 'b'])`: a select item once per column it
/// picks (`min(COLUMNS(*))` a min of each); in a WHERE, each picked column's condition, ANDed.
fn expand_columns(s: &mut Select, cols: &[Column]) -> Result<()> {
    let pattern_in = |e: &Expr| {
        let mut p = None;
        let _ = crate::walk::visit_expressions(e, |x| {
            if let Some(found) = columns_call(x) {
                p = Some(found);
                return ControlFlow::Break(());
            }
            ControlFlow::Continue(())
        });
        p
    };
    let mut out = Vec::new();
    for item in std::mem::take(&mut s.projection) {
        let (e, alias) = match &item {
            SelectItem::UnnamedExpr(e) => (e, None),
            SelectItem::ExprWithAlias { expr, alias } => (expr, Some(alias)),
            _ => {
                out.push(item);
                continue;
            }
        };
        let Some(p) = pattern_in(e) else {
            out.push(item);
            continue;
        };
        let picked = matching(&p, cols)?;
        ensure!(alias.is_none() || picked.len() == 1, "COLUMNS(…) AS a name: it picks {} columns", picked.len());
        for c in picked {
            let replaced = substituted(e, &reference(c, cols))?;
            // (each named after its column, `min(COLUMNS(*))` too, as DuckDB names them)
            out.push(SelectItem::ExprWithAlias { expr: replaced, alias: alias.cloned().unwrap_or_else(|| Ident::with_quote('"', &c.name)) });
        }
    }
    s.projection = out;
    if let Some(w) = s.selection.take() {
        let mut terms = Vec::new();
        let mut stack = vec![w];
        while let Some(t) = stack.pop() {
            match t {
                Expr::BinaryOp { left, op: BinaryOperator::And, right } => {
                    stack.push(*right);
                    stack.push(*left);
                }
                t => match pattern_in(&t) {
                    Some(p) => terms.extend(matching(&p, cols)?.into_iter().map(|c| substituted(&t, &reference(c, cols)).map(|e| Expr::Nested(Box::new(e)))).collect::<Result<Vec<_>>>()?),
                    None => terms.push(t),
                },
            }
        }
        s.selection = terms.into_iter().reduce(|a, b| Expr::BinaryOp { left: Box::new(a), op: BinaryOperator::And, right: Box::new(b) });
    }
    Ok(())
}

fn renames(s: &Select) -> bool {
    s.projection.iter().any(|i| matches!(i, SelectItem::Wildcard(o) | SelectItem::QualifiedWildcard(_, o) if o.opt_rename.is_some()))
}

/// `* RENAME (a AS b)`: the columns `*` gives, in their order, the renamed ones under their new
/// names (and EXCLUDE, EXCEPT, REPLACE with it, as they are).
fn rename(s: &mut Select, cols: &[Column]) -> Result<()> {
    if !renames(s) {
        return Ok(());
    }
    let mut out = Vec::new();
    for item in std::mem::take(&mut s.projection) {
        let (o, of) = match &item {
            SelectItem::Wildcard(o) if o.opt_rename.is_some() => (o, None),
            SelectItem::QualifiedWildcard(SelectItemQualifiedWildcardKind::ObjectName(n), o) if o.opt_rename.is_some() => (o, n.0.last().and_then(|p| p.as_ident()).map(|i| i.value.clone())),
            _ => {
                out.push(item);
                continue;
            }
        };
        let to: HashMap<String, Ident> = match &o.opt_rename {
            Some(RenameSelectItem::Single(r)) => [(r.ident.value.to_lowercase(), r.alias.clone())].into(),
            Some(RenameSelectItem::Multiple(rs)) => rs.iter().map(|r| (r.ident.value.to_lowercase(), r.alias.clone())).collect(),
            None => HashMap::new(),
        };
        let mut gone: HashSet<String> = HashSet::new();
        match &o.opt_exclude {
            Some(ExcludeSelectItem::Single(n)) => {
                gone.insert(n.to_string().to_lowercase());
            }
            Some(ExcludeSelectItem::Multiple(ns)) => gone.extend(ns.iter().map(|n| n.to_string().to_lowercase())),
            None => {}
        }
        if let Some(e) = &o.opt_except {
            gone.extend(std::iter::once(&e.first_element).chain(&e.additional_elements).map(|i| i.value.to_lowercase()));
        }
        let replaced: HashMap<String, &Expr> = o.opt_replace.iter().flat_map(|r| r.items.iter().map(|i| (i.column_name.value.to_lowercase(), &i.expr))).collect();
        for c in cols.iter().filter(|c| of.as_ref().is_none_or(|t| c.table.as_deref().is_some_and(|ct| ct.eq_ignore_ascii_case(t)))) {
            let key = c.name.to_lowercase();
            if gone.contains(&key) {
                continue;
            }
            let value = match replaced.get(&key) {
                Some(e) => (*e).clone(),
                None => expr(&match &of {
                    Some(t) => format!("{}.{}", name(t), name(&c.name)),
                    None => reference(c, cols),
                })?,
            };
            out.push(SelectItem::ExprWithAlias { expr: value, alias: to.get(&key).cloned().unwrap_or_else(|| Ident::with_quote('"', &c.name)) });
        }
    }
    s.projection = out;
    Ok(())
}

/// The select's aliases its WHERE names (DuckDB lets a WHERE use them): alias → expression,
/// leaving out an alias that is the column of its name.
fn aliases(s: &Select) -> HashMap<String, Expr> {
    let Some(w) = &s.selection else { return HashMap::new() };
    let named: HashMap<String, &Expr> = s.projection.iter().filter_map(|i| match i {
        SelectItem::ExprWithAlias { expr, alias } if !matches!(expr, Expr::Identifier(i) if i.value.eq_ignore_ascii_case(&alias.value)) => Some((alias.value.to_lowercase(), expr)),
        _ => None,
    }).collect();
    if named.is_empty() {
        return HashMap::new();
    }
    let mut used = HashMap::new();
    let _ = crate::walk::visit_expressions(w, |e| {
        if let Expr::Identifier(i) = e {
            if let Some(x) = named.get(&i.value.to_lowercase()).filter(|_| i.quote_style.is_none()) {
                used.insert(i.value.to_lowercase(), (*x).clone());
            }
        }
        ControlFlow::<()>::Continue(())
    });
    used
}

/// A WHERE naming the select's aliases: each name the FROM has no column of becomes its
/// expression (a column of that name wins, as in DuckDB). Subqueries in it keep their own names.
fn with_aliases(s: &mut Select, cols: &[Column]) -> Result<()> {
    let mut used = aliases(s);
    used.retain(|a, _| !cols.iter().any(|c| c.name.eq_ignore_ascii_case(a)));
    if used.is_empty() {
        return Ok(());
    }
    struct Put<'a>(&'a HashMap<String, Expr>, usize);
    impl VisitorMut for Put<'_> {
        type Break = ();
        fn pre_visit_query(&mut self, _: &mut Query) -> ControlFlow<()> {
            self.1 += 1;
            ControlFlow::Continue(())
        }
        fn post_visit_query(&mut self, _: &mut Query) -> ControlFlow<()> {
            self.1 -= 1;
            ControlFlow::Continue(())
        }
        fn post_visit_expr(&mut self, e: &mut Expr) -> ControlFlow<()> {
            if let (0, Expr::Identifier(i)) = (self.1, &*e) {
                if let Some(x) = self.0.get(&i.value.to_lowercase()).filter(|_| i.quote_style.is_none()) {
                    *e = Expr::Nested(Box::new(x.clone()));
                }
            }
            ControlFlow::Continue(())
        }
    }
    if let Some(w) = &mut s.selection {
        let _ = crate::walk::visit_mut(w, &mut Put(&used, 0));
    }
    Ok(())
}

// ---------------------------------------------------------------- SUMMARIZE

/// DuckDB's `SUMMARIZE t` (or `SUMMARIZE SELECT …`): a row per column (its type, least and
/// greatest value, distinct values about, mean, standard deviation, quartiles, count and share
/// of NULLs), from one pass over the rows.
pub async fn summarize<H: crate::front::Host>(host: &H, sql: &str) -> Result<Option<String>> {
    static SUMMARIZE: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"(?is)^\s*summarize\s+(.+?)\s*;?\s*$").expect("a regex"));
    let Some(m) = SUMMARIZE.captures(sql) else { return Ok(None) };
    let what = m[1].trim();
    let from = match Regex::new(r"(?i)^(select|from|with|values)\b").expect("a regex").is_match(what) {
        true => format!("({what}) AS __summarized"),
        false => what.to_string(),
    };
    let cols = Box::pin(answer(host, Ask::Columns(format!("SELECT * FROM {from} LIMIT 0")))).await?;
    ensure!(!cols.is_empty(), "SUMMARIZE: no columns");
    let list = |f: &dyn Fn(&Column) -> String| format!("[{}]", cols.iter().map(f).collect::<Vec<_>>().join(", "));
    let c = |col: &Column| name(&col.name);
    let numeric = |col: &Column, e: String, or: &str| if col.numeric { e } else { or.to_string() };
    let quartile = |q: f64| move |col: &Column| numeric(col, format!("CAST(approx_percentile_cont({}, {q}) AS VARCHAR)", c(col)), "CAST(NULL AS VARCHAR)");
    let stats = [
        ("mins", list(&|col| format!("CAST(min({}) AS VARCHAR)", c(col)))),
        ("maxs", list(&|col| format!("CAST(max({}) AS VARCHAR)", c(col)))),
        ("uniques", list(&|col| format!("approx_distinct(CAST({} AS VARCHAR))", c(col)))),
        ("avgs", list(&|col| numeric(col, format!("avg(CAST({} AS DOUBLE))", c(col)), "CAST(NULL AS DOUBLE)"))),
        ("stds", list(&|col| numeric(col, format!("stddev(CAST({} AS DOUBLE))", c(col)), "CAST(NULL AS DOUBLE)"))),
        ("q25s", list(&quartile(0.25))),
        ("q50s", list(&quartile(0.5))),
        ("q75s", list(&quartile(0.75))),
        ("nulls", list(&|col| format!("round(100.0 * (count(*) - count({})) / NULLIF(count(*), 0), 2)", c(col)))),
    ];
    let inner = stats.iter().map(|(n, l)| format!("{l} AS {n}")).collect::<Vec<_>>().join(", ");
    Ok(Some(format!(
        "SELECT unnest({}) AS column_name, unnest({}) AS column_type, unnest(mins) AS min, unnest(maxs) AS max, unnest(uniques) AS approx_unique, \
         unnest(avgs) AS avg, unnest(stds) AS std, unnest(q25s) AS q25, unnest(q50s) AS q50, unnest(q75s) AS q75, unnest(counts) AS count, \
         unnest(nulls) AS null_percentage FROM (SELECT {inner}, {} AS counts FROM {from}) AS __summary",
        list(&|col| quote(&col.name)),
        list(&|col| quote(&col.sql_type)),
        list(&|_| "count(*)".into()),
    )))
}

// ---------------------------------------------------------------- lambdas

/// The higher-order functions, by every name they go by: their second argument is a lambda.
const HIGHER: &[&str] = &["array_transform", "list_transform", "array_filter", "list_filter", "array_any_match", "any_match", "list_any_match", "array_first", "list_first"];

/// A query whose text holds a higher-order function's `x -> …`, as a statement whose lambdas are
/// lambdas. SQL's text keeps them as `->` everywhere (the generic dialect reads that as JSON's
/// arrow, which `->` elsewhere still is), so such a query is planned from this tree (`engine::sql`),
/// the same way on every node. None for any other text.
pub fn lambdas(sql: &str) -> Option<datafusion::sql::parser::Statement> {
    if !sql.contains("->") {
        return None;
    }
    let mut stmts = Parser::parse_sql(&GenericDialect {}, sql).ok()?;
    let [stmt @ Statement::Query(_)] = stmts.as_mut_slice() else { return None };
    let mut found = false;
    let _ = crate::walk::visit_expressions_mut(stmt, |e| {
        if let Expr::Function(f) = e {
            if HIGHER.contains(&function_name(f).as_str()) {
                if let FunctionArguments::List(l) = &mut f.args {
                    for a in l.args.iter_mut().skip(1) {
                        if let FunctionArg::Unnamed(FunctionArgExpr::Expr(x)) = a {
                            if let Some(lambda) = lambda(x) {
                                *x = lambda;
                                found = true;
                            }
                        }
                    }
                }
            }
        }
        ControlFlow::<()>::Continue(())
    });
    found.then(|| datafusion::sql::parser::Statement::Statement(Box::new(stmts.remove(0))))
}

/// `x -> body` (or `(x, y) -> body`) read as JSON's arrow, as a lambda. The arrow binds tighter
/// than AND and OR, so `x -> a AND b` came as `(x -> a) AND b`: the body takes what follows.
fn lambda(e: &Expr) -> Option<Expr> {
    match e {
        Expr::BinaryOp { left, op: BinaryOperator::Arrow, right } => {
            let param = |i: &Ident| LambdaFunctionParameter { name: i.clone(), data_type: None };
            let params = match &**left {
                Expr::Identifier(i) => OneOrManyWithParens::One(param(i)),
                Expr::Nested(n) => match &**n {
                    Expr::Identifier(i) => OneOrManyWithParens::One(param(i)),
                    _ => return None,
                },
                Expr::Tuple(t) => OneOrManyWithParens::Many(t.iter().map(|x| match x {
                    Expr::Identifier(i) => Some(param(i)),
                    _ => None,
                }).collect::<Option<Vec<_>>>()?),
                _ => return None,
            };
            Some(Expr::Lambda(LambdaFunction { params, body: right.clone(), syntax: LambdaSyntax::Arrow }))
        }
        Expr::BinaryOp { left, op: op @ (BinaryOperator::And | BinaryOperator::Or | BinaryOperator::Xor), right } => {
            let Expr::Lambda(l) = lambda(left)? else { return None };
            Some(Expr::Lambda(LambdaFunction { params: l.params, body: Box::new(Expr::BinaryOp { left: l.body, op: op.clone(), right: right.clone() }), syntax: LambdaSyntax::Arrow }))
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn texts() {
        // (list comprehensions and LAMBDA are the fallback dialect's: `dialect::tests`)
        assert_eq!(text("SELECT [x * 2 FOR x IN l IF x > 1] FROM t").unwrap(), "SELECT [x * 2 FOR x IN l IF x > 1] FROM t");
        assert_eq!(text("SELECT list_transform(l, LAMBDA x: x + 1)").unwrap(), "SELECT list_transform(l, LAMBDA x: x + 1)");
        let read = |sql| crate::parsed::read(sql).unwrap();
        assert_eq!(read("SELECT list_transform(l, LAMBDA x: x + 1)"), read("SELECT list_transform(l, x -> x + 1)"));
        assert_eq!(read("SELECT [x * 2 FOR x IN l IF x > 1] FROM t"), read("SELECT array_transform(array_filter(l, x -> (x > 1)), x -> (x * 2)) FROM t"));
        assert_eq!(read("SELECT [[y FOR y IN x] FOR x IN l]"), read("SELECT array_transform((l), x -> (array_transform((x), y -> (y))))"));
        assert!(text("SELECT * FROM t ASOF JOIN q ON t.k = q.k AND t.ts >= q.ts").unwrap().contains("MATCH_CONDITION (pondra_duckdb_asof) ON"));
        assert!(text("SELECT * FROM t ASOF LEFT JOIN q ON t.ts >= q.ts").unwrap().contains("ASOF JOIN q MATCH_CONDITION (pondra_duckdb_asof_left) ON"));
        assert_eq!(text("SELECT * FROM t USING SAMPLE 10% WHERE x > 1").unwrap(), "SELECT * FROM t WHERE pondra_sample('10%') AND x > 1");
        assert_eq!(text("SELECT * FROM t USING SAMPLE 5 ROWS").unwrap(), "SELECT * FROM t WHERE pondra_sample('5 ROWS') ");
        assert_eq!(text("PIVOT t ON g USING sum(x) GROUP BY id").unwrap(), "SELECT * FROM (SELECT id, g, x FROM t) AS __pivot PIVOT (sum(x) FOR g IN (ANY)) ");
        assert_eq!(text("UNPIVOT t ON a, b INTO NAME k VALUE v ORDER BY 1").unwrap(), "SELECT * FROM t UNPIVOT (v FOR k IN (a, b)) ORDER BY 1");
        assert_eq!(text("SELECT 'a lambda: no', 1").unwrap(), "SELECT 'a lambda: no', 1");
        assert_eq!(
            text("ALTER TABLE t SET TBLPROPERTIES (write.delete.mode = 'merge-on-read', 'a.b' = '1', c = 2)").unwrap(),
            "ALTER TABLE t SET TBLPROPERTIES ('write.delete.mode' = 'merge-on-read', 'a.b' = '1', c = 2)"
        );
        assert_eq!(text("SELECT tblproperties.x FROM tblproperties").unwrap(), "SELECT tblproperties.x FROM tblproperties");
        // (SQL Server's @@version at a code position, any case; not in a string or a comment; another @@x refused by name)
        assert_eq!(text("SELECT @@VERSION AS version, @@Version").unwrap(), "SELECT version() AS version, version()");
        assert_eq!(text("SELECT '@@x', 1 -- @@y").unwrap(), "SELECT '@@x', 1 -- @@y");
        assert!(text("SELECT @@spid").unwrap_err().to_string().contains("@@spid is SQL Server's: Pondra's settings are SHOW spid"));
    }

    #[test]
    fn a_call_written_as_an_expression_keeps_its_place_inside_an_operator() {
        // (11-03 audit H4: `SAFE_DIVIDE(10, 100) / SAFE_DIVIDE(5, 100)` was written `CAST(10 AS DOUBLE) / nullif(100, 0) /
        // CAST(5 AS DOUBLE) / nullif(100, 0)`, 0.0002, and `x - DATEADD(day, 1, d)` subtracted d and added the day)
        let all = |sql: &str| {
            let mut e = expr(sql).unwrap();
            let _ = crate::walk::visit_expressions_mut(&mut e, |x| match expression(x) {
                Ok(()) => ControlFlow::Continue(()),
                Err(e) => ControlFlow::Break(e),
            });
            e.to_string()
        };
        assert_eq!(all("x - DATEADD(day, 1, d)"), "x - ((d) + CAST(1 AS BIGINT) * INTERVAL '1 day')");
        assert_eq!(all("ts - TIMESTAMP_TRUNC(ts, WEEK)"), "ts - (date_trunc('day', ts) - CAST(date_part('dow', ts) AS BIGINT) * INTERVAL '1 day')");
        // (a call that stays a call, or a cast, needs none; SAFE_DIVIDE is a function of its own since the 11-03 audit's L8)
        assert_eq!(all("8 / SAFE_DIVIDE(a, b)"), "8 / SAFE_DIVIDE(a, b)");
        assert_eq!(all("2 * ZEROIFNULL(n)"), "2 * coalesce(n, 0)");
        assert_eq!(all("1 + DATE(ts)"), "1 + CAST(ts AS DATE)");
    }

    #[test]
    fn top_is_a_name_unless_it_is_sql_servers_top() {
        // (a LATERAL named top is an alias: quoted, so it parses; SELECT TOP n, TOP (n) and TOP … PERCENT are left as they are)
        let lateral = "SELECT c.name, top.amount FROM customers c LEFT JOIN LATERAL (SELECT o.amount FROM orders o WHERE o.customer_id = c.id ORDER BY o.amount DESC LIMIT 1) top ON true";
        let read = text(lateral).unwrap();
        assert!(read.contains(r#""top".amount"#) && read.contains(r#") "top" ON true"#), "{read}");
        assert!(Parser::parse_sql(&GenericDialect {}, &read).is_ok());
        assert_eq!(text("SELECT TOP 3 * FROM t").unwrap(), "SELECT TOP 3 * FROM t");
        assert_eq!(text("SELECT DISTINCT TOP (2) x FROM t").unwrap(), "SELECT DISTINCT TOP (2) x FROM t");
        assert_eq!(text("SELECT TOP 10 PERCENT x FROM t").unwrap(), "SELECT TOP 10 PERCENT x FROM t");
    }

    #[test]
    fn duckdbs_dates_and_patterns_and_the_part_first_datediff() {
        let call = |sql: &str| {
            let mut e = expr(sql).unwrap();
            expression(&mut e).map(|_| e.to_string())
        };
        assert_eq!(call("strftime(created_at, '%Y-%m-%d')").unwrap(), "to_char(created_at, '%Y-%m-%d')");
        assert_eq!(call("strftime('%Y', created_at)").unwrap(), "to_char(created_at, '%Y')");
        assert_eq!(call("strptime('01/03/2026', '%d/%m/%Y')").unwrap(), "to_timestamp('01/03/2026', '%d/%m/%Y')");
        assert_eq!(call("epoch(ts)").unwrap(), "date_part('epoch', ts)");
        assert_eq!(call("split(tags, ',')").unwrap(), "string_to_array(tags, ',')");
        assert_eq!(call("regexp_extract(email, '@(.+)$', 1)").unwrap(), "coalesce(regexp_match(email, '(@(.+)$)')[2], CASE WHEN (email) IS NOT NULL THEN '' END)");
        assert_eq!(call("regexp_extract(email, p)").unwrap(), "coalesce(regexp_match(email, concat('(', p, ')'))[1], CASE WHEN (email) IS NOT NULL THEN '' END)");
        assert!(call("date_diff('week', a, b)").unwrap().starts_with("CAST(trunc(round((date_part('epoch', date_trunc('day', b))"));
        assert_eq!(call("date_diff('day', a, b)").unwrap(), "CAST(round((date_part('epoch', date_trunc('day', b)) - date_part('epoch', date_trunc('day', a))) / 86400) AS BIGINT)");
        assert_eq!(call("DATEDIFF(month, a, b)").unwrap(), "CAST((date_part('year', b) * 12 + date_part('month', b)) - (date_part('year', a) * 12 + date_part('month', a)) AS BIGINT)");
        assert_eq!(call("DATEADD(week, 2, ts)").unwrap(), "((ts) + CAST(2 AS BIGINT) * 7 * INTERVAL '1 day')");
        assert_eq!(call("date_add(d, INTERVAL 7 DAY)").unwrap(), "((d) + INTERVAL 7 DAY)");
        assert_eq!(call("date_sub(d, INTERVAL '1 month')").unwrap(), "((d) - INTERVAL '1 month')");
        // (Spark's own: its days as an INT, once; a column holding the part is left as written)
        assert_eq!(call("date_add(d, 7)").unwrap(), "date_add(d, CAST(7 AS INT))");
        assert_eq!(call("date_add(d, CAST(7 AS INT))").unwrap(), "date_add(d, CAST(7 AS INT))");
        assert_eq!(call("datediff(b, a)").unwrap(), "datediff(b, a)");
        assert_eq!(call("date_diff(unit, a, b)").unwrap(), "date_diff(unit, a, b)");
        let m = call("date_diff('m', a, b)").unwrap_err();
        assert!(crate::codes::of(&m) == "22023" && m.to_string().contains("'m' is DuckDB's minute and SQL Server's month"), "{m:#}");
        assert!(call("DATEDIFF(y, a, b)").unwrap_err().to_string().contains("'y' is DuckDB's year and SQL Server's day of the year"));
        assert!(call("date_diff('fortnight', a, b)").unwrap_err().to_string().contains("'fortnight' isn't a date part here"));
        assert!(wanted("SELECT strftime(ts, '%Y') FROM t") && wanted("SELECT DATEDIFF (day, a, b) FROM t") && !wanted("SELECT epoch FROM t"));
        // (11-01 audit M6, M7: DuckDB's and SQLite's codes as chrono's; a part-first week refused by name)
        assert_eq!(call("strftime(ts, '%S.%f %g %n %c|%x %-d %%')").unwrap(), "to_char(ts, '%S.%6f %3f %9f %Y-%m-%d %H:%M:%S|%Y-%m-%d %-d %%')");
        assert_eq!(call("strftime('%H:%M:%f %s', ts)").unwrap(), "to_char(ts, '%H:%M:%S%.3f %s')");
        assert_eq!(call("strptime(s, '%d.%m.%Y %H:%M:%S.%f')").unwrap(), "to_timestamp(s, '%d.%m.%Y %H:%M:%S.%6f')");
        let f = call("strftime(ts, '%F')").unwrap_err();
        assert!(crate::codes::of(&f) == "22023" && f.to_string().contains("%F isn't one of DuckDB's format codes"), "{f:#}");
        let w = call("DATEDIFF(week, a, b)").unwrap_err();
        assert!(crate::codes::of(&w) == "22023" && w.to_string().contains("Sundays crossed in SQL Server and Mondays in Snowflake"), "{w:#}");
        assert!(call("datediff('week', a, b)").is_ok()); // (DuckDB's, the part a string)
    }

    #[test]
    fn top_group_concat_array_agg_limit_and_other_databases_types() {
        let call = |sql: &str| {
            let mut e = expr(sql).unwrap();
            expression(&mut e).map(|_| e.to_string())
        };
        assert_eq!(call("GROUP_CONCAT(status ORDER BY id SEPARATOR ', ')").unwrap(), "string_agg(status, ', ' ORDER BY id)");
        assert_eq!(call("group_concat(status)").unwrap(), "string_agg(status, ',')");
        assert_eq!(call("ARRAY_AGG(x ORDER BY y DESC LIMIT 2)").unwrap(), "array_slice(ARRAY_AGG(x ORDER BY y DESC), 1, 2)");
        assert_eq!(call("CAST(x AS INT64)").unwrap(), "CAST(x AS BIGINT)");
        assert_eq!(call("x::NUMBER(10,2)").unwrap(), "x::DECIMAL(10,2)");
        assert_eq!(call("CAST(x AS NUMBER)").unwrap(), "CAST(x AS DECIMAL(38,0))");
        assert_eq!(call("CAST(x AS FLOAT64)").unwrap(), "CAST(x AS DOUBLE)");
        let mut q = query("SELECT TOP 3 id FROM t ORDER BY id DESC").unwrap();
        top(&mut q).unwrap();
        assert_eq!(q.to_string(), "SELECT id FROM t ORDER BY id DESC LIMIT 3");
        let mut q = query("SELECT TOP (2) PERCENT id FROM t").unwrap();
        let e = top(&mut q).unwrap_err();
        assert!(e.to_string().contains("PERCENT: not taken; LIMIT n") && crate::codes::of(&e) == "0A000", "{e:#}"); // (11-01 audit L1: coded, not XX000)
        let mut q = query("SELECT TOP 2 id FROM t LIMIT 3").unwrap();
        assert_eq!(crate::codes::of(&top(&mut q).unwrap_err()), "42601");
        assert!(wanted("SELECT TOP 3 id FROM t") && wanted("SELECT CAST(x AS INT64)") && wanted("SELECT GROUP_CONCAT(s) FROM t") && !wanted("SELECT topping FROM t"));
    }

    #[test]
    fn duckdbs_type_names_are_pondras_and_others_are_refused_by_name() {
        let call = |sql: &str| {
            let mut e = expr(sql).unwrap();
            expression(&mut e).map(|_| e.to_string())
        };
        assert_eq!(call("CAST(x AS DATETIME)").unwrap(), "CAST(x AS TIMESTAMP)");
        // (looked for only where a type stands: `year` or `money` as a column's name is no reason to parse)
        assert!(!wanted("SELECT year, long, short, money FROM t WHERE bit = 1"));
        assert!(wanted("SELECT CAST(x AS ubigint) FROM t") && wanted("SELECT x::Datetime FROM t") && wanted("SELECT DATETIME '2026-01-01 10:00'"));
        assert!(wanted("CREATE TABLE t (a INT, b MONEY)") && wanted("SELECT 1; ALTER TABLE t ADD COLUMN y HUGEINT") && wanted("-- made\nCREATE TABLE t (y year)"));
        assert_eq!(call("CAST(x AS UBIGINT)").unwrap(), "CAST(x AS BIGINT UNSIGNED)");
        assert_eq!(call("CAST(x AS UINTEGER)").unwrap(), "CAST(x AS INT UNSIGNED)");
        assert_eq!(call("CAST(x AS TIMESTAMP_MS)").unwrap(), "CAST(x AS TIMESTAMP(3))");
        assert_eq!(call("x::BLOB").unwrap(), "x::BYTEA");
        assert_eq!(call("CAST(x AS LOGICAL)").unwrap(), "CAST(x AS BOOLEAN)");
        let e = call("CAST(x AS HUGEINT)").unwrap_err();
        assert!(crate::codes::of(&e) == "0A000" && e.to_string().contains("HUGEINT is DuckDB's 128-bit integer: write DECIMAL(38, 0)"), "{e:#}");
        assert!(call("CAST(x AS DATETIME2)").unwrap_err().to_string().contains("DATETIME2 is SQL Server's: write TIMESTAMP"));
        assert!(call("CAST(x AS TIME WITH TIME ZONE)").unwrap_err().to_string().contains("TIMETZ is a time of day with a zone"));
        assert!(wanted("CREATE TABLE t (a UBIGINT, b DATETIME)") && wanted("SELECT CAST(x AS hugeint)") && !wanted("SELECT datetimes FROM t"));
    }

    #[test]
    fn within_group_is_the_aggregates_own_order() {
        // (SQL Server's and Snowflake's string_agg and listagg WITHIN GROUP: the ORDER BY moves into the arguments, as
        // string_agg(x, sep ORDER BY y) has it; another aggregate keeps its WITHIN GROUP)
        let call = |sql: &str| {
            let mut e = expr(sql).unwrap();
            expression(&mut e).map(|_| e.to_string())
        };
        assert_eq!(call("STRING_AGG(status, ', ') WITHIN GROUP (ORDER BY id)").unwrap(), "string_agg(status, ', ' ORDER BY id)");
        assert_eq!(call("LISTAGG(status, ',') WITHIN GROUP (ORDER BY id DESC)").unwrap(), "string_agg(status, ',' ORDER BY id DESC)");
        assert_eq!(call("percentile_cont(0.5) WITHIN GROUP (ORDER BY amount)").unwrap(), "percentile_cont(0.5) WITHIN GROUP (ORDER BY amount)");
        assert!(wanted("SELECT STRING_AGG(status, ', ') WITHIN GROUP (ORDER BY id) FROM orders"));
    }

    #[test]
    fn parenthesized_queries() {
        // (dbt's `AS (SELECT …)`: the parentheses come off, as many as there are, and nothing else does)
        let mut q = query("((SELECT a FROM t))").unwrap();
        unparen(&mut q);
        assert_eq!(q.to_string(), "SELECT a FROM t");
        let mut q = query("(SELECT a FROM t ORDER BY a)").unwrap();
        unparen(&mut q);
        assert_eq!(q.to_string(), "SELECT a FROM t ORDER BY a");
        let mut q = query("(SELECT a FROM t) ORDER BY a").unwrap();
        unparen(&mut q);
        assert!(q.to_string().starts_with("(SELECT"), "{q}");
    }

    #[test]
    fn exponents_bytea_and_like_escapes() {
        // (a number with an exponent is its plain digits, exactly; None when it has none)
        assert_eq!(plain_decimal("1E-7").unwrap().as_deref(), Some("0.0000001"));
        assert_eq!(plain_decimal("-2.5e3").unwrap().as_deref(), Some("-2500"));
        assert_eq!(plain_decimal("0001e2").unwrap().as_deref(), Some("100"));
        assert_eq!(plain_decimal("12.5").unwrap(), None);
        // (a bytea literal in either of Postgres's forms is its X'…': `\xff00`, and the escape form's `a\\b`, whose
        // `\\` is one backslash; a refusal is Postgres's own code and message, and the gate looks at the word)
        let bytea = |sql: &str| typed_literal(&mut expr(sql).unwrap()).unwrap();
        assert_eq!((bytea("CAST('\\xff00' AS BYTEA)"), bytea("CAST('a\\\\b' AS BYTEA)")), (Some("X'ff00'".to_string()), Some("X'615c62'".to_string())));
        assert_eq!(typed_literal(&mut expr("CAST('a\\b' AS BYTEA)").unwrap()).unwrap_err().to_string(), "invalid input syntax for type bytea");
        assert!(wanted("SELECT 'a\\\\b'::bytea"));
        // (LIKE's pattern with `/` as the escape, as backslash: `/%` is a literal %, `//` a literal /)
        assert_eq!(backslash_pattern("a/%b", Some('/')).unwrap(), "a\\%b");
        assert_eq!(backslash_pattern("a//b", Some('/')).unwrap(), "a/b");
        assert_eq!(backslash_pattern("a\\b", Some('/')).unwrap(), "a\\\\b");
        assert_eq!(backslash_pattern("a\\b", None).unwrap(), "a\\\\b");
        assert!(backslash_pattern("a/", Some('/')).is_err());
        // (Postgres's N'…' is a string; the gate sees it after a space or a bracket, and not inside a word or a string)
        let mut n = expr("N'gift'").unwrap();
        expression(&mut n).unwrap();
        assert_eq!(n.to_string(), "'gift'");
        assert!(wanted("INSERT INTO t VALUES (1, N'gift')") && wanted("SELECT n'x'"));
        assert!(!wanted("SELECT 'ann' AS x") && !wanted("SELECT 'it''s' AS x"));
    }

    #[test]
    fn sha_functions_take_a_bytea_literal() {
        // (Postgres's sha functions take only a bytea: a string literal in their call is one, in either form; a column,
        // another function and a literal that isn't a bytea are as Postgres answers them)
        let call = |sql: &str| {
            let mut e = expr(sql).unwrap();
            expression(&mut e).map(|_| e.to_string())
        };
        assert_eq!(call("sha256('\\x01')").unwrap(), "sha256(X'01')");
        assert_eq!(call("encode('\\x01', 'hex')").unwrap(), "encode(X'01', 'hex')");
        assert_eq!(call("get_byte('ab', 0)").unwrap(), "get_byte(X'6162', 0)");
        assert_eq!(call("decode('ff', 'hex')").unwrap(), "decode('ff', 'hex')");
        assert_eq!(call("sha512('a\\\\b')").unwrap(), "sha512(X'615c62')");
        assert_eq!(call("sha256(t.a)").unwrap(), "sha256(t.a)");
        assert_eq!(call("md5('\\x01')").unwrap(), "md5('\\x01')");
        let refusal = call("sha256('\\x0')").unwrap_err();
        assert_eq!((crate::codes::of(&refusal), refusal.to_string()), ("22023", "invalid hexadecimal data: odd number of digits".to_string()));
        assert!(wanted("SELECT sha256('\\x01')"));
    }

    #[test]
    fn an_update_or_delete_has_an_alias_only_when_one_is_written() {
        // (the word after a DELETE's or UPDATE's table is an alias unless it is one that can follow the table there)
        for sql in [
            "UPDATE k SET v = v + 1 WHERE id = 1",
            r#"update "s"."k" set v = 0"#,
            "DELETE FROM k WHERE id = 1",
            "DELETE FROM k USING j WHERE k.id = j.id",
            "DELETE FROM k RETURNING id",
        ] {
            assert!(!wanted(sql), "{sql}");
        }
        for sql in ["UPDATE k AS u SET v = 0", "UPDATE k u SET v = 0", "DELETE FROM k AS d WHERE d.id = 1", "DELETE FROM k d WHERE d.id = 1"] {
            assert!(wanted(sql), "{sql}");
        }
    }

    #[test]
    fn paths() {
        assert_eq!(json_path("$.a.b[0]").unwrap(), ["'a'", "'b'", "0"]);
        assert_eq!(json_path("/a/1").unwrap(), ["'a'", "1"]);
        assert_eq!(json_path("$").unwrap(), Vec::<String>::new());
        assert!(json_path("$.a[*]").is_err());
    }

    #[test]
    fn json_arrows_read_jsonpaths_and_other_databases_names_are_refused() {
        let call = |sql: &str| {
            let mut e = expr(sql).unwrap();
            expression(&mut e).map(|_| e.to_string())
        };
        // (DuckDB's and MySQL's JSONPath after the arrows; Postgres's key after `->>` is as it was)
        assert_eq!(call("j->>'$.a.b'").unwrap(), "json_as_text(j, 'a', 'b')");
        assert_eq!(call("j->'$[0].x'").unwrap(), "json_get_json(j, 0, 'x')");
        assert_eq!(call("j->>'$'").unwrap(), "json_as_text(j)");
        assert_eq!(call("j->'$'").unwrap(), "j");
        assert_eq!(call("j->>'page'").unwrap(), "j ->> 'page'");
        assert_eq!(call("doc->'_id'->>'$oid'").unwrap(), "doc -> '_id' ->> '$oid'"); // (a key starting with `$`, MongoDB's: Postgres's key)
        assert_eq!(call("s->>'$ref'").unwrap(), "s ->> '$ref'");
        // (Spark's get_json_object and Spark's transform; a transform whose second argument isn't a lambda is as it was)
        assert_eq!(call("get_json_object(j, '$.a')").unwrap(), "json_as_text(j, 'a')");
        assert_eq!(call("transform(l, x -> x + 1)").unwrap(), "array_transform(l, x -> x + 1)");
        assert_eq!(call("transform(l, 1)").unwrap(), "transform(l, 1)");
        // (Snowflake's path, and BigQuery's SAFE. prefix, refused with what to write)
        let refusal = call("p:a.b").unwrap_err();
        assert!(crate::codes::of(&refusal) == "0A000" && refusal.to_string().contains("p->>'a'") && refusal.to_string().contains("json_extract_string(p, '$.a.b')"), "{refusal:#}");
        let refusal = call("LAMBDA (x, i): x").unwrap_err();
        assert!(crate::codes::of(&refusal) == "42601" && refusal.to_string().contains("LAMBDA x, i: …"), "{refusal:#}");
        let refusal = call("SAFE.PARSE_DATE('%Y', s)").unwrap_err();
        assert!(crate::codes::of(&refusal) == "0A000" && refusal.to_string().contains("SAFE.PARSE_DATE(…) is BigQuery's"), "{refusal:#}");
        // (the look: a colon after a word and before a letter is a path, not `x::INT` nor a parameter `:p`)
        assert!(wanted("SELECT p:a FROM t") && !wanted("SELECT x::INT FROM t") && !wanted("SELECT * FROM t WHERE a = :p"));
        assert!(wanted("SELECT payload->>'$.a', payload -> '$.a' FROM t") && !wanted("SELECT payload->>'page' FROM t"));
        assert!(wanted("SELECT SAFE.PARSE_DATE('%Y', s) FROM t") && wanted("SELECT PARSE_JSON(p) FROM t") && wanted("SELECT format(ts, 'yyyy') FROM t"));
    }

    #[test]
    fn array_literals_are_make_array() {
        // (where SQL comes in, an array literal is the `make_array` call it stands for, nested ones too;
        // a subscript is still a subscript)
        let mut stmts = Parser::parse_sql(&GenericDialect {}, "SELECT [1, [z.n]], ARRAY[z.n], [], arr[1], [10, 20, 30][2] FROM z").unwrap();
        arrays(&mut stmts[0]);
        assert_eq!(stmts[0].to_string(), "SELECT make_array(1, make_array(z.n)), make_array(z.n), make_array(), arr[1], make_array(10, 20, 30)[2] FROM z");
        // (a COLUMNS list is read as written, before the statement's arrays are rewritten)
        let stmts = Parser::parse_sql(&GenericDialect {}, "SELECT COLUMNS(['a', b]) FROM t").unwrap();
        let Statement::Query(q) = &stmts[0] else { panic!("a query") };
        let SetExpr::Select(s) = &*q.body else { panic!("a select") };
        let SelectItem::UnnamedExpr(e) = &s.projection[0] else { panic!("an item") };
        assert!(matches!(columns_call(e), Some(Pattern::Names(n)) if n == ["a", "b"]));
    }

    #[test]
    fn lambdas_read() {
        let s = lambdas("SELECT array_filter(l, x -> x > 1 AND x < 5), j -> 'a' FROM t").unwrap().to_string();
        assert!(s.contains("x -> x > 1 AND x < 5"), "{s}");
        assert!(lambdas("SELECT j -> 'a' FROM t").is_none());
    }
}

