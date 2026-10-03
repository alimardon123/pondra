//! Friendly SQL, where SQL comes in (round 34; the parity list in
//! `designs/scripting-design-review.md`): what users of DuckDB, Snowflake and Postgres type and
//! DataFusion doesn't plan, rewritten into SQL it does before anything else sees it
//! (`routines::expand`), as `FROM`-first and `ASOF JOIN` already are. A view, a frame, a spread
//! query and every door then get plain SQL.
//!
//! - In the text, before it is parsed (`text`): `LAMBDA x: …` and list comprehensions
//!   (`[x * 2 FOR x IN l IF x > 0]`) as `x -> …`; DuckDB's `PIVOT` and `UNPIVOT` statements as the
//!   standard's; DuckDB's `ASOF JOIN … ON a.t >= b.t` as Snowflake's; `USING SAMPLE`.
//! - In the syntax tree (`rewrite`): `FETCH FIRST`, `ORDER BY ALL`, `({…}).a`, `max_by` and
//!   `arg_max`, `string_split`, `::json`, `json_extract`, `PIVOT`, `UNPIVOT`, `TABLESAMPLE` (which
//!   DataFusion ignored), `COLUMNS(…)`, `* RENAME`, a select's alias in its WHERE.
//! - Whole statements: `SUMMARIZE` (`summarize`).
//! - Where a query is planned (`lambdas`): a higher-order function's `x -> …` as a lambda.
//!
//! What needs the data (a PIVOT's values, a FROM's columns) is asked with a query of its own, as
//! the caller, before the tree is rewritten: a first pass collects the questions, a second takes
//! the answers in the same order (`Asks`).
use crate::store::Lake;
use anyhow::{bail, ensure, Context, Result};
use datafusion::sql::sqlparser::ast::*;
use datafusion::sql::sqlparser::{dialect::GenericDialect, keywords::Keyword, parser::Parser, tokenizer::{Token, Tokenizer}};
use regex::Regex;
use std::borrow::Cow;
use std::collections::{HashMap, HashSet, VecDeque};
use std::ops::ControlFlow;
use std::sync::LazyLock;

fn expr(sql: &str) -> Result<Expr> { Ok(Parser::new(&GenericDialect {}).try_with_sql(sql)?.parse_expr()?) }
fn query(sql: &str) -> Result<Query> { Ok(*Parser::new(&GenericDialect {}).try_with_sql(sql)?.parse_query()?) }
fn quote(s: &str) -> String { format!("'{}'", s.replace('\'', "''")) }
fn name(s: &str) -> String { format!("\"{}\"", s.replace('"', "\"\"")) }

/// Whether `sql` may hold any of them: a look before anything is parsed.
pub fn wanted(sql: &str) -> bool {
    static WORDS: LazyLock<Regex> = LazyLock::new(|| {
        Regex::new(r"(?i)\b(?:fetch|order\s+by\s+all|max_by|min_by|arg_max|arg_min|string_split|str_split|jsonb?|json_extract\w*|json_value|tablesample|sample|pondra_sample|pivot|unpivot|rename|apply|list_apply|array_apply)\b|\b(?:columns|filter|list)\s*\(|\)\s*\.\s*[a-z_]").expect("a regex")
    });
    WORDS.is_match(sql) || alias_in_where(sql)
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

// ---------------------------------------------------------------- the text

/// `sql` with what the parser can't read as it is written turned into what it can.
pub fn text(sql: &str) -> Result<Cow<'_, str>> {
    static LOOK: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"(?i)\blambda\b|\[[^\]]*\bfor\b|^\s*(?:--[^\n]*\n\s*)*(?:pivot|unpivot)\b|\basof\b|\busing\s+sample\b").expect("a regex"));
    if !LOOK.is_match(sql) {
        return Ok(Cow::Borrowed(sql));
    }
    let mut s = statements(sql)?.unwrap_or_else(|| sql.to_string());
    s = comprehensions(&s)?;
    for pass in [lambda_words, asof_on, using_sample] {
        if let Some(t) = Tokens::of(&s) {
            s = t.edit(pass(&t)?);
        }
    }
    Ok(Cow::Owned(s))
}

/// The tokens of a text, with where each starts, so edits keep the rest of it as written.
struct Tokens<'a> {
    sql: &'a str,
    toks: Vec<Token>,
    at: Vec<usize>,
}

impl<'a> Tokens<'a> {
    fn of(sql: &'a str) -> Option<Self> {
        let spans = Tokenizer::new(&GenericDialect {}, sql).tokenize_with_location().ok()?;
        let lines: Vec<usize> = std::iter::once(0).chain(sql.match_indices('\n').map(|(i, _)| i + 1)).collect();
        let at = spans.iter().map(|t| {
            let start = lines.get(t.span.start.line.saturating_sub(1) as usize).copied().unwrap_or(sql.len());
            sql[start..].char_indices().nth(t.span.start.column.saturating_sub(1) as usize).map_or(sql.len(), |(i, _)| start + i)
        });
        let at: Vec<usize> = at.collect();
        Some(Tokens { sql, toks: spans.into_iter().map(|t| t.token).collect(), at })
    }
    fn end(&self, i: usize) -> usize { self.at.get(i + 1).copied().unwrap_or(self.sql.len()) }
    fn span(&self, from: usize, to: usize) -> &'a str { &self.sql[self.at.get(from).copied().unwrap_or(self.sql.len())..self.at.get(to).copied().unwrap_or(self.sql.len())] }
    /// The next token after `i` that isn't whitespace.
    fn next(&self, i: usize) -> usize {
        (i + 1..self.toks.len()).find(|&j| !matches!(self.toks[j], Token::Whitespace(_))).unwrap_or(self.toks.len())
    }
    fn word(&self, i: usize, k: Keyword) -> bool { matches!(self.toks.get(i), Some(Token::Word(w)) if w.keyword == k && w.quote_style.is_none()) }
    fn edit(&self, mut edits: Vec<(usize, usize, String)>) -> String {
        let mut out = self.sql.to_string();
        edits.sort_by_key(|e| std::cmp::Reverse(e.0));
        for (from, to, with) in edits {
            out.replace_range(from..to, &with);
        }
        out
    }
}

/// DuckDB's `LAMBDA x, y: body` as `(x, y) -> body`.
fn lambda_words(t: &Tokens) -> Result<Vec<(usize, usize, String)>> {
    let mut edits = Vec::new();
    for i in (0..t.toks.len()).filter(|&i| t.word(i, Keyword::LAMBDA)) {
        let (mut j, mut params) = (t.next(i), Vec::new());
        let parens = t.toks.get(j) == Some(&Token::LParen);
        if parens {
            j = t.next(j);
        }
        while let Some(Token::Word(w)) = t.toks.get(j) {
            params.push(w.to_string());
            j = t.next(j);
            if t.toks.get(j) != Some(&Token::Comma) {
                break;
            }
            j = t.next(j);
        }
        if parens && t.toks.get(j) == Some(&Token::RParen) {
            j = t.next(j);
        }
        if params.is_empty() || t.toks.get(j) != Some(&Token::Colon) {
            continue;
        }
        let params = if params.len() == 1 { params[0].clone() } else { format!("({})", params.join(", ")) };
        edits.push((t.at[i], t.end(j), format!("{params} ->")));
    }
    Ok(edits)
}

/// List comprehensions, `[expr FOR x IN list IF cond]`, as `array_transform(array_filter(list,
/// x -> cond), x -> expr)`, inside out.
fn comprehensions(sql: &str) -> Result<String> {
    let Some(t) = Tokens::of(sql) else { return Ok(sql.to_string()) };
    for i in (0..t.toks.len()).filter(|&i| t.toks[i] == Token::LBracket) {
        let (mut depth, mut parts) = (0i32, Vec::new());
        let mut close = None;
        for j in i + 1..t.toks.len() {
            match &t.toks[j] {
                Token::LBracket | Token::LParen | Token::LBrace => depth += 1,
                Token::RBracket if depth == 0 => {
                    close = Some(j);
                    break;
                }
                Token::RBracket | Token::RParen | Token::RBrace => depth -= 1,
                Token::Word(w) if depth == 0 && w.quote_style.is_none() && matches!(w.keyword, Keyword::FOR | Keyword::IN | Keyword::IF) => parts.push((w.keyword, j)),
                _ => {}
            }
        }
        let Some(close) = close else { continue };
        let [(Keyword::FOR, f), (Keyword::IN, n), rest @ ..] = parts.as_slice() else { continue };
        let var = t.span(*f + 1, *n).trim();
        ensure!(Regex::new(r"^[A-Za-z_][A-Za-z0-9_]*$").expect("a regex").is_match(var), "a list comprehension takes one name: [… FOR x IN list]");
        let cond = rest.iter().find(|(k, _)| *k == Keyword::IF).map(|(_, c)| *c);
        let list = comprehensions(t.span(*n + 1, cond.unwrap_or(close)).trim())?;
        let body = comprehensions(t.span(i + 1, *f))?;
        let list = match cond {
            Some(c) => format!("array_filter({list}, {var} -> ({}))", comprehensions(t.span(c + 1, close).trim())?),
            None => format!("({list})"),
        };
        let done = format!("array_transform({list}, {var} -> ({}))", body.trim());
        return Ok(format!("{}{done}{}", &sql[..t.at[i]], comprehensions(&sql[t.end(close)..])?));
    }
    Ok(sql.to_string())
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
            edits.push((t.end(i), t.at[j], " ".into()));
        }
        edits.push((t.at[on], t.at[on], format!("MATCH_CONDITION ({}) ", if left { "pondra_duckdb_asof_left" } else { "pondra_duckdb_asof" })));
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
            true => edits.push((t.at[i], t.at.get(t.next(end)).copied().unwrap_or(t.sql.len()), format!("WHERE pondra_sample({spec}) AND "))),
            false => edits.push((t.at[i], t.at.get(end).copied().unwrap_or(t.sql.len()), format!("WHERE pondra_sample({spec}) "))),
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
    let Some((on_at, _)) = found[0] else { bail!("{}: ON names the column", if pivot { "PIVOT" } else { "UNPIVOT" }) };
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
            let _ = visit_expressions(&aggs, |e| {
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
    with: Vec<Option<With>>,
    orders: Vec<Option<usize>>,
    selects: Vec<Option<Vec<Column>>>,
    names: Vec<Vec<Option<String>>>,
}

/// `stmts` with every friendly form in them rewritten, asking the lake what they need to know;
/// whether any was.
pub async fn rewrite(lake: &Lake, stmts: &mut [Statement]) -> Result<bool> {
    let before = stmts.to_vec();
    let mut f = Friendly { asks: Asks::Collect(Vec::new()), with: Vec::new(), orders: Vec::new(), selects: Vec::new(), names: Vec::new() };
    // (a stored view keeps its text: it is expanded as it is read, as its functions are)
    let kept = |s: &Statement| matches!(s, Statement::CreateMacro { .. } | Statement::CreateView(CreateView { materialized: false, .. }));
    for s in stmts.iter_mut().filter(|s| !kept(s)) {
        if let ControlFlow::Break(e) = s.visit(&mut f) {
            return Err(e);
        }
    }
    let Asks::Collect(asks) = std::mem::replace(&mut f.asks, Asks::Apply(VecDeque::new())) else { unreachable!() };
    let mut answers = VecDeque::new();
    for a in asks {
        answers.push_back(Box::pin(answer(lake, a)).await?); // (its query is expanded too)
    }
    f.asks = Asks::Apply(answers);
    for s in stmts.iter_mut().filter(|s| !kept(s)) {
        if let ControlFlow::Break(e) = s.visit(&mut f) {
            return Err(e);
        }
    }
    Ok(before != stmts)
}

/// The answer to an ask, from a query run as the caller (itself expanded: it may hold friendly
/// SQL too).
async fn answer(lake: &Lake, ask: Ask) -> Result<Vec<Column>> {
    let (sql, values) = match ask {
        Ask::Values(q) => (q, true),
        Ask::Columns(q) => (q, false),
    };
    let sql = crate::routines::expand(lake, &sql).await?;
    let sql = crate::asof::rewrite(&sql)?.into_owned();
    let ctx = crate::query::session(lake, &sql, "").await?;
    let df = crate::query::sql(&ctx, &sql).await?;
    if !values {
        return Ok(df.schema().iter().map(|(q, f)| Column { table: q.map(|q| q.table().to_string()), name: f.name().clone(), numeric: f.data_type().is_numeric(), sql_type: sql_type(f.data_type()) }).collect());
    }
    let mut out = Vec::new();
    for b in df.collect().await? {
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
    /// A query over `from`, under every WITH it is inside.
    fn over(&self, sql: String) -> String {
        let ctes: Vec<String> = self.with.iter().flatten().flat_map(|w| w.cte_tables.iter().map(|c| c.to_string())).collect();
        let recursive = self.with.iter().flatten().any(|w| w.recursive);
        match ctes.is_empty() {
            true => sql,
            false => format!("WITH {}{} {sql}", if recursive { "RECURSIVE " } else { "" }, ctes.join(", ")),
        }
    }
}

impl VisitorMut for Friendly {
    type Break = anyhow::Error;

    fn pre_visit_query(&mut self, q: &mut Query) -> ControlFlow<anyhow::Error> {
        self.with.push(q.with.clone());
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
        or_break(fetch(q).and_then(|_| order_by_all(q, n)))
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
        if let (false, Statement::CreateTable(c)) = (self.collecting(), s) {
            for col in c.columns.iter_mut() {
                if matches!(col.data_type, DataType::JSON | DataType::JSONB) {
                    col.data_type = DataType::Text; // (JSON is text, as VARIANT is: `->`, `->>`, json_get read it)
                }
            }
        }
        ControlFlow::Continue(())
    }
}

fn or_break(r: Result<()>) -> ControlFlow<anyhow::Error> { r.map_or_else(ControlFlow::Break, ControlFlow::Continue) }

/// `FETCH FIRST n ROWS ONLY` as `LIMIT n`.
fn fetch(q: &mut Query) -> Result<()> {
    let Some(f) = q.fetch.take() else { return Ok(()) };
    ensure!(!f.with_ties && !f.percent, "FETCH FIRST … {}: not taken; LIMIT n", if f.percent { "PERCENT" } else { "WITH TIES" });
    let n = f.quantity.unwrap_or_else(|| expr("1").expect("a literal"));
    match &mut q.limit_clause {
        None => q.limit_clause = Some(LimitClause::LimitOffset { limit: Some(n), offset: None, limit_by: vec![] }),
        Some(LimitClause::LimitOffset { limit: l @ None, .. }) => *l = Some(n),
        _ => bail!("FETCH FIRST and LIMIT together"),
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

fn function_name(f: &Function) -> String { f.name.0.last().and_then(|p| p.as_ident()).map(|i| i.value.to_lowercase()).unwrap_or_default() }

fn args(f: &Function) -> Vec<&Expr> {
    match &f.args {
        FunctionArguments::List(l) => l.args.iter().filter_map(|a| match a {
            FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => Some(e),
            _ => None,
        }).collect(),
        _ => vec![],
    }
}

/// One expression's own forms.
fn expression(e: &mut Expr) -> Result<()> {
    match e {
        // `({'a': 1}).a`, `f(x).a`: a field of a struct that isn't a column, as `…['a']`
        Expr::CompoundFieldAccess { root, access_chain } if !matches!(**root, Expr::Identifier(_) | Expr::CompoundIdentifier(_)) => {
            for a in access_chain.iter_mut() {
                if let AccessExpr::Dot(Expr::Identifier(i)) = a {
                    *a = AccessExpr::Subscript(Subscript::Index { index: expr(&quote(&i.value))? });
                }
            }
        }
        Expr::Cast { data_type, .. } if matches!(data_type, DataType::JSON | DataType::JSONB) => *data_type = DataType::Text,
        Expr::Function(f) => {
            let n = function_name(f);
            let a = args(f);
            match (n.as_str(), a.len()) {
                ("max_by" | "arg_max" | "min_by" | "arg_min", 2) => {
                    ensure!(f.over.is_none(), "{n}(…) OVER (…): first_value(a) OVER (… ORDER BY b DESC) is the window's form");
                    let (desc, (x, by)) = (n.starts_with("max") || n == "arg_max", (a[0].to_string(), a[1].to_string()));
                    let only = match &f.filter {
                        Some(w) => format!("({w}) AND ({by}) IS NOT NULL"),
                        None => format!("({by}) IS NOT NULL"),
                    };
                    *e = expr(&format!("first_value({x} ORDER BY {by} {} NULLS LAST) FILTER (WHERE {only})", if desc { "DESC" } else { "ASC" }))?;
                }
                ("string_split" | "str_split", 2) => f.name = ObjectName::from(vec![Ident::new("string_to_array")]),
                ("list", 1) => f.name = ObjectName::from(vec![Ident::new("array_agg")]), // (DuckDB's name for it)
                ("apply" | "list_apply" | "array_apply", 2) => f.name = ObjectName::from(vec![Ident::new("array_transform")]),
                ("filter", 2) if matches!(a[1], Expr::BinaryOp { op: BinaryOperator::Arrow, .. }) => f.name = ObjectName::from(vec![Ident::new("array_filter")]),
                ("json_extract" | "json_extract_string" | "json_extract_path_text" | "json_value", 2) => {
                    let Expr::Value(ValueWithSpan { value: Value::SingleQuotedString(path), .. }) = a[1] else { bail!("{n}(json, path): the path is a literal, such as '$.a.b'; json_get(json, key, …) takes expressions") };
                    let keys = json_path(path)?.join(", ");
                    let call = if n == "json_extract" { "json_get_json" } else { "json_as_text" };
                    *e = expr(&match keys.is_empty() {
                        true => a[0].to_string(),
                        false => format!("{call}({}, {keys})", a[0]),
                    })?;
                }
                _ => {}
            }
        }
        _ => {}
    }
    Ok(())
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

/// A table factor's `TABLESAMPLE` / `SAMPLE`, which DataFusion ignores, as a subquery that samples.
fn table_sample(t: &mut TableFactor) -> Result<()> {
    let TableFactor::Table { sample, alias, name, .. } = t else { return Ok(()) };
    let s = match sample.take() {
        Some(TableSampleKind::BeforeTableAlias(s) | TableSampleKind::AfterTableAlias(s)) => s,
        None => return Ok(()),
    };
    ensure!(s.seed.is_none() && s.bucket.is_none() && s.offset.is_none(), "a sample with a seed, a bucket or an offset isn't taken");
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
        let _ = visit_expressions(&a.expr, |e| {
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
    let found = |e: &Expr| visit_expressions(e, |e| match columns_call(e) {
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
    let _ = visit_expressions_mut(&mut e, |x| {
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
        let _ = visit_expressions(e, |x| {
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
    let _ = visit_expressions(w, |e| {
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
        let _ = w.visit(&mut Put(&used, 0));
    }
    Ok(())
}

// ---------------------------------------------------------------- SUMMARIZE

/// DuckDB's `SUMMARIZE t` (or `SUMMARIZE SELECT …`): a row per column (its type, least and
/// greatest value, distinct values about, mean, standard deviation, quartiles, count and share
/// of NULLs), from one pass over the rows.
pub async fn summarize(lake: &Lake, sql: &str) -> Result<Option<String>> {
    static SUMMARIZE: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"(?is)^\s*summarize\s+(.+?)\s*;?\s*$").expect("a regex"));
    let Some(m) = SUMMARIZE.captures(sql) else { return Ok(None) };
    let what = m[1].trim();
    let from = match Regex::new(r"(?i)^(select|from|with|values)\b").expect("a regex").is_match(what) {
        true => format!("({what}) AS __summarized"),
        false => what.to_string(),
    };
    let cols = Box::pin(answer(lake, Ask::Columns(format!("SELECT * FROM {from} LIMIT 0")))).await?;
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
/// arrow, which `->` elsewhere still is), so such a query is planned from this tree (`query::sql`),
/// the same way on every node. None for any other text.
pub fn lambdas(sql: &str) -> Option<datafusion::sql::parser::Statement> {
    if !sql.contains("->") {
        return None;
    }
    let mut stmts = Parser::parse_sql(&GenericDialect {}, sql).ok()?;
    let [stmt @ Statement::Query(_)] = stmts.as_mut_slice() else { return None };
    let mut found = false;
    let _ = visit_expressions_mut(stmt, |e| {
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
        assert_eq!(text("SELECT list_transform(l, LAMBDA x: x + 1)").unwrap(), "SELECT list_transform(l, x -> x + 1)");
        assert_eq!(text("SELECT [x * 2 FOR x IN l IF x > 1] FROM t").unwrap(), "SELECT array_transform(array_filter(l, x -> (x > 1)), x -> (x * 2)) FROM t");
        assert_eq!(text("SELECT [[y FOR y IN x] FOR x IN l]").unwrap(), "SELECT array_transform((l), x -> (array_transform((x), y -> (y))))");
        assert!(text("SELECT * FROM t ASOF JOIN q ON t.k = q.k AND t.ts >= q.ts").unwrap().contains("MATCH_CONDITION (pondra_duckdb_asof) ON"));
        assert!(text("SELECT * FROM t ASOF LEFT JOIN q ON t.ts >= q.ts").unwrap().contains("ASOF JOIN q MATCH_CONDITION (pondra_duckdb_asof_left) ON"));
        assert_eq!(text("SELECT * FROM t USING SAMPLE 10% WHERE x > 1").unwrap(), "SELECT * FROM t WHERE pondra_sample('10%') AND x > 1");
        assert_eq!(text("SELECT * FROM t USING SAMPLE 5 ROWS").unwrap(), "SELECT * FROM t WHERE pondra_sample('5 ROWS') ");
        assert_eq!(text("PIVOT t ON g USING sum(x) GROUP BY id").unwrap(), "SELECT * FROM (SELECT id, g, x FROM t) AS __pivot PIVOT (sum(x) FOR g IN (ANY)) ");
        assert_eq!(text("UNPIVOT t ON a, b INTO NAME k VALUE v ORDER BY 1").unwrap(), "SELECT * FROM t UNPIVOT (v FOR k IN (a, b)) ORDER BY 1");
        assert_eq!(text("SELECT 'a lambda: no', 1").unwrap(), "SELECT 'a lambda: no', 1");
    }

    #[test]
    fn paths() {
        assert_eq!(json_path("$.a.b[0]").unwrap(), ["'a'", "'b'", "0"]);
        assert_eq!(json_path("/a/1").unwrap(), ["'a'", "1"]);
        assert_eq!(json_path("$").unwrap(), Vec::<String>::new());
        assert!(json_path("$.a[*]").is_err());
    }

    #[test]
    fn lambdas_read() {
        let s = lambdas("SELECT array_filter(l, x -> x > 1 AND x < 5), j -> 'a' FROM t").unwrap().to_string();
        assert!(s.contains("x -> x > 1 AND x < 5"), "{s}");
        assert!(lambdas("SELECT j -> 'a' FROM t").is_none());
    }
}
