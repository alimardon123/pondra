//! `MATCH_RECOGNIZE` (ADR-073), rewritten where SQL comes in into plain SQL around the window functions `pondra_match`,
//! `pondra_matches` and `pondra_nav` (`matcher.rs`), so the query after it is ordinary SQL: the result cache, a spread
//! query, a view's stored SQL and history see nothing new. The clause becomes a derived table of layers, each reading
//! the one before it:
//!
//! - L1: the table's rows, with each symbol's DEFINE as a Boolean column, the ORDER BY keys, and the PREV and NEXT
//!   columns (`lag` and `lead` over the partition);
//! - L2: `pondra_match` over the partition (`pondra_matches` where matches may share rows): each row's match, its
//!   symbol and its place in the match;
//! - L3: the rows that are in a match (all of them, WITH UNMATCHED ROWS), with those as columns (an UNNEST first where
//!   matches may share rows);
//! - L4: ONE ROW PER MATCH groups them into one row per match; ALL ROWS PER MATCH keeps each row, with its measures as
//!   window columns over its match (RUNNING, or FINAL over the whole match);
//! - L5 (ALL ROWS only): the partition and ORDER BY columns, the measures, then the input's other columns, without the
//!   rows a `{- … -}` leaves out and the internal columns.
//!
//! `text` writes FINAL and RUNNING before a measure as calls that parse, and each PATTERN as one quoted name, since
//! sqlparser refuses some standard forms; `matcher::read` reads the text. What isn't taken is refused by name.
use crate::codes::coded;
use crate::matcher::Pattern;
use crate::text::{ident, object, ROW_ID};
use anyhow::{bail, Result};
use datafusion::sql::sqlparser::ast::*;
use datafusion::sql::sqlparser::{dialect::GenericDialect, parser::Parser, tokenizer::Token};
use regex::Regex;
use serde_json::json;
use std::borrow::Cow;
use std::collections::{BTreeSet, HashMap};
use std::fmt::Display;
use std::ops::ControlFlow;
use std::sync::LazyLock;

/// The aggregates a measure may take over the rows of a variable (their FILTER).
const AGGREGATES: &[&str] = &[
    "count", "sum", "avg", "min", "max", "array_agg", "string_agg", "bool_and", "bool_or", "every", "stddev", "stddev_pop",
    "stddev_samp", "variance", "var_pop", "var_samp", "median", "approx_distinct", "approx_median",
];

/// The functions that navigate the match: they are refused where a navigation can't be taken.
const NAVIGATIONS: &[&str] = &["first", "last", "prev", "next", "classifier", "match_number"];

/// Whether `sql` may hold a `MATCH_RECOGNIZE`: a look before anything is parsed.
pub fn wanted(sql: &str) -> bool {
    static WORD: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"(?i)\bmatch_recognize\b").expect("a regex"));
    WORD.is_match(sql)
}

/// `sql` as the parser reads it, with two rewrites. FINAL and RUNNING before a call are wrapped as calls that parse
/// (`FINAL last(x)` is `pondra_final(last(x))`; the rewrite takes the wrapper away, since ONE ROW PER MATCH reads at the
/// last row, where both agree). Each PATTERN is written as one quoted name that holds its text in hex, because sqlparser
/// refuses forms standard SQL has (`{- b -}` of more than a symbol, PERMUTE of alternatives); `matcher::read` reads the
/// text instead. A word in a string or a comment is left as it is.
pub fn text(sql: &str) -> Cow<'_, str> {
    if !wanted(sql) {
        return Cow::Borrowed(sql);
    }
    let code = code_mask(sql);
    let mut edits = calls(sql, &code);
    edits.extend(patterns(sql, &code));
    if edits.is_empty() {
        return Cow::Borrowed(sql);
    }
    edits.sort_by_key(|e| e.0);
    let mut out = String::new();
    let mut last = 0;
    for (start, end, new) in edits {
        if start < last {
            continue; // inside an edit already made: a FINAL call in a PATTERN is read as the pattern
        }
        out.push_str(&sql[last..start]);
        out.push_str(&new);
        last = end;
    }
    out.push_str(&sql[last..]);
    Cow::Owned(out)
}

/// The FINAL and RUNNING calls of `sql`, each as its `pondra_` call: (start, end, the call).
fn calls(sql: &str, code: &[bool]) -> Vec<(usize, usize, String)> {
    static CALL: LazyLock<Regex> =
        LazyLock::new(|| Regex::new(r"(?i)\b(final|running)\s+([a-z_][a-z0-9_]*)\s*\(").expect("a regex"));
    CALL.captures_iter(sql)
        .filter_map(|c| {
            let whole = c.get(0)?;
            let name = c.get(2)?;
            if !code[whole.start()] {
                return None;
            }
            let close = close_of(sql, code, whole.end() - 1)?;
            Some((whole.start(), close + 1, format!("pondra_{}({})", c[1].to_lowercase(), &sql[name.start()..=close])))
        })
        .collect()
}

/// Each MATCH_RECOGNIZE's PATTERN, as `PATTERN ("__mr_pattern_<hex>")`: the one quoted name sqlparser takes, whatever the
/// pattern is. A WITHIN after it goes into the name too (`__within_<hex>`), as `within_at` reads it. `pattern_text` reads
/// the text back out.
fn patterns(sql: &str, code: &[bool]) -> Vec<(usize, usize, String)> {
    static CLAUSE: LazyLock<Regex> =
        LazyLock::new(|| Regex::new(r"(?i)(?P<clause>\bmatch_recognize\b)|\bpattern\s*(?P<open>\()").expect("a regex"));
    let mut out = vec![];
    let mut in_clause = false;
    let mut clause_end = sql.len();
    for c in CLAUSE.captures_iter(sql) {
        let whole = c.get(0).expect("a whole match");
        if !code[whole.start()] {
            continue;
        }
        if c.name("clause").is_some() {
            in_clause = true;
            // the clause's own parenthesis closes it: a WITHIN runs no further
            clause_end = (whole.end()..sql.len())
                .find(|&i| code[i] && sql.as_bytes()[i] == b'(')
                .and_then(|open| close_of(sql, code, open))
                .unwrap_or(sql.len());
            continue;
        }
        let Some(open) = c.name("open") else { continue };
        if !std::mem::take(&mut in_clause) {
            continue;
        }
        let Some(close) = close_of(sql, code, open.start()) else { continue };
        let text = &sql[open.start() + 1..close];
        if text.trim_start().starts_with("\"__mr_pattern_") {
            continue; // written already
        }
        let (end, name) = match within_at(sql, code, close + 1, clause_end) {
            Some((end, within)) => (end, format!("PATTERN (\"__mr_pattern_{}__within_{}\")", hex(text), hex(&within))),
            None => (close + 1, format!("PATTERN (\"__mr_pattern_{}\")", hex(text))),
        };
        out.push((whole.start(), end, name));
    }
    out
}

/// The WITHIN that follows a PATTERN's `)` at `from`, when one does: where it ends (the interval's last character, before
/// the DEFINE or the clause's end that comes first), and the interval as written. sqlparser can't read WITHIN, so it is
/// taken out here and carried in the pattern's name.
fn within_at(sql: &str, code: &[bool], from: usize, clause_end: usize) -> Option<(usize, String)> {
    static WITHIN: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"(?i)^\s*within\s+").expect("a regex"));
    static DEFINE: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"(?i)\bdefine\b").expect("a regex"));
    let at = from + WITHIN.find(&sql[from..clause_end])?.end();
    let stop = DEFINE.find_iter(&sql[at..clause_end]).map(|m| at + m.start()).find(|&p| code[p]).unwrap_or(clause_end);
    let interval = &sql[at..stop];
    Some((at + interval.trim_end().len(), interval.trim().to_string()))
}

/// WITHIN's interval as SQL: one interval literal (`INTERVAL '1' HOUR`), written into the call as parsed. Anything else (a
/// number, a subquery, or more SQL after the interval) is refused, so the text can't change the query's shape.
fn within_interval(text: &str) -> Result<String> {
    let refused = || coded("42601", "MATCH_RECOGNIZE: WITHIN takes an interval, as INTERVAL '1' HOUR");
    let dialect = GenericDialect {};
    let mut parser = Parser::new(&dialect).try_with_sql(text).map_err(|_| refused())?;
    let expr = parser.parse_expr().map_err(|_| refused())?;
    if !matches!(parser.peek_token().token, Token::EOF) {
        return Err(refused());
    }
    match &expr {
        Expr::Interval(Interval { value, .. })
            if matches!(value.as_ref(), Expr::Value(ValueWithSpan { value: Value::SingleQuotedString(_) | Value::Number(..), .. })) =>
        {
            Ok(expr.to_string())
        }
        _ => Err(refused()),
    }
}

/// `s` in hex: two digits a byte.
fn hex(s: &str) -> String {
    s.bytes().map(|b| format!("{b:02x}")).collect()
}

/// The text `hex` wrote, or None when `s` is not hex.
fn unhex(s: &str) -> Option<String> {
    let bytes: Vec<u8> = (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(s.get(i..i + 2)?, 16).ok())
        .collect::<Option<Vec<u8>>>()?;
    String::from_utf8(bytes).ok()
}

/// A clause's PATTERN as the text `matcher::read` reads, and its WITHIN's interval as written when it has one: the name
/// `patterns` wrote, or else sqlparser's form of the pattern. A name written before WITHIN has no `__within_` part (stored
/// views hold such names, so they still read).
fn pattern_text(p: &MatchRecognizePattern) -> (String, Option<String>) {
    if let MatchRecognizePattern::Symbol(MatchRecognizeSymbol::Named(id)) = p {
        if let Some(rest) = id.value.strip_prefix("__mr_pattern_") {
            let (pattern, within) = match rest.split_once("__within_") {
                Some((pattern, within)) => (pattern, unhex(within)),
                None => (rest, None),
            };
            if let Some(text) = unhex(pattern) {
                return (text, within);
            }
        }
    }
    (p.to_string(), None)
}

/// Which bytes of `sql` are code: not inside a string, a quoted name or a comment (so a word there is not read).
fn code_mask(sql: &str) -> Vec<bool> {
    let b = sql.as_bytes();
    let mut mask = vec![true; b.len()];
    let mut i = 0;
    while i < b.len() {
        let end = match (b[i], b.get(i + 1)) {
            (q @ (b'\'' | b'"'), _) => (i + 1..b.len()).find(|&j| b[j] == q).map_or(b.len(), |j| j + 1),
            (b'-', Some(b'-')) => (i..b.len()).find(|&j| b[j] == b'\n').unwrap_or(b.len()),
            (b'/', Some(b'*')) => sql[i + 2..].find("*/").map_or(b.len(), |j| i + 2 + j + 2),
            _ => {
                i += 1;
                continue;
            }
        };
        mask[i..end].fill(false);
        i = end;
    }
    mask
}

/// The `)` that closes the `(` at `open`, counting only the parentheses that are code.
fn close_of(sql: &str, code: &[bool], open: usize) -> Option<usize> {
    let mut depth = 0;
    for (i, c) in sql.bytes().enumerate().skip(open) {
        if !code[i] {
            continue;
        }
        match c {
            b'(' => depth += 1,
            b')' => {
                depth -= 1;
                if depth == 0 {
                    return Some(i);
                }
            }
            _ => {}
        }
    }
    None
}

/// Every `MATCH_RECOGNIZE` in `stmts`, as the derived table it stands for; whether any was. A stored view keeps its
/// clause (it is read again, as its functions are), and so does a macro (expanded where it is called).
pub async fn rewrite<H: crate::front::Host>(host: &H, stmts: &mut [Statement]) -> Result<bool> {
    let kept = |s: &Statement| matches!(s, Statement::CreateMacro { .. } | Statement::CreateView(CreateView { materialized: false, .. }));
    let mut scan = Scan::default();
    for s in stmts.iter().filter(|s| !kept(s)) {
        let _ = Visit::visit(s, &mut scan);
    }
    if !scan.found {
        return Ok(false);
    }
    // The lake inputs' columns, by the input's name: a lake table's rows that have `_row_id` are tied on their ORDER BY in
    // that order, so every run finds the same matches (a CTE of the same name is not the table), and a measure named like
    // one of the columns is refused. An input that isn't a lake table has no entry, so DataFusion's own errors stand.
    let lake = host.name();
    let mut local = BTreeSet::new();
    let mut named = vec![];
    for name in scan.inputs.difference(&scan.ctes) {
        if let Some(l) = crate::text::local_in(&lake, name) {
            local.insert(l.clone());
            named.push((name.clone(), l));
        }
    }
    let lake_cols: HashMap<String, Vec<String>> = match local.is_empty() {
        true => HashMap::new(),
        false => {
            let cols = host.columns(local).await?;
            named.into_iter().filter_map(|(n, l)| cols.get(&l).map(|c| (n, c.clone()))).collect()
        }
    };
    let mut r = Rewrite { lake: &lake_cols };
    for s in stmts.iter_mut().filter(|s| !kept(s)) {
        if let ControlFlow::Break(e) = VisitMut::visit(s, &mut r) {
            return Err(e);
        }
    }
    Ok(true)
}

/// The statements' clauses (outside the kept ones): whether any is there, and the names its inputs and CTEs have.
#[derive(Default)]
struct Scan {
    found: bool,
    inputs: BTreeSet<String>,
    ctes: BTreeSet<String>,
}

impl Visitor for Scan {
    type Break = ();

    fn pre_visit_query(&mut self, q: &Query) -> ControlFlow<()> {
        for c in q.with.iter().flat_map(|w| &w.cte_tables) {
            self.ctes.insert(ident(&c.alias.name));
        }
        ControlFlow::Continue(())
    }

    fn pre_visit_table_factor(&mut self, t: &TableFactor) -> ControlFlow<()> {
        if let TableFactor::MatchRecognize { table, .. } = t {
            self.found = true;
            if let TableFactor::Table { name, .. } = table.as_ref() {
                self.inputs.insert(object(name));
            }
        }
        ControlFlow::Continue(())
    }
}

/// Replaces each clause, innermost first (a clause's input is rewritten before the clause).
struct Rewrite<'a> {
    lake: &'a HashMap<String, Vec<String>>,
}

impl VisitorMut for Rewrite<'_> {
    type Break = anyhow::Error;

    fn post_visit_table_factor(&mut self, t: &mut TableFactor) -> ControlFlow<anyhow::Error> {
        if !matches!(t, TableFactor::MatchRecognize { .. }) {
            return ControlFlow::Continue(());
        }
        match layer(t, self.lake) {
            Ok(d) => {
                *t = d;
                ControlFlow::Continue(())
            }
            Err(e) => ControlFlow::Break(e),
        }
    }
}

/// How an expression of a clause is read: a DEFINE of one symbol, a measure, or inside a measure's aggregate.
#[derive(Clone, Copy, PartialEq)]
enum Mode {
    /// A DEFINE of the symbol at this index: its columns are the current row's.
    Define(usize),
    /// A measure: a column is the last matched row's value, and a navigation takes the rows it names.
    Measure,
    /// Inside an aggregate of a measure: a column is the aggregate's, over the rows the FILTER takes.
    Aggregate,
}

/// How a measure reads its match's rows in ALL ROWS PER MATCH: up to the row (RUNNING, the default), or all of them (FINAL).
#[derive(Clone, Copy, PartialEq)]
enum Frame {
    Running,
    Final,
}

/// What the translation of one clause knows: its symbols, how the expression at hand is read, the window the lag and
/// lead columns are over (the partition, and the order as written), and those columns, which L1 computes per row. `frame`
/// is set in ALL ROWS PER MATCH, where a measure is a window over its match's rows (partitioned by `part`, and the
/// match); None is ONE ROW PER MATCH, where it is over the match's group.
struct Cx {
    syms: Vec<String>,
    mode: Mode,
    window: String,
    nav: Vec<String>,
    frame: Option<Frame>,
    part: Vec<String>,
    /// The columns a DEFINE that reads the match hands the matcher, as L1 columns: `rows` are the row-only parts of its
    /// glue (`__mr_r{j}`), `values` the values its navigations and aggregates read (`__mr_v{j}`), each as its text.
    rows: Vec<String>,
    values: Vec<String>,
    /// The slots numbered so far (a slot is `__mr_s{index}` in its glue), and those of the DEFINE at hand, as the spec's.
    slots: usize,
    defined: Vec<serde_json::Value>,
    /// Set while a glue's aggregate values are written: a PREV or NEXT there is its window written out, since L1's own
    /// list can't read a `__mr_n` column it names.
    inline: bool,
}

/// One `MATCH_RECOGNIZE` as the derived table it stands for (the layers of the module doc).
fn layer(t: &TableFactor, lake: &HashMap<String, Vec<String>>) -> Result<TableFactor> {
    let TableFactor::MatchRecognize { table, partition_by, order_by, measures, rows_per_match, after_match_skip, pattern, symbols, alias } = t
    else {
        bail!("MATCH_RECOGNIZE: not a MATCH_RECOGNIZE clause");
    };
    let all = matches!(rows_per_match, Some(RowsPerMatch::AllRows(_)));
    let omit = matches!(rows_per_match, Some(RowsPerMatch::AllRows(Some(EmptyMatchesMode::Omit))));
    let unmatched = matches!(rows_per_match, Some(RowsPerMatch::AllRows(Some(EmptyMatchesMode::WithUnmatched))));
    let (text, within) = pattern_text(pattern);
    let (read_pattern, syms) = crate::matcher::read(&text)?;
    if unmatched && excludes(&read_pattern) {
        return Err(coded("42601", "MATCH_RECOGNIZE: WITH UNMATCHED ROWS can't go with {- … -}: a row left out would be neither in a match nor unmatched; use SHOW EMPTY MATCHES, or keep the rows in the pattern"));
    }
    // WITHIN reads the first ORDER BY as a time, ascending: its key is the first one, `__mr_o0`. Its interval goes into the
    // call as parsed, so nothing else of the text does.
    let within = match within {
        Some(text) => {
            match order_by.first() {
                None => return Err(coded("42601", "MATCH_RECOGNIZE: WITHIN reads the first ORDER BY: add one, a date or a timestamp")),
                Some(o) if o.options.asc == Some(false) => {
                    return Err(coded("0A000", "MATCH_RECOGNIZE: WITHIN reads the first ORDER BY ascending"));
                }
                Some(_) => {}
            }
            Some(within_interval(&text)?)
        }
        None => None,
    };
    // A lake table's columns (see `rewrite`): `_row_id` ties its rows, and ALL ROWS PER MATCH gives its columns beside the
    // measures, so a measure can't have the name of one of them
    let input = match table.as_ref() {
        TableFactor::Table { name, .. } => Some(object(name)),
        _ => None,
    };
    let input_cols = input.as_ref().and_then(|n| lake.get(n));
    if let (true, Some(cols), Some(table_name)) = (all, input_cols, &input) {
        for m in measures {
            let alias = ident(&m.alias);
            if cols.contains(&alias) {
                return Err(coded(
                    "42701",
                    format!("MATCH_RECOGNIZE: the measure {alias} has the name of a column of {table_name}; ALL ROWS PER MATCH gives both, so name the measure otherwise"),
                ));
            }
        }
    }
    let has_row = input_cols.is_some_and(|c| c.iter().any(|x| x == ROW_ID));
    let part_written: Vec<String> = partition_by.iter().map(|e| e.to_string()).collect();
    let part: Vec<String> = partition_by.iter().map(column_name).collect::<Result<Vec<String>>>()?;
    let order_names: Vec<String> = match all {
        true => order_by.iter().map(|o| order_column(&o.expr)).collect::<Result<Vec<String>>>()?,
        false => vec![],
    };
    let dirs: Vec<String> = order_by.iter().map(|o| direction(&o.options)).collect();
    let mut order_w: Vec<String> = order_by.iter().zip(&dirs).map(|(o, d)| format!("{}{d}", o.expr)).collect();
    if has_row {
        order_w.push(ROW_ID.to_string());
    }
    let frame = all.then_some(Frame::Running);
    let mut cx = Cx {
        syms,
        mode: Mode::Measure,
        window: over(&part_written, &order_w),
        nav: vec![],
        frame,
        part: part.clone(),
        rows: vec![],
        values: vec![],
        slots: 0,
        defined: vec![],
        inline: false,
    };
    let (conds, defines) = conditions(symbols, &text, &mut cx)?;
    let mut texts = vec![];
    for m in measures {
        texts.push(written(m.expr.clone(), &mut cx)?);
    }

    // L1: the rows, with the keys, the conditions, the glue's columns and the navigation columns
    let row = if has_row { ", _row_id AS __mr_row" } else { "" };
    let orders: String = order_by.iter().enumerate().map(|(i, o)| format!(", {} AS __mr_o{i}", o.expr)).collect();
    let defined: String = conds.iter().enumerate().map(|(k, c)| format!(", {c} AS __mr_d{k}")).collect();
    let glue_rows: String = cx.rows.iter().enumerate().map(|(j, r)| format!(", {r} AS __mr_r{j}")).collect();
    let glue_values: String = cx.values.iter().enumerate().map(|(j, v)| format!(", {v} AS __mr_v{j}")).collect();
    let navs: String = cx.nav.iter().map(|n| format!(", {n}")).collect();
    let l1 = format!("SELECT *{row}{orders}{defined}{glue_rows}{glue_values}{navs} FROM {table}");

    // L2: the window that matches the partition (`pondra_matches` where a row may be in several matches)
    let overlap = matches!(after_match_skip, Some(AfterMatchSkip::ToNextRow | AfterMatchSkip::ToFirst(_) | AfterMatchSkip::ToLast(_)));
    let skip = skip_json(after_match_skip.as_ref(), &cx.syms)?;
    let mut spec = json!({ "pattern": &text, "skip": skip, "unmatched": unmatched, "within": within.is_some() });
    if !defines.is_empty() {
        spec["defines"] = json!(defines);
        spec["rows"] = json!(cx.rows.len());
        spec["values"] = json!(cx.values.len());
    }
    let spec = spec.to_string();
    let mut call: String = (0..cx.syms.len()).map(|k| format!(", __mr_d{k}")).collect();
    if let Some(interval) = &within {
        call.push_str(&format!(", __mr_o0, {interval}"));
    }
    call.extend((0..cx.rows.len()).map(|j| format!(", __mr_r{j}")));
    call.extend((0..cx.values.len()).map(|j| format!(", __mr_v{j}")));
    let mut order2: Vec<String> = dirs.iter().enumerate().map(|(i, d)| format!("__mr_o{i}{d}")).collect();
    if has_row {
        order2.push("__mr_row".to_string());
    }
    let (fun, col) = match overlap {
        true => ("pondra_matches", "__mr_ms"),
        false => ("pondra_match", "__mr_m"),
    };
    let l2 = format!("SELECT *, {fun}({}{call}) {} AS {col} FROM ({l1}) AS __mr_c", quote(&spec), over(&part, &order2));

    // L3: the rows in a match, with its fields as columns. SHOW keeps the rows in a match, OMIT those with a place in it
    // (an empty match's row has none), WITH UNMATCHED ROWS all of them (a row in no match has NULL fields).
    let fields = "SELECT *, __mr_m['match'] AS __mr_match, __mr_m['symbol'] AS __mr_symbol, __mr_m['seq'] AS __mr_seq, __mr_m['excluded'] AS __mr_x";
    let keep = match (omit, unmatched) {
        (true, _) => " WHERE __mr_m['seq'] IS NOT NULL",
        (false, false) => " WHERE __mr_m IS NOT NULL",
        (false, true) => "",
    };
    let l3 = match overlap {
        true => format!("{fields} FROM (SELECT *, unnest(__mr_ms) AS __mr_m FROM ({l2}) AS __mr_mm) AS __mr_u{keep}"),
        false => format!("{fields} FROM ({l2}) AS __mr_mm{keep}"),
    };

    // L4 and L5: one row per match (ONE ROW), or a row per match row with its measures (ALL ROWS)
    let body = match all {
        false => {
            let mut group = part.clone();
            group.push("__mr_match".to_string());
            let mut proj = part.clone();
            proj.extend(texts.iter().zip(measures).map(|(t, m)| format!("{t} AS {}", m.alias)));
            if proj.is_empty() {
                return Err(coded("0A000", "MATCH_RECOGNIZE: no PARTITION BY and no MEASURES: no column to give"));
            }
            format!("SELECT {} FROM ({l3}) AS __mr_r GROUP BY {}", proj.join(", "), group.join(", "))
        }
        true => {
            // L4: each measure over the match's rows; WITH UNMATCHED ROWS, NULL where a row is in no match
            let mut sel = vec!["*".to_string()];
            for (i, t) in texts.iter().enumerate() {
                let wrapped = match unmatched {
                    true => format!("CASE WHEN __mr_match IS NOT NULL THEN ({t}) END"),
                    false => t.clone(),
                };
                sel.push(format!("{wrapped} AS __mr_q{i}"));
            }
            let l4 = format!("SELECT {} FROM ({l3}) AS __mr_r", sel.join(", "));

            // L5: the partition and order columns, the measures, then the input's other columns in their order, without
            // the internal columns and the rows that a `{- … -}` leaves out
            let mut own = part.clone();
            for c in &order_names {
                if !own.contains(c) {
                    own.push(c.clone());
                }
            }
            let mut hidden: Vec<String> = ["__mr_m", "__mr_match", "__mr_symbol", "__mr_seq", "__mr_x"].iter().map(|c| c.to_string()).collect();
            if overlap {
                hidden.push("__mr_ms".to_string());
            }
            if has_row {
                hidden.push("__mr_row".to_string());
            }
            hidden.extend((0..order_by.len()).map(|i| format!("__mr_o{i}")));
            hidden.extend((0..cx.syms.len()).map(|k| format!("__mr_d{k}")));
            hidden.extend((0..cx.rows.len()).map(|j| format!("__mr_r{j}")));
            hidden.extend((0..cx.values.len()).map(|j| format!("__mr_v{j}")));
            hidden.extend((0..cx.nav.len()).map(|j| format!("__mr_n{j}")));
            hidden.extend((0..texts.len()).map(|i| format!("__mr_q{i}")));
            let mut exclude = own.clone();
            exclude.extend(hidden);
            let mut out = own;
            out.extend(measures.iter().enumerate().map(|(i, m)| format!("__mr_q{i} AS {}", m.alias)));
            out.push(format!("* EXCLUDE ({})", exclude.join(", ")));
            format!("SELECT {} FROM ({l4}) AS __mr_a WHERE __mr_x IS NOT TRUE", out.join(", "))
        }
    };
    Ok(TableFactor::Derived { lateral: false, subquery: Box::new(crate::expander::parse_query(&body)?), alias: alias.clone(), sample: None })
}

/// Each symbol's condition as SQL over the rows: its DEFINE, read as the current row's, and `true` for a symbol with
/// none (a pattern may name a symbol that matches any row). A DEFINE that reads the match so far gives its row-only
/// conjuncts here and its other conjuncts as the glue the matcher evaluates (the spec's `defines`, one per such symbol).
fn conditions(symbols: &[SymbolDefinition], text: &str, cx: &mut Cx) -> Result<(Vec<String>, Vec<serde_json::Value>)> {
    let mut defs: Vec<Option<Expr>> = vec![None; cx.syms.len()];
    for d in symbols {
        let name = ident(&d.symbol);
        let Some(k) = cx.syms.iter().position(|s| *s == name) else {
            return Err(coded("42601", format!("MATCH_RECOGNIZE: DEFINE {name} names no symbol of PATTERN ({text})")));
        };
        if defs[k].is_some() {
            return Err(coded("42601", format!("MATCH_RECOGNIZE: DEFINE {name} is given twice")));
        }
        defs[k] = Some(d.definition.clone());
    }
    let mut out = vec![];
    let mut glued = vec![];
    for (k, def) in defs.into_iter().enumerate() {
        let Some(e) = def else {
            out.push("true".to_string());
            continue;
        };
        cx.mode = Mode::Define(k);
        let row = one_define(e, k, cx, &mut glued);
        cx.mode = Mode::Measure;
        out.push(row?);
    }
    Ok((out, glued))
}

/// One DEFINE of symbol `k`: its row-only conjuncts as the symbol's L1 condition (`true` when there are none), and its
/// match-reading conjuncts as one glue, pushed onto `glued` with its slots. A DEFINE that reads nothing of the match is
/// written as it always was.
fn one_define(e: Expr, k: usize, cx: &mut Cx, glued: &mut Vec<serde_json::Value>) -> Result<String> {
    let mut e = e;
    unwrap_running(&mut e)?;
    let parts = conjuncts(&e);
    let reads: Vec<bool> = parts.iter().map(|c| reads_match(c, k, cx)).collect();
    if !reads.contains(&true) {
        return written(e, cx);
    }
    let mut rows = vec![];
    let mut glue = vec![];
    for (c, r) in parts.into_iter().zip(reads) {
        match r {
            true => glue.push(c),
            false => rows.push(format!("({})", written(c, cx)?)),
        }
    }
    let row = match rows.is_empty() {
        true => "true".to_string(),
        false => rows.join(" AND "),
    };
    let outer = std::mem::take(&mut cx.defined);
    let mut texts = vec![];
    for c in glue {
        texts.push(format!("({})", glue_text(c, k, cx)?));
    }
    let slots = std::mem::replace(&mut cx.defined, outer);
    glued.push(json!({ "symbol": k, "glue": texts.join(" AND "), "slots": slots }));
    Ok(row)
}

/// RUNNING takes its argument as it is (a DEFINE reads the rows so far); FINAL is for MEASURES and is refused.
fn unwrap_running(e: &mut Expr) -> Result<()> {
    let mut err = None;
    let _ = visit_expressions_mut(e, |x| {
        let replacement = match x {
            Expr::Function(f) if lower_name(f) == "pondra_running" => match positional(f).and_then(|a| first_arg(&a, "RUNNING").cloned()) {
                Ok(inner) => Some(inner),
                Err(e) => {
                    err = Some(e);
                    return ControlFlow::Break(());
                }
            },
            Expr::Function(f) if lower_name(f) == "pondra_final" => {
                err = Some(coded("42601", "MATCH_RECOGNIZE: FINAL is for MEASURES: a DEFINE reads the rows so far, so drop FINAL"));
                return ControlFlow::Break(());
            }
            _ => None,
        };
        if let Some(inner) = replacement {
            *x = inner;
        }
        ControlFlow::Continue(())
    });
    match err {
        Some(e) => Err(e),
        None => Ok(()),
    }
}

/// The conjuncts of a condition: its top-level ANDs, each as an expression of its own.
fn conjuncts(e: &Expr) -> Vec<Expr> {
    match e {
        Expr::BinaryOp { left, op: BinaryOperator::And, right } => conjuncts(left).into_iter().chain(conjuncts(right)).collect(),
        Expr::Nested(x) => conjuncts(x),
        _ => vec![e.clone()],
    }
}

/// Whether a DEFINE's expression reads the match so far: another variable's column, a navigation, an aggregate, or
/// MATCH_NUMBER(). A PREV or NEXT of the row's own columns reads the row only, and so does CLASSIFIER().
fn reads_match(e: &Expr, k: usize, cx: &Cx) -> bool {
    visit_expressions(e, |x| {
        let reads = match x {
            Expr::CompoundIdentifier(p) => match p.as_slice() {
                [q, _] => cx.syms.iter().position(|s| *s == ident(q)).is_some_and(|j| j != k),
                _ => false,
            },
            Expr::Function(f) => {
                let name = lower_name(f);
                matches!(name.as_str(), "first" | "last" | "match_number") || AGGREGATES.contains(&name.as_str())
            }
            _ => false,
        };
        match reads {
            true => ControlFlow::Break(()),
            false => ControlFlow::Continue(()),
        }
    })
    .is_break()
}

/// The glue of a match-reading conjunct, as SQL over `__mr_r{j}` and `__mr_s{index}` (see `glue_node`).
fn glue_text(e: Expr, k: usize, cx: &mut Cx) -> Result<String> {
    let mut x = e;
    let mut glue = Glue { cx, k, err: None };
    let _ = VisitMut::visit(&mut x, &mut glue);
    if let Some(err) = glue.err {
        return Err(err);
    }
    Ok(x.to_string())
}

/// Walks a glue's expression in order, putting each node `glue_node` gives a name in place of the node.
struct Glue<'a> {
    cx: &'a mut Cx,
    k: usize,
    err: Option<anyhow::Error>,
}

impl VisitorMut for Glue<'_> {
    type Break = ();

    fn pre_visit_expr(&mut self, e: &mut Expr) -> ControlFlow<()> {
        match glue_node(&*e, self.k, &mut *self.cx) {
            Ok(None) => ControlFlow::Continue(()),
            Ok(Some(name)) => {
                *e = Expr::Identifier(Ident::new(name));
                ControlFlow::Continue(())
            }
            Err(err) => {
                self.err = Some(err);
                ControlFlow::Break(())
            }
        }
    }
}

/// The glue's name for a sub-expression of a DEFINE of symbol `k`: a row-only part is `__mr_r{j}` (its text an L1 column),
/// a navigation or an aggregate is `__mr_s{index}` (its slot), and an operator or function over those is walked into
/// (None). A literal stays as it is in the glue.
fn glue_node(e: &Expr, k: usize, cx: &mut Cx) -> Result<Option<String>> {
    if matches!(e, Expr::Value(_)) {
        return Ok(None);
    }
    if !reads_match(e, k, cx) {
        return row_part(e, cx).map(Some);
    }
    match e {
        Expr::CompoundIdentifier(_) => nav_slot(e, false, 0, 0, k, cx).map(Some),
        Expr::Function(f) => function_slot(f, k, cx),
        Expr::Subquery(_) | Expr::Exists { .. } | Expr::InSubquery { .. } => {
            Err(coded("0A000", "MATCH_RECOGNIZE: a subquery in MEASURES or DEFINE isn't supported yet"))
        }
        _ => Ok(None),
    }
}

/// A row-only part of a DEFINE's glue: its text, written as the row's own DEFINE, is the next `__mr_r` column.
fn row_part(e: &Expr, cx: &mut Cx) -> Result<String> {
    let text = written(e.clone(), cx)?;
    cx.rows.push(text);
    Ok(format!("__mr_r{}", cx.rows.len() - 1))
}

/// A function of a DEFINE's glue that reads the match: a navigation, MATCH_NUMBER or an aggregate gives its slot;
/// anything else is walked into (None).
fn function_slot(f: &Function, k: usize, cx: &mut Cx) -> Result<Option<String>> {
    let name = lower_name(f);
    Ok(Some(match name.as_str() {
        "first" | "last" => {
            let args = positional(f)?;
            let base = first_arg(&args, if name == "first" { "FIRST" } else { "LAST" })?;
            let m = whole(args.get(1).copied())?.unwrap_or(0);
            nav_slot(base, name == "first", m, 0, k, cx)?
        }
        "prev" | "next" => {
            let args = positional(f)?;
            let e = first_arg(&args, if name == "prev" { "PREV" } else { "NEXT" })?;
            let n = whole(args.get(1).copied())?.unwrap_or(1) as i64;
            let offset = if name == "prev" { -n } else { n };
            // PREV(FIRST(a.x, m), n) is the navigation of FIRST with its offset; PREV(a.x) is PREV(LAST(a.x))
            match e {
                Expr::Function(g) if matches!(lower_name(g).as_str(), "first" | "last") => {
                    let inner = positional(g)?;
                    let base = first_arg(&inner, "FIRST or LAST")?;
                    let m = whole(inner.get(1).copied())?.unwrap_or(0);
                    nav_slot(base, lower_name(g) == "first", m, offset, k, cx)?
                }
                _ => nav_slot(e, false, 0, offset, k, cx)?,
            }
        }
        "match_number" => {
            let index = cx.slots;
            cx.slots += 1;
            cx.defined.push(json!({ "index": index, "kind": "match_number" }));
            format!("__mr_s{index}")
        }
        n if AGGREGATES.contains(&n) => agg_slot(f, n, k, cx)?,
        _ => return Ok(None),
    }))
}

/// The name of the first navigation or aggregate in `e` (lower case), when there is one.
fn nested(e: &Expr) -> Option<String> {
    let mut found = None;
    let _ = visit_expressions(e, |x| match x {
        Expr::Function(f) => {
            let name = lower_name(f);
            if NAVIGATIONS.contains(&name.as_str()) || AGGREGATES.contains(&name.as_str()) {
                found = Some(name);
                ControlFlow::Break(())
            } else {
                ControlFlow::Continue(())
            }
        }
        _ => ControlFlow::Continue(()),
    });
    found
}

/// A refusal of a DEFINE (SQLSTATE 0A000), named by its variable.
fn refuse_define(sym: &str, what: impl Display) -> anyhow::Error {
    coded("0A000", format!("MATCH_RECOGNIZE: DEFINE {sym}: {what}"))
}

/// A value a glue reads, as L1's column: its index among the `values`.
fn value_column(cx: &mut Cx, text: String) -> usize {
    cx.values.push(text);
    cx.values.len() - 1
}

/// A navigation of a DEFINE's glue: the value of `base`'s column at the first (or last) m-th row of its variable, moved
/// by `offset` physical rows (PREV is negative, NEXT positive). The slot is numbered, and its value column written.
fn nav_slot(base: &Expr, first: bool, m: u64, offset: i64, k: usize, cx: &mut Cx) -> Result<String> {
    let sym = cx.syms[k].clone();
    if let Some(inner) = nested(base) {
        let what = match inner.as_str() {
            "classifier" => "CLASSIFIER() inside a navigation isn't supported in a DEFINE yet: test CLASSIFIER() on its own, as CLASSIFIER() = 'b'".to_string(),
            n if AGGREGATES.contains(&n) => format!("{n} inside a navigation isn't supported in a DEFINE yet: take {n} on its own, as {n}(a.x) > 2"),
            _ => "a navigation inside a navigation isn't supported in a DEFINE yet: write PREV(LAST(a.x)) or FIRST(a.x, m), and nothing deeper".to_string(),
        };
        return Err(refuse_define(&sym, what));
    }
    let var = variable_of(base, cx)?;
    let value = value_column(cx, plain(base));
    let index = cx.slots;
    cx.slots += 1;
    cx.defined.push(json!({ "index": index, "kind": "nav", "var": var, "first": first, "m": m, "offset": offset, "value": value }));
    Ok(format!("__mr_s{index}"))
}

/// An aggregate of a DEFINE's glue over the rows of its variable (every row of the match, when it has none): count, sum,
/// avg, min or max, of a value the glue reads. The aggregate's own FILTER, ORDER BY and DISTINCT are refused.
fn agg_slot(f: &Function, name: &str, k: usize, cx: &mut Cx) -> Result<String> {
    let sym = cx.syms[k].clone();
    if f.over.is_some() {
        return Err(refuse_define(&sym, format!("a window ({name} … OVER) isn't supported in a DEFINE yet: write the test over the match, as {name}(a.*) > 2")));
    }
    if f.filter.is_some() {
        return Err(refuse_define(&sym, format!("{name}'s FILTER isn't supported in a DEFINE yet: give those rows their own variable, and {name} that one")));
    }
    if has_order_by(f) || !f.within_group.is_empty() {
        return Err(refuse_define(&sym, format!("{name}'s ORDER BY isn't supported in a DEFINE yet: drop it: the rows come in the match's order")));
    }
    if matches!(&f.args, FunctionArguments::List(l) if l.duplicate_treatment == Some(DuplicateTreatment::Distinct)) {
        return Err(refuse_define(&sym, format!("{name}(DISTINCT …) isn't supported in a DEFINE yet: drop DISTINCT")));
    }
    if !matches!(name, "count" | "sum" | "avg" | "min" | "max") {
        return Err(refuse_define(&sym, format!("{name} isn't supported in a DEFINE yet: use count, sum, avg, min or max over the match")));
    }
    let args: Vec<&FunctionArg> = match &f.args {
        FunctionArguments::List(list) => list.args.iter().collect(),
        _ => vec![],
    };
    let (var, value) = match args.as_slice() {
        [] | [FunctionArg::Unnamed(FunctionArgExpr::Wildcard)] if name == "count" => (None, None),
        [FunctionArg::Unnamed(FunctionArgExpr::QualifiedWildcard(n))] if name == "count" => (Some(symbol_named(&object(n), cx)?), None),
        [FunctionArg::Unnamed(FunctionArgExpr::Expr(e))] => {
            if let Some(inner) = nested(e) {
                return Err(refuse_define(&sym, format!("{inner} inside {name} isn't supported in a DEFINE yet: take {inner} on its own")));
            }
            let var = variable_of(e, cx)?;
            let saved = std::mem::replace(&mut cx.inline, true);
            let text = inside(e.clone(), cx);
            cx.inline = saved;
            (var, Some(text?))
        }
        _ => return Err(refuse_define(&sym, format!("{name} takes one expression of the rows, as {name}(a.x)"))),
    };
    let value = value.map(|text| value_column(cx, text));
    let index = cx.slots;
    cx.slots += 1;
    cx.defined.push(json!({ "index": index, "kind": "agg", "fun": name, "var": var, "value": value }));
    Ok(format!("__mr_s{index}"))
}

/// A PARTITION BY item as the column it names (its last part, when it is qualified).
fn column_name(e: &Expr) -> Result<String> {
    let last = match e {
        Expr::Identifier(i) => Some(i),
        Expr::CompoundIdentifier(p) => p.last(),
        _ => None,
    };
    last.map(|i| i.to_string()).ok_or_else(|| {
        coded("0A000", format!("MATCH_RECOGNIZE: PARTITION BY takes columns; compute {e} in a subquery and partition by its name"))
    })
}

/// An ORDER BY item of ALL ROWS PER MATCH as the column it names: its rows carry the ORDER BY columns, not expressions.
fn order_column(e: &Expr) -> Result<String> {
    column_name(e).map_err(|_| {
        coded("0A000", format!("MATCH_RECOGNIZE: ALL ROWS PER MATCH gives the ORDER BY columns; compute {e} in a subquery and order by its name"))
    })
}

/// An ORDER BY item's direction and NULLS placement, as SQL (ascending and the engine's NULLS order by default).
fn direction(o: &OrderByOptions) -> String {
    let asc = if o.asc == Some(false) { " DESC" } else { "" };
    let nulls = match o.nulls_first {
        Some(true) => " NULLS FIRST",
        Some(false) => " NULLS LAST",
        None => "",
    };
    format!("{asc}{nulls}")
}

/// An `OVER (…)` clause: the partition, then the order.
fn over(partition: &[String], order: &[String]) -> String {
    let mut parts = vec![];
    if !partition.is_empty() {
        parts.push(format!("PARTITION BY {}", partition.join(", ")));
    }
    if !order.is_empty() {
        parts.push(format!("ORDER BY {}", order.join(", ")));
    }
    format!("OVER ({})", parts.join(" "))
}

/// The rows a navigation or aggregate takes, as a FILTER: the variable's rows, or every row of the match.
fn filter_of(var: Option<usize>, cx: &Cx) -> String {
    match var {
        Some(k) => format!("__mr_symbol = {}", quote(&cx.syms[k])),
        None => "__mr_seq IS NOT NULL".to_string(),
    }
}

fn quote(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

/// The symbol a clause's expression reads its columns through, when they all read one (`down.price` reads `down`); None
/// when it reads none, which a navigation takes as every row of the match.
fn variable_of<V: Visit + Display>(v: &V, cx: &Cx) -> Result<Option<usize>> {
    let mut found: Vec<usize> = vec![];
    let mut err = None;
    let _ = visit_expressions(v, |e| {
        if let Expr::CompoundIdentifier(p) = e {
            if let [q, _] = p.as_slice() {
                match symbol_named(&ident(q), cx) {
                    Ok(k) if !found.contains(&k) => found.push(k),
                    Ok(_) => {}
                    Err(x) => {
                        err = Some(x);
                        return ControlFlow::Break(());
                    }
                }
            }
        }
        ControlFlow::Continue(())
    });
    if let Some(x) = err {
        return Err(x);
    }
    match found.as_slice() {
        [] => Ok(None),
        [k] => Ok(Some(*k)),
        [a, b, ..] => Err(coded(
            "42601",
            format!("MATCH_RECOGNIZE: {v} reads {} and {} at once: one variable a measure's function", cx.syms[*a], cx.syms[*b]),
        )),
    }
}

/// The index of a pattern variable, or the refusal that names it.
fn symbol_named(name: &str, cx: &Cx) -> Result<usize> {
    cx.syms.iter().position(|s| s == name).ok_or_else(|| {
        coded("42601", format!("MATCH_RECOGNIZE: {name} isn't a pattern variable ({}): name the column plainly or by a variable", cx.syms.join(", ")))
    })
}

/// An expression as SQL with its variables dropped (`down.price` is `price`): the column a navigation reads.
fn plain(e: &Expr) -> String {
    let mut e = e.clone();
    let _ = visit_expressions_mut(&mut e, |x| {
        if let Some(last) = match x {
            Expr::CompoundIdentifier(p) => p.last().cloned(),
            _ => None,
        } {
            *x = Expr::Identifier(last);
        }
        ControlFlow::<()>::Continue(())
    });
    e.to_string()
}

/// Whether an expression reads the match (a navigation, CLASSIFIER or MATCH_NUMBER) anywhere in it.
fn navigates(e: &Expr) -> bool {
    visit_expressions(e, |x| match x {
        Expr::Function(f) if NAVIGATIONS.contains(&lower_name(f).as_str()) => ControlFlow::Break(()),
        _ => ControlFlow::Continue(()),
    })
    .is_break()
}

fn lower_name(f: &Function) -> String { object(&f.name).to_lowercase() }

/// Whether a pattern has a `{- … -}` anywhere in it.
fn excludes(p: &Pattern) -> bool {
    match p {
        Pattern::Exclude(_) => true,
        Pattern::Seq(v) | Pattern::Alt(v) | Pattern::Permute(v) => v.iter().any(excludes),
        Pattern::Rep { body, .. } => excludes(body),
        Pattern::Sym(_) | Pattern::Start | Pattern::End => false,
    }
}

/// The arguments of a call that are plain expressions (a `*` or a `name => value` is refused).
fn positional(f: &Function) -> Result<Vec<&Expr>> {
    let FunctionArguments::List(list) = &f.args else { return Ok(vec![]) };
    list.args
        .iter()
        .map(|a| match a {
            FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => Ok(e),
            _ => Err(coded("0A000", format!("MATCH_RECOGNIZE: {} takes expressions", object(&f.name)))),
        })
        .collect()
}

fn first_arg<'a>(args: &[&'a Expr], what: &str) -> Result<&'a Expr> {
    args.first().copied().ok_or_else(|| coded("42601", format!("MATCH_RECOGNIZE: {what} takes an expression")))
}

/// A count of rows written as a whole number (`LAST(v.x, 2)`'s 2); None when there is none.
fn whole(e: Option<&Expr>) -> Result<Option<u64>> {
    match e {
        None => Ok(None),
        Some(Expr::Value(ValueWithSpan { value: Value::Number(n, _), .. })) if !n.is_empty() && n.bytes().all(|b| b.is_ascii_digit()) => {
            Ok(Some(n.parse()?))
        }
        Some(e) => Err(coded("0A000", format!("MATCH_RECOGNIZE: {e} isn't a whole number of rows"))),
    }
}

/// The column of an aggregate's `v.*` (`count(v.*)`), when the call has one.
fn qualified_star(f: &Function) -> Option<String> {
    let FunctionArguments::List(list) = &f.args else { return None };
    list.args.iter().find_map(|a| match a {
        FunctionArg::Unnamed(FunctionArgExpr::QualifiedWildcard(n)) => Some(object(n)),
        _ => None,
    })
}

/// A match-reading part of a DEFINE that reached the row's own path: `conditions` sends those to the glue, so this is
/// only a guard against a part it missed.
fn define_error(sym: &str, what: String) -> anyhow::Error {
    refuse_define(sym, format!("{what} reads the match so far, which this part of the DEFINE doesn't take"))
}

/// `x` as SQL, with each expression that `node` translates swapped for the translation, in parentheses (so the
/// translation keeps its place among operators).
fn written<T: VisitMut + Display>(mut x: T, cx: &mut Cx) -> Result<String> {
    let mut swap = Swap { cx, texts: vec![], err: None };
    let _ = VisitMut::visit(&mut x, &mut swap);
    if let Some(e) = swap.err {
        return Err(e);
    }
    let mut out = x.to_string();
    for (k, t) in swap.texts.iter().enumerate() {
        out = out.replace(&format!("__mr_p{k}__"), &format!("({t})"));
    }
    Ok(out)
}

/// `x` read inside an aggregate (see `Mode::Aggregate`).
fn inside<T: VisitMut + Display>(x: T, cx: &mut Cx) -> Result<String> {
    let saved = std::mem::replace(&mut cx.mode, Mode::Aggregate);
    let text = written(x, cx);
    cx.mode = saved;
    text
}

/// Walks an expression in order, putting a placeholder in for each node `node` translates (the walk does not go into
/// a translated node, so nothing is translated twice).
struct Swap<'a> {
    cx: &'a mut Cx,
    texts: Vec<String>,
    err: Option<anyhow::Error>,
}

impl VisitorMut for Swap<'_> {
    type Break = ();

    fn pre_visit_expr(&mut self, e: &mut Expr) -> ControlFlow<()> {
        match node(&*e, &mut *self.cx) {
            Ok(None) => ControlFlow::Continue(()),
            Ok(Some(t)) => {
                *e = Expr::Identifier(Ident::new(format!("__mr_p{}__", self.texts.len())));
                self.texts.push(t);
                ControlFlow::Continue(())
            }
            Err(err) => {
                self.err = Some(err);
                ControlFlow::Break(())
            }
        }
    }
}

/// The translation of one node in the current mode, or None when the node is written as it is (its parts are read).
fn node(e: &Expr, cx: &mut Cx) -> Result<Option<String>> {
    match e {
        Expr::Identifier(_) | Expr::CompoundIdentifier(_) => Ok(Some(column(e, cx)?)),
        Expr::Function(f) => function(f, cx),
        Expr::Subquery(_) | Expr::Exists { .. } | Expr::InSubquery { .. } => {
            Err(coded("0A000", "MATCH_RECOGNIZE: a subquery in MEASURES or DEFINE isn't supported yet"))
        }
        _ => Ok(None),
    }
}

/// A column in the current mode: in a DEFINE, the current row's (another symbol's is refused, as the match so far is);
/// inside an aggregate, the column itself; in a measure, the last matched row's value over its variable.
fn column(e: &Expr, cx: &mut Cx) -> Result<String> {
    let var = variable_of(e, cx)?;
    let col = plain(e);
    Ok(match cx.mode {
        Mode::Define(k) => match var {
            Some(v) if v != k => return Err(define_error(&cx.syms[k], e.to_string())),
            _ => col,
        },
        Mode::Aggregate => col,
        Mode::Measure => nav_text(false, None, &col, var, cx),
    })
}

/// A call in the current mode, or None when it is written as it is.
fn function(f: &Function, cx: &mut Cx) -> Result<Option<String>> {
    let name = lower_name(f);
    let mode = cx.mode;
    match mode {
        Mode::Define(k) => define_call(f, &name, k, cx),
        Mode::Measure => measure_call(f, &name, cx),
        Mode::Aggregate => aggregate_call(f, &name, cx),
    }
}

/// A call in a DEFINE's row-only part: PREV and NEXT are the row's own lag and lead, and CLASSIFIER() is the symbol being
/// defined, as a literal. The calls that read the match are the glue's (`function_slot`), so they are not written here.
fn define_call(f: &Function, name: &str, k: usize, cx: &mut Cx) -> Result<Option<String>> {
    Ok(Some(match name {
        "prev" | "next" => {
            let args = positional(f)?;
            let e = first_arg(&args, name)?;
            let n = whole(args.get(1).copied())?.unwrap_or(1);
            if variable_of(&f.args, cx)?.is_some_and(|v| v != k) {
                return Err(define_error(&cx.syms[k], f.to_string()));
            }
            format!("{}({}, {n}) {}", if name == "prev" { "lag" } else { "lead" }, plain(e), cx.window)
        }
        "classifier" => quote(&cx.syms[k]),
        "first" | "last" | "match_number" => return Err(define_error(&cx.syms[k], f.to_string())),
        n if AGGREGATES.contains(&n) => return Err(define_error(&cx.syms[k], f.to_string())),
        _ => return Ok(None),
    }))
}

/// A call in a measure: the navigations, CLASSIFIER and MATCH_NUMBER, the aggregates, and pondra_final and
/// pondra_running. In ONE ROW PER MATCH they read at the last row, where both agree; in ALL ROWS PER MATCH they set the
/// frame that the measure inside them is read by.
fn measure_call(f: &Function, name: &str, cx: &mut Cx) -> Result<Option<String>> {
    Ok(Some(match name {
        "first" | "last" => navigation(f, name == "first", cx)?,
        "prev" | "next" => measure_lag(f, name == "prev", cx)?,
        "classifier" => nav_text(false, None, "__mr_symbol", None, cx),
        "match_number" => "__mr_match".to_string(),
        "pondra_final" | "pondra_running" => {
            let args = positional(f)?;
            let e = first_arg(&args, name)?.clone();
            let saved = cx.frame;
            cx.frame = saved.map(|_| if name == "pondra_final" { Frame::Final } else { Frame::Running });
            let text = written(e, cx);
            cx.frame = saved;
            text?
        }
        n if AGGREGATES.contains(&n) => aggregate(f, cx)?,
        _ => return Ok(None),
    }))
}

/// A call inside an aggregate: PREV and NEXT are their lag columns; a navigation there is refused.
fn aggregate_call(f: &Function, name: &str, cx: &mut Cx) -> Result<Option<String>> {
    Ok(Some(match name {
        "first" | "last" => {
            return Err(coded("0A000", format!("MATCH_RECOGNIZE: {name} inside an aggregate isn't supported yet: aggregate the column over its variable instead")))
        }
        "prev" | "next" => {
            let args = positional(f)?;
            let e = first_arg(&args, name)?;
            if navigates(e) {
                return Err(coded("0A000", "MATCH_RECOGNIZE: a navigation inside an aggregate isn't supported yet: PREV and NEXT take a column"));
            }
            let n = whole(args.get(1).copied())?.unwrap_or(1);
            push_lag(e, name == "prev", n, cx)
        }
        "classifier" => "__mr_symbol".to_string(),
        "match_number" => "__mr_match".to_string(),
        _ => return Ok(None),
    }))
}

/// The value of `col` at the rows of `var` in the match: the first or last of them, or the m-th from the first or last.
/// ONE ROW PER MATCH reads it over the match's group. ALL ROWS PER MATCH reads it at each row, over the match's rows up
/// to that row (RUNNING) or all of them (FINAL), through `pondra_nav`, which takes no frame of its own.
fn nav_text(first: bool, m: Option<u64>, col: &str, var: Option<usize>, cx: &Cx) -> String {
    let keep = filter_of(var, cx);
    let Some(frame) = cx.frame else {
        let at = match (first, m) {
            (true, None) => format!("first_value({col} ORDER BY __mr_seq)"),
            (false, None) => format!("last_value({col} ORDER BY __mr_seq)"),
            (true, Some(m)) => format!("nth_value({col}, {} ORDER BY __mr_seq)", m + 1),
            (false, Some(m)) => format!("nth_value({col}, {} ORDER BY __mr_seq DESC)", m + 1),
        };
        return format!("{at} FILTER (WHERE {keep})");
    };
    let fin = frame == Frame::Final;
    format!("pondra_nav({col}, {keep}, {}, {first}, {fin}) {}", m.unwrap_or(0), over_match(cx, false))
}

/// The window of a measure in ALL ROWS PER MATCH: the match's rows in their order, within the partition and the match.
/// An aggregate takes a frame (up to this row when RUNNING, all the match's rows when FINAL); a navigation reads the
/// match itself, so it takes none.
fn over_match(cx: &Cx, aggregate: bool) -> String {
    let frame = match (aggregate, cx.frame) {
        (false, _) => "",
        (true, Some(Frame::Final)) => " ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING",
        (true, _) => " ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW",
    };
    let mut parts = cx.part.clone();
    parts.push("__mr_match".to_string());
    format!("OVER (PARTITION BY {} ORDER BY __mr_seq{frame})", parts.join(", "))
}

/// An aggregate's text in a measure: in ALL ROWS PER MATCH, a window over the match's rows (see `over_match`).
fn windowed(call: String, cx: &Cx) -> String {
    match cx.frame {
        None => call,
        Some(_) => format!("{call} {}", over_match(cx, true)),
    }
}

/// FIRST or LAST of a column over the rows of its variable (`FIRST(v.x, m)` is the m-th from the first).
fn navigation(f: &Function, first: bool, cx: &Cx) -> Result<String> {
    let args = positional(f)?;
    let e = first_arg(&args, if first { "FIRST" } else { "LAST" })?;
    if navigates(e) {
        return Err(coded("0A000", "MATCH_RECOGNIZE: a navigation of a navigation outside PREV(LAST(…))'s standard forms isn't supported yet"));
    }
    let m = whole(args.get(1).copied())?;
    let var = variable_of(e, cx)?;
    Ok(nav_text(first, m, &plain(e), var, cx))
}

/// Adds the lag (or lead) of an expression over the partition as a column of L1, and returns its name. Every row has
/// its own, so a navigation reads the one at the rows it takes.
fn push_lag(e: &Expr, prev: bool, n: u64, cx: &mut Cx) -> String {
    let fun = if prev { "lag" } else { "lead" };
    let window = format!("{fun}({}, {n}) {}", plain(e), cx.window);
    if cx.inline {
        return window; // (a glue's value: L1 can't read a column of its own list, so the window is written out)
    }
    let col = format!("__mr_n{}", cx.nav.len());
    cx.nav.push(format!("{window} AS {col}"));
    col
}

/// PREV or NEXT in a measure: the lag (or lead) column, read at the rows of its variable as a column is (the last one).
/// `PREV(LAST(v.x, m), n)` reads LAST's rows: the lag of `x` at the m-th last `v` row.
fn measure_lag(f: &Function, prev: bool, cx: &mut Cx) -> Result<String> {
    let args = positional(f)?;
    let e = first_arg(&args, "PREV or NEXT")?;
    let n = whole(args.get(1).copied())?.unwrap_or(1);
    let nested = match e {
        Expr::Function(g) if matches!(lower_name(g).as_str(), "first" | "last") => Some(g),
        _ => None,
    };
    let (base, first, m) = match nested {
        Some(g) => {
            let inner = positional(g)?;
            (first_arg(&inner, "LAST")?, lower_name(g) == "first", whole(inner.get(1).copied())?)
        }
        None => (e, false, None),
    };
    if navigates(base) {
        return Err(coded("0A000", "MATCH_RECOGNIZE: a navigation of a navigation outside PREV(LAST(…))'s standard forms isn't supported yet"));
    }
    let var = variable_of(base, cx)?;
    let col = push_lag(base, prev, n, cx);
    Ok(nav_text(first, m, &col, var, cx))
}

/// Whether a call has an ORDER BY of its own (`string_agg(x, ',' ORDER BY x)`).
fn has_order_by(f: &Function) -> bool {
    match &f.args {
        FunctionArguments::List(list) => list.clauses.iter().any(|c| matches!(c, FunctionArgumentClause::OrderBy(_))),
        _ => false,
    }
}

/// An aggregate over the rows of its variable: the same call over the columns, with a FILTER for the match's rows of
/// that variable (and its own FILTER, if it has one). `count(v.*)` is the count of those rows. In ALL ROWS PER MATCH it
/// is a window over the match's rows, and an ORDER BY of its own is refused: its rows come in the match's order.
fn aggregate(f: &Function, cx: &mut Cx) -> Result<String> {
    if f.over.is_some() {
        return Err(coded("0A000", "MATCH_RECOGNIZE: a window inside a measure isn't supported yet"));
    }
    if cx.frame.is_some() && has_order_by(f) {
        return Err(coded(
            "0A000",
            "MATCH_RECOGNIZE: an aggregate's own ORDER BY isn't supported in ALL ROWS PER MATCH yet: its rows come in the match's order, so drop it",
        ));
    }
    let own = match &f.filter {
        Some(e) => format!(" AND ({})", inside((**e).clone(), cx)?),
        None => String::new(),
    };
    if let Some(v) = qualified_star(f) {
        if lower_name(f) != "count" {
            return Err(coded("42601", format!("MATCH_RECOGNIZE: {v}.* is an argument of count only")));
        }
        let k = symbol_named(&v, cx)?;
        return Ok(windowed(format!("count(*) FILTER (WHERE {}{own})", filter_of(Some(k), cx)), cx));
    }
    let var = variable_of(&f.args, cx)?;
    let mut call = f.clone();
    call.filter = None;
    let body = inside(call, cx)?;
    Ok(windowed(format!("{body} FILTER (WHERE {}{own})", filter_of(var, cx)), cx))
}

/// AFTER MATCH SKIP as the spec's node, its symbol by name (the matcher finds it in the pattern's). Past the last row is
/// the standard's default. A name that the pattern doesn't have is refused here, before planning.
fn skip_json(skip: Option<&AfterMatchSkip>, syms: &[String]) -> Result<serde_json::Value> {
    let symbol = |id: &Ident| -> Result<String> {
        let name = ident(id);
        match syms.contains(&name) {
            true => Ok(name),
            false => Err(coded("42601", format!("MATCH_RECOGNIZE: AFTER MATCH SKIP TO {name}: {name} is not a symbol of PATTERN"))),
        }
    };
    Ok(match skip {
        None | Some(AfterMatchSkip::PastLastRow) => json!({ "to": "past_last" }),
        Some(AfterMatchSkip::ToNextRow) => json!({ "to": "next_row" }),
        Some(AfterMatchSkip::ToFirst(id)) => {
            let name = symbol(id)?;
            json!({ "to": "first", "symbol": name })
        }
        Some(AfterMatchSkip::ToLast(id)) => {
            let name = symbol(id)?;
            json!({ "to": "last", "symbol": name })
        }
    })
}

/// A statement's text as the engine runs it, for the tests: its clauses written as their derived tables.
#[cfg(test)]
pub(crate) fn rewritten(sql: &str) -> anyhow::Result<String> {
    let t = text(sql);
    let mut stmts = Parser::parse_sql(&GenericDialect {}, &t)?;
    let mut r = Rewrite { lake: &HashMap::new() };
    for s in stmts.iter_mut() {
        if let ControlFlow::Break(e) = VisitMut::visit(s, &mut r) {
            return Err(e);
        }
    }
    Ok(stmts.iter().map(|s| s.to_string()).collect::<Vec<_>>().join("; "))
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::sql::sqlparser::{dialect::GenericDialect, parser::Parser};

    #[test]
    fn wanted_by_the_word() {
        assert!(wanted("SELECT * FROM t match_recognize (PATTERN (a))"));
        assert!(!wanted("SELECT final FROM t"));
    }

    #[test]
    fn final_and_running_wrap_their_call() {
        assert_eq!(
            text("FROM t MATCH_RECOGNIZE (MEASURES FINAL last(a.x) AS y, RUNNING max(b) AS z)"),
            "FROM t MATCH_RECOGNIZE (MEASURES pondra_final(last(a.x)) AS y, pondra_running(max(b)) AS z)"
        );
    }

    #[test]
    fn strings_and_comments_are_left_alone() {
        let sql = "MATCH_RECOGNIZE (-- FINAL f(x)\n MEASURES 'RUNNING g(x)' AS s, FINAL f(x) AS y)";
        assert_eq!(text(sql), "MATCH_RECOGNIZE (-- FINAL f(x)\n MEASURES 'RUNNING g(x)' AS s, pondra_final(f(x)) AS y)");
    }

    #[test]
    fn text_is_borrowed_without_the_word() {
        assert!(matches!(text("SELECT FINAL f(x)"), Cow::Borrowed(_)));
    }

    #[test]
    fn the_ticker_clause_is_one_derived_table() {
        let sql = "SELECT * FROM ticker MATCH_RECOGNIZE (PARTITION BY symbol ORDER BY day \
                   MEASURES strt.day AS start_day ONE ROW PER MATCH AFTER MATCH SKIP TO LAST up \
                   PATTERN (strt down+ up+) DEFINE down AS down.price < PREV(down.price), up AS up.price > PREV(up.price)) AS m";
        let Statement::Query(q) = Parser::parse_sql(&GenericDialect {}, &text(sql)).expect("a query").remove(0) else { panic!("a query") };
        let SetExpr::Select(mut s) = *q.body else { panic!("a select") };
        let clause = s.from.remove(0).relation;
        let TableFactor::Derived { subquery, .. } = layer(&clause, &HashMap::new()).expect("a layer") else { panic!("a derived table") };
        let text = subquery.to_string();
        assert!(text.contains("pondra_matches("));
        assert!(text.contains("GROUP BY symbol, __mr_match"));
    }

    #[test]
    fn the_pattern_is_written_as_one_name_and_read_back() {
        let written = text("SELECT * FROM t MATCH_RECOGNIZE (PATTERN (a {- b -} c) DEFINE b AS true)");
        assert!(written.contains("PATTERN (\"__mr_pattern_"));
        let Statement::Query(q) = Parser::parse_sql(&GenericDialect {}, &written).expect("a query").remove(0) else { panic!("a query") };
        let SetExpr::Select(s) = *q.body else { panic!("a select") };
        let TableFactor::MatchRecognize { pattern, .. } = &s.from[0].relation else { panic!("a clause") };
        assert_eq!(pattern_text(pattern).0, "a {- b -} c");
    }

    #[test]
    fn within_is_carried_in_the_pattern_name() {
        let written = text("SELECT * FROM t MATCH_RECOGNIZE (PARTITION BY s ORDER BY ts PATTERN (a b+ c) WITHIN INTERVAL '1' HOUR DEFINE b AS true)");
        assert!(!written.contains("WITHIN"));
        let Statement::Query(q) = Parser::parse_sql(&GenericDialect {}, &written).expect("a query").remove(0) else { panic!("a query") };
        let SetExpr::Select(s) = *q.body else { panic!("a select") };
        let TableFactor::MatchRecognize { pattern, .. } = &s.from[0].relation else { panic!("a clause") };
        assert_eq!(pattern_text(pattern), ("a b+ c".to_string(), Some("INTERVAL '1' HOUR".to_string())));
    }

    #[test]
    fn within_needs_an_ascending_first_order_by() {
        let no_order = "SELECT * FROM t MATCH_RECOGNIZE (PATTERN (a) WITHIN INTERVAL '1' HOUR DEFINE a AS true) AS m";
        let Statement::Query(q) = Parser::parse_sql(&GenericDialect {}, &text(no_order)).expect("a query").remove(0) else { panic!("a query") };
        let SetExpr::Select(mut s) = *q.body else { panic!("a select") };
        let err = layer(&s.from.remove(0).relation, &HashMap::new()).expect_err("a refusal");
        assert_eq!(crate::codes::of(&err), "42601");
        let desc = "SELECT * FROM t MATCH_RECOGNIZE (ORDER BY ts DESC PATTERN (a) WITHIN INTERVAL '1' HOUR DEFINE a AS true) AS m";
        let Statement::Query(q) = Parser::parse_sql(&GenericDialect {}, &text(desc)).expect("a query").remove(0) else { panic!("a query") };
        let SetExpr::Select(mut s) = *q.body else { panic!("a select") };
        let err = layer(&s.from.remove(0).relation, &HashMap::new()).expect_err("a refusal");
        assert_eq!(crate::codes::of(&err), "0A000");
    }

    #[test]
    fn a_measure_named_like_a_lake_column_is_refused() {
        let sql = "SELECT * FROM ticker MATCH_RECOGNIZE (PARTITION BY symbol ORDER BY day MEASURES LAST(up.price) AS price \
                   ALL ROWS PER MATCH PATTERN (strt up+) DEFINE up AS up.price > PREV(up.price)) AS m";
        let lake = HashMap::from([("ticker".to_string(), vec!["symbol".to_string(), "day".to_string(), "price".to_string()])]);
        let Statement::Query(q) = Parser::parse_sql(&GenericDialect {}, &text(sql)).expect("a query").remove(0) else { panic!("a query") };
        let SetExpr::Select(mut s) = *q.body else { panic!("a select") };
        let clause = s.from.remove(0).relation;
        let err = layer(&clause, &lake).expect_err("a refusal");
        assert_eq!(crate::codes::of(&err), "42701");
        // an input that isn't a lake table keeps DataFusion's own error
        assert!(layer(&clause, &HashMap::new()).is_ok());
    }

    #[test]
    fn the_all_rows_clause_is_one_derived_table() {
        let sql = "SELECT * FROM ticker MATCH_RECOGNIZE (PARTITION BY symbol ORDER BY day \
                   MEASURES MATCH_NUMBER() AS m, CLASSIFIER() AS c, FINAL count(*) AS fn, FIRST(price) AS f \
                   ALL ROWS PER MATCH PATTERN (strt down+ up+) \
                   DEFINE down AS down.price < PREV(down.price), up AS up.price > PREV(up.price)) AS t";
        let Statement::Query(q) = Parser::parse_sql(&GenericDialect {}, &text(sql)).expect("a query").remove(0) else { panic!("a query") };
        let SetExpr::Select(mut s) = *q.body else { panic!("a select") };
        let clause = s.from.remove(0).relation;
        let TableFactor::Derived { subquery, .. } = layer(&clause, &HashMap::new()).expect("a layer") else { panic!("a derived table") };
        let text = subquery.to_string();
        assert!(text.contains("pondra_match("));
        assert!(text.contains("pondra_nav("));
        assert!(text.contains("PARTITION BY symbol, __mr_match ORDER BY __mr_seq ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING"));
        assert!(text.contains("EXCLUDE ("));
    }

    #[test]
    fn unmatched_rows_cant_go_with_an_exclusion() {
        let sql = "SELECT * FROM t MATCH_RECOGNIZE (ALL ROWS PER MATCH WITH UNMATCHED ROWS PATTERN (a {- b -}) DEFINE b AS true) AS m";
        let Statement::Query(q) = Parser::parse_sql(&GenericDialect {}, &text(sql)).expect("a query").remove(0) else { panic!("a query") };
        let SetExpr::Select(mut s) = *q.body else { panic!("a select") };
        let clause = s.from.remove(0).relation;
        let err = layer(&clause, &HashMap::new()).expect_err("a refusal");
        assert_eq!(crate::codes::of(&err), "42601");
    }

    #[test]
    fn within_takes_one_interval_literal_only() {
        assert_eq!(within_interval("INTERVAL '1' HOUR").unwrap(), "INTERVAL '1' HOUR");
        for text in ["5", "(SELECT 1)", "INTERVAL '1' HOUR), x(", ""] {
            let err = within_interval(text).expect_err("a refusal");
            assert_eq!(crate::codes::of(&err), "42601", "{text}");
        }
    }

    /// The clause of `sql`'s one MATCH_RECOGNIZE, as `layer` gives it.
    fn layered(sql: &str) -> Result<TableFactor> {
        let Statement::Query(q) = Parser::parse_sql(&GenericDialect {}, &text(sql)).expect("a query").remove(0) else { panic!("a query") };
        let SetExpr::Select(mut s) = *q.body else { panic!("a select") };
        let clause = s.from.remove(0).relation;
        layer(&clause, &HashMap::new())
    }

    /// The clause of `sql` as its derived table's SQL, and the spec its matcher call carries.
    fn derived(sql: &str) -> (String, serde_json::Value) {
        let TableFactor::Derived { subquery, .. } = layered(sql).expect("a layer") else { panic!("a derived table") };
        let sql = subquery.to_string();
        let spec = spec_in(&sql);
        (sql, spec)
    }

    /// The spec in a derived table's SQL: the JSON of its quoted literal (`''` is a quote).
    fn spec_in(sql: &str) -> serde_json::Value {
        let from = sql.find("'{").expect("a spec") + 1;
        let mut out = String::new();
        let mut chars = sql[from..].chars().peekable();
        while let Some(c) = chars.next() {
            if c == '\'' {
                if chars.peek() == Some(&'\'') {
                    chars.next();
                    out.push('\'');
                    continue;
                }
                break;
            }
            out.push(c);
        }
        serde_json::from_str(&out).expect("the spec reads as JSON")
    }

    /// The refusal of `sql`'s clause: its SQLSTATE and its words.
    fn refused_clause(sql: &str) -> (&'static str, String) {
        let err = layered(sql).expect_err("a refusal");
        (crate::codes::of(&err), err.to_string())
    }

    const TICK: &str = "SELECT * FROM t MATCH_RECOGNIZE (PARTITION BY s ORDER BY ts PATTERN (a b+) DEFINE b AS ";

    /// The ticker's clause, whose DEFINEs read the row only: its derived table is what it was before the glue.
    #[test]
    fn the_ticker_clause_keeps_its_derived_text() {
        let sql = "SELECT * FROM ticker MATCH_RECOGNIZE (PARTITION BY symbol ORDER BY day \
                   MEASURES strt.day AS start_day ONE ROW PER MATCH AFTER MATCH SKIP TO LAST up \
                   PATTERN (strt down+ up+) DEFINE down AS down.price < PREV(down.price), up AS up.price > PREV(up.price)) AS m";
        let (derived_sql, spec) = derived(sql);
        assert!(spec.get("defines").is_none());
        assert_eq!(derived_sql, TICKER_GOLDEN);
    }

    const TICKER_GOLDEN: &str = "SELECT symbol, (last_value(day ORDER BY __mr_seq) FILTER (WHERE __mr_symbol = 'strt')) AS start_day FROM (SELECT *, __mr_m['match'] AS __mr_match, __mr_m['symbol'] AS __mr_symbol, __mr_m['seq'] AS __mr_seq, __mr_m['excluded'] AS __mr_x FROM (SELECT *, unnest(__mr_ms) AS __mr_m FROM (SELECT *, pondra_matches('{\"pattern\":\"strt down+ up+\",\"skip\":{\"to\":\"last\",\"symbol\":\"up\"},\"unmatched\":false,\"within\":false}', __mr_d0, __mr_d1, __mr_d2) OVER (PARTITION BY symbol ORDER BY __mr_o0) AS __mr_ms FROM (SELECT *, day AS __mr_o0, true AS __mr_d0, (price) < (lag(price, 1) OVER (PARTITION BY symbol ORDER BY day)) AS __mr_d1, (price) > (lag(price, 1) OVER (PARTITION BY symbol ORDER BY day)) AS __mr_d2 FROM ticker) AS __mr_c) AS __mr_mm) AS __mr_u WHERE __mr_m IS NOT NULL) AS __mr_r GROUP BY symbol, __mr_match";

    #[test]
    fn a_define_against_another_variable_is_glue() {
        let (sql, spec) = derived(&format!("{TICK}b.price < a.price) AS m"));
        assert!(sql.contains("true AS __mr_d0"), "{sql}");
        assert!(sql.contains("true AS __mr_d1"), "{sql}");
        assert!(sql.contains("(price) AS __mr_r0"), "{sql}");
        assert!(sql.contains("price AS __mr_v0"), "{sql}");
        assert!(sql.contains("__mr_d1, __mr_r0, __mr_v0)"), "{sql}");
        assert_eq!(
            spec["defines"],
            json!([{"symbol": 1, "glue": "(__mr_r0 < __mr_s0)", "slots": [{"index": 0, "kind": "nav", "var": 0, "first": false, "m": 0, "offset": 0, "value": 0}]}])
        );
        assert_eq!((spec["rows"].clone(), spec["values"].clone()), (json!(1), json!(1)));
    }

    #[test]
    fn a_count_over_the_match_is_an_aggregate_slot() {
        let (sql, spec) = derived(&format!("{TICK}count(*) > 2) AS m"));
        assert!(sql.contains("true AS __mr_d1"), "{sql}");
        assert_eq!(spec["defines"][0]["glue"], "(__mr_s0 > 2)");
        assert_eq!(spec["defines"][0]["slots"], json!([{"index": 0, "kind": "agg", "fun": "count", "var": null, "value": null}]));
        assert_eq!((spec["rows"].clone(), spec["values"].clone()), (json!(0), json!(0)));
    }

    #[test]
    fn prev_of_a_variable_is_prev_of_its_last_row() {
        let want = json!([{"index": 0, "kind": "nav", "var": 0, "first": false, "m": 0, "offset": -1, "value": 0}]);
        for define in ["b.price > PREV(LAST(a.price))", "b.price > PREV(a.price)"] {
            let (_, spec) = derived(&format!("{TICK}{define}) AS m"));
            assert_eq!(spec["defines"][0]["glue"], "(__mr_r0 > __mr_s0)", "{define}");
            assert_eq!(spec["defines"][0]["slots"], want, "{define}");
        }
        let (_, spec) = derived(&format!("{TICK}b.price > NEXT(FIRST(a.price, 1), 2)) AS m"));
        assert_eq!(spec["defines"][0]["slots"], json!([{"index": 0, "kind": "nav", "var": 0, "first": true, "m": 1, "offset": 2, "value": 0}]));
    }

    #[test]
    fn coalesce_of_a_variable_keeps_the_null_inside_the_glue() {
        let (sql, spec) = derived(&format!("{TICK}COALESCE(a.x, 0) < x) AS m"));
        assert_eq!(spec["defines"][0]["glue"], "(COALESCE(__mr_s0, 0) < __mr_r0)");
        assert!(sql.contains("(x) AS __mr_r0"), "{sql}");
        assert!(sql.contains("x AS __mr_v0"), "{sql}");
        assert_eq!(spec["defines"][0]["slots"][0]["value"], 0);
    }

    #[test]
    fn match_number_is_a_slot_with_no_value() {
        let (sql, spec) = derived(&format!("{TICK}MATCH_NUMBER() = 2) AS m"));
        assert_eq!(spec["defines"][0]["glue"], "(__mr_s0 = 2)");
        assert_eq!(spec["defines"][0]["slots"], json!([{"index": 0, "kind": "match_number"}]));
        assert!(!sql.contains("__mr_v0"), "{sql}");
    }

    #[test]
    fn classifier_is_the_symbol_as_a_literal_and_reads_no_match() {
        let (sql, spec) = derived(&format!("{TICK}CLASSIFIER() = 'b') AS m"));
        assert!(sql.contains("('b') = 'b' AS __mr_d1"), "{sql}");
        assert!(spec.get("defines").is_none(), "{spec}");
    }

    #[test]
    fn row_conjuncts_stay_in_the_column_and_the_rest_is_glue() {
        let (sql, spec) = derived(&format!("{TICK}b.price > 0 AND a.price < b.price) AS m"));
        assert!(sql.contains("((price) > 0) AS __mr_d1"), "{sql}");
        assert_eq!(spec["defines"][0]["glue"], "(__mr_s0 < __mr_r0)");
        assert_eq!((spec["rows"].clone(), spec["values"].clone()), (json!(1), json!(1)));
    }

    #[test]
    fn a_define_that_reads_no_match_keeps_its_text() {
        let (sql, spec) = derived(&format!("{TICK}b.price < PREV(b.price)) AS m"));
        assert!(sql.contains("lag(price, 1) OVER"), "{sql}");
        assert!(spec.get("defines").is_none());
        assert!(!sql.contains("__mr_r0"), "{sql}");
    }

    #[test]
    fn the_refusals_say_what_to_write() {
        let cases = [
            ("array_agg(a.price) > 1", "array_agg isn't supported in a DEFINE yet"),
            ("sum(a.price) FILTER (WHERE a.price > 0) > 1", "FILTER isn't supported in a DEFINE yet"),
            ("sum(DISTINCT a.price) > 1", "DISTINCT …) isn't supported in a DEFINE yet"),
            ("sum(a.price ORDER BY a.price) > 1", "ORDER BY isn't supported in a DEFINE yet"),
            ("LAST(CLASSIFIER()) = 'b'", "CLASSIFIER() inside a navigation isn't supported"),
            ("FIRST(PREV(a.price)) > 1", "a navigation inside a navigation isn't supported"),
            ("PREV(PREV(a.price)) > 1", "a navigation inside a navigation isn't supported"),
            ("sum(FIRST(a.price)) > 1", "first inside sum isn't supported"),
            ("a.price IN (SELECT 1) ", "a subquery in MEASURES or DEFINE isn't supported yet"),
        ];
        for (define, words) in cases {
            let (code, msg) = refused_clause(&format!("{TICK}{define}) AS m"));
            assert_eq!(code, "0A000", "{define}: {msg}");
            assert!(msg.contains(words), "{define}: {msg}");
        }
        let (_, msg) = refused_clause(&format!("{TICK}array_agg(a.price) > 1) AS m"));
        assert!(msg.starts_with("MATCH_RECOGNIZE: DEFINE b: "), "{msg}");
        let (code, msg) = refused_clause(&format!("{TICK}FINAL last(a.price) > 1) AS m"));
        assert_eq!(code, "42601", "{msg}");
        assert!(msg.contains("FINAL is for MEASURES"), "{msg}");
    }
}
