//! `MATCH_RECOGNIZE` (ADR-073, phase 1: ONE ROW PER MATCH), rewritten where SQL comes in into plain SQL around the window
//! function `pondra_match` (`matcher.rs`), so the query after it is ordinary SQL: the result cache, a spread query, a
//! view's stored SQL and history see nothing new. The clause becomes a derived table of four layers, each reading the
//! one before it:
//!
//! - L1: the table's rows, with each symbol's DEFINE as a Boolean column, the ORDER BY keys, and the PREV and NEXT
//!   columns (`lag` and `lead` over the partition);
//! - L2: `pondra_match` over the partition (`pondra_matches` where matches may share rows): each row's match, its
//!   symbol and its place in the match;
//! - L3: the rows that are in a match, with those as columns (an UNNEST first where matches may share rows);
//! - L4: the measures, grouped by the partition and the match, which is one row per match.
//!
//! `text` writes FINAL and RUNNING before a measure as calls that parse, and each PATTERN as one quoted name, since
//! sqlparser refuses some standard forms; `matcher::read` reads the text. What phase 1 doesn't take is refused by name.
use crate::codes::coded;
use crate::text::{ident, object, ROW_ID};
use anyhow::{bail, Result};
use datafusion::sql::sqlparser::ast::*;
use regex::Regex;
use serde_json::json;
use std::borrow::Cow;
use std::collections::{BTreeSet, HashSet};
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
/// pattern is. `pattern_text` reads the text back out.
fn patterns(sql: &str, code: &[bool]) -> Vec<(usize, usize, String)> {
    static CLAUSE: LazyLock<Regex> =
        LazyLock::new(|| Regex::new(r"(?i)(?P<clause>\bmatch_recognize\b)|\bpattern\s*(?P<open>\()").expect("a regex"));
    let mut out = vec![];
    let mut in_clause = false;
    for c in CLAUSE.captures_iter(sql) {
        let whole = c.get(0).expect("a whole match");
        if !code[whole.start()] {
            continue;
        }
        if c.name("clause").is_some() {
            in_clause = true;
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
        out.push((whole.start(), close + 1, format!("PATTERN (\"__mr_pattern_{}\")", hex(text))));
    }
    out
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

/// A clause's PATTERN as the text `matcher::read` reads: the text `patterns` wrote, or else sqlparser's form of it.
fn pattern_text(p: &MatchRecognizePattern) -> String {
    if let MatchRecognizePattern::Symbol(MatchRecognizeSymbol::Named(id)) = p {
        if let Some(text) = id.value.strip_prefix("__mr_pattern_").and_then(unhex) {
            return text;
        }
    }
    p.to_string()
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
    // A lake table's rows that have `_row_id` are tied on their ORDER BY in that order, so every run finds the same
    // matches (a CTE of the same name is not the table).
    let lake = host.name();
    let mut local = BTreeSet::new();
    let mut named = vec![];
    for name in scan.inputs.difference(&scan.ctes) {
        if let Some(l) = crate::text::local_in(&lake, name) {
            local.insert(l.clone());
            named.push((name.clone(), l));
        }
    }
    let ties: HashSet<String> = match local.is_empty() {
        true => HashSet::new(),
        false => {
            let cols = host.columns(local).await?;
            named.into_iter().filter(|(_, l)| cols.get(l).is_some_and(|c| c.iter().any(|n| n == ROW_ID))).map(|(n, _)| n).collect()
        }
    };
    let mut r = Rewrite { ties: &ties };
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
    ties: &'a HashSet<String>,
}

impl VisitorMut for Rewrite<'_> {
    type Break = anyhow::Error;

    fn post_visit_table_factor(&mut self, t: &mut TableFactor) -> ControlFlow<anyhow::Error> {
        if !matches!(t, TableFactor::MatchRecognize { .. }) {
            return ControlFlow::Continue(());
        }
        match layer(t, self.ties) {
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

/// What the translation of one clause knows: its symbols, how the expression at hand is read, the window the lag and
/// lead columns are over (the partition, and the order as written), and those columns, which L1 computes per row.
struct Cx {
    syms: Vec<String>,
    mode: Mode,
    window: String,
    nav: Vec<String>,
}

/// One `MATCH_RECOGNIZE` as the derived table it stands for (the layers of the module doc).
fn layer(t: &TableFactor, ties: &HashSet<String>) -> Result<TableFactor> {
    let TableFactor::MatchRecognize { table, partition_by, order_by, measures, rows_per_match, after_match_skip, pattern, symbols, alias } = t
    else {
        bail!("MATCH_RECOGNIZE: not a MATCH_RECOGNIZE clause");
    };
    if let Some(RowsPerMatch::AllRows(_)) = rows_per_match {
        return Err(coded("0A000", "MATCH_RECOGNIZE: ALL ROWS PER MATCH isn't supported yet: ONE ROW PER MATCH with MEASURES gives each match, or join its first and last row back to the table"));
    }
    let text = pattern_text(pattern);
    let (_, syms) = crate::matcher::read(&text)?;
    let has_row = match table.as_ref() {
        TableFactor::Table { name, .. } => ties.contains(&object(name)),
        _ => false,
    };
    let part_written: Vec<String> = partition_by.iter().map(|e| e.to_string()).collect();
    let part: Vec<String> = partition_by.iter().map(column_name).collect::<Result<Vec<String>>>()?;
    let dirs: Vec<String> = order_by.iter().map(|o| direction(&o.options)).collect();
    let mut order_w: Vec<String> = order_by.iter().zip(&dirs).map(|(o, d)| format!("{}{d}", o.expr)).collect();
    if has_row {
        order_w.push(ROW_ID.to_string());
    }
    let mut cx = Cx { syms, mode: Mode::Measure, window: over(&part_written, &order_w), nav: vec![] };
    let conds = conditions(symbols, &text, &mut cx)?;
    let mut cols = vec![];
    for m in measures {
        cols.push(format!("{} AS {}", written(m.expr.clone(), &mut cx)?, m.alias));
    }

    // L1: the rows, with the keys, the conditions and the navigation columns
    let row = if has_row { ", _row_id AS __mr_row" } else { "" };
    let orders: String = order_by.iter().enumerate().map(|(i, o)| format!(", {} AS __mr_o{i}", o.expr)).collect();
    let defined: String = conds.iter().enumerate().map(|(k, c)| format!(", {c} AS __mr_d{k}")).collect();
    let navs: String = cx.nav.iter().map(|n| format!(", {n}")).collect();
    let l1 = format!("SELECT *{row}{orders}{defined}{navs} FROM {table}");

    // L2: the window that matches the partition (`pondra_matches` where a row may be in several matches)
    let overlap = matches!(after_match_skip, Some(AfterMatchSkip::ToNextRow | AfterMatchSkip::ToFirst(_) | AfterMatchSkip::ToLast(_)));
    let skip = skip_json(after_match_skip.as_ref(), &cx.syms)?;
    let spec = json!({ "pattern": &text, "skip": skip }).to_string();
    let call: String = (0..cx.syms.len()).map(|k| format!(", __mr_d{k}")).collect();
    let mut order2: Vec<String> = dirs.iter().enumerate().map(|(i, d)| format!("__mr_o{i}{d}")).collect();
    if has_row {
        order2.push("__mr_row".to_string());
    }
    let (fun, col) = match overlap {
        true => ("pondra_matches", "__mr_ms"),
        false => ("pondra_match", "__mr_m"),
    };
    let l2 = format!("SELECT *, {fun}({}{call}) {} AS {col} FROM ({l1}) AS __mr_c", quote(&spec), over(&part, &order2));

    // L3: the rows in a match, with its fields as columns
    let fields = "SELECT *, __mr_m['match'] AS __mr_match, __mr_m['symbol'] AS __mr_symbol, __mr_m['seq'] AS __mr_seq";
    let l3 = match overlap {
        true => format!("{fields} FROM (SELECT *, unnest(__mr_ms) AS __mr_m FROM ({l2}) AS __mr_mm) AS __mr_u"),
        false => format!("{fields} FROM ({l2}) AS __mr_mm WHERE __mr_m IS NOT NULL"),
    };

    // L4: one row per match
    let mut group = part.clone();
    group.push("__mr_match".to_string());
    let mut proj = part.clone();
    proj.extend(cols);
    if proj.is_empty() {
        return Err(coded("0A000", "MATCH_RECOGNIZE: no PARTITION BY and no MEASURES: no column to give"));
    }
    let l4 = format!("SELECT {} FROM ({l3}) AS __mr_r GROUP BY {}", proj.join(", "), group.join(", "));
    Ok(TableFactor::Derived { lateral: false, subquery: Box::new(crate::expander::parse_query(&l4)?), alias: alias.clone(), sample: None })
}

/// Each symbol's condition as SQL over the rows: its DEFINE, read as the current row's, and `true` for a symbol with
/// none (a pattern may name a symbol that matches any row).
fn conditions(symbols: &[SymbolDefinition], text: &str, cx: &mut Cx) -> Result<Vec<String>> {
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
    for (k, def) in defs.into_iter().enumerate() {
        out.push(match def {
            Some(e) => {
                cx.mode = Mode::Define(k);
                let text = written(e, cx);
                cx.mode = Mode::Measure;
                text?
            }
            None => "true".to_string(),
        });
    }
    Ok(out)
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

fn define_error(sym: &str, what: String) -> anyhow::Error {
    coded("0A000", format!("MATCH_RECOGNIZE: DEFINE {sym} reads the match so far ({what}), which isn't supported yet: compare with the row before, PREV(price)"))
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

/// A call in a DEFINE: PREV and NEXT are the row's own lag and lead; the match so far is refused by name.
fn define_call(f: &Function, name: &str, k: usize, cx: &Cx) -> Result<Option<String>> {
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
        "first" | "last" | "classifier" | "match_number" => return Err(define_error(&cx.syms[k], f.to_string())),
        n if AGGREGATES.contains(&n) => return Err(define_error(&cx.syms[k], f.to_string())),
        _ => return Ok(None),
    }))
}

/// A call in a measure: the navigations, CLASSIFIER and MATCH_NUMBER, the aggregates, and pondra_final and
/// pondra_running (which read at the last row, where both agree).
fn measure_call(f: &Function, name: &str, cx: &mut Cx) -> Result<Option<String>> {
    Ok(Some(match name {
        "first" | "last" => navigation(f, name == "first", cx)?,
        "prev" | "next" => measure_lag(f, name == "prev", cx)?,
        "classifier" => format!("last_value(__mr_symbol ORDER BY __mr_seq) FILTER (WHERE {})", filter_of(None, cx)),
        "match_number" => "__mr_match".to_string(),
        "pondra_final" | "pondra_running" => {
            let args = positional(f)?;
            written(first_arg(&args, name)?.clone(), cx)?
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
fn nav_text(first: bool, m: Option<u64>, col: &str, var: Option<usize>, cx: &Cx) -> String {
    let at = match (first, m) {
        (true, None) => format!("first_value({col} ORDER BY __mr_seq)"),
        (false, None) => format!("last_value({col} ORDER BY __mr_seq)"),
        (true, Some(m)) => format!("nth_value({col}, {} ORDER BY __mr_seq)", m + 1),
        (false, Some(m)) => format!("nth_value({col}, {} ORDER BY __mr_seq DESC)", m + 1),
    };
    format!("{at} FILTER (WHERE {})", filter_of(var, cx))
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
    let col = format!("__mr_n{}", cx.nav.len());
    cx.nav.push(format!("{fun}({}, {n}) {} AS {col}", plain(e), cx.window));
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

/// An aggregate over the rows of its variable: the same call over the columns, with a FILTER for the match's rows of
/// that variable (and its own FILTER, if it has one). `count(v.*)` is the count of those rows.
fn aggregate(f: &Function, cx: &mut Cx) -> Result<String> {
    if f.over.is_some() {
        return Err(coded("0A000", "MATCH_RECOGNIZE: a window inside a measure isn't supported yet"));
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
        return Ok(format!("count(*) FILTER (WHERE {}{own})", filter_of(Some(k), cx)));
    }
    let var = variable_of(&f.args, cx)?;
    let mut call = f.clone();
    call.filter = None;
    let body = inside(call, cx)?;
    Ok(format!("{body} FILTER (WHERE {}{own})", filter_of(var, cx)))
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
        let TableFactor::Derived { subquery, .. } = layer(&clause, &HashSet::new()).expect("a layer") else { panic!("a derived table") };
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
        assert_eq!(pattern_text(pattern), "a {- b -} c");
    }
}
