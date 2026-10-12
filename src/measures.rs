//! Measures in views (ADR-065 §3): `expr AS MEASURE name` in a view's SELECT.
//!
//! A view keeps its measures as aliases `AS "pondra_measure:name"`, so its stored SQL is still SQL and an older reader sees
//! plain columns. A plain read gives the dimensions and measures worked out, grouped by the dimensions (`default_form`). A
//! query naming a measure (`MEASURE(name)`, `AGG(name)`, `sum(name)` or the bare name) aggregates the view's base rows, so a
//! measure means the same wherever it is used. `check` checks the view's own joins where it is made (each onto a key of the
//! joined table); a query joining a view to anything else is refused by name in the rewrite, with no key check.

use anyhow::{anyhow, bail, ensure, Result};
use datafusion::sql::sqlparser::{
    ast::{self, visit_expressions, visit_expressions_mut, Expr, ObjectName, OrderBy, OrderByKind, SelectItem},
    ast::{SetExpr, Statement, TableFactor, VisitMut, VisitorMut},
    dialect::GenericDialect,
    keywords::RESERVED_FOR_COLUMN_ALIAS,
    parser::Parser,
    tokenizer::{Location, Token, TokenWithSpan, Tokenizer},
};
use std::{borrow::Cow, collections::{HashMap, HashSet}, ops::ControlFlow, sync::{Arc, LazyLock, Mutex}};

use crate::{constraints::Kind, ddl, routines, store::{self, Lake}, write};

/// The alias a measure is stored under: `sum(x) AS MEASURE m` is kept as `sum(x) AS "pondra_measure:m"`.
pub const MARK: &str = "pondra_measure:";
/// `MEASURE`, `AGG` and `AGGREGATE` take one measure by name, unless the lake has a function of that name (see `measure_call`).
const CALLS: &str = "measure agg aggregate";
/// Aggregates BI tools put around a measure (`sum(m)`): what they mean is the measure itself.
const PLAIN_USES: &str = "sum min max avg any_value";
/// Aggregates: a measure inside one (other than those uses) is refused, since it is already an aggregate.
const AGGREGATES: &str = "count sum min max avg median stddev stddev_pop stddev_samp variance var_pop var_samp \
    string_agg array_agg listagg approx_distinct approx_median approx_percentile_cont first_value last_value \
    bool_and bool_or corr covar_pop covar_samp any_value count_distinct";
/// Words that end a name: in `x AS measure FROM t` the alias is `measure` and names no measure.
const ENDS_A_NAME: &str = "from where group having order limit offset union except intersect window qualify into as on join";

fn listed_in(list: &str, word: &str) -> bool {
    list.split_whitespace().any(|w| w == word)
}

/// A plain lower-case name, which SQL reads back the same bare.
static BARE: LazyLock<regex::Regex> = LazyLock::new(|| regex::Regex::new(r"^[a-z_][a-z0-9_]*$").expect("a regex"));
/// A measure's stored alias, as sqlparser writes it.
static STORED: LazyLock<regex::Regex> =
    LazyLock::new(|| regex::Regex::new(&format!(r#""{}((?:[^"]|"")*)""#, regex::escape(MARK))).expect("a regex"));
/// A `MEASURE(`, `AGG(` or `AGGREGATE(` call in a text, looked for before any view is read.
static CALL_TEXT: LazyLock<regex::Regex> =
    LazyLock::new(|| regex::Regex::new(r"(?i)\b(?:measure|agg|aggregate)\s*\(").expect("a regex"));

// The text: `AS MEASURE name` is stored as `AS "pondra_measure:name"`, and back again for display.

/// Turns `expr AS MEASURE name` into the stored form; only tokens are looked at, so strings and comments are left alone.
pub fn text(sql: &str) -> Cow<'_, str> {
    // (every statement passes here: looked for without copying the text)
    if !sql.as_bytes().windows(7).any(|w| w.eq_ignore_ascii_case(b"measure")) {
        return Cow::Borrowed(sql);
    }
    let dialect = GenericDialect {};
    let Ok(tokens) = Tokenizer::new(&dialect, sql).tokenize_with_location() else { return Cow::Borrowed(sql) };
    let words: Vec<&TokenWithSpan> = tokens.iter().filter(|t| !matches!(t.token, Token::Whitespace(_))).collect();
    let edits: Vec<(usize, usize, String)> = words.windows(3).filter_map(|w| measure_edit(sql, w)).collect();
    if edits.is_empty() {
        return Cow::Borrowed(sql);
    }
    let (mut out, mut from) = (String::new(), 0);
    for (start, end, with) in edits {
        out.push_str(&sql[from..start]);
        out.push_str(&with);
        from = end;
    }
    out.push_str(&sql[from..]);
    Cow::Owned(out)
}

/// The edit (byte range and text) for an `AS MEASURE name` whose three tokens are `words`, if it is one.
fn measure_edit(sql: &str, words: &[&TokenWithSpan]) -> Option<(usize, usize, String)> {
    let [a, m, n] = words else { return None };
    if !plain(a, "as") || !plain(m, "measure") {
        return None;
    }
    let name = match &n.token {
        Token::Word(w) if w.quote_style.is_some() => w.value.clone(),
        Token::Word(w) if !listed_in(ENDS_A_NAME, &w.value.to_lowercase()) => w.value.to_lowercase(),
        _ => return None,
    };
    let (start, end) = (offset(sql, &m.span.start)?, offset(sql, &n.span.end)?); // (a span's end is just past its token)
    Some((start, end, format!("\"{MARK}{}\"", name.replace('"', "\"\""))))
}

/// Whether a token is the unquoted word `w`, in any case.
fn plain(t: &TokenWithSpan, w: &str) -> bool {
    matches!(&t.token, Token::Word(x) if x.quote_style.is_none() && x.value.eq_ignore_ascii_case(w))
}

/// The byte offset of a 1-based line and column (sqlparser counts columns in characters), or the
/// text's end when the column is just past its last character.
fn offset(sql: &str, at: &Location) -> Option<usize> {
    let line_start: usize = sql.split_inclusive('\n').take((at.line as usize).checked_sub(1)?).map(str::len).sum();
    let line = sql.get(line_start..)?;
    line.char_indices().map(|(i, _)| i).chain([line.len()]).nth((at.column as usize).checked_sub(1)?).map(|i| line_start + i)
}

/// The reverse, for showing a stored view: `AS "pondra_measure:name"` reads `AS MEASURE name`.
pub fn written(sql: &str) -> Cow<'_, str> {
    if !sql.contains(MARK) {
        return Cow::Borrowed(sql);
    }
    STORED.replace_all(sql, |c: &regex::Captures| format!("MEASURE {}", bare_or_quoted(&c[1].replace("\"\"", "\""))))
}

/// A name as SQL reads it: bare when plain, quoted otherwise.
fn bare_or_quoted(name: &str) -> String {
    if BARE.is_match(name) { name.to_string() } else { format!("\"{}\"", name.replace('"', "\"\"")) }
}

// The definition: a view's SELECT, split into its dimensions and its measures.

/// A view with measures: its one SELECT, its dimensions and its measures (each a name and an expression, as written).
#[derive(Clone, Debug)]
pub struct Definition {
    pub select: ast::Select,
    pub dimensions: Vec<(String, Expr)>,
    pub measures: Vec<(String, Expr)>,
}

impl Definition {
    /// The definition in a view's stored SQL, or None when it has no measure (a plain view's columns are not read).
    pub fn parse(sql: &str) -> Result<Option<Definition>> {
        if !sql.contains(MARK) {
            return Ok(None);
        }
        match Parser::parse_sql(&GenericDialect {}, sql)?.as_slice() {
            [Statement::Query(q)] => Definition::of(q),
            _ => bail!("a view with measures is one SELECT"),
        }
    }

    /// The definition of a parsed query, or None when its SELECT has no measure. A set operation can't hold one.
    pub fn of(query: &ast::Query) -> Result<Option<Definition>> {
        let SetExpr::Select(select) = &*query.body else {
            ensure!(!has_marks(query), "a view with measures is one SELECT (no UNION, INTERSECT or EXCEPT)");
            return Ok(None);
        };
        if !select.projection.iter().any(|i| matches!(i, SelectItem::ExprWithAlias { alias, .. } if alias.value.starts_with(MARK))) {
            return Ok(None);
        }
        let (mut dimensions, mut measures) = (Vec::new(), Vec::new());
        for item in &select.projection {
            match item {
                SelectItem::ExprWithAlias { expr, alias } => match alias.value.strip_prefix(MARK) {
                    Some(name) => measures.push((name.to_string(), expr.clone())),
                    None => dimensions.push((write::ident(alias), expr.clone())),
                },
                SelectItem::UnnamedExpr(expr) => dimensions.push((dimension_name(expr)?, expr.clone())),
                _ => {} // a wildcard: `shape` refuses it
            }
        }
        Ok(Some(Definition { select: (**select).clone(), dimensions, measures }))
    }
}

/// A stored view's measures, each with its expression as written, for listings (`pondra.parts`): none for a plain view.
pub fn listed(sql: &str) -> Vec<(String, String)> {
    match Definition::parse(sql) {
        Ok(Some(d)) => d.measures.iter().map(|(name, e)| (name.clone(), e.to_string())).collect(),
        _ => vec![],
    }
}

/// A dimension without an alias is named by its column: `c.region` is `region`.
fn dimension_name(expr: &Expr) -> Result<String> {
    match expr {
        Expr::Identifier(i) => Ok(write::ident(i)),
        Expr::CompoundIdentifier(parts) => parts.last().map(write::ident).ok_or_else(|| anyhow!("name the dimension {expr} (... AS name)")),
        _ => bail!("name the dimension {expr} (... AS name)"),
    }
}

/// Whether a query's text carries a measure.
fn has_marks(query: &ast::Query) -> bool {
    query.to_string().contains(MARK)
}

// Shape: what a view with measures can't be, refused by name with what to write instead.

/// Refuses the shapes a view with measures can't take; each refusal says what to write instead.
pub fn shape(d: &Definition, query: &ast::Query) -> Result<()> {
    let s = &d.select;
    ensure!(query.with.is_none(), "a view with measures has no WITH: make its parts views of their own");
    ensure!(query.order_by.is_none() && query.limit_clause.is_none() && query.fetch.is_none(), "a view with measures has no ORDER BY, LIMIT or FETCH: put them in the query that reads it");
    ensure!(s.distinct.is_none(), "a view with measures has no DISTINCT: a distinct count is a measure, count(DISTINCT x) AS MEASURE n");
    ensure!(!is_grouped(s), "a view with measures has no GROUP BY: its dimensions are its grouping");
    ensure!(s.having.is_none(), "a view with measures has no HAVING: put the condition in the query that reads it");
    ensure!(s.qualify.is_none(), "a view with measures has no QUALIFY: put the condition in the query that reads it");
    ensure!(!s.projection.iter().any(is_wildcard), "name the dimensions of a view with measures (no *)");
    ensure!(s.from.len() == 1, "a view with measures reads one FROM item: write its tables as JOIN ... ON");
    for join in &s.from[0].joins {
        let Some(c) = constraint_of(&join.join_operator) else { bail!("a view with measures joins with JOIN, INNER JOIN or LEFT JOIN, not RIGHT, FULL, CROSS, SEMI, ANTI or ASOF") };
        ensure!(matches!(c, ast::JoinConstraint::On(_) | ast::JoinConstraint::Using(_)), "a join in a view with measures names its keys: ON or USING");
    }
    let mut seen = HashSet::new();
    for (name, _) in d.dimensions.iter().chain(&d.measures) {
        ensure!(seen.insert(name.as_str()), "the view names {name} twice: each column and measure needs a name of its own");
    }
    for (name, expr) in &d.dimensions {
        ensure!(!any_expr(expr, is_aggregate_or_window), "dimension {name} is an aggregate or a window function: a dimension is one value per row");
    }
    for (name, expr) in &d.measures {
        ensure!(!any_expr(expr, is_subquery_or_window), "measure {name} has a subquery or a window function: a measure is an aggregate over the view's rows");
    }
    inline(d).map(|_| ()) // refuses measures that name each other in a loop
}

/// Whether a SELECT groups its rows (GROUP BY ALL, or with expressions).
fn is_grouped(s: &ast::Select) -> bool {
    match &s.group_by {
        ast::GroupByExpr::Expressions(e, m) => !e.is_empty() || !m.is_empty(),
        _ => true,
    }
}

fn is_wildcard(item: &SelectItem) -> bool {
    matches!(item, SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(..))
}

fn is_window(e: &Expr) -> bool {
    matches!(e, Expr::Function(f) if f.over.is_some())
}

fn is_aggregate_or_window(e: &Expr) -> bool {
    is_window(e) || matches!(e, Expr::Function(f) if listed_in(AGGREGATES, &write::object(&f.name)))
}

fn is_subquery_or_window(e: &Expr) -> bool {
    is_window(e) || matches!(e, Expr::Subquery(_) | Expr::Exists { .. } | Expr::InSubquery { .. })
}

/// Whether any node of `e`, itself included, passes `pred` (subqueries too).
fn any_expr(e: &Expr, pred: fn(&Expr) -> bool) -> bool {
    matches!(visit_expressions(e, |x| if pred(x) { ControlFlow::Break(()) } else { ControlFlow::Continue(()) }), ControlFlow::Break(_))
}

// Inlining: a measure's expression with the measures it names written in.

/// Each measure's expression with the measures it names written in (`revenue / customers` becomes `(sum(o.amount)) /
/// (count(DISTINCT o.customer_id))`). Only bare names are replaced, so a column named like a measure is written qualified.
pub fn inline(d: &Definition) -> Result<Vec<(String, Expr)>> {
    d.measures.iter().map(|(name, _)| expanded(d, name, &mut Vec::new()).map(|e| (name.clone(), e))).collect()
}

/// A measure's expression with the measures it names worked in; `path` holds those being worked in now, so a loop is refused.
fn expanded(d: &Definition, name: &str, path: &mut Vec<String>) -> Result<Expr> {
    if path.iter().any(|p| p == name) {
        bail!("measures {} and {name} refer to each other", path.last().map_or(name, String::as_str));
    }
    let Some((_, expr)) = d.measures.iter().find(|(n, _)| n == name) else { bail!("no measure named {name}") };
    path.push(name.to_string());
    let mut e = expr.clone();
    let worked = visit_expressions_mut(&mut e, |x: &mut Expr| {
        let Expr::Identifier(id) = x else { return ControlFlow::Continue(()) };
        let inner = write::ident(id);
        if !d.measures.iter().any(|(n, _)| *n == inner) {
            return ControlFlow::Continue(());
        }
        match expanded(d, &inner, path) {
            Ok(w) => {
                *x = Expr::Nested(Box::new(w));
                ControlFlow::Continue(())
            }
            Err(err) => ControlFlow::Break(err),
        }
    });
    path.pop();
    match worked {
        ControlFlow::Break(err) => Err(err),
        ControlFlow::Continue(()) => Ok(e),
    }
}

// The default form: what a plain read of the view gives.

/// The view read as it is: its dimensions and measures worked out, grouped by the dimensions (`SELECT * FROM sales`).
pub fn default_form(d: &Definition) -> Result<ast::Query> {
    let mut select = d.select.clone();
    let measures = inline(d)?;
    select.projection = d.dimensions.iter().chain(&measures).map(|(name, expr)| named(expr, name)).collect();
    // GROUP BY 1, ..., k over the dimensions; with none, there is no GROUP BY and one row in all.
    select.group_by = ast::GroupByExpr::Expressions((1..=d.dimensions.len()).map(position).collect(), vec![]);
    Ok(as_query(select))
}

fn position(n: usize) -> Expr {
    Expr::value(ast::Value::Number(n.to_string(), false))
}

/// An expression as a named select item.
fn named(expr: &Expr, name: &str) -> SelectItem {
    SelectItem::ExprWithAlias { expr: expr.clone(), alias: ident_for(name) }
}

/// A query made of one SELECT and nothing else.
fn as_query(select: ast::Select) -> ast::Query {
    ast::Query { with: None, body: Box::new(SetExpr::Select(Box::new(select))), order_by: None, limit_clause: None, fetch: None, locks: vec![], for_clause: None, settings: None, format_clause: None, pipe_operators: vec![] }
}

/// A name as an identifier: bare when SQL reads it back the same (a plain, unreserved name), quoted otherwise.
fn ident_for(name: &str) -> ast::Ident {
    let reserved = RESERVED_FOR_COLUMN_ALIAS.iter().any(|k| k.to_string().eq_ignore_ascii_case(name));
    if BARE.is_match(name) && !reserved { ast::Ident::new(name) } else { ast::Ident::with_quote('"', name) }
}

// The base: the rows a query's measures are worked out over.

/// The base's hidden column `n` (`__pondra_m0`, ...).
fn hidden(n: usize) -> ast::Ident {
    ident_for(&format!("__pondra_m{n}"))
}

/// The base rows a query's measures are worked out over: the view's FROM and WHERE, with the dimensions the query uses and the
/// columns the measures read under hidden names. The measures come back over those names, so the query aggregates by its own grouping.
fn base(d: &Definition, used_dims: &HashSet<String>, used: &[String], alias: &str) -> Result<(ast::Query, HashMap<String, Expr>)> {
    let inlined: HashMap<String, Expr> = inline(d)?.into_iter().collect();
    let mut refs: Vec<Expr> = Vec::new();
    for m in used {
        let body = inlined.get(m).ok_or_else(|| anyhow!("no measure {m}"))?;
        let _ = visit_expressions(body, |x| {
            if matches!(x, Expr::Identifier(_) | Expr::CompoundIdentifier(_)) && !refs.iter().any(|r| r.to_string() == x.to_string()) {
                refs.push(x.clone());
            }
            ControlFlow::<()>::Continue(())
        });
    }
    let hidden_refs: Vec<(String, Expr)> = refs.iter().enumerate().map(|(n, r)| (r.to_string(), Expr::CompoundIdentifier(vec![ident_for(alias), hidden(n)]))).collect();
    let mut over = HashMap::new();
    for m in used {
        let mut e = inlined[m].clone();
        let _ = visit_expressions_mut(&mut e, |x: &mut Expr| {
            if let Some((_, h)) = hidden_refs.iter().find(|(k, _)| *k == x.to_string()) {
                *x = h.clone();
            }
            ControlFlow::<()>::Continue(())
        });
        over.insert(m.clone(), e);
    }
    let dims: Vec<&(String, Expr)> = d.dimensions.iter().filter(|(n, _)| used_dims.contains(n)).collect();
    let mut items: Vec<SelectItem> = dims.iter().map(|(n, e)| named(e, n)).collect();
    items.extend(refs.iter().enumerate().map(|(n, r)| SelectItem::ExprWithAlias { expr: r.clone(), alias: hidden(n) }));
    // Nothing is read (count(*)): one row per base row still counts them.
    if items.is_empty() {
        items.push(SelectItem::ExprWithAlias { expr: position(1), alias: ident_for("__pondra_rows") });
    }
    let mut others: Vec<Expr> = dims.iter().map(|(_, e)| e.clone()).collect();
    others.extend(refs.iter().cloned().chain(d.select.selection.iter().cloned()));
    let mut select = d.select.clone();
    select.projection = items;
    select.group_by = ast::GroupByExpr::Expressions(vec![], vec![]);
    select.having = None;
    select.qualify = None;
    drop_unread_joins(&mut select, &others);
    Ok((as_query(select), over))
}

/// Drops each LEFT JOIN that nothing the base reads: a LEFT JOIN keeps every row of its left side and adds only columns.
fn drop_unread_joins(s: &mut ast::Select, others: &[Expr]) {
    loop {
        let Some(i) = (0..s.from[0].joins.len()).find(|&i| unread(s, i, others)) else { break };
        s.from[0].joins.remove(i);
    }
}

/// Whether join `i` is a LEFT JOIN that no column of the base, or of another join's condition, reads. An unqualified column
/// could be any relation's, so it keeps every join.
fn unread(s: &ast::Select, i: usize, others: &[Expr]) -> bool {
    let joins = &s.from[0].joins;
    let join = &joins[i];
    if !matches!(join.join_operator, ast::JoinOperator::Left(_) | ast::JoinOperator::LeftOuter(_)) {
        return false;
    }
    let Some(name) = relation_name(&join.relation) else { return false };
    let mut reads = Vec::new();
    for e in others {
        qualifiers(e, &mut reads);
    }
    for (_, other) in joins.iter().enumerate().filter(|&(j, _)| j != i) {
        match constraint_of(&other.join_operator) {
            Some(ast::JoinConstraint::On(cond)) => qualifiers(cond, &mut reads),
            _ => reads.push(None), // USING and NATURAL name columns with no qualifier
        }
    }
    reads.iter().all(|q| q.as_deref().is_some_and(|q| q != name.as_str()))
}

/// The qualifier of each column an expression reads (None for a bare name).
fn qualifiers(e: &Expr, out: &mut Vec<Option<String>>) {
    let _ = visit_expressions(e, |x| {
        match x {
            Expr::Identifier(_) => out.push(None),
            Expr::CompoundIdentifier(p) if p.len() >= 2 => out.push(Some(write::ident(&p[p.len() - 2]))),
            _ => {}
        }
        ControlFlow::<()>::Continue(())
    });
}

/// The name a relation is known by in a query: its alias, or the last part of its table name.
fn relation_name(f: &TableFactor) -> Option<String> {
    match f {
        TableFactor::Table { alias: Some(a), .. } | TableFactor::Derived { alias: Some(a), .. } => Some(write::ident(&a.name)),
        TableFactor::Table { name, alias: None, .. } => Some(last_part(name)),
        _ => None,
    }
}

/// The last part of a name, as SQL resolves it (`customers` of `public.customers`).
fn last_part(name: &ObjectName) -> String {
    name.0.last().and_then(|p| p.as_ident()).map(write::ident).unwrap_or_default()
}

/// A join's condition, for the joins that have one.
fn constraint_of(op: &ast::JoinOperator) -> Option<&ast::JoinConstraint> {
    match op {
        ast::JoinOperator::Join(c) | ast::JoinOperator::Inner(c) | ast::JoinOperator::Left(c) | ast::JoinOperator::LeftOuter(c) => Some(c),
        _ => None,
    }
}

// The rewrite: a query that names a measure reads the base, and aggregates it.

/// The measure uses in one part of a select. Each is noted in `found`; with `over`, it is replaced by its measure over the base.
/// `depth` counts the queries the walk is inside: a subquery has its own rewrite.
struct Uses<'a> {
    d: &'a Definition,
    alias: &'a str,
    over: Option<&'a HashMap<String, Expr>>,
    routine: &'a dyn Fn(&str) -> bool,
    found: Vec<String>,
    depth: usize,
}

impl<'a> Uses<'a> {
    fn new(d: &'a Definition, alias: &'a str, over: Option<&'a HashMap<String, Expr>>, routine: &'a dyn Fn(&str) -> bool) -> Self {
        Uses { d, alias, over, routine, found: Vec::new(), depth: 0 }
    }

    /// The measure a bare reference names: `revenue`, or `v.revenue` where `v` is the view's name here.
    fn measure_ref(&self, e: &Expr) -> Option<String> {
        let name = match e {
            Expr::Identifier(m) => write::ident(m),
            Expr::CompoundIdentifier(p) => match p.as_slice() {
                [.., q, m] if write::ident(q) == self.alias => write::ident(m),
                _ => return None,
            },
            _ => return None,
        };
        self.d.measures.iter().any(|(n, _)| *n == name).then_some(name)
    }

    /// The first measure named anywhere inside `e`.
    fn measure_inside(&self, e: &Expr) -> Option<String> {
        let mut found = None;
        let _ = visit_expressions(e, |x| match self.measure_ref(x) {
            Some(m) => {
                found = Some(m);
                ControlFlow::Break(())
            }
            None => ControlFlow::Continue(()),
        });
        found
    }

    /// The measure a use names: `MEASURE(m)`, `AGG(m)`, `AGGREGATE(m)`, `sum(m)` and the other BI aggregates (no DISTINCT, FILTER
    /// or OVER), or the bare `m`.
    fn use_of(&self, e: &Expr) -> Result<Option<String>> {
        if let Some(m) = self.measure_ref(e) {
            return Ok(Some(m));
        }
        let Expr::Function(f) = e else { return Ok(None) };
        let name = write::object(&f.name);
        let arg = single_arg(f);
        if measure_call(f, self.routine) {
            let Some(arg) = arg else { bail!("{name}() takes one measure of {}", self.alias) };
            return self.measure_ref(arg).map(Some).ok_or_else(|| anyhow!("no measure {arg} in {} (its measures: {})", self.alias, self.names()));
        }
        let plain = listed_in(PLAIN_USES, &name) && f.filter.is_none() && f.over.is_none();
        Ok(arg.filter(|_| plain).and_then(|a| self.measure_ref(a)))
    }

    /// The view's measure names, for a message.
    fn names(&self) -> String {
        self.d.measures.iter().map(|(n, _)| n.as_str()).collect::<Vec<_>>().join(", ")
    }

    /// One expression, before the parts inside it: a use is noted (and replaced, with `over`); an aggregate holding a measure
    /// that isn't a use is refused.
    fn step(&mut self, e: &mut Expr) -> Result<()> {
        if let Some(m) = self.use_of(e)? {
            if let Some(over) = self.over {
                // In brackets, so the replacement stays one value.
                *e = over.get(&m).map(|o| Expr::Nested(Box::new(o.clone()))).ok_or_else(|| anyhow!("no measure {m} to replace"))?;
            }
            self.found.push(m);
            return Ok(());
        }
        let name = match &*e {
            Expr::Function(f) => write::object(&f.name),
            _ => String::new(),
        };
        if !listed_in(AGGREGATES, &name) {
            return Ok(());
        }
        match self.measure_inside(e) {
            Some(m) => bail!("measure {m} inside {name}(...): use MEASURE({m}) alone, or a measure of its own"),
            None => Ok(()),
        }
    }
}

impl VisitorMut for Uses<'_> {
    type Break = anyhow::Error;

    fn pre_visit_query(&mut self, _: &mut ast::Query) -> ControlFlow<anyhow::Error> {
        self.depth += 1;
        ControlFlow::Continue(())
    }

    fn post_visit_query(&mut self, _: &mut ast::Query) -> ControlFlow<anyhow::Error> {
        self.depth -= 1;
        ControlFlow::Continue(())
    }

    fn pre_visit_expr(&mut self, e: &mut Expr) -> ControlFlow<anyhow::Error> {
        if self.depth > 0 {
            return ControlFlow::Continue(());
        }
        match self.step(e) {
            Ok(()) => ControlFlow::Continue(()),
            Err(err) => ControlFlow::Break(err),
        }
    }
}

/// The one argument of a call with nothing else on it: no DISTINCT, no ORDER BY inside, no FILTER and no OVER.
/// A call of `MEASURE`, `AGG` or `AGGREGATE` with one argument at most, unless the lake has a function of that name. With more,
/// it is another engine's function of that name (Spark's and DuckDB's `aggregate(list, …)`), left for the planner to answer.
fn measure_call(f: &ast::Function, routine: &dyn Fn(&str) -> bool) -> bool {
    let name = write::object(&f.name);
    let one = match &f.args {
        ast::FunctionArguments::List(list) => list.args.len() <= 1,
        ast::FunctionArguments::None => true,
        ast::FunctionArguments::Subquery(_) => false,
    };
    one && listed_in(CALLS, &name) && !routine(&name)
}

fn single_arg(f: &ast::Function) -> Option<&Expr> {
    let ast::FunctionArguments::List(list) = &f.args else { return None };
    if list.duplicate_treatment.is_some() || !list.clauses.is_empty() || f.filter.is_some() || f.over.is_some() {
        return None;
    }
    match list.args.as_slice() {
        [ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(e))] => Some(e),
        _ => None,
    }
}

/// Runs the walk over one part of a select, stopping at the first refusal.
fn walk(part: &mut impl VisitMut, uses: &mut Uses<'_>) -> Result<()> {
    match part.visit(uses) {
        ControlFlow::Break(err) => Err(err),
        ControlFlow::Continue(()) => Ok(()),
    }
}

/// The terms of an ORDER BY, when it lists any.
fn order_list(order: Option<&mut OrderBy>) -> Vec<&mut Expr> {
    match order.map(|o| &mut o.kind) {
        Some(OrderByKind::Expressions(list)) => list.iter_mut().map(|o| &mut o.expr).collect(),
        _ => vec![],
    }
}

/// Every column name a select reads: the dimensions among them are the ones the base must project.
fn names_used(s: &ast::Select, order: Option<&OrderBy>) -> HashSet<String> {
    let mut exprs: Vec<&Expr> = s.projection.iter().filter_map(|i| match i {
        SelectItem::UnnamedExpr(e) | SelectItem::ExprWithAlias { expr: e, .. } => Some(e),
        _ => None,
    }).collect();
    exprs.extend(s.selection.iter().chain(s.having.iter()).chain(s.qualify.iter()));
    if let ast::GroupByExpr::Expressions(group, _) = &s.group_by {
        exprs.extend(group.iter());
    }
    if let Some(OrderBy { kind: OrderByKind::Expressions(list), .. }) = order {
        exprs.extend(list.iter().map(|o| &o.expr));
    }
    let mut names = HashSet::new();
    for e in exprs {
        let _ = visit_expressions(e, |x| {
            match x {
                Expr::Identifier(i) => {
                    names.insert(write::ident(i));
                }
                Expr::CompoundIdentifier(p) => {
                    if let Some(l) = p.last() {
                        names.insert(write::ident(l));
                    }
                }
                _ => {}
            }
            ControlFlow::<()>::Continue(())
        });
    }
    names
}

fn unique(names: Vec<String>) -> Vec<String> {
    let mut seen = HashSet::new();
    names.into_iter().filter(|n| seen.insert(n.clone())).collect()
}

/// Rewrites one SELECT that reads a view with measures and names one of them; returns whether it did. The first pass finds the
/// uses (a bare use in an item is named after its measure); the base then replaces the FROM, and the second pass replaces each
/// use by its measure over the base.
fn rewrite_select(d: &Definition, alias: &str, s: &mut ast::Select, mut order: Option<&mut OrderBy>, routine: &dyn Fn(&str) -> bool, kept: Option<Expr>) -> Result<bool> {
    let mut found = Uses::new(d, alias, None, routine);
    let mut plain: Vec<usize> = Vec::new();
    for (i, item) in s.projection.iter_mut().enumerate() {
        let before = found.found.len();
        let exact = match item {
            SelectItem::UnnamedExpr(e) => found.use_of(e)?.map(|m| (e.clone(), m)),
            _ => None,
        };
        if let Some((expr, m)) = exact {
            *item = SelectItem::ExprWithAlias { expr, alias: ident_for(&m) };
        }
        walk(item, &mut found)?;
        let aggregate = matches!(item, SelectItem::UnnamedExpr(e) | SelectItem::ExprWithAlias { expr: e, .. } if any_expr(e, is_aggregate_or_window));
        if found.found.len() == before && !aggregate {
            plain.push(i + 1); // (an aggregate beside a measure, `count(*)`, is worked out per group, not grouped by)
        }
    }
    for e in s.having.iter_mut().chain(s.qualify.iter_mut()).chain(order_list(order.as_deref_mut())) {
        walk(e, &mut found)?;
    }
    if let Some(m) = s.selection.as_ref().and_then(|w| found.measure_inside(w)) {
        bail!("measure {m} has no value per row: filter it in HAVING");
    }
    if let ast::GroupByExpr::Expressions(exprs, _) = &s.group_by {
        if let Some(m) = exprs.iter().find_map(|e| found.measure_inside(e)) {
            bail!("measure {m} can't group rows: group by a dimension instead");
        }
    }
    if found.found.is_empty() {
        return Ok(false);
    }
    if let Some(w) = kept {
        // (read through a CTE that kept some of the view's rows: its WHERE is the query's too)
        if let Some(m) = found.measure_inside(&w) {
            bail!("measure {m} has no value per row: filter it in HAVING");
        }
        s.selection = Some(match s.selection.take() {
            Some(x) => Expr::BinaryOp { left: Box::new(Expr::Nested(Box::new(x))), op: ast::BinaryOperator::And, right: Box::new(Expr::Nested(Box::new(w))) },
            None => w,
        });
    }
    ensure!(s.from.len() == 1 && s.from[0].joins.is_empty(), "a query with measures reads its view alone: join inside the view, where its joins are checked");
    ensure!(!s.projection.iter().any(is_wildcard), "name the dimensions beside a measure: a * can't be used with one");
    let used = unique(found.found);
    let names = names_used(s, order.as_deref());
    let used_dims: HashSet<String> = d.dimensions.iter().map(|(n, _)| n.clone()).filter(|n| names.contains(n)).collect();
    let (inner, over) = base(d, &used_dims, &used, alias)?;
    let alias_name = Some(ast::TableAlias { explicit: true, name: ident_for(alias), columns: vec![], at: None });
    s.from[0].relation = TableFactor::Derived { lateral: false, subquery: Box::new(inner), alias: alias_name, sample: None };
    let mut replace = Uses::new(d, alias, Some(&over), routine);
    for item in s.projection.iter_mut() {
        walk(item, &mut replace)?;
    }
    for e in s.having.iter_mut().chain(s.qualify.iter_mut()).chain(order_list(order.as_deref_mut())) {
        walk(e, &mut replace)?;
    }
    // Without a GROUP BY, the query groups by the items that read no measure (none: one row in all).
    if let ast::GroupByExpr::Expressions(exprs, _) = &mut s.group_by {
        if exprs.is_empty() && !plain.is_empty() {
            *exprs = plain.iter().map(|&i| position(i)).collect();
        }
    }
    if let Some((flat, flat_order)) = flattened(s, order.as_deref(), alias) {
        *s = flat;
        if let (Some(o), Some(f)) = (order, flat_order) {
            *o = f;
        }
    }
    Ok(true)
}

/// The rewritten SELECT with its base written in place of the subquery: the GROUP BY a person would write by hand, planned
/// as one (planning the subquery cost each query on three nodes about 3 ms more). Each of the base's columns the query names
/// becomes its expression. None, and the subquery stays, when anything in the query could mean something else without it: a
/// name the base doesn't have (outside ORDER BY's names of the query's own columns), a subquery, a window, DISTINCT, or an
/// unnamed item that isn't a plain column (its name would change).
fn flattened(s: &ast::Select, order: Option<&OrderBy>, alias: &str) -> Option<(ast::Select, Option<OrderBy>)> {
    let TableFactor::Derived { subquery, .. } = &s.from[0].relation else { return None };
    let SetExpr::Select(inner) = &*subquery.body else { return None };
    let plain_inner = inner.distinct.is_none() && inner.top.is_none() && inner.having.is_none() && inner.qualify.is_none() && inner.lateral_views.is_empty() && inner.named_window.is_empty() && inner.prewhere.is_none();
    if !plain_inner || s.distinct.is_some() || s.qualify.is_some() || !s.named_window.is_empty() || subquery.with.is_some() {
        return None;
    }
    let columns: HashMap<String, Expr> = inner.projection.iter().filter_map(|i| match i {
        SelectItem::ExprWithAlias { expr, alias } => Some((write::ident(alias), expr.clone())),
        _ => None,
    }).collect();
    let mut flat = s.clone();
    // Items first: a plain column keeps its name as an alias; any other unnamed item would be named after its new text.
    for item in flat.projection.iter_mut() {
        if let SelectItem::UnnamedExpr(e) = item {
            let name = match e {
                Expr::Identifier(i) => write::ident(i),
                Expr::CompoundIdentifier(p) => write::ident(p.last()?),
                _ => return None,
            };
            *item = SelectItem::ExprWithAlias { expr: e.clone(), alias: ident_for(&name) };
        }
    }
    let outputs: HashSet<String> = flat.projection.iter().filter_map(|i| match i {
        SelectItem::ExprWithAlias { alias, .. } => Some(write::ident(alias)),
        _ => None,
    }).collect();
    let mut flat_order = order.cloned();
    let mut parts: Vec<&mut Expr> = flat.projection.iter_mut().filter_map(|i| match i {
        SelectItem::ExprWithAlias { expr, .. } => Some(expr),
        _ => None,
    }).collect();
    parts.extend(flat.selection.iter_mut().chain(flat.having.iter_mut()));
    if let ast::GroupByExpr::Expressions(exprs, _) = &mut flat.group_by {
        parts.extend(exprs.iter_mut());
    }
    for e in parts {
        written_in(e, &columns, alias, None)?;
    }
    for e in order_list(flat_order.as_mut()) {
        written_in(e, &columns, alias, Some(&outputs))?;
    }
    flat.from = inner.from.clone();
    flat.selection = match (inner.selection.clone(), flat.selection.take()) {
        (Some(a), Some(b)) => Some(Expr::BinaryOp { left: Box::new(Expr::Nested(Box::new(a))), op: ast::BinaryOperator::And, right: Box::new(Expr::Nested(Box::new(b))) }),
        (a, b) => a.or(b),
    };
    Some((flat, flat_order))
}

/// Writes each of the base's columns that `e` names (`region`, `sales.region`, `sales.__pondra_m0`) as its expression. None
/// when `e` names anything else (in an ORDER BY, the query's own columns, `ours`, may be named) or holds a subquery or a window.
fn written_in(e: &mut Expr, columns: &HashMap<String, Expr>, alias: &str, ours: Option<&HashSet<String>>) -> Option<()> {
    let done = visit_expressions_mut(e, |x: &mut Expr| {
        let name = match &*x {
            Expr::Subquery(_) | Expr::Exists { .. } | Expr::InSubquery { .. } | Expr::Lambda(_) => return ControlFlow::Break(()),
            Expr::Function(f) if f.over.is_some() => return ControlFlow::Break(()),
            Expr::Identifier(i) => write::ident(i),
            Expr::CompoundIdentifier(p) => match p.as_slice() {
                [q, n] if write::ident(q) == alias => write::ident(n),
                _ => return ControlFlow::Break(()),
            },
            _ => return ControlFlow::Continue(()),
        };
        match columns.get(&name) {
            Some(c) if !(matches!(x, Expr::Identifier(_)) && ours.is_some_and(|o| o.contains(&name))) => {
                *x = Expr::Nested(Box::new(c.clone()));
                ControlFlow::Continue(())
            }
            _ if matches!(x, Expr::Identifier(_)) && ours.is_some_and(|o| o.contains(&name)) => ControlFlow::Continue(()), // (ORDER BY a column of the query's own, by its name)
            _ => ControlFlow::Break(()),
        }
    });
    matches!(done, ControlFlow::Continue(())).then_some(())
}

/// Walks a statement's queries from the inside out, so a subquery is rewritten before the query around it. `view` finds a view
/// with measures by the name a query gives it; `routine` says whether the lake has a function of a name.
struct Rewrite<'a> {
    view: &'a dyn Fn(&ObjectName) -> Option<Arc<Definition>>,
    routine: &'a dyn Fn(&str) -> bool,
    changed: bool,
    /// The CTEs in scope, innermost last, that read a view with measures as it is, a WHERE aside (`WITH f AS (SELECT * FROM sales
    /// WHERE day > …)`, as a frame's steps are): a query reading one reads the view, with that WHERE. Every other CTE is there as
    /// None: it hides a view of its name.
    passes: Vec<HashMap<String, Option<Passing>>>,
}

/// A view with measures, and the WHERE of the CTEs it was read through.
type Passing = (Arc<Definition>, Option<Expr>);

impl VisitorMut for Rewrite<'_> {
    type Break = anyhow::Error;

    fn pre_visit_query(&mut self, query: &mut ast::Query) -> ControlFlow<anyhow::Error> {
        let mut scope = HashMap::new();
        for cte in query.with.iter().flat_map(|w| w.cte_tables.iter()) {
            let found = self.passing(&scope, &cte.query);
            scope.insert(write::ident(&cte.alias.name), found);
        }
        self.passes.push(scope);
        ControlFlow::Continue(())
    }

    fn post_visit_query(&mut self, query: &mut ast::Query) -> ControlFlow<anyhow::Error> {
        let done = self.query(query);
        self.passes.pop();
        match done {
            Ok(()) => ControlFlow::Continue(()),
            Err(err) => ControlFlow::Break(err),
        }
    }
}

impl Rewrite<'_> {
    /// One query: a view with measures read as stored becomes its default form; otherwise its SELECT is rewritten if it names one.
    fn query(&mut self, query: &mut ast::Query) -> Result<()> {
        if let Some(d) = Definition::of(query)? {
            shape(&d, query)?;
            *query = default_form(&d)?;
            self.changed = true;
            return Ok(());
        }
        let order = query.order_by.as_mut();
        self.body(&mut query.body, order)
    }

    /// A query body: a SELECT, or each arm of a set operation (which has no ORDER BY of its own).
    fn body(&mut self, body: &mut SetExpr, order: Option<&mut OrderBy>) -> Result<()> {
        match body {
            SetExpr::Select(select) => self.select(select, order),
            SetExpr::SetOperation { left, right, .. } => {
                self.body(left, None)?;
                self.body(right, None)
            }
            _ => Ok(()),
        }
    }

    /// One SELECT, rewritten when it reads a view with measures.
    fn select(&mut self, select: &mut ast::Select, order: Option<&mut OrderBy>) -> Result<()> {
        let Some(((d, kept), alias)) = self.measured(select) else { return Ok(()) };
        if rewrite_select(&d, &alias, select, order, self.routine, kept)? {
            self.changed = true;
        }
        Ok(())
    }

    /// The view with measures a SELECT reads as its one FROM item (itself, or through CTEs that pass its rows on), and the name it
    /// reads it by.
    fn measured(&self, select: &ast::Select) -> Option<(Passing, String)> {
        let [item] = select.from.as_slice() else { return None };
        let TableFactor::Table { name, alias, .. } = &item.relation else { return None };
        let alias = alias.as_ref().map_or_else(|| last_part(name), |a| write::ident(&a.name));
        Some((self.found(&HashMap::new(), name)?, alias))
    }

    /// The view with measures a name reads: a CTE in scope (`scope`, the one being made, first), else a view.
    fn found(&self, scope: &HashMap<String, Option<Passing>>, name: &ObjectName) -> Option<Passing> {
        if let [part] = name.0.as_slice() {
            let cte = part.as_ident().map(write::ident).unwrap_or_default();
            if let Some(p) = std::iter::once(scope).chain(self.passes.iter().rev()).find_map(|s| s.get(&cte)) {
                return p.clone();
            }
        }
        Some(((self.view)(name)?, None))
    }

    /// A CTE that passes a view's rows on as they are, a WHERE aside: `SELECT * FROM v [WHERE …]`, the WHERE naming columns bare.
    fn passing(&self, scope: &HashMap<String, Option<Passing>>, q: &ast::Query) -> Option<Passing> {
        let plain = q.with.is_none() && q.order_by.is_none() && q.limit_clause.is_none() && q.fetch.is_none();
        let SetExpr::Select(s) = &*q.body else { return None };
        let ([item], [SelectItem::Wildcard(_)]) = (s.from.as_slice(), s.projection.as_slice()) else { return None };
        let TableFactor::Table { name, alias: None, .. } = &item.relation else { return None };
        let bare = s.selection.as_ref().is_none_or(|w| !any_expr(w, |e| matches!(e, Expr::CompoundIdentifier(_))));
        if !plain || !bare || !item.joins.is_empty() || is_grouped(s) || s.having.is_some() || s.qualify.is_some() || s.distinct.is_some() {
            return None;
        }
        let (d, before) = self.found(scope, name)?;
        let kept = match (before, s.selection.clone()) {
            (Some(a), Some(b)) => Some(Expr::BinaryOp { left: Box::new(Expr::Nested(Box::new(a))), op: ast::BinaryOperator::And, right: Box::new(Expr::Nested(Box::new(b))) }),
            (a, b) => a.or(b),
        };
        Some((d, kept))
    }
}

/// A `MEASURE`, `AGG` or `AGGREGATE` call the rewrite left alone reads a measure of a view that has none: refused, not run.
struct Leftover<'a> {
    routine: &'a dyn Fn(&str) -> bool,
}

impl VisitorMut for Leftover<'_> {
    type Break = anyhow::Error;

    fn pre_visit_expr(&mut self, e: &mut Expr) -> ControlFlow<anyhow::Error> {
        let Expr::Function(f) = e else { return ControlFlow::Continue(()) };
        if measure_call(f, self.routine) {
            return ControlFlow::Break(anyhow!("MEASURE(...) reads a measure of a view with measures, and this query reads none"));
        }
        ControlFlow::Continue(())
    }
}

/// Rewrites the statements in place and returns whether anything changed. `view` finds a view with measures by the name a query
/// gives it; `routine` says whether the lake has a function or macro of a name. CREATE VIEW (not materialized), CREATE MACRO and
/// CREATE FUNCTION are stored as written: their queries are rewritten when read. A materialized view can't hold the alias yet.
pub fn rewrite_with(stmts: &mut [Statement], view: &dyn Fn(&ObjectName) -> Option<Arc<Definition>>, routine: &dyn Fn(&str) -> bool) -> Result<bool> {
    let mut rewrite = Rewrite { view, routine, changed: false, passes: vec![] };
    for stmt in stmts.iter_mut() {
        let stored = match stmt {
            Statement::CreateMacro { .. } | Statement::CreateFunction(_) => true,
            Statement::CreateView(v) => {
                ensure!(!(v.materialized && has_marks(&v.query)), "AS MEASURE in a materialized view isn't taken yet: make a view with measures, and a materialized view of MEASURE(...) over it");
                ensure!(v.columns.is_empty() || !has_marks(&v.query), "a view with measures names its columns in its SELECT (... AS name, ... AS MEASURE name), not in a list after its name");
                !v.materialized
            }
            _ => false,
        };
        if stored {
            continue;
        }
        if let ControlFlow::Break(err) = stmt.visit(&mut rewrite) {
            return Err(err);
        }
        if let ControlFlow::Break(err) = stmt.visit(&mut Leftover { routine }) {
            return Err(err);
        }
    }
    Ok(rewrite.changed)
}

// Joins: a view with measures joins each table onto one of its keys, so no row of the view is counted twice.

/// Checks that each join of a view with measures is onto a key of the table it joins. `keys` gives a table's keys (PRIMARY KEY
/// and UNIQUE columns, as SQL names) by the name SQL gives it. A query joining a view to something else is refused by the rewrite.
pub fn joins_checked(d: &Definition, keys: &dyn Fn(&ObjectName) -> Option<Vec<Vec<String>>>) -> Result<()> {
    let joins: &[ast::Join] = match d.select.from.first() {
        Some(item) => &item.joins,
        None => &[],
    };
    for join in joins {
        let TableFactor::Table { name, alias, .. } = &join.relation else { bail!("a view with measures joins tables by their keys: {} is not a table", join.relation) };
        let full = write::object(name);
        let Some(found) = keys(name).filter(|k| !k.is_empty()) else { bail!("{full} has no key (a PRIMARY KEY or UNIQUE; NOT ENFORCED is enough): a join to it could count rows twice") };
        let rel = alias.as_ref().map_or_else(|| last_part(name), |a| write::ident(&a.name));
        let covered = covered_by(constraint_of(&join.join_operator), &rel);
        let keys_text = found.iter().map(|k| format!("({})", k.join(", "))).collect::<Vec<_>>().join(", ");
        ensure!(found.iter().any(|k| k.iter().all(|c| covered.contains(c))), "{full} is joined on ({}), not on a key ({keys_text}): a join in a view with measures must be many-to-one", covered.join(", "));
    }
    Ok(())
}

/// The columns of the joined relation `rel` that a join condition equates to another column; USING names its columns outright.
fn covered_by(constraint: Option<&ast::JoinConstraint>, rel: &str) -> Vec<String> {
    match constraint {
        Some(ast::JoinConstraint::On(cond)) => conjuncts(cond).into_iter().filter_map(|c| equal_column(c, rel)).collect(),
        Some(ast::JoinConstraint::Using(cols)) => cols.iter().map(last_part).collect(),
        _ => vec![],
    }
}

/// The conditions a condition is ANDed from.
fn conjuncts(e: &Expr) -> Vec<&Expr> {
    match e {
        Expr::BinaryOp { left, op: ast::BinaryOperator::And, right } => [conjuncts(left), conjuncts(right)].concat(),
        Expr::Nested(inner) => conjuncts(inner),
        _ => vec![e],
    }
}

/// The column of `rel` that an equality names (`o.customer_id = c.id` names `id`); an equality between two of its columns pins neither.
fn equal_column(e: &Expr, rel: &str) -> Option<String> {
    let Expr::BinaryOp { left, op: ast::BinaryOperator::Eq, right } = e else { return None };
    match (column_of(left, rel), column_of(right, rel)) {
        (Some(c), None) | (None, Some(c)) => Some(c),
        _ => None,
    }
}

/// The column a qualified reference names, when its qualifier is `rel`.
fn column_of(e: &Expr, rel: &str) -> Option<String> {
    match e {
        Expr::CompoundIdentifier(parts) => match parts.as_slice() {
            [.., q, c] if write::ident(q) == rel => Some(write::ident(c)),
            _ => None,
        },
        _ => None,
    }
}

// The lake: the views with measures it holds, and the checks that need its tables.

/// The views with measures a lake holds, by local name (`sales`, or `crm.sales`). Read again only after a commit, as
/// `routines::listed` is: every query asks.
async fn own_views(lake: &Lake) -> Result<Arc<HashMap<String, Arc<Definition>>>> {
    type Seen = Mutex<HashMap<String, (u64, Arc<HashMap<String, Arc<Definition>>>)>>;
    static SEEN: LazyLock<Seen> = LazyLock::new(Default::default);
    let version = lake.cat.version();
    if let (Some(v), Some((at, all))) = (version, SEEN.lock().unwrap().get(&lake.url)) {
        if *at == v {
            return Ok(all.clone());
        }
    }
    let mut out = HashMap::new();
    for (key, view) in lake.cat.scan::<ddl::StoredView>("q/", "q0").await? {
        let Some(local) = key.strip_prefix("q/") else { continue };
        // A stored view that won't parse can't be read either way: it is skipped, not a failure of every query.
        if let Ok(Some(d)) = Definition::parse(&view.sql) {
            out.insert(local.to_string(), Arc::new(d));
        }
    }
    let all = Arc::new(out);
    if let Some(v) = version {
        SEEN.lock().unwrap().insert(lake.url.clone(), (v, all.clone()));
    }
    Ok(all)
}

/// The views with measures a lake can read: its own by local name, and those of the lakes attached to it (`lake.sales`).
async fn views(lake: &Lake, sql: &str) -> Result<HashMap<String, Arc<Definition>>> {
    let mut out: HashMap<String, Arc<Definition>> = own_views(lake).await?.iter().map(|(k, d)| (k.clone(), d.clone())).collect();
    // (another lake's only when the statement names it: its catalog has no version to keep them by, so each look reads it)
    let attached: Vec<(String, Arc<Lake>)> = lake.attached.read().unwrap().iter().filter(|(name, _)| ddl::mentions(sql, name)).cloned().collect();
    for (name, other) in attached {
        for (local, d) in own_views(&other).await?.iter() {
            if !local.contains('.') {
                out.insert(format!("{name}.public.{local}"), d.clone());
            }
            out.insert(format!("{name}.{local}"), d.clone());
        }
    }
    Ok(out)
}

/// Whether a statement may need the rewrite: it names a measure, or may read a view with measures. The words come first,
/// then this lake's views as kept, then an attached lake's only when the statement names it (every statement asks: no copy).
pub async fn wanted(lake: &Lake, sql: &str) -> Result<bool> {
    if sql.contains(MARK) || CALL_TEXT.is_match(sql) {
        return Ok(true);
    }
    if own_views(lake).await?.keys().any(|name| ddl::mentions(sql, name)) {
        return Ok(true);
    }
    let attached: Vec<(String, Arc<Lake>)> = lake.attached.read().unwrap().iter().filter(|(name, _)| ddl::mentions(sql, name)).cloned().collect();
    for (name, other) in attached {
        if own_views(&other).await?.keys().any(|local| ddl::mentions(sql, &format!("{name}.{local}")) || ddl::mentions(sql, &format!("{name}.public.{local}"))) {
            return Ok(true);
        }
    }
    Ok(false)
}

/// Rewrites statements against the views with measures of `lake`, and the functions it has (see `rewrite_with`).
pub async fn rewrite(lake: &Lake, sql: &str, stmts: &mut [Statement]) -> Result<bool> {
    let views = views(lake, sql).await?;
    let routines = routines::listed(lake).await?;
    let me = format!("{}.", ddl::lake_name(lake));
    let view = |name: &ObjectName| -> Option<Arc<Definition>> { candidates(&write::object(name), &me).iter().find_map(|c| views.get(c).cloned()) };
    rewrite_with(stmts, &view, &|name: &str| routines.contains_key(name))
}

/// The names a view may be found under for a name a statement gives: with `public.` and this lake's own name optional.
fn candidates(full: &str, me: &str) -> Vec<String> {
    let inner = full.strip_prefix(me).unwrap_or(full);
    let mut out = vec![full.to_string(), inner.to_string()];
    if let Some(rest) = inner.strip_prefix("public.") {
        out.push(rest.to_string());
    }
    out
}

/// Checks a view with measures where it is made: `sql` is its SELECT as stored. Its shape is checked, and each join of its FROM
/// must be onto a key of the table it joins.
pub async fn check(lake: &Lake, sql: &str) -> Result<()> {
    if !sql.contains(MARK) {
        return Ok(());
    }
    let stmts = Parser::parse_sql(&GenericDialect {}, sql)?;
    let [Statement::Query(query)] = stmts.as_slice() else { bail!("a view with measures is one SELECT") };
    let Some(d) = Definition::of(query)? else { return Ok(()) };
    shape(&d, query)?;
    let keys = join_keys(lake, query).await?;
    joins_checked(&d, &|name: &ObjectName| keys.get(&write::object(name)).cloned())
}

/// The keys of each table a SELECT joins, by the name the query gives it.
async fn join_keys(lake: &Lake, query: &ast::Query) -> Result<HashMap<String, Vec<Vec<String>>>> {
    let mut keys = HashMap::new();
    let SetExpr::Select(select) = &*query.body else { return Ok(keys) };
    for join in select.from.iter().flat_map(|item| item.joins.iter()) {
        let TableFactor::Table { name, .. } = &join.relation else { continue };
        let full = write::object(name);
        let (other, local) = ddl::resolve(lake, &full).await?;
        let owner: &Lake = other.as_deref().unwrap_or(lake);
        let mut found = Vec::new();
        if let Some(meta) = owner.cat.get::<store::TableMeta>(&store::table_key(&local)).await? {
            let meta = meta.logical();
            if !meta.key.is_empty() {
                found.push(meta.key.clone());
            }
            found.extend(meta.constraints.iter().filter(|c| matches!(c.kind, Kind::PrimaryKey | Kind::Unique)).map(|c| c.columns.clone()));
        }
        if let Some(view) = owner.cat.get::<ddl::StoredView>(&ddl::query_key(&local)).await? {
            found.extend(view.facts.iter().filter(|c| matches!(c.kind, Kind::PrimaryKey | Kind::Unique)).map(|c| c.columns.clone())); // (a view's key facts: ADR-065 §2)
        }
        keys.insert(full, found);
    }
    Ok(keys)
}

// Tests: the text, the default form, the rewrite, the refusals, the shape and the joins.

#[cfg(test)]
mod tests {
    use super::*;

    /// The sales view, as the user writes it.
    const SALES: &str = "SELECT o.ordered_at::date AS day, c.region, sum(o.amount) AS MEASURE revenue, count(DISTINCT o.customer_id) AS MEASURE customers, revenue / customers AS MEASURE per_customer FROM orders o LEFT JOIN customers c ON o.customer_id = c.id";

    fn sales() -> Arc<Definition> {
        Arc::new(Definition::parse(&text(SALES)).expect("parses").expect("has measures"))
    }

    /// Rewrites statements where `sales` is the only view with measures and the lake's functions are `routine`.
    fn rewritten_with(sql: &str, routine: &dyn Fn(&str) -> bool) -> Result<(bool, String)> {
        let mut stmts = Parser::parse_sql(&GenericDialect {}, sql)?;
        let view = |name: &ObjectName| -> Option<Arc<Definition>> { (write::object(name) == "sales").then(sales) };
        let changed = rewrite_with(&mut stmts, &view, routine)?;
        Ok((changed, stmts.iter().map(|s| s.to_string()).collect::<Vec<_>>().join("; ")))
    }

    fn rewritten(sql: &str) -> Result<(bool, String)> {
        rewritten_with(sql, &|_: &str| false)
    }

    #[test]
    fn a_frame_step_reads_the_view_through_its_ctes() {
        let sql = "WITH _1 AS (SELECT * FROM sales WHERE day >= DATE '2026-01-01'), _2 AS (SELECT * FROM _1 WHERE region = 'north') SELECT day, MEASURE(revenue) AS r FROM _2 GROUP BY day";
        let (changed, sql) = rewritten(sql).expect("rewrites");
        assert!(changed);
        assert!(sql.contains(">= DATE '2026-01-01'") && sql.contains("(c.region) = 'north'"), "{sql}");
        assert!(sql.contains("sum((o.amount))") && sql.contains("LEFT JOIN customers") && !sql.contains("__pondra"), "{sql}");
    }

    #[test]
    fn a_cte_of_a_view_s_name_hides_the_view() {
        let (changed, _) = rewritten("WITH sales AS (SELECT 1 AS revenue) SELECT sum(revenue) FROM sales").expect("parses");
        assert!(!changed);
    }

    /// The shape check, on a view as the user writes it.
    fn shaped(sql: &str) -> Result<()> {
        let stmts = Parser::parse_sql(&GenericDialect {}, &text(sql))?;
        let [Statement::Query(query)] = stmts.as_slice() else { bail!("one query") };
        shape(&Definition::of(query)?.ok_or_else(|| anyhow!("no measure"))?, query)
    }

    #[test]
    fn text_stores_each_measure_under_its_alias() {
        for (sql, stored) in [
            ("SELECT sum(x) AS MEASURE revenue FROM t", "SELECT sum(x) AS \"pondra_measure:revenue\" FROM t"),
            ("SELECT sum(x) AS MEASURE \"Revenue\" FROM t", "SELECT sum(x) AS \"pondra_measure:Revenue\" FROM t"),
            ("SELECT sum(x) AS MEASURE a, count(*) AS MEASURE b FROM t", "SELECT sum(x) AS \"pondra_measure:a\", count(*) AS \"pondra_measure:b\" FROM t"),
        ] {
            assert_eq!(&*text(sql), stored, "{sql}");
        }
    }

    #[test]
    fn text_leaves_aliases_strings_and_comments_alone() {
        for sql in ["SELECT x AS measure FROM t", "SELECT x AS measure, y FROM t", "SELECT 'x as measure foo' AS s FROM t", "SELECT 1 AS n -- as measure bar\nFROM t"] {
            assert!(matches!(text(sql), Cow::Borrowed(_)), "{sql}");
        }
    }

    #[test]
    fn written_shows_a_measure_again() {
        let stored = text("SELECT sum(x) AS MEASURE revenue, y AS MEASURE \"Per Day\" FROM t");
        assert_eq!(&*written(&stored), "SELECT sum(x) AS MEASURE revenue, y AS MEASURE \"Per Day\" FROM t");
    }

    #[test]
    fn a_plain_view_is_not_a_definition() {
        assert!(Definition::parse("SELECT o.x AS x FROM orders AS o").expect("parses").is_none());
    }

    #[test]
    fn a_plain_read_gives_the_dimensions_and_the_measures_grouped() {
        let read = default_form(&sales()).expect("builds").to_string();
        assert!(read.contains("(sum(o.amount)) / (count(DISTINCT o.customer_id)) AS per_customer"), "{read}");
        assert!(read.contains("LEFT JOIN customers"), "{read}");
        assert!(read.ends_with("GROUP BY 1, 2"), "{read}");
    }

    #[test]
    fn a_query_naming_a_measure_reads_the_base_with_the_join_it_needs() {
        let (changed, sql) = rewritten("SELECT region, MEASURE(per_customer) FROM sales GROUP BY region").expect("rewrites");
        assert!(changed && sql.contains("LEFT JOIN customers"), "{sql}");
        assert!(sql.contains("count(DISTINCT (o.customer_id))") && sql.contains("AS per_customer"), "{sql}");
        assert!(sql.ends_with("GROUP BY (c.region)"), "{sql}");
    }

    #[test]
    fn a_query_naming_a_measure_is_the_group_by_written_by_hand() {
        let (changed, sql) = rewritten("SELECT region, MEASURE(revenue) AS r FROM sales WHERE day > DATE '2026-01-01' GROUP BY region ORDER BY r DESC").expect("rewrites");
        assert!(changed && !sql.contains("__pondra") && !sql.contains("(SELECT"), "{sql}");
        assert!(sql.contains("(c.region) AS region") && sql.contains("sum((o.amount))") && sql.contains("AS r") && sql.contains("FROM orders o LEFT JOIN customers"), "{sql}");
        assert!(sql.contains("GROUP BY (c.region) ORDER BY r DESC"), "{sql}");
    }

    #[test]
    fn a_query_that_needs_the_base_as_a_subquery_keeps_it() {
        for sql in [
            "SELECT region, MEASURE(revenue) AS r FROM sales GROUP BY region HAVING r > 5", // (an alias in HAVING)
            "SELECT upper(region), MEASURE(revenue) FROM sales GROUP BY 1",                 // (an unnamed item would change its name)
            "SELECT region, MEASURE(revenue) FROM sales WHERE region IN (SELECT region FROM regions) GROUP BY 1",
            "SELECT region, MEASURE(revenue), rank() OVER (ORDER BY MEASURE(revenue)) FROM sales GROUP BY 1",
        ] {
            let (changed, out) = rewritten(sql).expect(sql);
            assert!(changed && out.contains("__pondra_m0") && out.contains("AS sales"), "{sql}: {out}");
        }
    }

    #[test]
    fn a_measure_alone_drops_the_join_nothing_reads() {
        let (changed, sql) = rewritten("SELECT MEASURE(revenue) FROM sales").expect("rewrites");
        assert!(changed && !sql.contains("customers") && !sql.contains("GROUP BY") && sql.contains("AS revenue"), "{sql}");
    }

    #[test]
    fn a_column_beside_a_measure_groups_by_its_position() {
        let (changed, sql) = rewritten("SELECT day, revenue FROM sales").expect("rewrites");
        assert!(changed && sql.ends_with("GROUP BY 1") && !sql.contains("LEFT JOIN"), "{sql}");
    }

    #[test]
    fn an_aggregate_beside_a_measure_is_not_grouped_by() {
        let (changed, sql) = rewritten("SELECT day, revenue, count(*) AS n FROM sales").expect("rewrites");
        assert!(changed && sql.ends_with("GROUP BY 1"), "{sql}");
    }

    #[test]
    fn a_sum_of_a_measure_is_the_measure_itself() {
        let (changed, sql) = rewritten("SELECT region, sum(revenue) AS total FROM sales GROUP BY 1").expect("rewrites");
        assert!(changed && sql.contains("sum((o.amount))") && sql.contains("AS total") && sql.ends_with("GROUP BY 1"), "{sql}");
    }

    #[test]
    fn a_plain_read_of_the_view_is_left_alone() {
        assert!(!rewritten("SELECT * FROM sales").expect("parses").0);
    }

    #[test]
    fn a_stored_view_keeps_its_measure_calls_as_written() {
        let (changed, out) = rewritten("CREATE VIEW top AS SELECT region, MEASURE(revenue) FROM sales GROUP BY region").expect("stored as written");
        assert!(!changed && out.contains("MEASURE(revenue)"), "{out}");
    }

    #[test]
    fn a_function_of_the_lake_named_agg_is_no_measure_call() {
        let agg = |name: &str| name == "agg";
        let (changed, sql) = rewritten_with("SELECT agg(x) FROM orders", &agg).expect("left alone");
        assert!(!changed && sql.contains("agg(x)"), "{sql}");
        assert!(rewritten("SELECT agg(x) FROM orders").is_err(), "without the function, a call of no measure is refused");
    }

    #[test]
    fn an_aggregate_of_several_arguments_is_another_engines_function() {
        let sql = "SELECT aggregate(array(1, 2, 3), 0, (acc, x) -> acc + x) AS total";
        let (changed, out) = rewritten(sql).expect("left for the planner");
        assert!(!changed && out.contains("aggregate("), "{out}");
    }

    #[test]
    fn a_query_with_measures_refuses_what_it_cannot_say() {
        for (sql, why) in [
            ("SELECT region FROM sales WHERE revenue > 5", "no value per row"),
            ("SELECT sum(revenue * 2) FROM sales", "inside sum"),
            ("SELECT MEASURE(revenue) FROM sales AS s JOIN customers AS c ON s.customer_id = c.id", "reads its view alone"),
            ("SELECT MEASURE(nothing) FROM sales", "no measure nothing"),
            ("SELECT MEASURE(revenue) FROM orders", "reads none"),
        ] {
            let err = rewritten(sql).expect_err(sql).to_string();
            assert!(err.contains(why), "{sql}: {err}");
        }
    }

    #[test]
    fn a_definition_refuses_the_shapes_it_cannot_take() {
        for (sql, why) in [
            ("SELECT region, sum(x) AS MEASURE m FROM t GROUP BY region", "GROUP BY"),
            ("SELECT *, sum(x) AS MEASURE m FROM t", "no *"),
            ("SELECT sum(x) AS MEASURE m FROM a RIGHT JOIN b ON a.id = b.id", "LEFT JOIN"),
            ("SELECT y AS m, sum(x) AS MEASURE m FROM t", "twice"),
            ("SELECT x + b AS MEASURE a, a + 1 AS MEASURE b FROM t", "refer to each other"),
            ("SELECT sum(x) OVER () AS MEASURE m FROM t", "window"),
            ("SELECT sum(x) AS d, sum(y) AS MEASURE m FROM t", "aggregate"),
        ] {
            let err = shaped(sql).expect_err(sql).to_string();
            assert!(err.contains(why), "{sql}: {err}");
        }
    }

    #[test]
    fn a_view_joins_only_onto_keys() {
        let by_id = |n: &ObjectName| (write::object(n) == "customers").then(|| vec![vec!["id".to_string()]]);
        joins_checked(&sales(), &by_id).expect("the join is onto the key");

        let on_name = "SELECT o.region AS region, sum(o.amount) AS MEASURE revenue FROM orders o LEFT JOIN customers c ON o.name = c.name";
        let d = Definition::parse(&text(on_name)).expect("parses").expect("has measures");
        let err = joins_checked(&d, &by_id).expect_err("not a key").to_string();
        assert!(err.contains("not on a key"), "{err}");

        let no_keys = |_: &ObjectName| -> Option<Vec<Vec<String>>> { None };
        let err = joins_checked(&sales(), &no_keys).expect_err("no key").to_string();
        assert!(err.contains("has no key"), "{err}");
    }
}
