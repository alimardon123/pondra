//! Macros, views, files, other engines' tables and FROM first, expanded in a statement's syntax tree: the front's middle pass, in every build.
use crate::text::{args, ident, object, text};
use anyhow::{bail, ensure, Context, Result};
use datafusion::sql::sqlparser::ast::{self, visit_expressions_mut, Expr, FunctionArg, FunctionArgExpr, FunctionArguments, Statement, TableFactor, VisitMut, VisitorMut};
use datafusion::sql::sqlparser::{dialect::GenericDialect, keywords::Keyword, parser::Parser, tokenizer::Token};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::ops::ControlFlow;
use std::sync::LazyLock;

/// A function or a procedure, as the catalog keeps it.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct Routine {
    pub kind: Kind,
    pub params: Vec<Param>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub language: String, // sql or python; none: a macro (DuckDB's CREATE MACRO)
    pub body: String,
    /// A function's result: a SQL type, or `TABLE (name type, …)`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub returns: Option<String>,
    #[serde(default, skip_serializing_if = "Options::is_empty")]
    pub with: Options,
}

#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum Kind {
    Macro,     // a function whose value is an expression's (a scalar function)
    Table,     // a function whose value is a query's rows (a table function)
    Procedure, // statements, or a Python program
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct Param {
    pub name: String, // (Postgres's unnamed parameters: 1, 2…, which the body calls $1, $2…)
    #[serde(default, rename = "type", skip_serializing_if = "Option::is_none")]
    pub ty: Option<String>, // (its arguments are cast to it)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default: Option<String>, // a SQL expression
}

/// Postgres's words for how a function behaves, and Pondra's options (`WITH (…)`).
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Default)]
pub struct Options {
    /// NULL in, NULL out, without running it (`STRICT`, `RETURNS NULL ON NULL INPUT`).
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub strict: bool,
    /// immutable, stable or volatile. A query calling a volatile Python function isn't answered
    /// from the result cache (a Python function is volatile unless it says otherwise).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub volatility: Option<String>,
    /// A Python function called once per batch with pyarrow arrays, not once per row.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub vectorized: bool,
    /// Packages its Python needs (`requests, jinja2`): each node installs them once.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub packages: String,
    /// The body is a module, and this function of it is what runs (the decorators' form).
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub entry: String,
    /// Seconds a call may take (a function's: each batch, 60 by default).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout: Option<f64>,
    /// Seconds an answer is reused for the same arguments (`cache = '5 minutes'`: `pyfn::Answers`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache: Option<u64>,
}

impl Options {
    fn is_empty(&self) -> bool { *self == Options::default() }
}

impl Routine {
    pub fn python(&self) -> bool { self.language == "python" }

    /// What SQL calls it: a function (a macro, if made as one) or a procedure.
    pub fn what(&self) -> &'static str {
        match (self.kind, self.language.is_empty()) {
            (Kind::Procedure, _) => "procedure",
            (_, true) => "macro",
            _ => "function",
        }
    }

    /// Does a query calling it give the same answer every time (so it may be remembered)?
    pub fn cacheable(&self) -> bool { !self.python() || matches!(self.with.volatility.as_deref(), Some("immutable" | "stable")) }
}

/// Parameters: `( [IN] [name] type [DEFAULT expr | = expr], … )`; a parameter without a name is
/// called by its place (`$1`).
pub(crate) fn params(p: &mut Parser) -> Result<Vec<Param>> {
    if !p.consume_token(&Token::LParen) || p.consume_token(&Token::RParen) {
        return Ok(vec![]);
    }
    let mut i = 0;
    let params = p.parse_comma_separated(|p| {
        i += 1;
        if let Some(k) = p.parse_one_of_keywords(&[Keyword::IN, Keyword::OUT, Keyword::INOUT, Keyword::VARIADIC]) {
            if k != Keyword::IN {
                return Err(datafusion::sql::sqlparser::parser::ParserError::ParserError(format!("{k:?} parameters: return a table instead (RETURNS TABLE (…))")));
            }
        }
        let unnamed = p.maybe_parse(|p| {
            let t = p.parse_data_type()?;
            match p.peek_token().token {
                Token::Comma | Token::RParen | Token::Eq => Ok(t),
                Token::Word(w) if w.keyword == Keyword::DEFAULT => Ok(t),
                t => Err(datafusion::sql::sqlparser::parser::ParserError::ParserError(format!("{t}"))),
            }
        })?;
        let (name, ty) = match unnamed {
            Some(ast::DataType::Custom(n, m)) if m.is_empty() && n.0.len() == 1 && !crate::text::loose(Some(&n.to_string())) => (object(&n), None), // (`f(x)`: a name, untyped, as DuckDB's)
            Some(t) => (i.to_string(), Some(t.to_string())),
            None => {
                let name = match p.peek_token().token {
                    Token::Placeholder(n) if n.len() > 1 && !n[1..].starts_with(|c: char| c.is_ascii_digit()) => {
                        p.next_token();
                        n[1..].to_lowercase() // (`$day DATE`, as a file's parameter is written)
                    }
                    _ => ident(&p.parse_identifier()?),
                };
                let typed = !matches!(p.peek_token().token, Token::Comma | Token::RParen | Token::Eq) && !matches!(p.peek_token().token, Token::Word(ref w) if w.keyword == Keyword::DEFAULT);
                (name, if typed { Some(p.parse_data_type()?.to_string()) } else { None })
            }
        };
        let default = match p.parse_keyword(Keyword::DEFAULT) || p.consume_token(&Token::Eq) {
            true => Some(p.parse_expr()?.to_string()),
            false => None,
        };
        Ok(Param { name, ty, default })
    })?;
    p.expect_token(&Token::RParen)?;
    Ok(params)
}

/// Does `e` hold a query of its own (its columns may be that query's)?
fn queries_in(e: &Expr) -> bool {
    ast::visit_expressions(e, |x| match x {
        Expr::Subquery(_) | Expr::Exists { .. } | Expr::InSubquery { .. } | Expr::Lambda(_) => ControlFlow::Break(()),
        _ => ControlFlow::Continue(()),
    })
    .is_break()
}

/// The names a function's body gives the relations it reads (a table's own, or its alias).
fn names_in<T: ast::Visit>(body: &T) -> std::collections::HashSet<String> {
    struct Names(std::collections::HashSet<String>);
    impl ast::Visitor for Names {
        type Break = ();
        fn pre_visit_table_factor(&mut self, t: &TableFactor) -> ControlFlow<()> {
            let (name, alias) = match t {
                TableFactor::Table { name, alias, .. } => (name.0.last().and_then(|p| p.as_ident()), alias),
                TableFactor::Derived { alias, .. } | TableFactor::Function { alias, .. } | TableFactor::UNNEST { alias, .. } => (None, alias),
                _ => (None, &None),
            };
            self.0.extend(name.into_iter().chain(alias.as_ref().map(|a| &a.name)).map(|i| i.value.to_lowercase()));
            ControlFlow::Continue(())
        }
    }
    let mut names = Names(Default::default());
    let _ = body.visit(&mut names);
    names.0
}

/// `TABLE (a BIGINT, "b" VARCHAR)` → its columns and their SQL types.
pub fn columns_of(returns: &str) -> Result<Vec<(String, String)>> {
    let inner = returns.trim().strip_prefix("TABLE").context("not a table")?.trim();
    let mut p = Parser::new(&GenericDialect {}).try_with_sql(inner)?;
    p.expect_token(&Token::LParen)?;
    let cols = p.parse_comma_separated(|p| Ok((ident(&p.parse_identifier()?), p.parse_data_type()?.to_string())))?;
    p.expect_token(&Token::RParen)?;
    Ok(cols)
}

/// Its parameters' names, in order.
pub fn names(r: &Routine) -> Vec<&str> { r.params.iter().map(|p| p.name.as_str()).collect() }

pub(crate) fn parse_expr(sql: &str) -> Result<Expr> { Ok(Parser::new(&GenericDialect {}).try_with_sql(sql)?.parse_expr()?) }

pub(crate) fn parse_query(sql: &str) -> Result<ast::Query> { Ok(*Parser::new(&GenericDialect {}).try_with_sql(sql)?.parse_query()?) }

/// A query that may start with FROM (DuckDB's `FROM t`, alone or as a subquery, a CTE or a view's).
pub(crate) static FROM_FIRST: std::sync::LazyLock<regex::Regex> =
    std::sync::LazyLock::new(|| regex::Regex::new(r"(?is)(?:^|[(;]|\bas)\s*(?:(?:--[^\n]*\n|/\*.*?\*/)\s*)*from\b").expect("a regex"));

/// DuckDB's `FROM t` with no SELECT is `SELECT * FROM t` (DataFusion took it as no columns at all).
fn select_star(body: &mut ast::SetExpr) {
    match body {
        ast::SetExpr::Select(s) if s.flavor == ast::SelectFlavor::FromFirstNoSelect => {
            s.flavor = ast::SelectFlavor::Standard;
            if s.projection.is_empty() {
                s.projection = vec![ast::SelectItem::Wildcard(Default::default())];
            }
        }
        ast::SetExpr::SetOperation { left, right, .. } => {
            select_star(left);
            select_star(right);
        }
        ast::SetExpr::Query(q) => select_star(&mut q.body),
        _ => {}
    }
}

/// `sql` with its answers' columns named apart (`output_names`), if any needed it; else None, and
/// the text is left as written.
pub(crate) fn named_apart(sql: &str) -> Option<String> {
    static MAY: std::sync::LazyLock<regex::Regex> = std::sync::LazyLock::new(|| regex::Regex::new(r"(?is)\bselect\b.*(?:::|\bcast\s*\(|,)").expect("a regex"));
    if sql.len() > 64 << 10 || !MAY.is_match(sql) && !VALUES_SUB.is_match(sql) {
        return None; // (a SELECT with more than one column or a cast, or VALUES with a subquery; not a long INSERT's rows)
    }
    struct Names(bool, usize);
    impl VisitorMut for Names {
        type Break = ();
        fn pre_visit_statement(&mut self, s: &mut Statement) -> ControlFlow<()> {
            self.1 = own_values(s);
            ControlFlow::Continue(())
        }
        fn post_visit_query(&mut self, q: &mut ast::Query) -> ControlFlow<()> {
            self.0 |= output_names(q) | values_apart(q, self.1);
            ControlFlow::Continue(())
        }
    }
    let mut stmts = Parser::parse_sql(&GenericDialect {}, sql).ok()?;
    let mut names = Names(false, 0);
    for s in stmts.iter_mut().filter(|s| !matches!(s, Statement::CreateMacro { .. })) {
        let _ = s.visit(&mut names);
    }
    names.0.then(|| text(&stmts))
}

/// VALUES that may hold a subquery (`values_apart`).
pub(crate) static VALUES_SUB: std::sync::LazyLock<regex::Regex> = std::sync::LazyLock::new(|| regex::Regex::new(r"(?is)\bvalues\b.*\bselect\b").expect("a regex"));

/// A VALUES list with a subquery in a row as `SELECT … AS column1, … UNION ALL SELECT …`, its
/// ORDER BY and LIMIT kept: DataFusion works a VALUES list's rows out while it plans it, before
/// any subquery has run ("ScalarSubqueryExpr evaluated before the subquery was executed"). True if
/// it was one. `own` is an INSERT's own VALUES (`own_values`), left as they are: `write.rs` takes
/// them through the log as VALUES, and `write::rows` sets them apart.
pub(crate) fn values_apart(q: &mut ast::Query, own: usize) -> bool {
    let ast::SetExpr::Values(v) = q.body.as_ref() else { return false };
    let sub = |e: &Expr| match e {
        Expr::Subquery(_) => ControlFlow::Break(()),
        _ => ControlFlow::Continue(()),
    };
    if &*q.body as *const ast::SetExpr as usize == own || ast::visit_expressions(&v.rows, sub).is_continue() {
        return false;
    }
    let select = |row: &[Expr]| format!("SELECT {}", row.iter().enumerate().map(|(i, e)| format!("{e} AS column{}", i + 1)).collect::<Vec<_>>().join(", "));
    let Ok(union) = parse_query(&v.rows.iter().map(|r| select(&r.content)).collect::<Vec<_>>().join(" UNION ALL ")) else { return false };
    q.body = union.body;
    true
}

/// Where an INSERT's own VALUES are (`values_apart`), or 0.
fn own_values(s: &Statement) -> usize {
    match s {
        Statement::Insert(ast::Insert { source: Some(q), .. }) if matches!(*q.body, ast::SetExpr::Values(_)) => &*q.body as *const ast::SetExpr as usize,
        _ => 0,
    }
}

/// A SELECT's columns named as other engines name them, where DataFusion would refuse the query or
/// name one badly; true if any was. DataFusion names an unnamed cast of a column (`ts::date`,
/// `CAST(ts AS DATE)`) as the column, qualified (`sales.orders.ts`), and refuses two columns of one
/// name, which Postgres, DuckDB and Snowflake all take (`SELECT ts::date, *`, `SELECT id, *`). So:
/// - a cast of a column is named as the column (`ts`), as Postgres names it; or, when the SELECT
///   has another column that may have that name (a `*`, or another of its items), as written
///   (`ts::DATE`), as Snowflake and DuckDB name every cast;
/// - a column the SELECT has again (`SELECT id, *`, `SELECT id, id`) is named `id_1`, as DuckDB
///   names the second in a frame (only the item named can be: a `*`'s columns are the table's);
/// - a scalar subquery DataFusion would name as another item (it names one by its column: two of
///   `count(*)`, `max(q)` beside `(SELECT max(q) FROM t)`), and an expression written again
///   (`q + 1, q + 1`, or but for a cast: `q * 2, q::DOUBLE * 2`), are named as written, as DuckDB
///   names them (`sum(c), sum(c::DOUBLE)` too, whose aggregates `casts_apart` tells apart).
///
/// (A column the ORDER BY names qualified, `ORDER BY o.ts`, is one more of the SELECT's: DataFusion
/// adds it, so a cast of `ts` is then named as written too.)
fn output_names(q: &mut ast::Query) -> bool {
    let sorted: Vec<String> = match q.order_by.as_ref().map(|o| &o.kind) {
        Some(ast::OrderByKind::Expressions(all)) => all.iter().filter_map(|o| match &o.expr {
            Expr::CompoundIdentifier(ids) => ids.last().map(|i| if i.quote_style.is_some() { i.value.clone() } else { i.value.to_lowercase() }),
            _ => None,
        }).collect(),
        _ => vec![],
    };
    select_names(&mut q.body, &sorted) | casts_apart(q)
}

/// A call's text as DataFusion names it, near enough to compare: without its casts (`CAST(c AS
/// DOUBLE) * 2` is named as `c * 2` is), lower-cased, unquoted.
fn said(e: &Expr) -> String {
    let mut e = e.clone();
    let _ = visit_expressions_mut(&mut e, |x| {
        if let Expr::Cast { expr, .. } = x {
            *x = (**expr).clone();
        }
        ControlFlow::<()>::Continue(())
    });
    e.to_string().to_lowercase().replace('"', "")
}

/// A name's qualifiers (`t.`), left out where names are compared (`max(q)` is named `max(t.q)`).
static QUALIFIER: LazyLock<regex::Regex> = LazyLock::new(|| regex::Regex::new(r"\b[a-z_][a-z0-9_]*\.").expect("a regex"));

/// Calls of a SELECT that differ only by a cast, told apart; true if any was. DataFusion names a cast
/// as what it casts, so `sum(c)` and `sum(c::DOUBLE)`, or `min(ts)` and `min(ts::DATE)`, are both
/// `sum(t.c)` or `min(t.ts)`, and an aggregate (or a window) refuses two columns of one name, which
/// DuckDB and Postgres both answer. So each later form's arguments that cast a column are wrapped in
/// `coalesce(…)`, once for the second form, twice for the third: a coalesce of one argument is that
/// argument, and the planner takes it out again (`coalesce`'s `simplify`), so only the name differs.
/// Every place that calls a form (the SELECT's items, HAVING, QUALIFY, ORDER BY) is wrapped alike, so
/// they still name one aggregate, and an item wrapped that had no name is named as written; a
/// subquery's calls are its own.
fn casts_apart(q: &mut ast::Query) -> bool {
    // (each call of this query, not of its subqueries, after its arguments: so `round(sum(c::DOUBLE))`
    // is met after its `sum`, in both walks)
    struct Calls<F>(usize, F);
    impl<F: FnMut(&mut ast::Function)> VisitorMut for Calls<F> {
        type Break = ();
        fn pre_visit_query(&mut self, _: &mut ast::Query) -> ControlFlow<()> {
            self.0 += 1;
            ControlFlow::Continue(())
        }
        fn post_visit_query(&mut self, _: &mut ast::Query) -> ControlFlow<()> {
            self.0 -= 1;
            ControlFlow::Continue(())
        }
        fn post_visit_expr(&mut self, e: &mut Expr) -> ControlFlow<()> {
            if let (0, Expr::Function(f)) = (self.0, e) {
                (self.1)(f);
            }
            ControlFlow::Continue(())
        }
    }
    fn calls(q: &mut ast::Query, each: impl FnMut(&mut ast::Function)) {
        let ast::Query { body, order_by, .. } = q;
        let ast::SetExpr::Select(s) = body.as_mut() else { return };
        let mut walk = Calls(0, each);
        for item in &mut s.projection {
            if let ast::SelectItem::UnnamedExpr(e) | ast::SelectItem::ExprWithAlias { expr: e, .. } = item {
                let _ = e.visit(&mut walk);
            }
        }
        for e in s.having.iter_mut().chain(s.qualify.iter_mut()) {
            let _ = e.visit(&mut walk);
        }
        if let Some(ast::OrderBy { kind: ast::OrderByKind::Expressions(all), .. }) = order_by {
            for o in all {
                let _ = o.expr.visit(&mut walk);
            }
        }
    }
    // (an argument that casts a column: a cast of a value alone may be one a function needs as written)
    fn casts(a: &FunctionArg) -> bool {
        let (FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) | FunctionArg::Named { arg: FunctionArgExpr::Expr(e), .. }) = a else { return false };
        let (mut cast, mut column) = (false, false);
        let _ = ast::visit_expressions(e, |x| {
            cast |= matches!(x, Expr::Cast { .. });
            column |= matches!(x, Expr::Identifier(_) | Expr::CompoundIdentifier(_));
            ControlFlow::<()>::Continue(())
        });
        cast && column
    }
    let cast = |f: &ast::Function| matches!(&f.args, FunctionArguments::List(l) if l.args.iter().any(casts));
    // (most queries cast no column in a call: nothing to tell apart, and nothing written out)
    let mut any = false;
    calls(q, |f| any |= cast(f));
    if !any {
        return false;
    }
    // Each call as written (its qualifiers left out: `sum(t.c)` is `sum(c)`), and as DataFusion names it.
    let mut met: Vec<(String, String, bool)> = vec![];
    calls(q, |f| {
        let e = Expr::Function(f.clone());
        met.push((QUALIFIER.replace_all(&e.to_string().to_lowercase(), "").into_owned(), QUALIFIER.replace_all(&said(&e), "").into_owned(), cast(f)));
    });
    let mut forms: HashMap<&str, Vec<(&str, bool)>> = HashMap::new();
    for (written, named, cast) in &met {
        let all = forms.entry(named).or_default();
        if !all.iter().any(|(w, _)| w == written) {
            all.push((written, *cast));
        }
    }
    // (how many times each later form's casts are wrapped: the forms without a cast name themselves)
    let times: HashMap<&str, usize> = forms.into_values().filter(|all| all.len() > 1)
        .flat_map(|all| all.into_iter().filter(|(_, cast)| *cast).enumerate().map(|(i, (w, _))| (w, i + 1)))
        .collect();
    if times.is_empty() {
        return false;
    }
    let mut n = 0;
    let times: Vec<usize> = met.iter().map(|(w, ..)| times.get(w.as_str()).copied().unwrap_or(0)).collect();
    let items = |q: &ast::Query| match &*q.body {
        ast::SetExpr::Select(s) => s.projection.iter().map(|i| i.to_string()).collect(),
        _ => vec![],
    };
    let written: Vec<String> = items(q);
    calls(q, |f| {
        if let FunctionArguments::List(l) = &mut f.args {
            for a in l.args.iter_mut().filter(|a| casts(a)) {
                if let FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) | FunctionArg::Named { arg: FunctionArgExpr::Expr(e), .. } = a {
                    *e = (0..times[n]).fold(e.clone(), |e, _| crate::text::function("coalesce", vec![e]));
                }
            }
        }
        n += 1;
    });
    // (an item whose calls were wrapped, and that had no name, is named as written)
    if let ast::SetExpr::Select(s) = q.body.as_mut() {
        for (item, written) in s.projection.iter_mut().zip(written) {
            if let ast::SelectItem::UnnamedExpr(e) = item {
                if e.to_string() != written {
                    *item = ast::SelectItem::ExprWithAlias { expr: e.clone(), alias: ast::Ident::with_quote('"', written) };
                }
            }
        }
    }
    true
}

fn select_names(body: &mut ast::SetExpr, sorted: &[String]) -> bool {
    fn plain(i: &ast::Ident) -> String { if i.quote_style.is_some() { i.value.clone() } else { i.value.to_lowercase() } }
    // (a column's qualifier, if written, and name)
    fn column(e: &Expr) -> Option<(Option<String>, String)> {
        match e {
            Expr::Identifier(i) => Some((None, plain(i))),
            Expr::CompoundIdentifier(ids) if ids.len() > 1 => Some((Some(plain(&ids[ids.len() - 2])), plain(&ids[ids.len() - 1]))),
            _ => None,
        }
    }
    fn cast(e: &Expr) -> Option<String> {
        match e {
            Expr::Cast { expr, .. } => cast(expr).or_else(|| column(expr).map(|c| c.1)),
            _ => None,
        }
    }
    // (a scalar subquery's column: its alias, name or text; "" when it can't be told)
    fn inner(q: &ast::Query) -> String {
        match &*q.body {
            ast::SetExpr::Select(s) if s.projection.len() == 1 => match &s.projection[0] {
                ast::SelectItem::ExprWithAlias { alias, .. } => plain(alias),
                ast::SelectItem::UnnamedExpr(e) => column(e).map(|c| c.1).unwrap_or_else(|| said(e)),
                _ => String::new(),
            },
            _ => String::new(),
        }
    }
    match body {
        ast::SetExpr::Select(s) => {
            use ast::SelectItem::{ExprWithAlias, QualifiedWildcard, UnnamedExpr, Wildcard};
            let star = s.projection.iter().any(|i| matches!(i, Wildcard(_)));
            let stars: Vec<String> = s.projection.iter().filter_map(|i| match i {
                QualifiedWildcard(ast::SelectItemQualifiedWildcardKind::ObjectName(n), _) => n.0.last().and_then(|p| p.as_ident()).map(plain),
                _ => None,
            }).collect();
            let named = |i: &ast::SelectItem| match i {
                UnnamedExpr(e) => column(e).map(|c| c.1).or_else(|| cast(e)),
                ExprWithAlias { alias, .. } => Some(plain(alias)),
                _ => None,
            };
            let mut names: Vec<Option<String>> = s.projection.iter().map(named).collect();
            let mut changed = false;
            for k in 0..s.projection.len() {
                let UnnamedExpr(e) = &s.projection[k] else { continue };
                let Some(n) = names[k].clone() else { continue };
                let again = |j: usize| j != k && names[j].as_deref() == Some(n.as_str());
                let name = match (cast(e).is_some(), column(e)) {
                    (true, _) if star || !stars.is_empty() || (0..names.len()).any(again) || sorted.contains(&n) => e.to_string(),
                    (true, _) => n.clone(),
                    // (a `*` leaves out the system columns, and a keyed table's `_deleted`: `sys::hide`)
                    (false, Some((q, _))) if ((star || q.as_ref().is_some_and(|q| stars.contains(q))) && !crate::text::NAMES.contains(&n.as_str()) && n != "_deleted") || (0..k).any(again) => {
                        (1..).map(|i| format!("{n}_{i}")).find(|m| !names.iter().any(|x| x.as_deref() == Some(m.as_str()))).expect("a free name")
                    }
                    _ => continue,
                };
                names[k] = Some(name.clone());
                s.projection[k] = ExprWithAlias { expr: e.clone(), alias: ast::Ident::with_quote('"', name) };
                changed = true;
            }
            // What each item may be named, and whether it is a subquery's (compared without the
            // qualifiers: `max(q)` is named `max(t.q)`, and so is `(SELECT max(q) FROM t)`).
            let mut keys: Vec<Option<(bool, String)>> = s.projection.iter().zip(&names).map(|(i, n)| match (i, n) {
                (UnnamedExpr(Expr::Subquery(q)), _) => Some((true, inner(q))),
                (UnnamedExpr(e), None) => Some((false, said(e))),
                _ => n.clone().map(|n| (false, n)),
            }).collect();
            let same = |a: &(bool, String), b: &(bool, String)| match a.0 || b.0 {
                true => QUALIFIER.replace_all(&a.1, "") == QUALIFIER.replace_all(&b.1, ""),
                false => a.1 == b.1,
            };
            let first = keys.clone();
            for k in 0..s.projection.len() {
                let (UnnamedExpr(e), None, Some(key)) = (&s.projection[k], &names[k], keys[k].clone()) else { continue };
                // (every subquery of a name as written; of an expression written again, the later ones)
                let again = match key.0 {
                    true => (0..first.len()).any(|j| j != k && first[j].as_ref().is_some_and(|o| same(o, &key))),
                    false => (0..k).any(|j| keys[j].as_ref().is_some_and(|o| same(o, &key))),
                };
                if !again {
                    continue;
                }
                // (as written only where DataFusion can't have named another item so: it names a subquery
                // by its column, a literal by its type, `Int64(1)`, and a cast by what it casts; but `NULL`,
                // `count(*)` and `t.a + t.b` as they are written)
                let unlike = key.0 || ast::visit_expressions(e, |x| match x {
                    Expr::Value(v) if v.value != ast::Value::Null => ControlFlow::Break(()),
                    Expr::Cast { .. } => ControlFlow::Break(()),
                    _ => ControlFlow::Continue(()),
                }).is_break();
                let written = e.to_string();
                let name = (usize::from(!unlike)..).map(|i| if i == 0 { written.clone() } else { format!("{written}_{i}") })
                    .find(|m| !names.iter().any(|x| x.as_deref() == Some(m.as_str()))).expect("a free name");
                keys[k] = Some((false, said(e)));
                names[k] = Some(name.clone());
                s.projection[k] = ExprWithAlias { expr: e.clone(), alias: ast::Ident::with_quote('"', name) };
                changed = true;
            }
            changed
        }
        ast::SetExpr::SetOperation { left, right, .. } => select_names(left, sorted) | select_names(right, sorted),
        ast::SetExpr::Query(q) => output_names(q),
        _ => false,
    }
}

/// A scalar SQL function's body as an expression: an expression (`RETURN x * 2`), a query's one
/// value (`AS $$ SELECT x * 2 $$`), or a scalar subquery (`AS $$ SELECT max(v) FROM t WHERE k =
/// x $$`).
pub(crate) fn scalar_body(body: &str) -> Result<Expr> {
    let text = query_text(body);
    if !is_query(text) {
        return parse_expr(text);
    }
    let q = parse_query(text)?;
    if let ast::SetExpr::Select(s) = &*q.body {
        if let [ast::SelectItem::UnnamedExpr(e) | ast::SelectItem::ExprWithAlias { expr: e, .. }] = &s.projection[..] {
            if q.to_string() == format!("SELECT {}", s.projection[0]) {
                return Ok(e.clone()); // (nothing but the value)
            }
        }
    }
    Ok(Expr::Subquery(Box::new(q)))
}

/// A body's statement without the `;` that may end it.
pub(crate) fn query_text(body: &str) -> &str { body.trim().trim_end_matches(';').trim_end() }

/// Whether a body is a query (`SELECT`, `WITH`, `VALUES`) rather than one expression.
pub(crate) fn is_query(body: &str) -> bool {
    let first = query_text(body).split(|c: char| !c.is_alphanumeric()).next().unwrap_or_default().to_lowercase();
    ["select", "with", "values"].contains(&first.as_str())
}

pub(crate) struct Expander<'a> {
    pub(crate) name: &'a str, // (this lake's name: three-part names)
    pub(crate) all: &'a HashMap<String, Routine>,
    pub(crate) views: &'a HashMap<String, String>, // (a request's own: `FROM name` is its query)
    pub(crate) outside: &'a [(String, crate::ext_names::Attached)], // other engines' tables attached (`ext.rs`)
    pub(crate) depth: usize,
    pub(crate) own: usize, // (an INSERT's own VALUES: `values_apart`)
    pub(crate) columns: &'a HashMap<String, Vec<String>>, // (the lake's tables named, for an argument's columns: `owner`)
    pub(crate) scopes: Vec<Option<Scope<'a>>>, // (each query's FROM around a call, innermost last; None: a UNION's)
}

/// A FROM's relations, and the columns its joins' USING (or NATURAL: `*`) make one.
type Scope<'a> = (Vec<Relation<'a>>, Vec<String>);

/// A relation in a FROM: the name its columns go by, and its columns if it is one of the lake's
/// tables (a view, a subquery or a CTE: not known before planning).
type Relation<'a> = (Option<ast::ObjectName>, Option<&'a [String]>);

impl<'a> Expander<'a> {
    /// The relations `from` reads, as an argument's columns are found in them.
    fn relations(&self, from: &[ast::TableWithJoins], ctes: &[String]) -> Scope<'a> {
        let columns = self.columns;
        let one = |f: &TableFactor| match f {
            TableFactor::Table { name, alias, args: None, .. } => {
                let local = crate::text::local_in(self.name, &object(name)).filter(|n| !ctes.contains(n));
                (Some(alias.as_ref().map_or_else(|| name.clone(), |a| ast::ObjectName::from(vec![a.name.clone()]))), local.and_then(|n| columns.get(&n)).map(Vec::as_slice))
            }
            TableFactor::Table { alias, .. } | TableFactor::Derived { alias, .. } | TableFactor::Function { alias, .. } | TableFactor::UNNEST { alias, .. } => {
                (alias.as_ref().map(|a| ast::ObjectName::from(vec![a.name.clone()])), None)
            }
            _ => (None, None),
        };
        use ast::{JoinConstraint as C, JoinOperator as J};
        let merged = |j: &ast::Join| match &j.join_operator {
            J::Join(c) | J::Inner(c) | J::Left(c) | J::LeftOuter(c) | J::Right(c) | J::RightOuter(c) | J::FullOuter(c) | J::Semi(c) | J::LeftSemi(c) | J::RightSemi(c) | J::Anti(c) | J::LeftAnti(c) | J::RightAnti(c) => match c {
                C::Using(names) => names.iter().map(object).collect(),
                C::Natural => vec!["*".to_string()],
                _ => vec![],
            },
            _ => vec![],
        };
        let relations = from.iter().flat_map(|t| std::iter::once(one(&t.relation)).chain(t.joins.iter().map(|j| one(&j.relation)))).collect();
        (relations, from.iter().flat_map(|t| &t.joins).flat_map(merged).collect())
    }

    /// The relation a column of an argument is from, as SQL finds it: the innermost query whose
    /// FROM has it. One that might (a view, a subquery) stops the search, unless it is all its
    /// query reads.
    fn owner(&self, c: &str) -> Result<Option<ast::ObjectName>> {
        for scope in self.scopes.iter().rev() {
            let Some((scope, merged)) = scope else { return Ok(None) };
            let has: Vec<_> = scope.iter().filter(|(_, cols)| cols.is_some_and(|cols| cols.iter().any(|x| x == c))).collect();
            match (&has[..], &scope[..]) {
                ([(q, _)], _) | ([], [(q, None)]) => return Ok(q.clone()),
                ([(q, _), ..], _) if merged.iter().any(|m| m == c || m == "*") => return Ok(q.clone()), // (USING's one column: the first side's)
                ([a, b, ..], _) => bail!("{c} is a column of {} and {}: name it with its table", a.0.as_ref().map(|n| n.to_string()).unwrap_or_default(), b.0.as_ref().map(|n| n.to_string()).unwrap_or_default()),
                ([], s) if s.iter().any(|(_, cols)| cols.is_none()) => return Ok(None),
                _ => {}
            }
        }
        Ok(None)
    }

    /// An argument's columns named with their tables (`id` → `orders.id`), for a body that reads
    /// a table of its own: Postgres passes a function values, so `f(id)` must not become its
    /// table's `id` inside `SELECT max(v) FROM t WHERE k = x`.
    fn qualify(&self, e: &mut Expr) -> Result<()> {
        if queries_in(e) {
            return Ok(()); // (its columns may be its own query's)
        }
        let mut out = Ok(());
        let _ = visit_expressions_mut(e, |x| {
            if let Expr::Identifier(i) = x {
                match self.owner(&ident(i)) {
                    Ok(Some(q)) => *x = Expr::CompoundIdentifier(q.0.iter().filter_map(|p| p.as_ident().cloned()).chain([i.clone()]).collect()),
                    Ok(None) => {}
                    Err(e) => {
                        out = Err(e);
                        return ControlFlow::Break(());
                    }
                }
            }
            ControlFlow::Continue(())
        });
        out
    }

    fn find(&self, name: &ast::ObjectName, kind: Kind) -> Option<(String, &Routine)> {
        let name = crate::text::local_in(self.name, &object(name))?;
        self.all.get(&name).filter(|r| r.kind == kind).map(|r| (name, r))
    }

    /// The body with the arguments in its parameters' places (`p`, or `$1`), cast to their types,
    /// its own function calls expanded; and the arguments.
    fn call<T: VisitMut + ast::Visit>(&self, name: &str, r: &Routine, args: &[FunctionArg], mut body: T) -> Result<(T, Vec<Expr>)> {
        ensure!(self.depth < 16, "{name}: functions calling functions 16 deep (a loop?)");
        let mut values = arguments(name, r, args)?;
        if crate::text::mentions(&r.body, "from") {
            let theirs = names_in(&body);
            for v in values.values_mut() {
                self.qualify(v).map_err(|e| e.context(format!("{name}'s argument {v}")))?;
                // A column named by a relation the body names too would be read as the body's.
                let mut clash = None;
                let _ = ast::visit_expressions(v, |x| {
                    if let Expr::CompoundIdentifier(p) = x {
                        clash = clash.take().or_else(|| p.len().checked_sub(2).map(|i| p[i].value.to_lowercase()).filter(|q| theirs.contains(q)));
                    }
                    ControlFlow::<()>::Continue(())
                });
                if let Some(q) = clash.filter(|_| !queries_in(v)) {
                    bail!("{name}'s argument {v} is a column of {q}, and {name} reads {q} too: name it otherwise in the query (FROM {q} AS x)");
                }
            }
        }
        let values = typed(r, values)?;
        let param = |e: &Expr| match e {
            Expr::Identifier(i) => values.get_key_value(&ident(i)),
            Expr::Value(v) => match &v.value {
                ast::Value::Placeholder(p) => match p.trim_start_matches('$') {
                    n if n.starts_with(|c: char| c.is_ascii_digit()) => n.parse::<usize>().ok().and_then(|n| r.params.get(n.wrapping_sub(1))).and_then(|p| values.get_key_value(&p.name)),
                    n => values.get_key_value(&n.to_lowercase()), // (`$x`: the parameter `x`, as everywhere else)
                },
                _ => None,
            },
            _ => None,
        };
        // (each use is a copy: a parameter used twice by functions calling functions doubles at every
        // level, and 16 levels of that never finished; counted before anything is copied)
        let sizes: HashMap<&String, usize> = values.iter().map(|(k, v)| (k, v.to_string().len())).collect();
        let mut size = 0;
        let _ = ast::visit_expressions(&body, |e| {
            size += param(e).map_or(0, |(k, _)| sizes[k]);
            ControlFlow::<()>::Continue(())
        });
        ensure!(size <= 1 << 20, "{name}: its arguments come to over 1 MB of SQL written into its body (a parameter used many times, by functions calling functions); pass the value once, from a CTE");
        let _ = visit_expressions_mut(&mut body, |e| {
            if let Some((_, v)) = param(e) {
                *e = grouped(v.clone());
            }
            ControlFlow::<()>::Continue(())
        });
        if let ControlFlow::Break(e) = VisitMut::visit(&mut body, &mut Expander { depth: self.depth + 1, scopes: self.scopes.clone(), ..*self }) {
            return Err(e);
        }
        // (a body calling functions several times grows as theirs do: four calls, ten deep, are a
        // million expressions, which took the node's memory before anything was planned)
        let mut n = 0;
        let _ = ast::visit_expressions(&body, |_| {
            n += 1;
            if n > LARGEST { ControlFlow::Break(()) } else { ControlFlow::Continue(()) }
        });
        ensure!(n <= LARGEST, "{name}: written out, it is over {LARGEST} expressions (functions calling functions several times each); work a value out once, in a CTE");
        Ok((body, r.params.iter().map(|p| values[&p.name].clone()).collect()))
    }
}

/// The most expressions one function's call may come to, written out.
const LARGEST: usize = 100_000;

/// The columns of the lake's tables that `stmts` and the SQL functions' bodies read, by name,
/// when a function reads a table: what an argument's columns are told apart by (`Expander::owner`).
pub(crate) async fn columns<H: crate::front::Host>(host: &H, stmts: &[Statement], all: &HashMap<String, Routine>) -> Result<HashMap<String, Vec<String>>> {
    let name = host.name();
    let reading: Vec<&Routine> = all.values().filter(|r| r.kind != Kind::Procedure && !r.python() && crate::text::mentions(&r.body, "from")).collect();
    if reading.is_empty() {
        return Ok(HashMap::new());
    }
    let mut names = std::collections::BTreeSet::new();
    let mut add = |n: &ast::ObjectName| {
        names.extend(crate::text::local_in(&name, &object(n)));
        ControlFlow::<()>::Continue(())
    };
    for s in stmts {
        let _ = ast::visit_relations(s, &mut add);
    }
    for r in reading {
        if let Ok(q) = parse_query(query_text(&r.body)) {
            let _ = ast::visit_relations(&q, &mut add);
        }
    }
    host.columns(names).await
}

/// Each argument cast to its parameter's type, if it has one.
fn typed(r: &Routine, values: HashMap<String, Expr>) -> Result<HashMap<String, Expr>> {
    let ty: HashMap<&str, &str> = r.params.iter().filter(|p| !crate::text::loose(p.ty.as_deref())).filter_map(|p| Some((p.name.as_str(), p.ty.as_deref()?))).collect();
    values.into_iter().map(|(k, v)| Ok(match ty.get(k.as_str()) {
        Some(t) => (k.clone(), cast(v, t)?),
        None => (k, v),
    })).collect()
}

/// `CAST(e AS t)`, with `e` never read again as text: a function's expansion holds its
/// arguments' and those of the functions it calls, and reading each again cost its whole size at
/// every level.
fn cast(e: Expr, t: &str) -> Result<Expr> {
    let mut c = parse_expr(&format!("CAST(NULL AS {t})"))?;
    if let Expr::Cast { expr, .. } = &mut c {
        **expr = e;
    }
    Ok(c)
}

/// `e` in parentheses where an operator around it could take part of it (`x + x` into `x * 2`),
/// and as it is otherwise: functions calling functions add their levels to every parser's limit
/// (DataFusion's is 50).
fn grouped(e: Expr) -> Expr {
    match e {
        Expr::Cast { kind: ast::CastKind::Cast | ast::CastKind::TryCast | ast::CastKind::SafeCast, .. }
        | Expr::Nested(_)
        | Expr::Identifier(_)
        | Expr::CompoundIdentifier(_)
        | Expr::Value(_)
        | Expr::Function(_)
        | Expr::Case { .. }
        | Expr::Subquery(_) => e,
        e => Expr::Nested(Box::new(e)),
    }
}

/// `template` with `$_1`, `$_2`, … replaced by `parts` (as `cast`).
fn around(template: &str, parts: Vec<Expr>) -> Result<Expr> {
    let mut e = parse_expr(template)?;
    let mut parts: Vec<Option<Expr>> = parts.into_iter().map(Some).collect();
    let _ = visit_expressions_mut(&mut e, |x| {
        if let Expr::Value(ast::ValueWithSpan { value: ast::Value::Placeholder(p), .. }) = x {
            if let Some(part) = p.strip_prefix("$_").and_then(|n| n.parse::<usize>().ok()).and_then(|n| parts.get_mut(n.wrapping_sub(1))).and_then(Option::take) {
                *x = grouped(part);
            }
        }
        ControlFlow::<()>::Continue(())
    });
    Ok(e)
}

/// A Python function's call as its DataFusion function takes it: every argument, in order, cast
/// to its parameter's type, under the name it is registered by.
fn whole(name: &str, r: &Routine, args: &[FunctionArg]) -> Result<(ast::ObjectName, Vec<FunctionArg>)> {
    let values = typed(r, arguments(name, r, args)?)?;
    let args = r.params.iter().map(|p| FunctionArg::Unnamed(FunctionArgExpr::Expr(values[&p.name].clone()))).collect();
    let name = match r.kind {
        Kind::Table => ast::Ident::new(name), // (DataFusion looks a table function up by its name as written)
        _ => ast::Ident::with_quote('"', name),
    };
    Ok((ast::ObjectName::from(vec![name]), args))
}

impl VisitorMut for Expander<'_> {
    type Break = anyhow::Error;

    fn pre_visit_statement(&mut self, s: &mut Statement) -> ControlFlow<Self::Break> {
        self.own = own_values(s);
        let from = match s {
            Statement::Update(u) => std::iter::once(u.table.clone()).chain(match &u.from {
                Some(ast::UpdateTableFromKind::BeforeSet(f) | ast::UpdateTableFromKind::AfterSet(f)) => f.clone(),
                None => vec![],
            }).collect(),
            Statement::Delete(d) => match &d.from {
                ast::FromTable::WithFromKeyword(f) | ast::FromTable::WithoutKeyword(f) => f.iter().chain(d.using.iter().flatten()).cloned().collect(),
            },
            _ => vec![],
        };
        self.scopes = vec![Some(self.relations(&from, &[]))];
        ControlFlow::Continue(())
    }

    fn pre_visit_query(&mut self, q: &mut ast::Query) -> ControlFlow<Self::Break> {
        let ctes: Vec<String> = q.with.iter().flat_map(|w| &w.cte_tables).map(|c| ident(&c.alias.name)).collect();
        let scope = match &*q.body {
            ast::SetExpr::Select(s) => Some(self.relations(&s.from, &ctes)),
            _ => None,
        };
        self.scopes.push(scope);
        ControlFlow::Continue(())
    }

    fn post_visit_query(&mut self, q: &mut ast::Query) -> ControlFlow<Self::Break> {
        self.scopes.pop();
        select_star(&mut q.body);
        output_names(q);
        values_apart(q, self.own);
        ControlFlow::Continue(())
    }

    fn post_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<Self::Break> {
        let Expr::Function(f) = expr else { return ControlFlow::Continue(()) };
        let Some((name, r)) = self.find(&f.name, Kind::Macro) else { return ControlFlow::Continue(()) };
        let args = match &f.args {
            FunctionArguments::List(l) => l.args.clone(),
            _ => vec![],
        };
        if r.python() {
            return match whole(&name, r, &args) {
                Ok((n, args)) => {
                    f.name = n;
                    f.args = FunctionArguments::List(ast::FunctionArgumentList { duplicate_treatment: None, args, clauses: vec![] });
                    ControlFlow::Continue(())
                }
                Err(e) => ControlFlow::Break(e),
            };
        }
        let expanded = scalar_body(&r.body).and_then(|body| self.call(&name, r, &args, body)).and_then(|(e, args)| {
            let e = match &r.returns {
                Some(t) => cast(e, t)?,
                None => e,
            };
            match (r.with.strict, args.is_empty()) {
                (true, false) => {
                    let nulls = (0..args.len()).map(|i| format!("$_{} IS NULL", i + 2)).collect::<Vec<_>>().join(" OR ");
                    around(&format!("CASE WHEN {nulls} THEN NULL ELSE $_1 END"), std::iter::once(e).chain(args).collect())
                }
                _ => Ok(e),
            }
        });
        match expanded {
            Ok(e) => *expr = grouped(e),
            Err(e) => return ControlFlow::Break(e),
        }
        ControlFlow::Continue(())
    }

    fn post_visit_table_factor(&mut self, t: &mut TableFactor) -> ControlFlow<Self::Break> {
        if let (TableFactor::Table { name, args: None, .. }, false) = (&mut *t, self.outside.is_empty()) {
            let parts: Vec<String> = object(name).split('.').map(str::to_string).collect();
            match crate::ext_names::attached_table(self.outside, &parts) {
                Ok(Some(files)) => {
                    *name = ast::ObjectName::from(vec![ast::Ident::with_quote('"', files)]); // (another engine's table: `ext.rs`)
                    return ControlFlow::Continue(());
                }
                Err(e) => return ControlFlow::Break(e),
                Ok(None) => {}
            }
        }
        match crate::past_names::table_factor(self.name, t) {
            Ok(true) => return ControlFlow::Continue(()),
            Err(e) => return ControlFlow::Break(e),
            Ok(false) => {}
        }
        match crate::ext_names::table(t) {
            Ok(Some(files)) => {
                if let TableFactor::Table { name, args, .. } = t {
                    (*name, *args) = (ast::ObjectName::from(vec![ast::Ident::with_quote('"', files)]), None); // (files anywhere: `ext.rs`)
                }
                return ControlFlow::Continue(());
            }
            Err(e) => return ControlFlow::Break(e),
            Ok(None) => {}
        }
        if let TableFactor::Table { name, alias, args: None, .. } = t {
            let Some(sql) = self.views.get(&object(name)) else { return ControlFlow::Continue(()) };
            static NONE: std::sync::LazyLock<HashMap<String, String>> = std::sync::LazyLock::new(HashMap::new);
            let mut q = match parse_query(sql) {
                Ok(q) => q,
                Err(e) => return ControlFlow::Break(e.context(format!("{name}"))),
            };
            q.visit(&mut Expander { views: &NONE, depth: self.depth + 1, scopes: vec![], ..*self })?; // (its functions; its names are the lake's)
            let shown = name.0.last().and_then(|part| part.as_ident()).map_or_else(|| object(name), crate::text::ident); // (`FROM page.public.v` reads as v, as in Postgres)
            let alias = alias.clone().or_else(|| Some(ast::TableAlias { explicit: true, name: ast::Ident::new(shown), columns: vec![], at: None }));
            *t = TableFactor::Derived { lateral: false, subquery: Box::new(q), alias, sample: None };
            return ControlFlow::Continue(());
        }
        let TableFactor::Table { name, alias, args: Some(args), .. } = t else { return ControlFlow::Continue(()) };
        let Some((found, r)) = self.find(name, Kind::Table) else { return ControlFlow::Continue(()) };
        if r.python() {
            return match whole(&found, r, &args.args) {
                Ok((n, a)) => {
                    (*name, args.args) = (n, a);
                    if alias.is_none() {
                        *alias = Some(ast::TableAlias { explicit: true, name: ast::Ident::new(crate::text::split(&found).1), columns: vec![], at: None });
                    }
                    ControlFlow::Continue(())
                }
                Err(e) => ControlFlow::Break(e),
            };
        }
        let q = parse_query(query_text(&r.body)).and_then(|body| self.call(&found, r, &args.args, body)).and_then(|(q, _)| match &r.returns {
            // (RETURNS TABLE: its columns, by place, as their types)
            Some(ret) => {
                let cols = columns_of(ret)?;
                let quoted = |c: &str| ast::Ident::with_quote('"', c).to_string();
                let select = cols.iter().map(|(c, t)| format!("CAST({} AS {t}) AS {}", quoted(c), quoted(c))).collect::<Vec<_>>().join(", ");
                parse_query(&format!("SELECT {select} FROM ({q}) AS \"_f\"({})", cols.iter().map(|(c, _)| quoted(c)).collect::<Vec<_>>().join(", ")))
            }
            None => Ok(q),
        });
        let q = match q {
            Ok(q) => q,
            Err(e) => return ControlFlow::Break(e),
        };
        let alias = alias.clone().or_else(|| Some(ast::TableAlias { explicit: true, name: ast::Ident::new(crate::text::split(&found).1), columns: vec![], at: None }));
        *t = TableFactor::Derived { lateral: false, subquery: Box::new(q), alias, sample: None };
        ControlFlow::Continue(())
    }
}

/// Each parameter's argument: by position, then by name (`rate := 0.3`, `rate => 0.3`), then its
/// default.
pub(crate) fn arguments(name: &str, r: &Routine, given: &[FunctionArg]) -> Result<HashMap<String, Expr>> {
    let a = args(name, given)?;
    let mut out = HashMap::new();
    for (i, e) in a.given.into_iter().enumerate() {
        let p = r.params.get(i).map(|p| p.name.clone()).with_context(|| format!("{name} takes {} arguments", r.params.len()))?;
        out.insert(p, e.clone());
    }
    for (p, e) in a.named {
        ensure!(r.params.iter().any(|q| q.name == p), "{name} has no parameter {p}");
        ensure!(out.insert(p.clone(), e.clone()).is_none(), "{name}: {p} given twice");
    }
    for p in &r.params {
        if !out.contains_key(&p.name) {
            let d = p.default.as_deref().with_context(|| format!("{name}: no value for {}", p.name))?;
            out.insert(p.name.clone(), parse_expr(d)?);
        }
    }
    Ok(out)
}
