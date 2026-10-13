//! The Postgres port's types that Arrow's don't say: which columns of an answer, and which
//! parameters of a statement, are uuids. A uuid is text in the lake (`types.rs`), so its Arrow type
//! says nothing; the port traces each column of an answer back, in the statement as written, to a
//! uuid column of a table or view it reads, a cast to `uuid` or a function that makes one, and marks
//! it on the answer's schema (`typed`). It then sends it as Postgres's `uuid` (oid 2950: `pg.rs`'s
//! `field_type`), and drivers hand it over as their uuid type (psycopg's `UUID`, pgjdbc's
//! `java.util.UUID`). Nothing plans or answers differently for the mark. A trace that can't tell
//! marks nothing, and the column goes as text, as before. The same parse gives each column's Postgres name
//! (`pg_names`), which `typed` keeps in the schema's metadata beside the mark: a statement is parsed once.

use std::cell::RefCell;
use std::collections::HashMap;
use std::ops::ControlFlow;
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::sql::sqlparser::ast::{self, Expr, ObjectName, Query, Select, SelectItem, SetExpr, Statement, TableFactor, TableWithJoins, Value, VisitMut, WildcardAdditionalOptions};
use datafusion::sql::sqlparser::{dialect::GenericDialect, parser::Parser};

use crate::pg_names;
use crate::store::{table_key, Lake};
use crate::write::{ident, object};

/// The field metadata that marks an answer's uuid column: the port reads it, other doors pass it by.
const MARK: &str = "pg_type";

/// A relation's columns: each one's name in SQL, and whether it is a uuid.
type Cols = Vec<(String, bool)>;

/// The columns of the tables and views a statement names, by name as written (None: not a table or
/// view here, so its columns are unknown).
type Known = HashMap<String, Option<Cols>>;

/// Is this answer's column a uuid (`typed` marked it)?
pub fn is_uuid(f: &Field) -> bool { f.metadata().get(MARK).is_some_and(|t| t == "uuid") }

/// What an answer's columns are, from its statement as written: which are uuids (`columns`), and the
/// names Postgres gives them (`pg_names::of`; None when they can't be read the same way as the columns).
#[derive(Default, Clone)]
pub struct Shape {
    pub uuids: Vec<bool>,
    names: Option<Vec<pg_names::Item>>,
}

impl From<Vec<bool>> for Shape {
    /// A key lookup's uuids (`serve::point`), with no names of their own.
    fn from(uuids: Vec<bool>) -> Self { Shape { uuids, names: None } }
}

/// `schema` as an answer sends it: the columns `shape` marks as uuids, and each column's Postgres name in its
/// metadata (`pg_names::shown` reads it), where `shape` has the names and they fit the schema. The fields keep
/// their Arrow names, and the batches are the same.
pub fn typed(schema: &SchemaRef, shape: &Shape) -> SchemaRef {
    let marked = marked(schema, &shape.uuids);
    let Some(names) = shape.names.as_ref().and_then(|items| pg_names::named(&marked, items)) else { return marked };
    let fields: Vec<Arc<Field>> = marked.fields().iter().zip(names).map(|(f, name)| {
        let mut meta = f.metadata().clone();
        meta.insert(pg_names::KEY.into(), name);
        Arc::new(f.as_ref().clone().with_metadata(meta))
    }).collect();
    Arc::new(Schema::new_with_metadata(fields, marked.metadata().clone()))
}

/// `schema` with the columns `marks` names marked as uuids. Only when there is a mark for each column
/// and each one marked holds text: anything else is a trace gone wrong, and marks nothing.
fn marked(schema: &SchemaRef, marks: &[bool]) -> SchemaRef {
    let text = |f: &Field| matches!(f.data_type(), DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8);
    let fields = schema.fields();
    if !marks.contains(&true) || marks.len() != fields.len() || fields.iter().zip(marks).any(|(f, m)| *m && !text(f)) {
        return schema.clone();
    }
    let fields: Vec<Arc<Field>> = fields.iter().zip(marks).map(|(f, m)| match m {
        true => {
            let mut meta = f.metadata().clone();
            meta.insert(MARK.into(), "uuid".into());
            Arc::new(f.as_ref().clone().with_metadata(meta))
        }
        false => f.clone(),
    }).collect();
    Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone()))
}

/// The shape of the answer `sql` gives: which of its columns are uuids, in order (a query's, or a write's
/// RETURNING; none when none is, or when it can't tell), and the Postgres names of its columns. A catalog
/// query gets names only, as before: its uuids are never traced. The statement is parsed once for both.
pub async fn columns(lake: &Lake, sql: &str) -> Shape {
    let Some(stmt) = one(sql) else { return Shape::default() };
    let names = pg_names::of(&stmt);
    if crate::pg_catalog::wanted(sql) {
        return Shape { uuids: vec![], names };
    }
    let mut known = Known::new();
    look_up(lake, &stmt, &mut known, 0).await;
    let uuids: Vec<bool> = Trace::new(&known).statement(&stmt).map(|c| c.into_iter().map(|(_, u)| u).collect()).unwrap_or_default();
    Shape { uuids, names }
}

/// Which of `sql`'s parameters are uuids, by their number less one (`$1` is 0): one cast to uuid, one
/// a uuid column is compared with in a WHERE or HAVING, an INSERT's value for a uuid column, an
/// UPDATE's `SET c = $1`.
pub async fn params(lake: &Lake, sql: &str) -> Vec<usize> {
    if !sql.contains('$') {
        return vec![];
    }
    let Some((stmt, known)) = read(lake, sql).await else { return vec![] };
    let t = Trace::new(&known);
    t.statement(&stmt);
    let _ = ast::visit_expressions(&stmt, |e| {
        t.params.borrow_mut().extend(match e {
            Expr::Cast { expr, data_type: ast::DataType::Uuid, .. } => param(expr),
            Expr::Function(f) if object(&f.name) == "pondra_uuid" => first_arg(f).and_then(param),
            _ => None,
        });
        ControlFlow::<()>::Continue(())
    });
    t.params.into_inner()
}

/// What Postgres reads by a uuid's type, read so where SQL comes in, at every door (`types::rewrite`): a
/// string compared with a uuid, or written into a uuid column (`VALUES`, `SET`), is a uuid (`id = 'A0EE…'`
/// finds `a0ee…`, as the column holds it, and `SET id = 'A0EE…'` writes it so), and
/// `pg_typeof(x)` of one is 'uuid' (the planner sees text). Only for a statement with such a string or
/// a `pg_typeof`; whether it changed anything.
pub async fn rewrite(lake: &Lake, stmt: &mut Statement) -> bool {
    let asks = |e: &Expr| matches!(e, Expr::Function(f) if object(&f.name) == "pg_typeof");
    let spelled = |e: &Expr| string(e).is_some_and(|s| !crate::types::is_uuid(s) && crate::types::uuid_of(s).is_ok());
    if !ast::visit_expressions(stmt, |e| if asks(e) || spelled(e) { ControlFlow::Break(()) } else { ControlFlow::Continue(()) }).is_break() {
        return false; // (nearly every statement)
    }
    let mut known = Known::new();
    look_up(lake, stmt, &mut known, 0).await;
    rewritten(&known, stmt)
}

/// `rewrite`'s work, over the relations `known` describes. What it changes is found by its place in the
/// statement (`at`), never by its text: `name = 'A0EE…'` beside `id = 'A0EE…'` stays as written.
fn rewritten(known: &Known, stmt: &mut Statement) -> bool {
    let t = Trace::new(known);
    t.statement(stmt);
    let (asked, compared) = (t.typeof_.into_inner(), t.strings.into_inner());
    if asked.is_empty() && compared.is_empty() {
        return false;
    }
    let mut named = Named(&asked, false);
    let _ = stmt.visit(&mut named); // (first: it finds the items by their places too)
    let mut changed = named.1;
    let _ = ast::visit_expressions_mut(stmt, |e| {
        let to = match at(e) {
            p if asked.contains(&p) => Some("uuid".to_string()),
            p if compared.contains(&p) => string(e).and_then(|s| crate::types::uuid_of(s).ok()),
            _ => None,
        };
        if let Some(to) = to.filter(|to| string(e) != Some(to.as_str())) {
            *e = Expr::value(Value::SingleQuotedString(to));
            changed = true;
        }
        ControlFlow::<()>::Continue(())
    });
    changed
}

/// An expression's place in its statement, which tells it from another of the same text.
fn at(e: &Expr) -> usize { std::ptr::from_ref(e) as usize }

/// Gives a query's item `pg_typeof(x)` of a uuid, without an alias, its name as written, as any other
/// item has, before `rewritten` puts 'uuid' in its place (whose own name would be `Utf8("uuid")`).
struct Named<'a>(&'a [usize], bool);

impl ast::VisitorMut for Named<'_> {
    type Break = ();

    fn pre_visit_query(&mut self, q: &mut Query) -> ControlFlow<()> {
        fn items(body: &mut SetExpr, asked: &[usize], changed: &mut bool) {
            match body {
                SetExpr::Select(s) => {
                    for item in s.projection.iter_mut() {
                        if let SelectItem::UnnamedExpr(e) = item {
                            if asked.contains(&at(e)) {
                                let alias = ast::Ident::with_quote('"', e.to_string());
                                *item = SelectItem::ExprWithAlias { expr: Expr::value(Value::SingleQuotedString("uuid".into())), alias };
                                *changed = true;
                            }
                        }
                    }
                }
                SetExpr::SetOperation { left, right, .. } => {
                    items(left, asked, changed);
                    items(right, asked, changed);
                }
                _ => {}
            }
        }
        items(&mut q.body, self.0, &mut self.1);
        ControlFlow::Continue(())
    }
}

/// `sql` parsed, with the columns of every table and view it names. None for one with nothing to trace:
/// a query of Postgres's catalog (never a uuid there), or one that doesn't parse as one statement.
async fn read(lake: &Lake, sql: &str) -> Option<(Statement, Known)> {
    if crate::pg_catalog::wanted(sql) {
        return None;
    }
    let stmt = one(sql)?;
    let mut known = Known::new();
    look_up(lake, &stmt, &mut known, 0).await;
    Some((stmt, known))
}

fn one(sql: &str) -> Option<Statement> {
    let mut parsed = Parser::parse_sql(&GenericDialect {}, sql).ok()?;
    (parsed.len() == 1).then(|| parsed.pop()).flatten()
}

/// The columns of each table and view `stmt` names that `known` lacks. A view's are its query's,
/// traced the same way, four views deep at most; anything else (a CTE, a table function, a file) is
/// unknown.
async fn look_up(lake: &Lake, stmt: &Statement, known: &mut Known, depth: usize) {
    let mut names = vec![];
    let _ = ast::visit_relations(stmt, |n| {
        names.push(object(n));
        ControlFlow::<()>::Continue(())
    });
    for name in names {
        if !known.contains_key(&name) {
            known.insert(name.clone(), None); // (a view that names itself ends here)
            let cols = Box::pin(of(lake, &name, known, depth)).await;
            known.insert(name, cols);
        }
    }
}

async fn of(lake: &Lake, name: &str, known: &mut Known, depth: usize) -> Option<Cols> {
    let (other, local) = crate::ddl::resolve(lake, name).await.ok()?;
    let lake = other.as_deref().unwrap_or(lake);
    if let Ok(Some(m)) = lake.cat.meta(&table_key(&local)).await {
        let l = m.described(); // (SQL's names, as `SELECT *` shows them)
        return Some(l.columns.iter().filter(|(c, _)| !l.marker(c)).map(|(c, _)| (c.clone(), l.uuid(c))).collect());
    }
    let view = match depth < 4 {
        true => lake.cat.get::<crate::ddl::StoredView>(&crate::ddl::query_key(&local)).await.ok()??,
        false => return None,
    };
    let stmt = one(&view.sql)?;
    let mut theirs = Known::new(); // (another lake's view names its own lake's tables)
    let known = if other.is_some() { &mut theirs } else { known };
    look_up(lake, &stmt, known, depth + 1).await;
    Trace::new(known).statement(&stmt)
}

/// One relation of a FROM: the names its columns may be qualified by, and its columns.
struct Rel {
    names: Vec<String>,
    cols: Option<Cols>,
}

/// A statement's walk, over the relations `known` describes.
struct Trace<'a> {
    known: &'a Known,
    /// The CTEs in scope, innermost last.
    ctes: RefCell<Vec<(String, Option<Cols>)>>,
    /// The parameters found to be uuids, by number less one.
    params: RefCell<Vec<usize>>,
    /// The calls `pg_typeof(x)` whose `x` is a uuid, by place (`at`).
    typeof_: RefCell<Vec<usize>>,
    /// The string literals read as uuids (compared with one, or written into one), by place.
    strings: RefCell<Vec<usize>>,
}

impl<'a> Trace<'a> {
    fn new(known: &'a Known) -> Self { Trace { known, ctes: Default::default(), params: Default::default(), typeof_: Default::default(), strings: Default::default() } }

    /// The columns `stmt` answers: a query's, or a write's RETURNING.
    fn statement(&self, stmt: &Statement) -> Option<Cols> {
        match stmt {
            Statement::Query(q) => self.query(q),
            Statement::Insert(i) => {
                let ast::TableObject::TableName(n) = &i.table else { return None };
                let (names, cols) = self.table(n);
                let into = Rel { names: i.table_alias.as_ref().map_or(names, |a| vec![ident(&a.alias)]), cols };
                if let Some(source) = &i.source {
                    self.inserted(source, &i.columns, &into);
                }
                self.items(i.returning.as_deref()?, &[into])
            }
            Statement::Update(u) => {
                let mut rels = self.from(std::slice::from_ref(&u.table));
                if let Some(ast::UpdateTableFromKind::BeforeSet(f) | ast::UpdateTableFromKind::AfterSet(f)) = &u.from {
                    rels.extend(self.from(f));
                }
                for a in &u.assignments {
                    if let ast::AssignmentTarget::ColumnName(c) = &a.target {
                        if self.column(None, &last(c), &rels[..1]) {
                            self.uuid(&a.value);
                        }
                    }
                }
                u.selection.iter().for_each(|e| self.compared(e, &rels));
                self.items(u.returning.as_deref()?, &rels)
            }
            Statement::Delete(d) => {
                let (ast::FromTable::WithFromKeyword(t) | ast::FromTable::WithoutKeyword(t)) = &d.from;
                let mut rels = self.from(t);
                rels.extend(self.from(d.using.as_deref().unwrap_or_default()));
                d.selection.iter().for_each(|e| self.compared(e, &rels));
                self.items(d.returning.as_deref()?, &rels)
            }
            _ => None,
        }
    }

    fn query(&self, q: &Query) -> Option<Cols> {
        let depth = self.ctes.borrow().len();
        for cte in q.with.iter().flat_map(|w| &w.cte_tables) {
            let cols = self.query(&cte.query).map(|c| renamed(c, &cte.alias.columns));
            self.ctes.borrow_mut().push((ident(&cte.alias.name), cols));
        }
        let out = self.body(&q.body);
        self.ctes.borrow_mut().truncate(depth);
        out
    }

    fn body(&self, s: &SetExpr) -> Option<Cols> {
        match s {
            SetExpr::Select(s) => self.select(s),
            SetExpr::Query(q) => self.query(q),
            // (a uuid only where both sides have one, as Postgres takes a UNION of two uuids)
            SetExpr::SetOperation { left, right, .. } => {
                let (l, r) = (self.body(left), self.body(right));
                let l = l?;
                Some(match r {
                    Some(r) if r.len() == l.len() => l.into_iter().zip(r).map(|((n, a), (_, b))| (n, a && b)).collect(),
                    _ => l.into_iter().map(|(n, _)| (n, false)).collect(),
                })
            }
            _ => None,
        }
    }

    fn select(&self, s: &Select) -> Option<Cols> {
        let rels = self.from(&s.from);
        s.selection.iter().chain(&s.having).for_each(|e| self.compared(e, &rels));
        self.items(&s.projection, &rels)
    }

    fn from(&self, from: &[TableWithJoins]) -> Vec<Rel> {
        let mut rels = vec![];
        for t in from {
            self.factor(&t.relation, &mut rels);
            t.joins.iter().for_each(|j| self.factor(&j.relation, &mut rels));
        }
        rels
    }

    fn factor(&self, f: &TableFactor, rels: &mut Vec<Rel>) {
        let (names, cols, alias) = match f {
            TableFactor::Table { name, alias, args: None, .. } => {
                let (names, cols) = self.table(name);
                (names, cols, alias)
            }
            TableFactor::Derived { subquery, alias, .. } => (vec![], self.query(subquery), alias),
            TableFactor::NestedJoin { table_with_joins, .. } => return rels.extend(self.from(std::slice::from_ref(table_with_joins))),
            _ => (vec![], None, &None),
        };
        rels.push(match alias {
            Some(a) => Rel { names: vec![ident(&a.name)], cols: cols.map(|c| renamed(c, &a.columns)) },
            None => Rel { names, cols },
        });
    }

    /// A table's (or view's, or CTE's) names to qualify its columns by (`s.t` and `t`), and its columns.
    fn table(&self, name: &ObjectName) -> (Vec<String>, Option<Cols>) {
        let n = object(name);
        let cte = self.ctes.borrow().iter().rev().find(|(c, _)| name.0.len() == 1 && *c == n).map(|(_, cols)| cols.clone());
        let cols = cte.unwrap_or_else(|| self.known.get(&n).cloned().flatten());
        let parts: Vec<&str> = n.split('.').collect();
        ((0..parts.len()).map(|i| parts[i..].join(".")).collect(), cols)
    }

    fn items(&self, items: &[SelectItem], rels: &[Rel]) -> Option<Cols> {
        let plain = |o: &WildcardAdditionalOptions| *o == WildcardAdditionalOptions::default();
        let mut out = vec![];
        for item in items {
            match item {
                SelectItem::UnnamedExpr(e) => out.push((name_of(e), self.expr(e, rels))),
                SelectItem::ExprWithAlias { expr, alias } => out.push((ident(alias), self.expr(expr, rels))),
                SelectItem::Wildcard(o) if plain(o) => {
                    for r in rels {
                        out.extend(r.cols.clone()?);
                    }
                }
                SelectItem::QualifiedWildcard(ast::SelectItemQualifiedWildcardKind::ObjectName(n), o) if plain(o) => {
                    out.extend(rels.iter().find(|r| r.names.contains(&object(n)))?.cols.clone()?)
                }
                _ => return None, // (`* EXCLUDE …` and the like: its columns aren't counted here)
            }
        }
        Some(out)
    }

    /// Is `e` a uuid: a uuid column, a cast to uuid, a function that makes one, or a choice among them.
    fn expr(&self, e: &Expr, rels: &[Rel]) -> bool {
        match e {
            Expr::Identifier(i) => self.column(None, &ident(i), rels),
            Expr::CompoundIdentifier(p) if p.len() >= 2 => {
                let qualifier = p[..p.len() - 1].iter().map(ident).collect::<Vec<_>>().join(".");
                self.column(Some(&qualifier), &ident(&p[p.len() - 1]), rels)
            }
            Expr::Cast { data_type, .. } => *data_type == ast::DataType::Uuid,
            Expr::Nested(e) => self.expr(e, rels),
            Expr::Function(f) => match object(&f.name).as_str() {
                "gen_random_uuid" | "uuidv4" | "uuidv7" | "uuid" | "pondra_uuid" => true,
                "nullif" => first_arg(f).is_some_and(|a| self.expr(a, rels)),
                "coalesce" => args(f).is_some_and(|a| !a.is_empty() && a.iter().all(|a| null(a) || self.expr(a, rels))),
                "pg_typeof" => {
                    if first_arg(f).is_some_and(|a| self.expr(a, rels)) {
                        self.typeof_.borrow_mut().push(at(e));
                    }
                    false
                }
                _ => false,
            },
            Expr::Case { conditions, else_result, .. } => {
                !conditions.is_empty() && conditions.iter().all(|w| null(&w.result) || self.expr(&w.result, rels)) && else_result.as_deref().is_none_or(|e| null(e) || self.expr(e, rels))
            }
            _ => false,
        }
    }

    /// Is column `c` (qualified by `q`) a uuid: only when it is found, and is one wherever it is
    /// (a join's USING names it on both sides). Relations whose columns are unknown don't count: the
    /// query would be ambiguous if one had it too.
    fn column(&self, q: Option<&str>, c: &str, rels: &[Rel]) -> bool {
        let mut found = rels.iter().filter(|r| q.is_none_or(|q| r.names.iter().any(|n| n == q))).filter_map(|r| r.cols.as_ref()?.iter().find(|(n, _)| n == c)).peekable();
        found.peek().is_some() && found.all(|(_, u)| *u)
    }

    /// The parameters and strings a uuid is compared with in a condition: `c = $1`, `'…' <> c`, `c IN ($1, '…')`.
    fn compared(&self, e: &Expr, rels: &[Rel]) {
        use ast::BinaryOperator as Op;
        match e {
            Expr::BinaryOp { left, op: Op::And | Op::Or, right } => {
                self.compared(left, rels);
                self.compared(right, rels);
            }
            Expr::BinaryOp { left, op: Op::Eq | Op::NotEq | Op::Lt | Op::LtEq | Op::Gt | Op::GtEq, right } | Expr::IsDistinctFrom(left, right) | Expr::IsNotDistinctFrom(left, right) => {
                for (a, b) in [(left, right), (right, left)] {
                    if (param(b).is_some() || string(b).is_some()) && self.expr(a, rels) {
                        self.uuid(b);
                    }
                }
            }
            Expr::InList { expr, list, .. } if self.expr(expr, rels) => list.iter().for_each(|e| self.uuid(e)),
            Expr::Nested(e) | Expr::UnaryOp { expr: e, .. } => self.compared(e, rels),
            _ => {}
        }
    }

    /// An INSERT's values for uuid columns, by place: `VALUES ($1, '…')`, or a `SELECT $1, '…'`'s items
    /// (and its query's own).
    fn inserted(&self, source: &Query, columns: &[ObjectName], into: &Rel) {
        let uuids: Vec<bool> = match (&into.cols, columns.is_empty()) {
            (None, _) => vec![],
            (Some(cols), true) => cols.iter().map(|(_, u)| *u).collect(),
            (Some(cols), false) => columns.iter().map(|c| cols.iter().any(|(n, u)| *n == last(c) && *u)).collect(),
        };
        let into_uuid = |row: &mut dyn Iterator<Item = &Expr>| row.zip(&uuids).filter(|(_, u)| **u).for_each(|(e, _)| self.uuid(e));
        match source.body.as_ref() {
            SetExpr::Values(v) => v.rows.iter().for_each(|row| into_uuid(&mut row.iter())),
            SetExpr::Select(s) => {
                into_uuid(&mut s.projection.iter().map_while(|i| match i {
                    SelectItem::UnnamedExpr(e) | SelectItem::ExprWithAlias { expr: e, .. } => Some(e),
                    _ => None, // (a `*`: the places after it aren't known)
                }));
                self.query(source);
            }
            _ => {
                self.query(source);
            }
        }
    }

    /// `e` is read as a uuid (compared with one, or written into a uuid column): a parameter is declared
    /// one, and a string is read as Postgres reads it, `'A0EE…'` as `a0ee…`.
    fn uuid(&self, e: &Expr) {
        self.params.borrow_mut().extend(param(e));
        if string(e).is_some() {
            self.strings.borrow_mut().push(at(e));
        }
    }
}

/// `$n`'s number less one.
fn param(e: &Expr) -> Option<usize> {
    match e {
        Expr::Value(v) => match &v.value {
            Value::Placeholder(p) => p.strip_prefix('$')?.parse::<usize>().ok()?.checked_sub(1),
            _ => None,
        },
        Expr::Nested(e) => param(e),
        _ => None,
    }
}

/// A string literal's text.
fn string(e: &Expr) -> Option<&str> {
    match e {
        Expr::Value(v) => match &v.value {
            Value::SingleQuotedString(s) => Some(s),
            _ => None,
        },
        _ => None,
    }
}

fn null(e: &Expr) -> bool { matches!(e, Expr::Value(v) if v.value == Value::Null) }

fn args(f: &ast::Function) -> Option<Vec<&Expr>> {
    let ast::FunctionArguments::List(l) = &f.args else { return None };
    l.args.iter().map(|a| match a {
        ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(e)) => Some(e),
        _ => None,
    }).collect()
}

fn first_arg(f: &ast::Function) -> Option<&Expr> { args(f)?.first().copied() }

/// A name's last part, as SQL reads it.
fn last(n: &ObjectName) -> String { n.0.last().and_then(|p| p.as_ident()).map(ident).unwrap_or_default() }

/// The name an item without an alias goes by in a query around it: a column's own name.
fn name_of(e: &Expr) -> String {
    match e {
        Expr::Identifier(i) => ident(i),
        Expr::CompoundIdentifier(p) => p.last().map(ident).unwrap_or_default(),
        e => e.to_string(),
    }
}

/// `cols` under the names an alias gives them (`AS x(a, b)`), by place.
fn renamed(cols: Cols, aliases: &[ast::TableAliasColumnDef]) -> Cols {
    cols.into_iter().enumerate().map(|(i, (n, u))| (aliases.get(i).map_or(n, |a| ident(&a.name)), u)).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn known() -> Known {
        let t = vec![("id".to_string(), true), ("name".to_string(), false), ("parent".to_string(), true)];
        let o = vec![("id".to_string(), false), ("ref".to_string(), true)];
        Known::from([("t".to_string(), Some(t)), ("s.o".to_string(), Some(o)), ("f".to_string(), None)])
    }

    fn cols(sql: &str) -> Vec<bool> {
        let k = known();
        Trace::new(&k).statement(&one(sql).unwrap()).map(|c| c.into_iter().map(|(_, u)| u).collect()).unwrap_or_default()
    }

    fn params(sql: &str) -> Vec<usize> {
        let k = known();
        let t = Trace::new(&k);
        t.statement(&one(sql).unwrap());
        let mut p = t.params.into_inner();
        p.sort();
        p
    }

    #[test]
    fn traced_columns() {
        assert_eq!(cols("SELECT id, name, parent FROM t"), [true, false, true]);
        assert_eq!(cols("SELECT * FROM t"), [true, false, true]);
        assert_eq!(cols("SELECT x.id AS a, o.ref, o.id FROM t AS x JOIN s.o ON x.id = o.ref"), [true, true, false]);
        assert_eq!(cols("SELECT t.* FROM t, s.o"), [true, false, true]);
        assert_eq!(cols("SELECT name::uuid, gen_random_uuid(), uuidv7(), 'x', CAST(name AS UUID) FROM t"), [true, true, true, false, true]);
        assert_eq!(cols("WITH c AS (SELECT parent AS p FROM t) SELECT p FROM c"), [true]);
        assert_eq!(cols("SELECT q.a FROM (SELECT id FROM t) AS q(a)"), [true]);
        assert_eq!(cols("SELECT id FROM t UNION ALL SELECT ref FROM s.o"), [true]);
        assert_eq!(cols("SELECT id FROM t UNION ALL SELECT name FROM t"), [false]);
        assert_eq!(cols("SELECT coalesce(id, parent), coalesce(id, name), CASE WHEN true THEN id ELSE NULL END FROM t"), [true, false, true]);
        // (unknown relations and ambiguous names mark nothing)
        assert_eq!(cols("SELECT * FROM f"), Vec::<bool>::new());
        assert_eq!(cols("SELECT f.id FROM f, t"), [false]);
        assert_eq!(cols("SELECT id FROM t JOIN s.o USING (id)"), [false]);
        assert_eq!(cols("INSERT INTO t (id, name) VALUES ('a', 'b') RETURNING id, name"), [true, false]);
        assert_eq!(cols("UPDATE t AS x SET name = 'a' FROM s.o WHERE x.id = o.ref RETURNING x.*, o.ref"), [true, false, true, true]);
        assert_eq!(cols("DELETE FROM t WHERE true RETURNING *"), [true, false, true]);
    }

    #[test]
    fn traced_params() {
        assert_eq!(params("SELECT name FROM t WHERE id = $1 AND name = $2 OR $3 = parent"), [0, 2]);
        assert_eq!(params("SELECT * FROM t AS x WHERE x.id IN ($1, $2) AND x.name IN ($3)"), [0, 1]);
        assert_eq!(params("INSERT INTO t (name, id) VALUES ($1, $2), ($3, $4)"), [1, 3]);
        assert_eq!(params("INSERT INTO t VALUES ($1, $2, $3)"), [0, 2]);
        assert_eq!(params("UPDATE t SET parent = $1, name = $2 WHERE id = $3"), [0, 2]);
        assert_eq!(params("DELETE FROM s.o WHERE ref = $1 AND id = $2"), [0]);
        assert_eq!(params("SELECT name FROM t WHERE id = $1 UNION SELECT name FROM t WHERE parent <> $2"), [0, 1]);
    }

    #[test]
    fn rewritten_by_place() {
        let k = known();
        let u = "A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11";
        let mut s = one(&format!("SELECT pg_typeof(id), pg_typeof(name), pg_typeof(uuidv7()) AS v, pg_typeof('x') FROM t WHERE id = '{u}' OR name = '{u}' OR parent IN ('{{a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a12}}', $1) UNION ALL SELECT pg_typeof(ref), 'x', 'y', 'z' FROM s.o")).unwrap();
        assert!(rewritten(&k, &mut s));
        assert_eq!(s.to_string(), format!("SELECT 'uuid' AS \"pg_typeof(id)\", pg_typeof(name), 'uuid' AS v, pg_typeof('x') FROM t WHERE id = 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11' OR name = '{u}' OR parent IN ('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a12', $1) UNION ALL SELECT 'uuid' AS \"pg_typeof(ref)\", 'x', 'y', 'z' FROM s.o"));
        let mut s = one(&format!("INSERT INTO t (name, id) VALUES ('{u}', '{u}'), ('x', 'nope')")).unwrap();
        assert!(rewritten(&k, &mut s));
        assert_eq!(s.to_string(), format!("INSERT INTO t (name, id) VALUES ('{u}', 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'), ('x', 'nope')"));
        let mut s = one(&format!("INSERT INTO t (parent, name) SELECT '{u}' AS p, '{u}' FROM s.o")).unwrap();
        assert!(rewritten(&k, &mut s));
        assert_eq!(s.to_string(), format!("INSERT INTO t (parent, name) SELECT 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11' AS p, '{u}' FROM s.o"));
        let mut s = one(&format!("UPDATE t SET parent = '{u}', name = '{u}'")).unwrap();
        assert!(rewritten(&k, &mut s));
        assert_eq!(s.to_string(), format!("UPDATE t SET parent = 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', name = '{u}'"));
        let mut s = one(&format!("SELECT name FROM t WHERE name = '{u}' AND id = 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'")).unwrap();
        assert!(!rewritten(&k, &mut s)); // (a string compared with text, and a uuid already written its way)
    }
}
