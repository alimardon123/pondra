//! The Postgres port's types that Arrow's don't say: which columns of an answer, and which
//! parameters of a statement, are uuids, json or jsonb. Each is text in the lake (`types.rs`), so its Arrow type
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
use datafusion::sql::sqlparser::ast::{self, Expr, ObjectName, Query, Select, SelectItem, SetExpr, Statement, TableFactor, TableWithJoins, Value, Visit, VisitMut, WildcardAdditionalOptions};
use datafusion::sql::sqlparser::{dialect::GenericDialect, parser::Parser};

use crate::codes::coded;
use crate::pg_names;
use crate::store::{table_key, Lake};
use crate::write::{ident, object};

/// The JSON makers: the functions that make JSON from values (`pg_json_make.rs`). A statement that calls one has its
/// arguments changed where SQL comes in (`rewritten`): a JSON argument is marked as its type prints it, a whole row is
/// made of its columns, and an aggregate is `pondra_json_agg` over `array_agg`. `makes_json` is the check that lets a
/// statement without one pass.
const MAKERS: &[&str] = &["to_json", "to_jsonb", "row_to_json", "json_build_object", "jsonb_build_object", "json_build_array",
    "jsonb_build_array", "json_agg", "jsonb_agg", "json_object_agg", "jsonb_object_agg"];

/// Whether `sql` calls a JSON maker (`MAKERS`), as a name followed by `(`. Only a statement with one has its arguments traced.
pub fn makes_json(sql: &str) -> bool {
    static CALL: std::sync::LazyLock<regex::Regex> = std::sync::LazyLock::new(|| regex::Regex::new(&format!(r"(?i)\b(?:{})\s*\(", MAKERS.join("|"))).expect("a regex"));
    CALL.is_match(sql)
}

/// What a maker's argument is, for the JSON it writes. A whole row is `Row`, a JSON value `Json` (wrapped as its type prints
/// it), anything else `Value`. An aggregate's elements are written by it: `json` and `row` elements are JSON text already.
#[derive(Clone, Copy, PartialEq, Debug)]
enum Kind {
    Json,
    Row,
    Value,
}

impl Kind {
    /// The kind's name as `pondra_json_agg` reads it.
    fn name(self) -> &'static str {
        match self {
            Kind::Json => "json",
            Kind::Row => "row",
            Kind::Value => "value",
        }
    }
}

/// A relation an argument names as a whole row: the name its columns are qualified by, and its columns.
struct Whole {
    name: String,
    cols: Cols,
}

/// The first character of an item's name when the item has no name of its own (an expression, which Postgres names by
/// its function or gives `?column?`). No identifier holds a NUL, so a lookup never finds such a name; the text follows it.
const UNNAMED: char = '\0';

/// The field metadata that marks an answer's uuid column: the port reads it, other doors pass it by.
pub const MARK: &str = "pg_type";

/// A Postgres type the lake holds as text, which the port names: `TableMeta::sql_types`' "uuid", "json" and
/// "jsonb". A column of one is sent as that type, so drivers hand its values over as theirs.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Ty {
    Uuid,
    Json,
    Jsonb,
}

impl Ty {
    /// The type a `sql_types` entry names, if the port names it (a `varchar(n)` isn't one).
    pub fn of(sql_type: &str) -> Option<Ty> {
        match sql_type {
            "uuid" => Some(Ty::Uuid),
            "json" => Some(Ty::Json),
            "jsonb" => Some(Ty::Jsonb),
            _ => None,
        }
    }

    /// Its name, as `sql_types` and the field mark keep it.
    pub fn name(self) -> &'static str {
        match self {
            Ty::Uuid => "uuid",
            Ty::Json => "json",
            Ty::Jsonb => "jsonb",
        }
    }
}

/// A relation's columns: each one's name in SQL, and its type when the port names it.
type Cols = Vec<(String, Option<Ty>)>;

/// The columns of the tables and views a statement names, by name as written (None: not a table or
/// view here, so its columns are unknown).
type Known = HashMap<String, Option<Cols>>;

/// The type an answer's column is marked with: `typed` marked it, or a catalog function that answers
/// json did (`pg_catalog.rs`).
pub fn marked_type(f: &Field) -> Option<Ty> { f.metadata().get(MARK).and_then(|t| Ty::of(t)) }

/// What an answer's columns are, from its statement as written: which types they are (`columns`), and the
/// names Postgres gives them (`pg_names::of`; None when they can't be read the same way as the columns).
#[derive(Default, Clone)]
pub struct Shape {
    pub types: Vec<Option<Ty>>,
    names: Option<Vec<pg_names::Item>>,
}

impl From<Vec<Option<Ty>>> for Shape {
    /// A key lookup's types (`serve::point`), with no names of their own.
    fn from(types: Vec<Option<Ty>>) -> Self { Shape { types, names: None } }
}

/// `schema` as an answer sends it: the columns `shape` marks with a type, and each column's Postgres name in its
/// metadata (`pg_names::shown` reads it), where `shape` has the names and they fit the schema. The fields keep
/// their Arrow names, and the batches are the same.
pub fn typed(schema: &SchemaRef, shape: &Shape) -> SchemaRef {
    let marked = marked(schema, &shape.types);
    let Some(names) = shape.names.as_ref().and_then(|items| pg_names::named(&marked, items)) else { return marked };
    let fields: Vec<Arc<Field>> = marked.fields().iter().zip(names).map(|(f, name)| {
        let mut meta = f.metadata().clone();
        meta.insert(pg_names::KEY.into(), name);
        Arc::new(f.as_ref().clone().with_metadata(meta))
    }).collect();
    Arc::new(Schema::new_with_metadata(fields, marked.metadata().clone()))
}

/// `schema` with each column `marks` gives a type marked with it. Only when there is a mark for each column
/// and each one marked holds text: anything else is a trace gone wrong, and marks nothing.
fn marked(schema: &SchemaRef, marks: &[Option<Ty>]) -> SchemaRef {
    let text = |f: &Field| matches!(f.data_type(), DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8);
    let fields = schema.fields();
    if marks.iter().all(Option::is_none) || marks.len() != fields.len() || fields.iter().zip(marks).any(|(f, m)| m.is_some() && !text(f)) {
        return schema.clone();
    }
    let fields: Vec<Arc<Field>> = fields.iter().zip(marks).map(|(f, m)| match m {
        Some(t) => {
            let mut meta = f.metadata().clone();
            meta.insert(MARK.into(), t.name().into());
            Arc::new(f.as_ref().clone().with_metadata(meta))
        }
        None => f.clone(),
    }).collect();
    Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone()))
}

/// The shape of the answer `sql` gives: which types its columns are, in order (a query's, or a write's
/// RETURNING; none when none is, or when it can't tell), and the Postgres names of its columns. A catalog
/// query gets names only, as before: its types are never traced. The statement is parsed once for both.
pub async fn columns(lake: &Lake, sql: &str) -> Shape {
    let Some(stmt) = one(sql) else { return Shape::default() };
    let names = pg_names::of(&stmt);
    if crate::pg_catalog::wanted(sql) {
        return Shape { types: vec![], names };
    }
    let mut known = Known::new();
    look_up(lake, &stmt, &mut known, 0).await;
    let types: Vec<Option<Ty>> = Trace::new(&known).statement(&stmt).map(|c| c.into_iter().map(|(_, t)| t).collect()).unwrap_or_default();
    Shape { types, names }
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
pub async fn rewrite(lake: &Lake, stmt: &mut Statement) -> anyhow::Result<bool> {
    let asks = |e: &Expr| matches!(e, Expr::Function(f) if object(&f.name) == "pg_typeof");
    let spelled = |e: &Expr| string(e).is_some_and(|s| !crate::types::is_uuid(s) && crate::types::uuid_of(s).is_ok());
    let maker = |e: &Expr| matches!(e, Expr::Function(f) if MAKERS.contains(&object(&f.name).as_str()));
    if !ast::visit_expressions(stmt, |e| if asks(e) || spelled(e) || maker(e) { ControlFlow::Break(()) } else { ControlFlow::Continue(()) }).is_break() {
        return Ok(false); // (nearly every statement)
    }
    let mut known = Known::new();
    look_up(lake, stmt, &mut known, 0).await;
    rewritten(&known, stmt)
}

/// `rewrite`'s work, over the relations `known` describes. What it changes is found by its place in the
/// statement (`at`), never by its text: `name = 'A0EE…'` beside `id = 'A0EE…'` stays as written. A refusal (a whole
/// row that can't be made, DISTINCT over json) is found before anything changes.
fn rewritten(known: &Known, stmt: &mut Statement) -> anyhow::Result<bool> {
    let t = Trace::new(known);
    t.statement(stmt);
    if let Some(refusal) = t.refused.into_inner() {
        return Err(refusal);
    }
    let (asked, compared) = (t.typeof_.into_inner(), t.strings.into_inner());
    let (wraps, rows, aggregates) = (t.wraps.into_inner(), t.rows.into_inner(), t.aggregates.into_inner());
    if asked.is_empty() && compared.is_empty() && wraps.is_empty() && rows.is_empty() && aggregates.is_empty() {
        return Ok(false);
    }
    // (`pg_typeof(id)` becomes 'uuid', whose own name would be `Utf8("uuid")`: `names_kept` names it as written)
    Ok(names_kept(stmt, |stmt| {
        let mut changed = false;
        let _ = ast::visit_expressions_mut(stmt, |e| {
            let place = at(e);
            let to = match asked.iter().find(|(p, _)| *p == place) {
                Some((_, ty)) => Some(ty.name().to_string()),
                None if compared.contains(&place) => string(e).and_then(|s| crate::types::uuid_of(s).ok()),
                None => None,
            };
            if let Some(to) = to.filter(|to| string(e) != Some(to.as_str())) {
                *e = Expr::value(Value::SingleQuotedString(to));
                changed = true;
            }
            changed |= change_makers(e, place, &wraps, &rows, &aggregates);
            ControlFlow::<()>::Continue(())
        });
        changed
    }))
}

/// Changes the expression at `place` as the JSON makers' notes say (`Trace::maker`). A node's children are changed before
/// the node (`visit_expressions_mut`), so an aggregate's arguments are already: the aggregate is rebuilt, then, at the
/// same place, wrapped if it is a JSON argument of a maker. A whole row is replaced by `pondra_row`. Whether it changed.
fn change_makers(e: &mut Expr, place: usize, wraps: &[(usize, Ty)], rows: &[(usize, Whole)], aggregates: &[(usize, Kind)]) -> bool {
    let mut changed = false;
    if let Some((_, kind)) = aggregates.iter().find(|(p, _)| *p == place) {
        *e = aggregate_expr(std::mem::replace(e, Expr::value(Value::Null)), *kind);
        changed = true;
    }
    if let Some((_, whole)) = rows.iter().find(|(p, _)| *p == place) {
        *e = row_expr(whole);
        changed = true;
    }
    if let Some((_, ty)) = wraps.iter().find(|(p, _)| *p == place) {
        *e = wrap_json(std::mem::replace(e, Expr::value(Value::Null)), Some(*ty));
        changed = true;
    }
    changed
}

/// `pondra_row('a', "t"."a", 'b', "t"."b", …)`: a whole row's columns in order, each JSON one as its type prints it.
fn row_expr(w: &Whole) -> Expr {
    let args = w.cols.iter().flat_map(|(col, ty)| {
        let column = Expr::CompoundIdentifier(vec![quoted(&w.name), quoted(col)]);
        [lit(col), wrap_json(column, *ty)]
    });
    call("pondra_row", args.collect())
}

/// `json_agg(x …)` as `pondra_json_agg(array_agg(x …), 'kind')`, and `json_object_agg(k, v …)` as
/// `pondra_json_object_agg(array_agg(named_struct('k', k, 'v', v) …), 'kind')`; `jsonb` forms alike. The call's own clauses
/// (DISTINCT, ORDER BY, FILTER, OVER, IGNORE NULLS) move onto `array_agg`, which holds the elements.
fn aggregate_expr(e: Expr, kind: Kind) -> Expr {
    let Expr::Function(mut inner) = e else { return e };
    let name = object(&inner.name);
    let object = name.ends_with("object_agg");
    let outer = format!("pondra_{}{}", if name.starts_with("jsonb") { "jsonb" } else { "json" }, if object { "_object_agg" } else { "_agg" });
    if let (ast::FunctionArguments::List(list), true) = (&mut inner.args, object) {
        if let [k, v] = list.args.as_slice() {  // (`Trace::aggregate` asks for a key and a value only)
            let pair = call("named_struct", vec![lit("k"), arg_expr(k), lit("v"), arg_expr(v)]);
            list.args = vec![unnamed(pair)];
        }
    }
    inner.name = ObjectName::from(vec![ast::Ident::new("array_agg")]);
    call(&outer, vec![Expr::Function(inner), lit(kind.name())])
}

/// A function argument that is an unnamed expression, as the expression (`aggregate_expr` reads only those).
fn arg_expr(a: &ast::FunctionArg) -> Expr {
    match a {
        ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(e)) => e.clone(),
        _ => Expr::value(Value::Null),
    }
}

/// `e` as a JSON argument of a maker, wrapped in `pondra_as_json` or `pondra_as_jsonb` as its type prints it
/// (`pg_json_make.rs`): a json column's text is quoted otherwise, and a jsonb one's keys are sorted. Any other `e` as it is.
fn wrap_json(e: Expr, ty: Option<Ty>) -> Expr {
    match ty {
        Some(Ty::Json) => call("pondra_as_json", vec![e]),
        Some(Ty::Jsonb) => call("pondra_as_jsonb", vec![e]),
        _ => e,
    }
}

/// A call of `name` on `args`, as SQL writes it.
fn call(name: &str, args: Vec<Expr>) -> Expr {
    Expr::Function(ast::Function {
        name: ObjectName::from(vec![ast::Ident::new(name)]),
        uses_odbc_syntax: false,
        parameters: ast::FunctionArguments::None,
        args: ast::FunctionArguments::List(ast::FunctionArgumentList { duplicate_treatment: None, args: args.into_iter().map(unnamed).collect(), clauses: vec![] }),
        filter: None,
        null_treatment: None,
        over: None,
        within_group: vec![],
    })
}

fn unnamed(e: Expr) -> ast::FunctionArg { ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(e)) }

fn lit(s: &str) -> Expr { Expr::value(Value::SingleQuotedString(s.to_string())) }

fn quoted(s: &str) -> ast::Ident { ast::Ident::with_quote('"', s) }

/// An expression's place in its statement, which tells it from another of the same text.
fn at(e: &Expr) -> usize { std::ptr::from_ref(e) as usize }

/// Runs `change` over `stmt`, then gives each item it changed that has no alias (in a SELECT, or a RETURNING)
/// its text as written for a name, so a change made where SQL comes in never renames a column:
/// `SELECT json_agg(t) FROM t` still answers `json_agg(t)`. Items are found by their places, so `change` must
/// change expressions where they are (moving a query is fine; a query it clones is new to it). Whether
/// `change` changed anything.
pub(crate) fn names_kept(stmt: &mut Statement, change: impl FnOnce(&mut Statement) -> bool) -> bool {
    let mut written = HashMap::new();
    let _ = stmt.visit(&mut Items(|item: &mut SelectItem| {
        if let SelectItem::UnnamedExpr(e) = item {
            written.insert(at(e), e.to_string());
        }
    }));
    if !change(stmt) {
        return false;
    }
    let _ = stmt.visit(&mut Items(|item: &mut SelectItem| {
        if let SelectItem::UnnamedExpr(e) = item {
            if let Some(text) = written.get(&at(e)).filter(|text| **text != e.to_string()) {
                let expr = std::mem::replace(e, Expr::value(Value::Null));
                *item = SelectItem::ExprWithAlias { expr, alias: ast::Ident::with_quote('"', text.clone()) };
            }
        }
    }));
    true
}

/// Calls its function with each item of every query in a statement (both sides of a UNION) and of every
/// RETURNING.
struct Items<F>(F);

impl<F: FnMut(&mut SelectItem)> ast::VisitorMut for Items<F> {
    type Break = ();

    fn pre_visit_query(&mut self, q: &mut Query) -> ControlFlow<()> {
        fn items(body: &mut SetExpr, f: &mut impl FnMut(&mut SelectItem)) {
            match body {
                SetExpr::Select(s) => s.projection.iter_mut().for_each(f),
                SetExpr::SetOperation { left, right, .. } => {
                    items(left, f);
                    items(right, f);
                }
                _ => {} // (a query in parentheses is a query of its own: this visits it too)
            }
        }
        items(&mut q.body, &mut self.0);
        ControlFlow::Continue(())
    }

    fn pre_visit_statement(&mut self, s: &mut Statement) -> ControlFlow<()> {
        let returning = match s {
            Statement::Insert(i) => i.returning.as_mut(),
            Statement::Update(u) => u.returning.as_mut(),
            Statement::Delete(d) => d.returning.as_mut(),
            _ => None,
        };
        returning.into_iter().flatten().for_each(&mut self.0);
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
        return Some(l.columns.iter().filter(|(c, _)| !l.marker(c)).map(|(c, _)| (c.clone(), l.sql_types.get(c).and_then(|t| Ty::of(t)))).collect());
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
    /// The calls `pg_typeof(x)` whose `x` is a uuid, json or jsonb, by place (`at`), with that type.
    typeof_: RefCell<Vec<(usize, Ty)>>,
    /// The string literals read as uuids (compared with one, or written into one), by place.
    strings: RefCell<Vec<usize>>,
    /// The JSON makers' JSON arguments, by place, with their type: each is wrapped as its type prints it.
    wraps: RefCell<Vec<(usize, Ty)>>,
    /// The JSON makers' whole-row arguments, by place: each becomes `pondra_row` of its relation's columns.
    rows: RefCell<Vec<(usize, Whole)>>,
    /// The JSON makers' aggregates, by place, with their elements' kind: each is rebuilt over `array_agg`.
    aggregates: RefCell<Vec<(usize, Kind)>>,
    /// The first refusal the statement earns (`rewritten` returns it before anything changes).
    refused: RefCell<Option<anyhow::Error>>,
}

impl<'a> Trace<'a> {
    fn new(known: &'a Known) -> Self {
        Trace { known, ctes: Default::default(), params: Default::default(), typeof_: Default::default(), strings: Default::default(), wraps: Default::default(), rows: Default::default(), aggregates: Default::default(), refused: Default::default() }
    }

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
                let rels = [into];
                self.item_makers(i.returning.as_deref().unwrap_or_default(), &rels);
                self.items(i.returning.as_deref()?, &rels)
            }
            Statement::Update(u) => {
                let mut rels = self.from(std::slice::from_ref(&u.table));
                if let Some(ast::UpdateTableFromKind::BeforeSet(f) | ast::UpdateTableFromKind::AfterSet(f)) = &u.from {
                    rels.extend(self.from(f));
                }
                for a in &u.assignments {
                    if let ast::AssignmentTarget::ColumnName(c) = &a.target {
                        if self.column(None, &last(c), &rels[..1]) == Some(Ty::Uuid) {
                            self.uuid(&a.value);
                        }
                    }
                    self.makers_in(&a.value, &rels);
                }
                u.selection.iter().for_each(|e| {
                    self.compared(e, &rels);
                    self.makers_in(e, &rels);
                });
                self.item_makers(u.returning.as_deref().unwrap_or_default(), &rels);
                self.items(u.returning.as_deref()?, &rels)
            }
            Statement::Delete(d) => {
                let (ast::FromTable::WithFromKeyword(t) | ast::FromTable::WithoutKeyword(t)) = &d.from;
                let mut rels = self.from(t);
                rels.extend(self.from(d.using.as_deref().unwrap_or_default()));
                d.selection.iter().for_each(|e| {
                    self.compared(e, &rels);
                    self.makers_in(e, &rels);
                });
                self.item_makers(d.returning.as_deref().unwrap_or_default(), &rels);
                self.items(d.returning.as_deref()?, &rels)
            }
            // (A materialized view and `CREATE TABLE … AS` run their query when made and when refreshed, so their
            // makers change now. A plain view is read as it was made, and changed where it is read: `expand_stored`.)
            Statement::CreateView(v) if v.materialized => self.query(&v.query),
            Statement::CreateTable(t) => self.query(t.query.as_deref()?),
            _ => None,
        }
    }

    /// The JSON makers in SELECT items or a write's RETURNING items, over their relations.
    fn item_makers(&self, items: &[SelectItem], rels: &[Rel]) {
        for item in items {
            if let SelectItem::UnnamedExpr(e) | SelectItem::ExprWithAlias { expr: e, .. } = item {
                self.makers_in(e, rels);
            }
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
            SetExpr::Values(v) => {
                self.values(v);
                None
            }
            // (a column keeps its type only where both sides have the same one, as Postgres takes a UNION)
            SetExpr::SetOperation { left, right, .. } => {
                let (l, r) = (self.body(left), self.body(right));
                let l = l?;
                Some(match r {
                    Some(r) if r.len() == l.len() => l.into_iter().zip(r).map(|((n, a), (_, b))| (n, if a == b { a } else { None })).collect(),
                    _ => l.into_iter().map(|(n, _)| (n, None)).collect(),
                })
            }
            _ => None,
        }
    }

    fn select(&self, s: &Select) -> Option<Cols> {
        let rels = self.from(&s.from);
        s.selection.iter().chain(&s.having).for_each(|e| self.compared(e, &rels));
        self.select_makers(s, &rels);
        self.items(&s.projection, &rels)
    }

    /// The JSON makers in a SELECT's own expressions, over its relations: its items, WHERE, GROUP BY, HAVING, QUALIFY and
    /// its joins' conditions.
    fn select_makers(&self, s: &Select, rels: &[Rel]) {
        self.item_makers(&s.projection, rels);
        s.selection.iter().chain(&s.having).chain(&s.qualify).for_each(|e| self.makers_in(e, rels));
        if let ast::GroupByExpr::Expressions(exprs, _) = &s.group_by {
            exprs.iter().for_each(|e| self.makers_in(e, rels));
        }
        on_conditions(&s.from).into_iter().for_each(|e| self.makers_in(e, rels));
    }

    /// The JSON makers in a VALUES list (no relations: a row's values are its own).
    fn values(&self, v: &ast::Values) {
        for e in v.rows.iter().flat_map(|row| row.iter()) {
            self.makers_in(e, &[]);
        }
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
            // (a table function or a file read by name: its columns are unknown, but its alias still names it)
            TableFactor::Table { alias, .. } | TableFactor::Function { alias, .. } | TableFactor::TableFunction { alias, .. } => (vec![], None, alias),
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

    /// The type of `e` when it is a uuid, json or jsonb: a column of one, a cast to one, a function that
    /// makes one, or a choice among them. None for anything else, or when the choice's parts differ.
    fn expr(&self, e: &Expr, rels: &[Rel]) -> Option<Ty> {
        match e {
            Expr::Identifier(i) => self.column(None, &ident(i), rels),
            Expr::CompoundIdentifier(p) if p.len() >= 2 => {
                let qualifier = p[..p.len() - 1].iter().map(ident).collect::<Vec<_>>().join(".");
                self.column(Some(&qualifier), &ident(&p[p.len() - 1]), rels)
            }
            Expr::Cast { data_type, .. } => of_type(data_type),
            Expr::Nested(e) => self.expr(e, rels),
            // (`doc -> 'a'` and `doc #> '{a}'` answer a document's own type; `->>` and `#>>` answer text)
            // (and `doc #- '{a}'` the document without a part; `b - 'a'` deletes from a jsonb, and json has no `-`)
            Expr::BinaryOp { left, op: ast::BinaryOperator::Arrow | ast::BinaryOperator::HashArrow | ast::BinaryOperator::HashMinus, .. } => self.document(left, rels),
            Expr::BinaryOp { left, op: ast::BinaryOperator::Minus, .. } => self.expr(left, rels).filter(|t| *t == Ty::Jsonb),
            Expr::Function(f) => match object(&f.name).as_str() {
                "gen_random_uuid" | "uuidv4" | "uuidv7" | "uuid" | "pondra_uuid" => Some(Ty::Uuid),
                "json_object_field" | "jsonb_object_field" | "json_array_element" | "jsonb_array_element" | "pondra_json_path" => first_arg(f).and_then(|a| self.document(a, rels)),
                "nullif" => first_arg(f).and_then(|a| self.expr(a, rels)),
                "coalesce" => args(f).and_then(|a| same_taking_literals(a.iter().filter(|a| !null(a)).map(|a| (string(a).is_some(), self.expr(a, rels))))),
                "pg_typeof" => {
                    if let Some(t) = first_arg(f).and_then(|a| self.expr(a, rels)) {
                        self.typeof_.borrow_mut().push((at(e), t));
                    }
                    None
                }
                name => json_type(name),
            },
            Expr::Case { conditions, else_result, .. } => same_taking_literals(conditions.iter().map(|w| &w.result).chain(else_result.as_deref()).filter(|e| !null(e)).map(|e| (string(e).is_some(), self.expr(e, rels)))),
            _ => None,
        }
    }

    /// The type of a JSON operator's document: json or jsonb as the document is, and none for anything else.
    fn document(&self, e: &Expr, rels: &[Rel]) -> Option<Ty> { self.expr(e, rels).filter(|t| matches!(t, Ty::Json | Ty::Jsonb)) }

    /// The type of column `c` (qualified by `q`): only when it is found, and of the same type wherever it is
    /// (a join's USING names it on both sides). Relations whose columns are unknown don't count: the
    /// query would be ambiguous if one had it too.
    fn column(&self, q: Option<&str>, c: &str, rels: &[Rel]) -> Option<Ty> {
        same(rels.iter().filter(|r| q.is_none_or(|q| r.names.iter().any(|n| n == q))).filter_map(|r| r.cols.as_ref()?.iter().find(|(n, _)| n == c).map(|(_, t)| *t)))
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
                    if (param(b).is_some() || string(b).is_some()) && self.expr(a, rels) == Some(Ty::Uuid) {
                        self.uuid(b);
                    }
                }
            }
            Expr::InList { expr, list, .. } if self.expr(expr, rels) == Some(Ty::Uuid) => list.iter().for_each(|e| self.uuid(e)),
            Expr::Nested(e) | Expr::UnaryOp { expr: e, .. } => self.compared(e, rels),
            _ => {}
        }
    }

    /// An INSERT's values for uuid columns, by place: `VALUES ($1, '…')`, or a `SELECT $1, '…'`'s items
    /// (and its query's own).
    fn inserted(&self, source: &Query, columns: &[ObjectName], into: &Rel) {
        let uuids: Vec<bool> = match (&into.cols, columns.is_empty()) {
            (None, _) => vec![],
            (Some(cols), true) => cols.iter().map(|(_, t)| *t == Some(Ty::Uuid)).collect(),
            (Some(cols), false) => columns.iter().map(|c| cols.iter().any(|(n, t)| *n == last(c) && *t == Some(Ty::Uuid))).collect(),
        };
        let into_uuid = |row: &mut dyn Iterator<Item = &Expr>| row.zip(&uuids).filter(|(_, u)| **u).for_each(|(e, _)| self.uuid(e));
        match source.body.as_ref() {
            SetExpr::Values(v) => {
                v.rows.iter().for_each(|row| into_uuid(&mut row.iter()));
                self.values(v);
            }
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

    /// The JSON makers in `e`, noted where they are (`change_makers` changes them). A query nested in `e` is traced with its
    /// own relations, which note its own makers: this walk leaves its expressions alone (`Makers`).
    fn makers_in(&self, e: &Expr, rels: &[Rel]) {
        let _ = e.visit(&mut Makers { trace: self, rels, depth: 0 });
    }

    /// Notes what a JSON maker's arguments need, when `e` is a call of one: an aggregate (`aggregate`), or each argument of a
    /// builder, whose object keys must be scalars (`key`).
    fn maker(&self, e: &Expr, rels: &[Rel]) {
        let Expr::Function(f) = e else { return };
        let name = object(&f.name);
        if !MAKERS.contains(&name.as_str()) {
            return;
        }
        let Some(args) = args(f) else { return };
        if name.ends_with("_agg") {
            self.aggregate(e, f, &args, rels);
        } else {
            let object_keys = name.ends_with("build_object");
            for (i, a) in args.iter().enumerate() {
                if object_keys && i % 2 == 0 {
                    self.key(a, rels);
                } else {
                    self.argument(f, a, rels);
                }
            }
        }
    }

    /// What an argument `a` of the maker call `f` is, for the JSON it writes. A whole row is noted for `pondra_row`, or
    /// refused by name; a JSON value is noted for its wrapper, unless it is one already; any other value needs nothing.
    fn argument(&self, f: &ast::Function, a: &Expr, rels: &[Rel]) -> Kind {
        if let Expr::Identifier(i) = a {
            if let Some(whole) = self.whole(i, rels) {
                return match whole {
                    Ok(w) => {
                        self.rows.borrow_mut().push((at(a), w));
                        Kind::Row
                    }
                    Err(why) => {
                        self.refuse(coded("0A000", format!("{}({}) of a whole row {why}", f.name, i.value)));
                        Kind::Value
                    }
                };
            }
        }
        match self.expr(a, rels) {
            Some(ty @ (Ty::Json | Ty::Jsonb)) => {
                if !is_marked(a) {
                    self.wraps.borrow_mut().push((at(a), ty));
                }
                Kind::Json
            }
            _ => Kind::Value,
        }
    }

    /// An object's key must be a scalar: a whole row or a JSON value is refused, as Postgres refuses it.
    fn key(&self, a: &Expr, rels: &[Rel]) {
        let row = matches!(a, Expr::Identifier(i) if self.whole(i, rels).is_some());
        if row || matches!(self.expr(a, rels), Some(Ty::Json | Ty::Jsonb)) {
            self.refuse(coded("22023", "key value must be scalar, not array, composite, or json"));
        }
    }

    /// An aggregate maker, `json_agg(x)` or `json_object_agg(k, v)`, noted with its elements' kind. The kind is worked out
    /// here, from the trace, before anything changes. DISTINCT over json is refused as Postgres refuses it.
    fn aggregate(&self, e: &Expr, f: &ast::Function, args: &[&Expr], rels: &[Rel]) {
        let name = object(&f.name);
        let object = name.ends_with("object_agg");
        if object && args.len() != 2 {
            return; // (DataFusion refuses the call: it has no such form)
        }
        let Some(&value) = args.last() else { return };
        if object {
            self.key(args[0], rels);
        }
        let kind = self.argument(f, value, rels);
        let distinct = matches!(&f.args, ast::FunctionArguments::List(l) if l.duplicate_treatment == Some(ast::DuplicateTreatment::Distinct));
        if distinct && kind == Kind::Json && !name.starts_with("jsonb") {
            self.refuse(coded("42883", "could not identify an equality operator for type json"));
        }
        self.aggregates.borrow_mut().push((at(e), kind));
    }

    /// The relation `i` names as a whole row: a bare name of a relation of this FROM, when no relation of it has a column of
    /// that name (a column wins, as in Postgres). Refused with why (the words after the call's text) when the row can't be
    /// made: its columns aren't known, or one has no name of its own. None when `i` isn't a whole row.
    fn whole(&self, i: &ast::Ident, rels: &[Rel]) -> Option<Result<Whole, String>> {
        let name = ident(i);
        let rel = rels.iter().find(|r| r.names.contains(&name))?;
        let has_column = |r: &Rel| r.cols.as_ref().is_some_and(|cols| cols.iter().any(|(c, _)| *c == name));
        if rels.iter().any(|r| has_column(r)) {
            return None;
        }
        // (every relation's columns must be known: one that isn't could have a column of this name too)
        let known = rels.iter().all(|r| r.cols.is_some());
        let cols = match (&rel.cols, known) {
            (Some(cols), true) => cols.clone(),
            _ => return Some(Err(format!("needs {name}'s columns: name them, as json_build_object('a', {name}.a, …)"))),
        };
        if let Some((col, _)) = cols.iter().find(|(c, _)| c.starts_with(UNNAMED)) {
            return Some(Err(format!("needs a name for each of {name}'s columns: give {} an alias", &col[UNNAMED.len_utf8()..])));
        }
        Some(Ok(Whole { name, cols }))
    }

    /// Keeps the first refusal: the statement fails with it, and nothing is changed.
    fn refuse(&self, e: anyhow::Error) { self.refused.borrow_mut().get_or_insert(e); }
}

/// Visits one query's expressions (`Trace::makers_in`). At depth 0 it notes each maker and traces each query nested in an
/// expression with its own relations; deeper, it skips a nested query, which is traced that way.
struct Makers<'t, 'k> {
    trace: &'t Trace<'k>,
    rels: &'t [Rel],
    depth: usize,
}

impl ast::Visitor for Makers<'_, '_> {
    type Break = ();

    fn pre_visit_query(&mut self, q: &Query) -> ControlFlow<()> {
        if self.depth == 0 {
            let _ = self.trace.query(q);
        }
        self.depth += 1;
        ControlFlow::Continue(())
    }

    fn post_visit_query(&mut self, _: &Query) -> ControlFlow<()> {
        self.depth -= 1;
        ControlFlow::Continue(())
    }

    fn pre_visit_expr(&mut self, e: &Expr) -> ControlFlow<()> {
        if self.depth == 0 {
            self.trace.maker(e, self.rels);
        }
        ControlFlow::Continue(())
    }
}

/// Whether an argument already marks its JSON as the maker reads it: a call of `pondra_as_json`, `pondra_as_jsonb` or
/// `pondra_row`, which a rewrite made. So a second rewrite wraps nothing again.
fn is_marked(a: &Expr) -> bool {
    matches!(a, Expr::Function(f) if ["pondra_as_json", "pondra_as_jsonb", "pondra_row"].contains(&object(&f.name).as_str()))
}

/// The conditions of a FROM's joins (`ON`, an as-of join's match condition), nested joins' too.
fn on_conditions(from: &[TableWithJoins]) -> Vec<&Expr> {
    let mut out = vec![];
    for t in from {
        if let TableFactor::NestedJoin { table_with_joins, .. } = &t.relation {
            out.extend(on_conditions(std::slice::from_ref(table_with_joins)));
        }
        for j in &t.joins {
            if let TableFactor::NestedJoin { table_with_joins, .. } = &j.relation {
                out.extend(on_conditions(std::slice::from_ref(table_with_joins)));
            }
            out.extend(join_conditions(&j.join_operator));
        }
    }
    out
}

/// The conditions a join operator carries.
fn join_conditions(op: &ast::JoinOperator) -> Vec<&Expr> {
    use ast::JoinOperator as J;
    let constraint = match op {
        J::AsOf { match_condition, constraint } => return std::iter::once(match_condition).chain(on_constraint(constraint)).collect(),
        J::Join(c) | J::Inner(c) | J::Left(c) | J::LeftOuter(c) | J::Right(c) | J::RightOuter(c) | J::FullOuter(c) | J::CrossJoin(c) | J::Semi(c)
        | J::LeftSemi(c) | J::RightSemi(c) | J::Anti(c) | J::LeftAnti(c) | J::RightAnti(c) | J::StraightJoin(c) => c,
        _ => return vec![],
    };
    on_constraint(constraint).into_iter().collect()
}

fn on_constraint(c: &ast::JoinConstraint) -> Option<&Expr> {
    match c {
        ast::JoinConstraint::On(e) => Some(e),
        _ => None,
    }
}

/// The type a cast to `t` makes its value, when the port names it.
/// What a function that makes or changes a whole JSON document answers, json or jsonb, by its name: one list,
/// which the trace reads (a column's type at the Postgres port, `pg_typeof`, what a JSON maker nests). A new
/// such function adds its name here. (`->` and its functions answer their document's type: `Trace::expr`.)
pub(crate) fn json_type(name: &str) -> Option<Ty> {
    match name {
        "pondra_json" | "json_get_json" | "pondra_as_json" | "to_json" | "row_to_json" | "pondra_row" | "json_build_object" | "json_build_array"
        | "json_agg" | "json_object_agg" | "pondra_json_agg" | "pondra_json_object_agg" | "json_extract_path" | "json_array_elements" | "json_strip_nulls" => Some(Ty::Json),
        "pondra_jsonb" | "pondra_as_jsonb" | "to_jsonb" | "jsonb_build_object" | "jsonb_build_array" | "jsonb_agg" | "jsonb_object_agg" | "pondra_jsonb_agg"
        | "pondra_jsonb_object_agg" | "jsonb_extract_path" | "jsonb_array_elements" | "jsonb_strip_nulls" | "jsonb_set" | "jsonb_set_lax" | "jsonb_insert" | "jsonb_delete"
        | "jsonb_delete_path" | "jsonb_concat" => Some(Ty::Jsonb),
        _ => None,
    }
}

fn of_type(t: &ast::DataType) -> Option<Ty> {
    match t {
        ast::DataType::Uuid => Some(Ty::Uuid),
        ast::DataType::JSON => Some(Ty::Json),
        ast::DataType::JSONB => Some(Ty::Jsonb),
        _ => None,
    }
}

/// The one type the items that aren't string literals have. A plain string literal takes its neighbours' type, as Postgres
/// reads an unknown literal beside a value: `coalesce(json_agg(t), '[]')` is json.
fn same_taking_literals(items: impl Iterator<Item = (bool, Option<Ty>)>) -> Option<Ty> { same(items.filter(|(literal, _)| !literal).map(|(_, t)| t)) }

/// The one type all of `types` have: none when they differ, or when there are none.
fn same(types: impl Iterator<Item = Option<Ty>>) -> Option<Ty> {
    let types: Vec<Option<Ty>> = types.collect();
    match types.split_first() {
        Some((first, rest)) if rest.iter().all(|t| t == first) => *first,
        _ => None,
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

/// The name an item without an alias goes by in a query around it: a column's own name. An expression has none of its
/// own, so it is named by its text after `UNNAMED` (`whole` refuses a row with such a column).
fn name_of(e: &Expr) -> String {
    match e {
        Expr::Identifier(i) => ident(i),
        Expr::CompoundIdentifier(p) => p.last().map(ident).unwrap_or_default(),
        e => format!("{UNNAMED}{e}"),
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
        let t = vec![("id".to_string(), Some(Ty::Uuid)), ("name".to_string(), None), ("parent".to_string(), Some(Ty::Uuid)), ("doc".to_string(), Some(Ty::Json))];
        let o = vec![("id".to_string(), None), ("ref".to_string(), Some(Ty::Uuid)), ("b".to_string(), Some(Ty::Jsonb))];
        Known::from([("t".to_string(), Some(t)), ("s.o".to_string(), Some(o)), ("f".to_string(), None)])
    }

    fn cols(sql: &str) -> Vec<Option<Ty>> {
        let k = known();
        Trace::new(&k).statement(&one(sql).unwrap()).map(|c| c.into_iter().map(|(_, t)| t).collect()).unwrap_or_default()
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
        assert_eq!(cols("SELECT id, name, parent FROM t"), [Some(Ty::Uuid), None, Some(Ty::Uuid)]);
        assert_eq!(cols("SELECT * FROM t"), [Some(Ty::Uuid), None, Some(Ty::Uuid), Some(Ty::Json)]);
        assert_eq!(cols("SELECT x.id AS a, o.ref, o.id FROM t AS x JOIN s.o ON x.id = o.ref"), [Some(Ty::Uuid), Some(Ty::Uuid), None]);
        assert_eq!(cols("SELECT t.* FROM t, s.o"), [Some(Ty::Uuid), None, Some(Ty::Uuid), Some(Ty::Json)]);
        assert_eq!(cols("SELECT name::uuid, gen_random_uuid(), uuidv7(), 'x', CAST(name AS UUID) FROM t"), [Some(Ty::Uuid), Some(Ty::Uuid), Some(Ty::Uuid), None, Some(Ty::Uuid)]);
        assert_eq!(cols("SELECT doc, name::json, '{}'::json, CAST(name AS JSONB), pondra_json(name), json_get_json(doc, 'a') FROM t"), [Some(Ty::Json), Some(Ty::Json), Some(Ty::Json), Some(Ty::Jsonb), Some(Ty::Json), Some(Ty::Json)]);
        // (`->` and `#>` answer their document's type, json or jsonb; `->>` and `#>>` text; a text document has none)
        assert_eq!(cols("SELECT doc -> 'a', doc #> '{a}', doc ->> 'a', doc #>> '{a}', name -> 'a' FROM t"), [Some(Ty::Json), Some(Ty::Json), None, None, None]);
        assert_eq!(cols("SELECT b -> 'a', b -> 'a' -> 'b', (b #> '{a}') -> 0 FROM s.o"), [Some(Ty::Jsonb), Some(Ty::Jsonb), Some(Ty::Jsonb)]);
        assert_eq!(cols("SELECT json_object_field(doc, 'a'), jsonb_array_element(b, 0), pondra_json_path(doc, '{a}') FROM t, s.o"), [Some(Ty::Json), Some(Ty::Jsonb), Some(Ty::Json)]);
        assert_eq!(cols("WITH c AS (SELECT parent AS p FROM t) SELECT p FROM c"), [Some(Ty::Uuid)]);
        assert_eq!(cols("WITH c AS (SELECT doc FROM t) SELECT doc FROM c"), [Some(Ty::Json)]);
        assert_eq!(cols("SELECT q.a FROM (SELECT id FROM t) AS q(a)"), [Some(Ty::Uuid)]);
        assert_eq!(cols("SELECT id FROM t UNION ALL SELECT ref FROM s.o"), [Some(Ty::Uuid)]);
        assert_eq!(cols("SELECT id FROM t UNION ALL SELECT name FROM t"), [None]);
        // (a json and a jsonb side are two types: neither is kept)
        assert_eq!(cols("SELECT doc FROM t UNION ALL SELECT b FROM s.o"), [None]);
        assert_eq!(cols("SELECT coalesce(id, parent), coalesce(id, name), CASE WHEN true THEN id ELSE NULL END FROM t"), [Some(Ty::Uuid), None, Some(Ty::Uuid)]);
        // (unknown relations and ambiguous names mark nothing)
        assert_eq!(cols("SELECT * FROM f"), Vec::<Option<Ty>>::new());
        assert_eq!(cols("SELECT f.id FROM f, t"), [None]);
        assert_eq!(cols("SELECT id FROM t JOIN s.o USING (id)"), [None]);
        assert_eq!(cols("INSERT INTO t (id, name) VALUES ('a', 'b') RETURNING id, name"), [Some(Ty::Uuid), None]);
        assert_eq!(cols("UPDATE t AS x SET name = 'a' FROM s.o WHERE x.id = o.ref RETURNING x.*, o.ref"), [Some(Ty::Uuid), None, Some(Ty::Uuid), Some(Ty::Json), Some(Ty::Uuid)]);
        assert_eq!(cols("DELETE FROM t WHERE true RETURNING *"), [Some(Ty::Uuid), None, Some(Ty::Uuid), Some(Ty::Json)]);
    }

    #[test]
    fn traced_json_functions() {
        assert_eq!(cols("SELECT to_json(name), row_to_json(t), json_build_object('a', 1), json_agg(name), pondra_as_json(name), pondra_jsonb(name), to_jsonb(1), jsonb_agg(1), jsonb_set(b, '{a}', '1') FROM t, s.o"), [Some(Ty::Json), Some(Ty::Json), Some(Ty::Json), Some(Ty::Json), Some(Ty::Json), Some(Ty::Jsonb), Some(Ty::Jsonb), Some(Ty::Jsonb), Some(Ty::Jsonb)]);
        // (`#-` answers its document's type; `-` only a jsonb's: a number minus a number is no document)
        assert_eq!(cols("SELECT doc #- '{a}', b #- '{a}', b - 'a', name #- '{a}', doc - 'a', id - 1 FROM t, s.o"), [Some(Ty::Json), Some(Ty::Jsonb), Some(Ty::Jsonb), None, None, None]);
        assert_eq!(cols("SELECT jsonb_pretty(b), json_typeof(doc), upper(name) FROM t, s.o"), [None, None, None]);
    }

    #[test]
    fn names_kept_by_place() {
        // (every `upper(…)` becomes 'x': the items it changed keep their names, the others are as they were)
        let upper = |s: &mut Statement| {
            let mut changed = false;
            let _ = ast::visit_expressions_mut(s, |e| {
                if matches!(e, Expr::Function(f) if object(&f.name) == "upper") {
                    *e = Expr::value(Value::SingleQuotedString("x".into()));
                    changed = true;
                }
                ControlFlow::<()>::Continue(())
            });
            changed
        };
        let kept = |sql: &str| {
            let mut s = one(sql).unwrap();
            let changed = names_kept(&mut s, upper);
            (changed, s.to_string())
        };
        assert_eq!(
            kept("SELECT upper(a), upper(a) AS b, lower(a), (SELECT upper(a) FROM t) FROM t UNION ALL SELECT upper(a), 'y', 'z', 'w' FROM t"),
            (true, r#"SELECT 'x' AS "upper(a)", 'x' AS b, lower(a), (SELECT 'x' AS "upper(a)" FROM t) AS "(SELECT upper(a) FROM t)" FROM t UNION ALL SELECT 'x' AS "upper(a)", 'y', 'z', 'w' FROM t"#.to_string())
        );
        assert_eq!(kept("WITH c AS (SELECT upper(a) FROM t) SELECT * FROM c"), (true, r#"WITH c AS (SELECT 'x' AS "upper(a)" FROM t) SELECT * FROM c"#.to_string()));
        assert_eq!(kept("UPDATE t SET a = upper(b) RETURNING upper(a), a"), (true, r#"UPDATE t SET a = 'x' RETURNING 'x' AS "upper(a)", a"#.to_string()));
        assert_eq!(kept("INSERT INTO t SELECT upper(a) FROM s RETURNING upper(a)"), (true, r#"INSERT INTO t SELECT 'x' AS "upper(a)" FROM s RETURNING 'x' AS "upper(a)""#.to_string()));
        assert_eq!(kept("SELECT lower(a) FROM t"), (false, "SELECT lower(a) FROM t".to_string()));
        // (a name holding a double quote is written so it reads back as itself)
        let (_, text) = kept(r#"SELECT upper("A") FROM t"#);
        match &one(&text).unwrap() {
            Statement::Query(q) => match &*q.body {
                SetExpr::Select(s) => assert!(matches!(&s.projection[0], SelectItem::ExprWithAlias { alias, .. } if alias.value == r#"upper("A")"#), "{text}"),
                _ => panic!("{text}"),
            },
            _ => panic!("{text}"),
        }
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
        // (a json column's parameters are the client's type, not a uuid)
        assert_eq!(params("SELECT name FROM t WHERE doc = $1"), Vec::<usize>::new());
    }

    #[test]
    fn types_by_name() {
        assert_eq!(Ty::of("uuid"), Some(Ty::Uuid));
        assert_eq!(Ty::of("jsonb"), Some(Ty::Jsonb));
        assert_eq!(Ty::of("varchar(3)"), None);
        assert_eq!(Ty::Jsonb.name(), "jsonb");
    }

    #[test]
    fn rewritten_by_place() {
        let k = known();
        let u = "A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11";
        let mut s = one(&format!("SELECT pg_typeof(id), pg_typeof(name), pg_typeof(uuidv7()) AS v, pg_typeof('x') FROM t WHERE id = '{u}' OR name = '{u}' OR parent IN ('{{a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a12}}', $1) UNION ALL SELECT pg_typeof(ref), 'x', 'y', 'z' FROM s.o")).unwrap();
        assert!(rewritten(&k, &mut s).unwrap());
        assert_eq!(s.to_string(), format!("SELECT 'uuid' AS \"pg_typeof(id)\", pg_typeof(name), 'uuid' AS v, pg_typeof('x') FROM t WHERE id = 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11' OR name = '{u}' OR parent IN ('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a12', $1) UNION ALL SELECT 'uuid' AS \"pg_typeof(ref)\", 'x', 'y', 'z' FROM s.o"));
        let mut s = one("SELECT pg_typeof(doc), pg_typeof(name) FROM t").unwrap();
        assert!(rewritten(&k, &mut s).unwrap());
        assert_eq!(s.to_string(), "SELECT 'json' AS \"pg_typeof(doc)\", pg_typeof(name) FROM t");
        let mut s = one("SELECT pg_typeof(b) FROM s.o").unwrap();
        assert!(rewritten(&k, &mut s).unwrap());
        assert_eq!(s.to_string(), "SELECT 'jsonb' AS \"pg_typeof(b)\" FROM s.o");
        let mut s = one(&format!("INSERT INTO t (name, id) VALUES ('{u}', '{u}'), ('x', 'nope')")).unwrap();
        assert!(rewritten(&k, &mut s).unwrap());
        assert_eq!(s.to_string(), format!("INSERT INTO t (name, id) VALUES ('{u}', 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'), ('x', 'nope')"));
        let mut s = one(&format!("INSERT INTO t (parent, name) SELECT '{u}' AS p, '{u}' FROM s.o")).unwrap();
        assert!(rewritten(&k, &mut s).unwrap());
        assert_eq!(s.to_string(), format!("INSERT INTO t (parent, name) SELECT 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11' AS p, '{u}' FROM s.o"));
        let mut s = one(&format!("UPDATE t SET parent = '{u}', name = '{u}'")).unwrap();
        assert!(rewritten(&k, &mut s).unwrap());
        assert_eq!(s.to_string(), format!("UPDATE t SET parent = 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', name = '{u}'"));
        let mut s = one(&format!("SELECT name FROM t WHERE name = '{u}' AND id = 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'")).unwrap();
        assert!(!rewritten(&k, &mut s).unwrap()); // (a string compared with text, and a uuid already written its way)
    }

    /// `sql` rewritten, as its text (the statement as it is, when nothing changes).
    fn rewrite_text(sql: &str) -> String {
        let k = known();
        let mut s = one(sql).unwrap();
        rewritten(&k, &mut s).unwrap();
        s.to_string()
    }

    /// The SQLSTATE and the words of the refusal `sql` earns.
    fn refusal(sql: &str) -> (&'static str, String) {
        let k = known();
        let mut s = one(sql).unwrap();
        let e = rewritten(&k, &mut s).unwrap_err();
        (crate::codes::of(&e), e.to_string())
    }

    /// `t`'s whole row, as `pondra_row` makes it for the relation named `n`.
    fn t_row(n: &str) -> String {
        format!(r#"pondra_row('id', "{n}"."id", 'name', "{n}"."name", 'parent', "{n}"."parent", 'doc', pondra_as_json("{n}"."doc"))"#)
    }

    #[test]
    fn makes_json_gate() {
        assert!(makes_json("SELECT json_agg(x) FROM t"));
        assert!(makes_json("select TO_JSONB ( doc ) FROM t"));
        assert!(makes_json("SELECT json_build_object('a', 1)"));
        assert!(!makes_json("SELECT pondra_json_agg(array_agg(x), 'value') FROM t")); // (a rewrite's own name is no maker)
        assert!(!makes_json("SELECT doc::json, doc -> 'a' FROM t"));
    }

    #[test]
    fn json_column_in_a_builder() {
        assert_eq!(rewrite_text("SELECT json_build_object('id', name, 'doc', doc) FROM t"), r#"SELECT json_build_object('id', name, 'doc', pondra_as_json(doc)) AS "json_build_object('id', name, 'doc', doc)" FROM t"#);
        assert_eq!(rewrite_text("SELECT jsonb_build_array(b, doc -> 'a') FROM s.o, t"), r#"SELECT jsonb_build_array(pondra_as_jsonb(b), pondra_as_json(doc -> 'a')) AS "jsonb_build_array(b, doc -> 'a')" FROM s.o, t"#);
        assert_eq!(rewrite_text("SELECT to_json(doc) FROM t"), r#"SELECT to_json(pondra_as_json(doc)) AS "to_json(doc)" FROM t"#);
    }

    #[test]
    fn whole_rows_from_a_table_an_alias_a_cte_and_a_subquery() {
        assert_eq!(rewrite_text("SELECT json_agg(t) FROM t"), format!(r#"SELECT pondra_json_agg(array_agg({r}), 'row') AS "json_agg(t)" FROM t"#, r = t_row("t")));
        assert_eq!(rewrite_text("SELECT to_json(u), row_to_json(u) FROM t AS u"), format!(r#"SELECT to_json({r}) AS "to_json(u)", row_to_json({r}) AS "row_to_json(u)" FROM t AS u"#, r = t_row("u")));
        assert_eq!(rewrite_text("SELECT json_agg(x) FROM (SELECT id, name FROM t) x"), r#"SELECT pondra_json_agg(array_agg(pondra_row('id', "x"."id", 'name', "x"."name")), 'row') AS "json_agg(x)" FROM (SELECT id, name FROM t) x"#);
        assert_eq!(rewrite_text("WITH c AS (SELECT id, doc FROM t) SELECT to_json(c) FROM c"), r#"WITH c AS (SELECT id, doc FROM t) SELECT to_json(pondra_row('id', "c"."id", 'doc', pondra_as_json("c"."doc"))) AS "to_json(c)" FROM c"#);
    }

    #[test]
    fn a_column_named_like_its_table_stays_a_column() {
        assert_eq!(rewrite_text("SELECT json_agg(doc) FROM t AS doc"), r#"SELECT pondra_json_agg(array_agg(pondra_as_json(doc)), 'json') AS "json_agg(doc)" FROM t AS doc"#);
        assert_eq!(rewrite_text("SELECT to_json(name) FROM t AS name"), "SELECT to_json(name) FROM t AS name");
    }

    #[test]
    fn json_agg_keeps_its_order_filter_distinct_and_window() {
        assert_eq!(rewrite_text("SELECT json_agg(name ORDER BY name DESC) FROM t"), r#"SELECT pondra_json_agg(array_agg(name ORDER BY name DESC), 'value') AS "json_agg(name ORDER BY name DESC)" FROM t"#);
        assert_eq!(rewrite_text("SELECT json_agg(name) FILTER (WHERE id IS NOT NULL) FROM t"), r#"SELECT pondra_json_agg(array_agg(name) FILTER (WHERE id IS NOT NULL), 'value') AS "json_agg(name) FILTER (WHERE id IS NOT NULL)" FROM t"#);
        assert_eq!(rewrite_text("SELECT jsonb_agg(DISTINCT name), json_agg(name) OVER (PARTITION BY parent) FROM t"), r#"SELECT pondra_jsonb_agg(array_agg(DISTINCT name), 'value') AS "jsonb_agg(DISTINCT name)", pondra_json_agg(array_agg(name) OVER (PARTITION BY parent), 'value') AS "json_agg(name) OVER (PARTITION BY parent)" FROM t"#);
        // (jsonb's elements are its own text, so DISTINCT is fine and the element is printed as jsonb)
        assert_eq!(rewrite_text("SELECT jsonb_agg(DISTINCT b) FROM s.o"), r#"SELECT pondra_jsonb_agg(array_agg(DISTINCT pondra_as_jsonb(b)), 'json') AS "jsonb_agg(DISTINCT b)" FROM s.o"#);
    }

    #[test]
    fn json_object_agg_takes_its_key_and_value() {
        assert_eq!(rewrite_text("SELECT json_object_agg(name, doc), jsonb_object_agg(name, id ORDER BY name) FROM t"), r#"SELECT pondra_json_object_agg(array_agg(named_struct('k', name, 'v', pondra_as_json(doc))), 'json') AS "json_object_agg(name, doc)", pondra_jsonb_object_agg(array_agg(named_struct('k', name, 'v', id) ORDER BY name), 'value') AS "jsonb_object_agg(name, id ORDER BY name)" FROM t"#);
    }

    #[test]
    fn whole_rows_and_columns_inside_an_aggregate_both_change() {
        let expected = format!(r#"SELECT pondra_json_agg(array_agg({r}), 'row') AS "json_agg(t)", pondra_json_agg(array_agg(pondra_as_json(json_build_object('a', pondra_as_json(doc)))), 'json') AS "json_agg(json_build_object('a', doc))" FROM t"#, r = t_row("t"));
        assert_eq!(rewrite_text("SELECT json_agg(t), json_agg(json_build_object('a', doc)) FROM t"), expected);
        assert_eq!(rewrite_text("SELECT json_build_object('n', name, 'row', t) FROM t AS t"), format!(r#"SELECT json_build_object('n', name, 'row', {r}) AS "json_build_object('n', name, 'row', t)" FROM t AS t"#, r = t_row("t")));
    }

    #[test]
    fn a_maker_in_a_where_is_changed() {
        assert_eq!(rewrite_text("SELECT id FROM t WHERE json_build_object('d', doc)::text <> '{}'"), "SELECT id FROM t WHERE json_build_object('d', pondra_as_json(doc))::TEXT <> '{}'");
    }

    #[test]
    fn a_table_or_materialized_view_made_from_a_query_changes_its_makers() {
        // (both run their query when they are made, so their makers change then; a plain view is changed where it is read)
        let made = rewrite_text("CREATE TABLE d AS SELECT name, json_agg(name) AS names FROM t GROUP BY name");
        assert!(made.contains("pondra_json_agg(array_agg(name), 'value') AS names") && !made.contains("json_agg(name)"), "{made}");
        let view = rewrite_text("CREATE MATERIALIZED VIEW m AS SELECT json_agg(x) AS j FROM (SELECT id, name FROM t) x");
        assert!(view.contains(r#"pondra_json_agg(array_agg(pondra_row('id', "x"."id", 'name', "x"."name")), 'row') AS j"#), "{view}");
        let plain = rewrite_text("CREATE VIEW v AS SELECT json_agg(name) AS names FROM t");
        assert!(plain.contains("json_agg(name) AS names") && !plain.contains("pondra_"), "{plain}");
    }

    #[test]
    fn whole_row_refused_by_name() {
        assert_eq!(refusal("SELECT json_agg(t) FROM (SELECT upper(name) FROM t) t"), ("0A000", "json_agg(t) of a whole row needs a name for each of t's columns: give upper(name) an alias".to_string()));
        assert_eq!(refusal("SELECT json_agg(f) FROM f"), ("0A000", "json_agg(f) of a whole row needs f's columns: name them, as json_build_object('a', f.a, …)".to_string()));
        assert_eq!(refusal("SELECT json_agg(DISTINCT doc) FROM t"), ("42883", "could not identify an equality operator for type json".to_string()));
        assert_eq!(refusal("SELECT json_object_agg(doc, name) FROM t"), ("22023", "key value must be scalar, not array, composite, or json".to_string()));
    }

    #[test]
    fn coalesce_and_case_let_a_literal_take_json() {
        assert_eq!(cols("SELECT coalesce(doc, '{}'), CASE WHEN name = 'a' THEN '[]' ELSE doc END, coalesce(json_agg(name), '[]') FROM t"), [Some(Ty::Json), Some(Ty::Json), Some(Ty::Json)]);
        assert_eq!(cols("SELECT coalesce(name, 'x') FROM t"), [None]);
        assert_eq!(rewrite_text("SELECT coalesce(json_agg(name), '[]') FROM t"), r#"SELECT coalesce(pondra_json_agg(array_agg(name), 'value'), '[]') AS "coalesce(json_agg(name), '[]')" FROM t"#);
    }

    #[test]
    fn a_second_rewrite_changes_nothing() {
        for sql in ["SELECT json_agg(t) FROM t", "SELECT json_build_object('d', doc), json_agg(json_build_object('a', doc)) FROM t", "SELECT coalesce(json_agg(name), '[]') FROM t", "SELECT jsonb_agg(DISTINCT b) FROM s.o", "SELECT json_object_agg(name, doc) FROM t"] {
            let once = rewrite_text(sql);
            assert_eq!(rewrite_text(&once), once, "{sql}");
        }
    }

    #[test]
    fn a_statement_without_makers_stays_as_it_is() {
        assert_eq!(rewrite_text("SELECT name, id, doc FROM t WHERE name = 'x' AND doc IS NOT NULL"), "SELECT name, id, doc FROM t WHERE name = 'x' AND doc IS NOT NULL");
    }
}
