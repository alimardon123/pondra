//! The Postgres port's types that Arrow's don't say: which columns of an answer, and which
//! parameters of a statement, are uuids, json or jsonb. Each is text in the lake (`types.rs`), so its Arrow type
//! says nothing; the port traces each column of an answer back, in the statement as written, to a
//! uuid column of a table or view it reads, a cast to `uuid` or a function that makes one, and marks
//! it on the answer's schema (`typed`). It then sends it as Postgres's `uuid` (oid 2950: `pg.rs`'s
//! `field_type`), and drivers hand it over as their uuid type (psycopg's `UUID`, pgjdbc's
//! `java.util.UUID`). Nothing plans or answers differently for the mark. A trace that can't tell
//! marks nothing, and the column goes as text, as before. The same parse gives each column's Postgres name
//! (`pg_names`), which `typed` keeps in the schema's metadata beside the mark: a statement is parsed once.

use std::ops::ControlFlow;
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::sql::sqlparser::ast::{self, Expr, Statement};

use crate::pg_functions::MARK;
use crate::pg_names;
use crate::pg_trace::{first_arg, known, param, Known, Trace};
pub(crate) use crate::pg_trace::{one, Ty};
use crate::store::Lake;
use crate::text::object;

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
    let known = known(lake, &stmt).await;
    let types: Vec<Option<Ty>> = Trace::new(&known).statement(&stmt).map(|c| c.into_iter().map(|(_, t)| t).collect()).unwrap_or_default();
    Shape { types, names }
}

/// Which of `sql`'s parameters are uuids, by their number less one (`$1` is 0): one cast to uuid, one
/// a uuid column is compared with in any expression (a WHERE, a join's ON, an item, a CASE), an INSERT's
/// value for a uuid column, an UPDATE's `SET c = $1`.
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

/// `sql` parsed, with the columns of every table and view it names. None for one with nothing to trace:
/// a query of Postgres's catalog (never a uuid there), or one that doesn't parse as one statement.
async fn read(lake: &Lake, sql: &str) -> Option<(Statement, Known)> {
    if crate::pg_catalog::wanted(sql) {
        return None;
    }
    let stmt = one(sql)?;
    let known = known(lake, &stmt).await;
    Some((stmt, known))
}
