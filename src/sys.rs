//! System columns: every row's identity and history, as Postgres's `ctid` and `xmin`, Delta's row
//! tracking and Iceberg v3's row lineage give theirs.
//!
//! - `_row_id`: which row it is, from its INSERT on. An UPDATE or MERGE keeps it; a new row — an
//!   INSERT, and an upsert that replaces a keyed table's row — gets a new one.
//! - `_version`: the commit that wrote this version of the row (a lake-wide sequence number).
//! - `_created_at`, `_updated_at`: when its first version and this one were committed.
//!
//! `SELECT *` leaves them out; a query that names one gets them (`SELECT _row_id, * FROM t`).
//!
//! Where they come from: the node that packs a row into the log stamps its `_row_id` from a block
//! it reserved from the leader (`(commit number << 32) + n`: `Ids`), and so does a bulk INSERT as
//! it writes its files. `_version` and the times are the commit's: a log row's are its segment's,
//! read off the catalog (`derive`); tiering writes all four into the Parquet files, and a bulk
//! INSERT writes the commit number and time it reserved.
use crate::store::TableMeta;
use anyhow::Result;
use datafusion::arrow::array::{Array, ArrayRef, AsArray, Int64Array, TimestampMicrosecondArray};
use datafusion::arrow::datatypes::{DataType, Field, Int64Type, Schema, TimeUnit, TimestampMicrosecondType};
use datafusion::arrow::record_batch::RecordBatch;
use std::borrow::Cow;
use std::sync::Arc;

pub const ROW_ID: &str = "_row_id";
pub const VERSION: &str = "_version";
pub const CREATED: &str = "_created_at";
pub const UPDATED: &str = "_updated_at";
pub const NAMES: [&str; 4] = [ROW_ID, VERSION, CREATED, UPDATED];

fn time() -> DataType { DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())) }

/// The system columns, as a table's columns list them (name, type).
pub fn columns() -> Vec<(String, String)> {
    let ts = crate::query::type_name(&time());
    vec![(ROW_ID.into(), "Int64".into()), (VERSION.into(), "Int64".into()), (CREATED.into(), ts.clone()), (UPDATED.into(), ts)]
}

/// The table with its system columns too. A merge table (a view's GROUP BY) combines them per
/// key: its first row and first commit, its last commit.
pub fn with_sys(meta: &TableMeta) -> TableMeta {
    let mut m = meta.clone();
    if m.columns.iter().any(|(c, _)| c == ROW_ID) {
        return m;
    }
    m.columns.extend(columns());
    if !m.merge.is_empty() {
        for (c, f) in [(ROW_ID, "min"), (CREATED, "min"), (VERSION, "max"), (UPDATED, "max")] {
            m.merge.insert(c.into(), f.into());
        }
    }
    m
}

/// Does `sql` name a system column? (A word match: one too many costs a few columns read.)
pub fn mentioned(sql: &str) -> bool {
    sql.to_lowercase().split(|c: char| !(c.is_alphanumeric() || c == '_')).any(|w| NAMES.contains(&w))
}

/// `*` without the system columns, in a query that names one (so they are in its tables):
/// `SELECT *, _row_id FROM t` shows the table's columns and `_row_id` once. Only a SELECT from one
/// table is rewritten; over a join, `*` shows each table's system columns too.
pub fn hide(sql: &str) -> Cow<'_, str> {
    use datafusion::sql::sqlparser::{ast::*, dialect::GenericDialect, parser::Parser};
    use std::ops::ControlFlow;
    if !mentioned(sql) {
        return Cow::Borrowed(sql);
    }
    let Ok(mut stmts) = Parser::parse_sql(&GenericDialect {}, sql) else { return Cow::Borrowed(sql) };
    struct Hide {
        ctes: Vec<String>,
        changed: bool,
    }
    impl VisitorMut for Hide {
        type Break = ();
        fn pre_visit_query(&mut self, q: &mut Query) -> ControlFlow<()> {
            self.ctes.extend(q.with.iter().flat_map(|w| &w.cte_tables).map(|c| c.alias.name.value.to_lowercase()));
            let SetExpr::Select(s) = q.body.as_mut() else { return ControlFlow::Continue(()) };
            let [TableWithJoins { relation: TableFactor::Table { name, .. }, joins }] = &s.from[..] else { return ControlFlow::Continue(()) };
            let cte = name.0.len() == 1 && name.0[0].as_ident().is_some_and(|i| self.ctes.contains(&i.value.to_lowercase()));
            if !joins.is_empty() || cte {
                return ControlFlow::Continue(());
            }
            for item in &mut s.projection {
                let (SelectItem::Wildcard(o) | SelectItem::QualifiedWildcard(_, o)) = item else { continue };
                if o.opt_exclude.is_none() && o.opt_except.is_none() {
                    o.opt_exclude = Some(ExcludeSelectItem::Multiple(NAMES.iter().map(|n| ObjectName::from(vec![Ident::new(*n)])).collect()));
                    self.changed = true;
                }
            }
            ControlFlow::Continue(())
        }
    }
    let mut v = Hide { ctes: vec![], changed: false };
    let _ = VisitMut::visit(&mut stmts, &mut v);
    match v.changed {
        true => Cow::Owned(stmts.iter().map(|s| s.to_string()).collect::<Vec<_>>().join("; ")),
        false => Cow::Borrowed(sql),
    }
}

/// The table holding a table's replaced rows (`{t}$deleted`, with UPDATE, DELETE and MERGE on an
/// append table): the lake's own, never listed or queried by name. (`$`: no name a user gives
/// has one, and object stores take it as it is.)
pub fn deleted(table: &str) -> String { format!("{table}$deleted") }

pub fn hidden(table: &str) -> bool { table.contains('$') }

/// Can a file (or manifest) with these column ranges hold one of the rows `dead` names — sorted
/// (`_row_id`, `_version`) pairs? By its `_row_id` and `_version` ranges; yes if it has none.
pub fn holds(dead: &[(i64, i64)], stats: &crate::manifest::Stats) -> bool {
    let range = |c: &str| stats.get(c).and_then(|(lo, hi)| Some((lo.parse::<i64>().ok()?, hi.parse::<i64>().ok()?)));
    match (range(ROW_ID), range(VERSION)) {
        (Some((lo, hi)), Some((vlo, vhi))) => dead[dead.partition_point(|d| d.0 < lo)..].iter().take_while(|d| d.0 <= hi).any(|d| (vlo..=vhi).contains(&d.1)),
        _ => true,
    }
}

/// A block of row ids reserved from the leader: `(commit number << 32) + n`, n counting up. A
/// node takes a new block when one runs out (or after a restart: ids are never reused).
#[derive(Default)]
pub struct Ids(std::sync::Mutex<Option<(u64, u64)>>); // (block, next n)

impl Ids {
    /// `rows` consecutive ids, if the block has them (else None: reserve a block first).
    pub fn take(&self, rows: u64) -> Option<i64> {
        let mut b = self.0.lock().unwrap();
        let (block, next) = (*b)?;
        if next + rows > 1 << 32 {
            *b = None;
            return None;
        }
        *b = Some((block, next + rows));
        Some(((block << 32) + next) as i64)
    }

    pub fn refill(&self, block: u64) { *self.0.lock().unwrap() = Some((block, 0)); }
}

/// A bulk INSERT's rows with their system columns: ids from `first` on, the commit number and
/// time the writer reserved (`log::To::reserve`).
pub fn stamp_new(b: &RecordBatch, first: i64, (n, ms): (u64, u64)) -> Result<RecordBatch> {
    let rows = b.num_rows();
    let at = || -> ArrayRef { Arc::new(TimestampMicrosecondArray::from(vec![(ms * 1000) as i64; rows]).with_timezone("UTC")) };
    let b = set(b, ROW_ID, Arc::new(Int64Array::from_iter_values(first..first + rows as i64)))?;
    let b = set(&b, VERSION, Arc::new(Int64Array::from(vec![n as i64; rows])))?;
    let b = set(&b, CREATED, at())?;
    set(&b, UPDATED, at())
}

/// A stream of a bulk INSERT's rows, stamped (`stamp_new`); `next` counts the rows its streams
/// have taken so far (they share the reservation).
pub fn stamp_stream(rows: datafusion::execution::SendableRecordBatchStream, next: Arc<std::sync::atomic::AtomicU64>, reserved: (u64, u64)) -> Result<datafusion::execution::SendableRecordBatchStream> {
    use futures::StreamExt;
    let mut fields = rows.schema().fields().to_vec();
    fields.extend(columns().iter().map(|(n, t)| Arc::new(Field::new(n, crate::query::dtype(t).expect("a type"), true))));
    let schema = Arc::new(Schema::new(fields));
    let stamped = rows.map(move |b| {
        let b = b?;
        let first = (reserved.0 << 32) as i64 + next.fetch_add(b.num_rows() as u64, std::sync::atomic::Ordering::Relaxed) as i64;
        stamp_new(&b, first, reserved).map_err(|e| datafusion::error::DataFusionError::External(e.into()))
    });
    Ok(Box::pin(datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(schema, stamped)))
}

/// `b` with a `_row_id` for every row: the ids it carries (an UPDATE's keep their row's), and new
/// ones from `first` for the rest.
pub fn stamp(b: &RecordBatch, first: i64) -> Result<RecordBatch> {
    let fresh = || Int64Array::from_iter_values(first..first + b.num_rows() as i64);
    let ids: ArrayRef = match b.column_by_name(ROW_ID) {
        Some(c) if c.null_count() == 0 => return Ok(b.clone()),
        Some(c) => {
            let old = datafusion::arrow::compute::cast(c, &DataType::Int64)?;
            Arc::new(old.as_primitive::<Int64Type>().iter().zip(fresh().iter()).map(|(o, f)| o.or(f)).collect::<Int64Array>())
        }
        None => Arc::new(fresh()),
    };
    set(b, ROW_ID, ids)
}

/// Where a batch whose `_row_id`s run on from one keeps just the first (`compact`).
const FIRST: &str = "pondra.first_row_id";

/// A batch as the log keeps it: `_row_id`s that run on from one — fresh ones, as `stamp` gives —
/// become that one number in the schema's metadata, so rows cost no more to write than before
/// they had ids (a column of them took a third off ingest: ZSTD). Carried ids, an UPDATE's, stay a
/// column. Reading the log puts the column back (`expand`).
pub fn compact(b: &RecordBatch) -> Result<RecordBatch> {
    let Some(i) = b.schema().index_of(ROW_ID).ok() else { return Ok(b.clone()) };
    let ids = b.column(i).as_primitive_opt::<Int64Type>().filter(|c| c.null_count() == 0 && !c.is_empty());
    let Some(ids) = ids.filter(|c| c.values().windows(2).all(|w| w[1] == w[0] + 1)) else { return Ok(b.clone()) };
    let first = ids.value(0);
    let keep: Vec<usize> = (0..b.num_columns()).filter(|&j| j != i).collect();
    let b = b.project(&keep)?;
    let mut meta = b.schema().metadata().clone();
    meta.insert(FIRST.into(), first.to_string());
    let schema = Arc::new(Schema::new_with_metadata(b.schema().fields().clone(), meta));
    Ok(b.with_schema(schema)?)
}

/// A batch as the log kept it, as it was: the `_row_id` column back (`compact`), the metadata gone.
pub fn expand(b: RecordBatch) -> Result<RecordBatch> {
    let Some(first) = b.schema().metadata().get(FIRST).and_then(|f| f.parse::<i64>().ok()) else { return Ok(b) };
    let b = RecordBatch::try_new(Arc::new(Schema::new(b.schema().fields().clone())), b.columns().to_vec())?;
    set(&b, ROW_ID, Arc::new(Int64Array::from_iter_values(first..first + b.num_rows() as i64)))
}

/// A log segment's rows with their system columns: `_row_id` as stamped (rows logged before
/// round 19: from their place in the log, `(segment << 32) + position`), `_version` the segment,
/// the times its commit's (`_created_at` kept where the row carries it: an UPDATE's).
pub fn derive(b: &RecordBatch, seg: u64, pos: u64, ms: u64) -> Result<RecordBatch> {
    let n = b.num_rows();
    let us = (ms * 1000) as i64;
    let keep = |name: &str, dt: &DataType| b.column_by_name(name).map(|c| datafusion::arrow::compute::cast(c, dt)).transpose();
    let ids: ArrayRef = match keep(ROW_ID, &DataType::Int64)? {
        Some(c) => Arc::new(c.as_primitive::<Int64Type>().iter().enumerate().map(|(i, v)| v.or(Some(((seg << 32) + pos + i as u64) as i64))).collect::<Int64Array>()),
        None => Arc::new(Int64Array::from_iter_values((0..n as i64).map(|i| ((seg << 32) + pos) as i64 + i))),
    };
    let at = |v: Option<ArrayRef>| -> ArrayRef {
        let now = TimestampMicrosecondArray::from(vec![us; n]);
        let a = match v {
            Some(c) => c.as_primitive::<TimestampMicrosecondType>().iter().map(|v| v.or(Some(us))).collect::<TimestampMicrosecondArray>(),
            None => now,
        };
        Arc::new(a.with_timezone("UTC"))
    };
    let b = set(b, ROW_ID, ids)?;
    let b = set(&b, VERSION, Arc::new(Int64Array::from(vec![seg as i64; n])))?;
    let b = set(&b, CREATED, at(keep(CREATED, &time())?))?;
    set(&b, UPDATED, at(None))
}

/// `b` with column `name` set to `values` (replaced if it has one, else added at the end).
fn set(b: &RecordBatch, name: &str, values: ArrayRef) -> Result<RecordBatch> {
    let (mut fields, mut cols): (Vec<_>, Vec<_>) = (b.schema().fields().to_vec(), b.columns().to_vec());
    let field = Arc::new(Field::new(name, values.data_type().clone(), true));
    match b.schema().index_of(name) {
        Ok(i) => (fields[i], cols[i]) = (field, values),
        Err(_) => {
            fields.push(field);
            cols.push(values);
        }
    }
    Ok(RecordBatch::try_new(Arc::new(Schema::new_with_metadata(fields, b.schema().metadata().clone())), cols)?)
}
