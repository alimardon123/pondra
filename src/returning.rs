//! `INSERT`, `UPDATE` and `DELETE … RETURNING …`, as Postgres answers them: the rows the write wrote (an INSERT's and an
//! UPDATE's new rows, a DELETE's old ones), as its RETURNING list asks, at every door that answers rows (Postgres, HTTP,
//! MCP, Flight SQL). ORMs write this way to learn a new row's key: Django, Rails, Prisma and SQLAlchemy all send
//! `INSERT … RETURNING id`.
//!
//! The rows are the ones the write worked out, kept as it wrote them (`wrote`), never read back after it: other writes may
//! have changed the table by then. Each path that works its rows out on this node keeps them: an INSERT through the log
//! (`write::run_listed`), a keyed table's write on its own (`txn::autocommit`) and a statement in a transaction
//! (`txn::statement`). A change the leader would work out (an append table's UPDATE or DELETE, a MERGE or ON CONFLICT, a
//! table with a UNIQUE: `write::changes`) runs as a transaction of its own instead (`txn::alone`), so its rows are worked
//! out here, where they can be returned, and still land in one commit. A job sent again writes nothing, and its first
//! answer's rows aren't kept, so it is refused by name (55000) rather than answered with no rows.

use crate::auth::Role;
use crate::server::App;
use crate::store::TableMeta;
use crate::write::Stmt;
use anyhow::{bail, Result};
use datafusion::arrow::compute::concat_batches;
use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::sql::sqlparser::ast::{self, Expr, SelectItem, Statement};
use serde_json::Value;
use std::ops::ControlFlow;
use std::sync::{Arc, Mutex};

/// The name the written rows go by in a query over them (`answer`).
const ROWS: &str = "__returning";

/// A write with RETURNING, taken apart.
pub struct Returning {
    /// The write without its RETURNING.
    pub write: String,
    items: Vec<SelectItem>,
    /// What its items may call the table: its name's last parts as written (`t`, `public.t`), and its alias.
    names: Vec<Vec<String>>,
    kind: Kind,
}

#[derive(Clone, Copy)]
enum Kind {
    Insert,
    Update,
    Delete,
}

/// The rows a write kept for its RETURNING: those it replaced and those it added.
struct Kept {
    old: Vec<RecordBatch>,
    new: Vec<RecordBatch>,
}

tokio::task_local! {
    /// Where the write being run keeps its rows (`wrote`), while a RETURNING waits for them.
    static KEPT: Arc<Mutex<Option<Kept>>>;
}

/// Does a RETURNING wait for the rows of the write being run? (An INSERT then goes through the log: `write::run_listed`.)
pub fn asked() -> bool { KEPT.try_with(|_| ()).is_ok() }

/// The rows a write worked out here, kept for its RETURNING if one asks: those it replaces (`old`) and adds (`new`). Every
/// path that writes rows it worked out on this node calls it; a statement worked out again keeps its last rows.
pub fn wrote(old: &[RecordBatch], new: &[RecordBatch]) {
    let _ = KEPT.try_with(|k| *k.lock().unwrap() = Some(Kept { old: old.to_vec(), new: new.to_vec() }));
}

/// `sql` as a write with RETURNING, if it is one.
pub fn of(sql: &str) -> Option<Returning> {
    if !sql.as_bytes().windows(9).any(|w| w.eq_ignore_ascii_case(b"returning")) {
        return None; // (nearly every statement: not parsed here)
    }
    let mut statement = crate::pg_types::one(sql)?; // (read as the port's traces read it: Postgres's JSON operators too)
    let alias = |a: &Option<ast::TableAlias>| a.as_ref().map(|a| vec![crate::text::ident(&a.name)]);
    let (kind, items, name, alias) = match &mut statement {
        Statement::Insert(i) => {
            let ast::TableObject::TableName(n) = &i.table else { return None };
            let alias = i.table_alias.as_ref().map(|a| vec![crate::text::ident(&a.alias)]);
            (Kind::Insert, i.returning.take()?, n.clone(), alias)
        }
        Statement::Update(u) => {
            let ast::TableFactor::Table { name, alias: a, .. } = &u.table.relation else { return None };
            (Kind::Update, u.returning.take()?, name.clone(), alias(a))
        }
        Statement::Delete(d) => {
            let (ast::FromTable::WithFromKeyword(t) | ast::FromTable::WithoutKeyword(t)) = &d.from;
            let ast::TableFactor::Table { name, alias: a, .. } = &t.first()?.relation else { return None };
            (Kind::Delete, d.returning.take()?, name.clone(), alias(a))
        }
        _ => return None,
    };
    let parts: Vec<String> = crate::text::object(&name).split('.').map(String::from).collect();
    let mut names: Vec<Vec<String>> = (0..parts.len()).map(|i| parts[i..].to_vec()).collect();
    names.extend(alias);
    Some(Returning { write: crate::text::sql(&statement), items, names, kind })
}

impl Returning {
    /// Postgres's command tag, which the row count follows (`INSERT 0 3`, `UPDATE 2`).
    pub fn tag(&self) -> &'static str {
        match self.kind {
            Kind::Insert => "INSERT 0",
            Kind::Update => "UPDATE",
            Kind::Delete => "DELETE",
        }
    }

    fn word(&self) -> &'static str {
        match self.kind {
            Kind::Insert => "INSERT",
            Kind::Update => "UPDATE",
            Kind::Delete => "DELETE",
        }
    }

    /// The items, with every column the table's name (or alias) qualifies read from the rows instead: `t.id` is
    /// `__returning.id`, `t.*` is `*`. A subquery naming the table reads the table.
    fn items(&self) -> Vec<SelectItem> {
        let ours = |q: &[ast::Ident]| self.names.iter().any(|n| n.len() == q.len() && n.iter().zip(q).all(|(a, b)| *a == crate::text::ident(b)));
        let mut items = self.items.clone();
        for item in &mut items {
            let star = match item {
                SelectItem::QualifiedWildcard(ast::SelectItemQualifiedWildcardKind::ObjectName(n), o) => {
                    let q: Option<Vec<ast::Ident>> = n.0.iter().map(|p| p.as_ident().cloned()).collect();
                    q.is_some_and(|q| ours(&q)).then(|| o.clone())
                }
                _ => None,
            };
            if let Some(o) = star {
                *item = SelectItem::Wildcard(o);
            }
        }
        let _ = ast::visit_expressions_mut(&mut items, |e| {
            if let Expr::CompoundIdentifier(parts) = e {
                if parts.len() >= 2 && ours(&parts[..parts.len() - 1]) {
                    *e = Expr::CompoundIdentifier(vec![ast::Ident::new(ROWS), parts[parts.len() - 1].clone()]);
                }
            }
            ControlFlow::<()>::Continue(())
        });
        items
    }
}

/// Runs the write and answers its rows as its RETURNING asks. Everything that can refuse it is checked before anything is
/// written: the write itself, the table, the items, and that the caller may read the table's rows.
pub async fn run(app: &App, r: &Returning, role: Role, files: bool, job: Option<String>) -> Result<RecordBatch> {
    let stmt = parsed(r)?;
    app.auth.allows(role, &stmt)?;
    let (table, shown) = target(app, r, &stmt).await?;
    let items = r.items();
    answer(app, &items, RecordBatch::new_empty(shown.clone())).await?; // (a wrong item refuses the write, as Postgres refuses it)
    may_read(&table)?;
    let kept = Arc::new(Mutex::new(None));
    let out = KEPT.scope(kept.clone(), write(app, r, &table, stmt, job.clone(), files)).await?;
    if out["duplicate"] == true {
        // A job sent again writes nothing, and its first answer's rows aren't kept: no rows would read as nothing written.
        return Err(crate::codes::coded("55000", format!("{} … RETURNING with job {} was applied already, by an earlier send of it: its rows went with that answer and aren't kept, so read them with a SELECT", r.word(), job.unwrap_or_default())));
    }
    let Some(kept) = kept.lock().unwrap().take() else {
        bail!("{} into {table} landed, but its rows weren't kept for RETURNING: read them with a SELECT", r.word());
    };
    let rows = match r.kind {
        Kind::Delete => kept.old,
        Kind::Insert | Kind::Update => kept.new,
    };
    let rows = rows.iter().map(|b| crate::query::conform(b, &shown)).collect::<Result<Vec<_>>>()?;
    answer(app, &items, concat_batches(&shown, &rows)?).await
}

/// The columns the answer will have, worked out without writing (a Postgres Describe).
pub async fn columns(app: &App, r: &Returning) -> Result<SchemaRef> {
    let stmt = parsed(r)?;
    let (_, shown) = target(app, r, &stmt).await?;
    Ok(answer(app, &r.items(), RecordBatch::new_empty(shown)).await?.schema())
}

fn parsed(r: &Returning) -> Result<Stmt> {
    match crate::write::parse(&r.write) {
        Some(Stmt::Invalid(why)) => bail!(why),
        Some(stmt) => Ok(stmt),
        None => bail!("{} … RETURNING: not a write Pondra takes", r.word()),
    }
}

/// The write, run where its rows are worked out on this node: as it always runs, or, for a change the leader would work out
/// (`write::changes`), as a transaction of its own (`txn::alone`).
async fn write(app: &App, r: &Returning, table: &str, stmt: Stmt, job: Option<String>, files: bool) -> Result<Value> {
    let leaders = match &stmt {
        Stmt::InsertInto(t, ..) => crate::write::changes(&app.lake, &Stmt::Insert(t.clone(), String::new())).await?, // (its columns don't decide it)
        s => crate::write::changes(&app.lake, s).await?,
    };
    if leaders.is_some() && !crate::txn::open() {
        if let Some(j) = &job {
            if crate::txn::committed(&app.lake, j, table).await? {
                return Ok(serde_json::json!({"duplicate": true})); // (asked first: run again, a row it wrote may refuse it, a key it holds)
            }
        }
        return crate::txn::alone(app, &r.write, job, files).await;
    }
    crate::write::on_node_as(app, stmt, job, files).await
}

/// The table a write changes, as the lake names it, and its columns as RETURNING sees them: as SQL names them, with no
/// system columns and no `_deleted`, as `*` reads them. What it can't answer for is refused by name.
async fn target(app: &App, r: &Returning, stmt: &Stmt) -> Result<(String, SchemaRef)> {
    let table = stmt.table();
    let refused = |what: &str| crate::codes::coded("0A000", format!("{} … RETURNING into {table}, {what}, isn't supported yet: read its rows with a SELECT after the write", r.word()));
    if crate::temp::own(stmt) {
        return Err(refused("a temporary table"));
    }
    let (other, name) = crate::ddl::resolve(&app.lake, &table).await?;
    if other.is_some() {
        return Err(refused("another database's table"));
    }
    let Some(meta) = app.lake.cat.get::<TableMeta>(&crate::store::table_key(&name)).await? else {
        return Err(crate::codes::coded("42P01", format!("no table {table} (RETURNING answers for this database's own tables)")));
    };
    let columns: Vec<(String, String)> = meta.logical_columns().into_iter().filter(|(c, _)| c != "_deleted").collect();
    Ok((name, crate::answer::schema(&columns)?))
}

/// RETURNING reads the rows it wrote, so a user granted some tables needs SELECT on this one, as Postgres asks.
fn may_read(table: &str) -> Result<()> {
    match crate::auth::current() {
        Some(p) if !p.may_read_all(table) => Err(crate::codes::coded("42501", format!("permission denied: RETURNING reads the rows written to {table}, which needs SELECT on it (GRANT SELECT ON {table} TO {})", p.name))),
        _ => Ok(()),
    }
}

/// `rows` as the items ask. Columns and `*` are picked from them; anything else is a query over them, run here and never
/// remembered (`query::SENT`, beside the tables the request sent).
async fn answer(app: &App, items: &[SelectItem], rows: RecordBatch) -> Result<RecordBatch> {
    if let Some(picks) = picks(items, &rows.schema()) {
        let schema = Schema::new(picks.iter().map(|(i, n)| rows.schema().field(*i).clone().with_name(n)).collect::<Vec<_>>());
        return Ok(RecordBatch::try_new(Arc::new(schema), picks.iter().map(|(i, _)| rows.column(*i).clone()).collect())?);
    }
    let sql = format!("SELECT {} FROM {ROWS}", items.iter().map(|i| i.to_string()).collect::<Vec<_>>().join(", "));
    let schema = rows.schema();
    let mut sent = crate::query::SENT.try_with(|t| t.as_ref().clone()).unwrap_or_default();
    sent.push((ROWS.to_string(), vec![rows]));
    let batches = crate::query::SENT.scope(Arc::new(sent), app.query(&sql, None)).await
        .map_err(|e| anyhow::anyhow!("RETURNING: {}", e.to_string().replace(&format!("{ROWS}."), "")))?; // (its errors name the table's columns, not ours)
    let Some(first) = batches.first() else { bail!("RETURNING over {schema:?}: no columns") };
    Ok(concat_batches(&first.schema(), &batches)?)
}

/// The columns `items` pick, by place in `shown`, and each one's name in the answer: when every item is a `*` or a column of
/// the table, perhaps renamed. None: an item is anything else (a query works it out).
fn picks(items: &[SelectItem], shown: &Schema) -> Option<Vec<(usize, String)>> {
    let mut out = vec![];
    for item in items {
        let (e, alias) = match item {
            SelectItem::Wildcard(o) if o.to_string().trim().is_empty() => {
                out.extend(shown.fields().iter().enumerate().map(|(i, f)| (i, f.name().clone())));
                continue;
            }
            SelectItem::UnnamedExpr(e) => (e, None),
            SelectItem::ExprWithAlias { expr, alias } => (expr, Some(crate::text::ident(alias))),
            _ => return None,
        };
        let column = match e {
            Expr::Identifier(i) => crate::text::ident(i),
            Expr::CompoundIdentifier(p) if p.len() == 2 && p[0].value == ROWS => crate::text::ident(&p[1]),
            _ => return None,
        };
        let i = shown.index_of(&column).ok()?; // (not a column: `current_date`, say, which a query answers or refuses)
        out.push((i, alias.unwrap_or(column)));
    }
    Some(out)
}
