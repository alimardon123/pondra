//! Indexes as objects (round 34). `CREATE INDEX` is kept in the catalog (`ix/{schema.name}`) and
//! is an object like any other: listed in `pondra.objects`, `pg_indexes` and `pg_class`, shown by
//! `SHOW CREATE INDEX`, described by `COMMENT ON INDEX`, renamed and dropped. Nothing is built for
//! one yet, and its notice says so: every file already carries its columns' ranges, which a filter
//! skips files and row groups by, and `CLUSTER BY` orders a table's rows so those ranges are
//! narrow, the work a B-tree does elsewhere. So a schema copied from Postgres, an ORM's migrations
//! or dbt's indexes run as they are. Vector and text indexes (`USING hnsw | ivfflat | bm25`) are
//! refused by name until ADR-051's phase 2.
//!
//! As in Postgres, an index is in its table's schema and shares names with tables, views and
//! sequences; it names its columns by their stored names (a renamed column is followed, ADR-022)
//! and goes with its table, or with a column it names (`follow`, from `ddl::apply`).
use crate::store::{json, table_key, Lake, TableMeta};
use crate::write::Stmt;
use anyhow::{bail, ensure, Context, Result};
use datafusion::sql::sqlparser::ast::{self, visit_expressions, visit_expressions_mut};
use datafusion::sql::sqlparser::{dialect::GenericDialect, parser::Parser};
use serde::{Deserialize, Serialize};
use serde_json::{json as j, Value};
use std::ops::ControlFlow;

pub fn key(name: &str) -> String { format!("ix/{name}") }

const NOTICE: &str = "nothing is built for it: every file keeps its columns' ranges, which filters skip files and row groups by, and CLUSTER BY orders a table's rows to keep them narrow";

/// An index as the catalog keeps it: its expressions over stored column names, quoted.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Index {
    pub table: String,
    pub keys: Vec<Key>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub using: Option<String>, // as written: btree, hash, brin, art…
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub include: Vec<String>, // stored names
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub predicate: Option<String>, // a partial index's WHERE
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub with: Vec<String>, // storage parameters, as written
}

/// One of an index's keys: an expression, then its operator class and order as written.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Key {
    pub expr: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub rest: String,
}

#[derive(Serialize, Deserialize, Clone)]
#[serde(rename_all = "snake_case")]
pub enum Change {
    Create { name: Option<String>, table: String, keys: Vec<Key>, using: Option<String>, include: Vec<String>, predicate: Option<String>, with: Vec<String>, if_not_exists: bool },
    Rename { name: String, to: String, if_exists: bool },
    Drop { names: Vec<String>, if_exists: bool },
}

/// `CREATE INDEX`, `ALTER INDEX … RENAME TO`, `DROP INDEX`, or None for anything else.
pub fn statement(sql: &str) -> Option<Stmt> {
    use std::sync::LazyLock;
    static CREATE: LazyLock<regex::Regex> = LazyLock::new(|| regex::Regex::new(r"(?is)^\s*CREATE\s+(UNIQUE\s+)?INDEX\b").expect("a regex"));
    static ALTER: LazyLock<regex::Regex> = LazyLock::new(|| regex::Regex::new(r#"(?is)^\s*ALTER\s+INDEX\s+(IF\s+EXISTS\s+)?([\w."$-]+)\s+RENAME\s+TO\s+([\w."$-]+)\s*;?\s*$"#).expect("a regex"));
    static ALTERED: LazyLock<regex::Regex> = LazyLock::new(|| regex::Regex::new(r"(?is)^\s*ALTER\s+INDEX\b").expect("a regex"));
    static DROP: LazyLock<regex::Regex> = LazyLock::new(|| regex::Regex::new(r"(?is)^\s*DROP\s+INDEX\b").expect("a regex"));
    let first = crate::write::first_word(sql);
    let ddl = |c: Change| Some(Stmt::Ddl(vec![crate::ddl::Ddl::Index(c)]));
    if CREATE.is_match(first) {
        return Some(match Parser::parse_sql(&GenericDialect {}, sql).map(|mut s| s.pop()) {
            Ok(Some(ast::Statement::CreateIndex(c))) => match create(c) {
                Ok(c) => return ddl(c),
                Err(e) => Stmt::Invalid(format!("{e:#}")),
            },
            Ok(_) => Stmt::Invalid("CREATE INDEX [IF NOT EXISTS] [name] ON table [USING method] (column [, …]) [WHERE condition]".into()),
            Err(e) => Stmt::Invalid(format!("{e}")),
        });
    }
    if let Some(c) = ALTER.captures(first) {
        return ddl(Change::Rename { name: crate::seq::object_of(&c[2]), to: crate::seq::object_of(&c[3]), if_exists: c.get(1).is_some() });
    }
    if ALTERED.is_match(first) {
        return Some(Stmt::Invalid("ALTER INDEX [IF EXISTS] name RENAME TO new_name (an index has nothing else to change: nothing is built for it)".into()));
    }
    if DROP.is_match(first) {
        return Some(match Parser::parse_sql(&GenericDialect {}, sql).map(|mut s| s.pop()) {
            Ok(Some(ast::Statement::Drop { object_type: ast::ObjectType::Index, names, if_exists, .. })) => {
                return ddl(Change::Drop { names: names.iter().map(crate::write::object).collect(), if_exists })
            }
            _ => Stmt::Invalid("DROP INDEX [IF EXISTS] name [, …]".into()),
        });
    }
    None
}

/// What a `CREATE INDEX` says, checked as far as it can be without the table.
fn create(c: ast::CreateIndex) -> Result<Change> {
    ensure!(!c.unique, "CREATE UNIQUE INDEX: a unique index isn't taken yet (a PRIMARY KEY keeps one row a key)");
    ensure!(c.alter_options.is_empty(), "CREATE INDEX … {}: MySQL's options for ALTER TABLE aren't taken", c.alter_options.iter().map(|o| o.to_string()).collect::<Vec<_>>().join(" "));
    let using = c.using.as_ref().or_else(|| c.index_options.iter().find_map(|o| match o {
        ast::IndexOption::Using(u) => Some(u),
        _ => None,
    }));
    let using = using.map(|u| u.to_string().to_lowercase());
    if let Some(u) = using.as_deref().filter(|u| matches!(*u, "hnsw" | "ivfflat" | "bm25" | "fts" | "diskann" | "vector")) {
        bail!("USING {u}: vector and text indexes come with ADR-051's phase 2; until then a nearest-neighbour query scans exactly (ORDER BY array_cosine_distance(…) LIMIT k)");
    }
    ensure!(!c.columns.is_empty(), "CREATE INDEX … ON table (column [, …]): the columns it is on");
    let keys = c.columns.iter().map(|k| {
        let class = k.operator_class.as_ref().map_or(String::new(), |c| format!(" {c}"));
        Key { expr: k.column.expr.to_string(), rest: format!("{class}{}", k.column.options) }
    }).collect();
    Ok(Change::Create {
        name: c.name.as_ref().map(crate::write::object),
        table: crate::write::object(&c.table_name),
        keys,
        using,
        include: c.include.iter().map(crate::write::ident).collect(),
        predicate: c.predicate.as_ref().map(|p| match p {
            ast::Expr::Nested(p) => p.to_string(), // (`WHERE (a IS NOT NULL)` as SHOW CREATE writes it)
            p => p.to_string(),
        }),
        with: c.with.iter().map(|w| w.to_string()).collect(),
        if_not_exists: c.if_not_exists,
    })
}

/// Leader: carry one out.
pub async fn apply(lake: &Lake, c: Change) -> Result<Value> {
    match c {
        Change::Create { name, table, keys, using, include, predicate, with, if_not_exists } => {
            let (other, table) = crate::ddl::resolve(lake, &table).await?;
            ensure!(other.is_none(), "{table} is an attached lake's: make its index from a node of that lake");
            let meta = match lake.cat.get::<TableMeta>(&table_key(&table)).await?.filter(|_| !crate::sys::hidden(&table)) {
                Some(m) => m,
                None if lake.cat.get::<Value>(&crate::ddl::query_key(&table)).await?.is_some() => bail!("\"{table}\" is a view: an index is on a table or a materialized view"),
                None => bail!("relation \"{table}\" does not exist"),
            };
            let stored = |n: &str| meta.stored(n).map(str::to_string).with_context(|| format!("column \"{n}\" does not exist in {table}"));
            let mut kept = vec![];
            for k in &keys {
                kept.push(Key { expr: rewrite(&k.expr, true, &stored)?, rest: k.rest.clone() });
            }
            let include = include.iter().map(|c| stored(c)).collect::<Result<Vec<_>>>()?;
            let predicate = predicate.map(|p| rewrite(&p, true, &stored)).transpose()?;
            let (schema, t) = crate::ddl::split(&table);
            let name = match name {
                Some(n) => match n.rsplit_once('.') {
                    Some((s, n)) => {
                        ensure!(s == schema, "an index is in its table's schema ({schema}), not {s}");
                        crate::ddl::join(schema, n)
                    }
                    None => crate::ddl::join(schema, &n),
                },
                None => {
                    // (Postgres's name: the table's, its columns', `idx`; then idx1, idx2… while taken)
                    let words: Vec<String> = kept.iter().map(|k| plain(&k.expr).map_or("expr".to_string(), |c| meta.name_of(&c).to_string())).collect();
                    let base = crate::ddl::join(schema, &format!("{t}_{}_idx", words.join("_")));
                    let mut i = 0;
                    loop {
                        let n = if i == 0 { base.clone() } else { format!("{base}{i}") };
                        if !taken(lake, &n).await? {
                            break n;
                        }
                        i += 1;
                    }
                }
            };
            crate::ddl::new_name(lake, &name).await?;
            if lake.cat.get::<Value>(&key(&name)).await?.is_some() && if_not_exists {
                return Ok(j!({"index": name, "exists": true, "notice": format!("relation \"{name}\" already exists, skipping")}));
            }
            ensure!(!taken(lake, &name).await?, "relation \"{name}\" already exists");
            let index = Index { table: table.clone(), keys: kept, using, include, predicate, with };
            lake.cat.commit(vec![(key(&name), json(&index))], &[]).await?;
            Ok(j!({"index": name, "on": table, "notice": format!("index {name} on {table} is kept as an object; {NOTICE}")}))
        }
        Change::Rename { name, to, if_exists } => {
            let Some((name, index)) = find(lake, &name).await? else {
                ensure!(if_exists, "index \"{name}\" does not exist");
                return Ok(j!({"index": name, "exists": false}));
            };
            let to = match to.split('.').collect::<Vec<_>>()[..] {
                [t] => crate::ddl::join(crate::ddl::split(&name).0, t),
                _ => to,
            };
            ensure!(crate::ddl::split(&to).0 == crate::ddl::split(&name).0, "an index stays in its table's schema");
            let to = crate::ddl::new_name(lake, &to).await?;
            ensure!(!taken(lake, &to).await?, "relation \"{to}\" already exists");
            lake.cat.commit(vec![(key(&to), json(&index))], &[key(&name)]).await?;
            Ok(j!({"index": name, "renamed": to}))
        }
        Change::Drop { names, if_exists } => {
            let mut gone = vec![];
            for n in names {
                match find(lake, &n).await? {
                    Some((n, _)) => gone.push(n),
                    None => ensure!(if_exists, "index \"{n}\" does not exist"),
                }
            }
            lake.cat.commit(vec![], &gone.iter().map(|n| key(n)).collect::<Vec<_>>()).await?;
            Ok(j!({"index": gone.first(), "dropped": gone}))
        }
    }
}

/// Does a relation of the lake (a table, a view, a sequence, an index) have this name?
async fn taken(lake: &Lake, name: &str) -> Result<bool> {
    for k in [table_key(name), crate::ddl::query_key(name), crate::seq::key(name), key(name)] {
        if lake.cat.get::<Value>(&k).await?.is_some() {
            return Ok(true);
        }
    }
    Ok(false)
}

/// The index a name resolves to in this lake.
async fn find(lake: &Lake, name: &str) -> Result<Option<(String, Index)>> {
    let (other, local) = crate::ddl::resolve(lake, name).await?;
    ensure!(other.is_none(), "{name} is an attached lake's index: change it from a node of that lake");
    Ok(lake.cat.get::<Index>(&key(&local)).await?.map(|i| (local, i)))
}

/// An expression with each column it names put another way (`to`: SQL's name to the stored one,
/// kept quoted, or back to SQL's as it is written), or the first column `to` refuses.
fn rewrite(text: &str, quoted: bool, to: &dyn Fn(&str) -> Result<String>) -> Result<String> {
    let mut e = Parser::new(&GenericDialect {}).try_with_sql(text)?.parse_expr()?;
    let mut failed = None;
    let _ = visit_expressions_mut(&mut e, |x| {
        let named = match x {
            ast::Expr::Identifier(i) => Some(crate::write::ident(i)),
            ast::Expr::CompoundIdentifier(p) => p.last().map(crate::write::ident), // (t.c is c: an index is on one table)
            _ => None,
        };
        if let Some(n) = named {
            match to(&n) {
                Ok(s) => *x = ast::Expr::Identifier(if quoted { ast::Ident::with_quote('"', s) } else { ast::Ident::new(s) }),
                Err(err) => {
                    failed = Some(err);
                    return ControlFlow::Break(());
                }
            }
        }
        ControlFlow::Continue(())
    });
    match failed {
        Some(e) => Err(e),
        None => Ok(e.to_string()),
    }
}

/// The columns an expression names, as kept (stored names).
fn columns(text: &str) -> Vec<String> {
    let mut out = vec![];
    if let Ok(e) = Parser::new(&GenericDialect {}).try_with_sql(text).and_then(|mut p| p.parse_expr()) {
        let _ = visit_expressions(&e, |x| {
            if let ast::Expr::Identifier(i) = x {
                out.push(i.value.clone());
            }
            ControlFlow::<()>::Continue(())
        });
    }
    out
}

/// The column a key is, if it is one and not an expression.
fn plain(text: &str) -> Option<String> {
    match Parser::new(&GenericDialect {}).try_with_sql(text).and_then(|mut p| p.parse_expr()) {
        Ok(ast::Expr::Identifier(i)) => Some(i.value),
        _ => None,
    }
}

/// The statement that makes it, in SQL's names (`index` and `table` as they are to be written).
pub fn create_sql(index: &str, table: &str, i: &Index, meta: Option<&TableMeta>) -> String {
    let sql = |c: &str| crate::objects::ident(meta.map_or(c, |m| m.name_of(c)));
    let back = |t: &str| rewrite(t, false, &|c| Ok(sql(c))).unwrap_or_else(|_| t.to_string());
    let keys: Vec<String> = i.keys.iter().map(|k| format!("{}{}", shown(&back(&k.expr)), k.rest)).collect();
    let mut out = format!("CREATE INDEX {index} ON {table}");
    if let Some(u) = &i.using {
        out += &format!(" USING {u}");
    }
    out += &format!(" ({})", keys.join(", "));
    if !i.include.is_empty() {
        out += &format!(" INCLUDE ({})", i.include.iter().map(|c| sql(c)).collect::<Vec<_>>().join(", "));
    }
    if !i.with.is_empty() {
        out += &format!(" WITH ({})", i.with.join(", "));
    }
    if let Some(p) = &i.predicate {
        out += &format!(" WHERE ({})", back(p));
    }
    out
}

/// A key as written back: an expression that isn't a column or a call goes in parentheses, as
/// Postgres writes it (`lower(email)`, `(a + b)`).
fn shown(expr: &str) -> String {
    let bare = Parser::new(&GenericDialect {}).try_with_sql(expr).and_then(|mut p| p.parse_expr()).is_ok_and(|e| matches!(e, ast::Expr::Identifier(_) | ast::Expr::Function(_)));
    if bare { expr.to_string() } else { format!("({expr})") }
}

/// After a drop or a rename (`out`: what it answered): indexes go with their table, and with a
/// column they name; a renamed table's follow it.
pub async fn follow(lake: &Lake, out: &Value) -> Result<()> {
    let all = lake.cat.scan::<Index>("ix/", "ix0").await?;
    if all.is_empty() {
        return Ok(());
    }
    let renamed = match (out["table"].as_str(), out["renamed"].as_str()) {
        (Some(from), Some(to)) => Some((from, to)),
        _ => None,
    };
    let (mut put, mut gone) = (vec![], vec![]);
    for (k, mut i) in all {
        if let Some((_, to)) = renamed.filter(|(from, _)| *from == i.table) {
            i.table = to.to_string();
            put.push((k.clone(), json(&i)));
        }
        let meta = lake.cat.get::<TableMeta>(&table_key(&i.table)).await?;
        let live = |c: &String| meta.as_ref().is_some_and(|m| m.live().any(|(s, _, _)| s == c));
        let named: Vec<String> = i.keys.iter().flat_map(|k| columns(&k.expr)).chain(i.predicate.iter().flat_map(|p| columns(p))).chain(i.include.iter().cloned()).collect();
        if meta.is_none() || !named.iter().all(live) {
            put.retain(|(p, _)| *p != k);
            gone.push(k);
        }
    }
    if !put.is_empty() || !gone.is_empty() {
        lake.cat.commit(put, &gone).await?;
    }
    Ok(())
}
