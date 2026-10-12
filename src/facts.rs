//! A view's key facts (ADR-065 §2): PRIMARY KEY, UNIQUE and FOREIGN KEY, always NOT ENFORCED. A
//! view's rows are its sources' rows, so nothing could check them; the facts say what the tools that
//! read them may assume (`pg_constraint`, `information_schema`, `pondra.parts`, a measure's join,
//! `SHOW CREATE`), and nothing answers from them. A stored view keeps its facts in its own entry
//! (`StoredView::facts`), a materialized view in its table's (`TableMeta::constraints`), so a rename,
//! a drop and a clone take them along.
use crate::codes::coded;
use crate::constraints::{informational, Change, Constraint, Kind};
use crate::ddl::{query_key, Ddl, StoredView};
use crate::store::{json, table_key, Lake, TableMeta};
use crate::write::{first_word, object, Stmt};
use anyhow::{anyhow, bail, ensure, Context, Result};
use datafusion::common::DFSchema;
use datafusion::sql::sqlparser::{ast::{AlterTableOperation, Statement, TableConstraint}, dialect::GenericDialect, parser::Parser};
use regex::Regex;
use serde_json::{json as j, Value};
use std::collections::{BTreeMap, HashSet};
use std::sync::LazyLock;

const WANT: &str = "ALTER VIEW … ADD | DROP CONSTRAINT: a view takes PRIMARY KEY, UNIQUE and FOREIGN KEY, said NOT ENFORCED";

/// A name as SQL writes it: parts bare or in double quotes, joined by dots (one group).
const NAME: &str = r#"((?:"(?:[^"]|"")*"|\w+)(?:\s*\.\s*(?:"(?:[^"]|"")*"|\w+))*)"#;
static CREATE: LazyLock<Regex> = LazyLock::new(|| Regex::new(&format!(r#"(?is)^\s*CREATE\s+(OR\s+REPLACE\s+)?(TEMP\s+|TEMPORARY\s+)?(MATERIALIZED\s+)?VIEW\s+(IF\s+NOT\s+EXISTS\s+)?{NAME}\s*\("#)).expect("a regex"));
static ALTER: LazyLock<Regex> = LazyLock::new(|| Regex::new(&format!(r#"(?is)^\s*ALTER\s+(MATERIALIZED\s+)?VIEW\s+(IF\s+EXISTS\s+)?{NAME}\s+((?:ADD|DROP)\b.*)$"#)).expect("a regex"));
// A list item that is a fact: its kind is group 2 (a CHECK is a fact of a stored view only).
static ITEM: LazyLock<Regex> = LazyLock::new(|| Regex::new(r#"(?is)^\s*(CONSTRAINT\s+(?:"(?:[^"]|"")*"|\w+)\s+)?(PRIMARY\s+KEY|UNIQUE|FOREIGN\s+KEY|CHECK)\b"#).expect("a regex"));
static ADD_IF_NOT: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"(?is)^ADD\s+CONSTRAINT\s+IF\s+NOT\s+EXISTS\s+").expect("a regex"));

/// The statements that make or change a view's facts, or None for any other (which costs one
/// regex on its first words): `CREATE … VIEW v (items)` with fact items in its list, and
/// `ALTER [MATERIALIZED] VIEW v ADD | DROP CONSTRAINT`, which the parser doesn't take.
pub fn parse(sql: &str) -> Option<Stmt> {
    let s = first_word(sql);
    let head: String = s.chars().take(64).collect::<String>().to_ascii_uppercase();
    if !(head.starts_with("ALTER") || head.starts_with("CREATE")) || !head.contains("VIEW") {
        return None;
    }
    match head.starts_with("ALTER") {
        true => alter(s),
        false => create(s),
    }
}

/// `ALTER [MATERIALIZED] VIEW v ADD [CONSTRAINT [IF NOT EXISTS] n] … | DROP CONSTRAINT [IF EXISTS] n`.
fn alter(s: &str) -> Option<Stmt> {
    let c = ALTER.captures(s)?;
    let if_exists = c.get(2).is_some(); // (the view: a view that isn't there is then nothing)
    let rest = c.get(4)?.as_str();
    let if_not_exists = ADD_IF_NOT.is_match(rest);
    let rest = ADD_IF_NOT.replace(rest, "ADD CONSTRAINT "); // (the parser doesn't take IF NOT EXISTS here)
    let Some((view, ops)) = alter_table(&format!("{} {rest}", &c[3])) else { return Some(Stmt::Invalid(WANT.into())) };
    Some(match &ops[..] {
        [AlterTableOperation::AddConstraint { constraint, .. }] => match fact(constraint, &view) {
            Ok(f) => Stmt::Ddl(vec![Ddl::ViewFacts { view, if_exists, change: Change::Add(f, if_not_exists) }]),
            Err(e) => Stmt::Invalid(format!("{e:#}")),
        },
        [AlterTableOperation::DropConstraint { if_exists: constraint_if_exists, name, .. }] => {
            Stmt::Ddl(vec![Ddl::ViewFacts { view, if_exists, change: Change::Drop { name: crate::write::ident(name), if_exists: *constraint_if_exists } }])
        }
        _ => Stmt::Invalid(WANT.into()),
    })
}

/// `CREATE [OR REPLACE] [MATERIALIZED] VIEW [IF NOT EXISTS] v (items) …`: the fact items are taken
/// out of the list, the statement is read as it is without them, and the facts go into its definition.
fn create(s: &str) -> Option<Stmt> {
    let c = CREATE.captures(s)?;
    let open = c.get(0)?.end(); // (just past the list's `(`)
    let (items, close) = list(s, open)?;
    let materialized = c.get(3).is_some();
    // A materialized view's CHECK items are its expectations (`views::constraints`), not facts.
    let is_fact = |item: &str| ITEM.captures(item).is_some_and(|k| !k[2].eq_ignore_ascii_case("CHECK") || !materialized);
    let (facts, rest): (Vec<&str>, Vec<&str>) = items.into_iter().partition(|i| is_fact(*i));
    if facts.is_empty() {
        return None;
    }
    if c.get(2).is_some() {
        return Some(Stmt::Invalid("a temporary view keeps no facts: make it a view (CREATE VIEW …)".into()));
    }
    let keys = match facts.iter().map(|f| item(&c[5], f)).collect::<Result<Vec<_>>>() {
        Ok(keys) => keys,
        Err(e) => return Some(Stmt::Invalid(format!("{e:#}"))),
    };
    let (head, tail) = (&s[..open - 1], &s[close + 1..]);
    let text = match rest.is_empty() {
        true => format!("{head}{tail}"),
        false => format!("{head}({}){tail}", rest.join(", ")),
    };
    Some(kept_in(crate::write::parse(&text), &keys))
}

/// The items of the list whose `(` ends just before `from`, split at its top-level commas (a comma
/// inside brackets or quotes belongs to its item), and the byte of its `)`. None if it never closes.
fn list(s: &str, from: usize) -> Option<(Vec<&str>, usize)> {
    let (mut items, mut depth, mut quote, mut start) = (vec![], 0i32, None::<char>, from);
    for (i, ch) in s[from..].char_indices() {
        let at = from + i;
        match (quote, ch) {
            (Some(q), c) if c == q => quote = None,
            (Some(_), _) => {}
            (None, '"' | '\'') => quote = Some(ch),
            (None, '(') => depth += 1,
            (None, ',') if depth == 0 => {
                items.push(s[start..at].trim());
                start = at + 1;
            }
            (None, ')') if depth == 0 => {
                items.push(s[start..at].trim());
                return Some((items.into_iter().filter(|i| !i.is_empty()).collect(), at));
            }
            (None, ')') => depth -= 1,
            _ => {}
        }
    }
    None
}

/// One fact item of view `name` as `ALTER TABLE name ADD item` would make it.
fn item(name: &str, text: &str) -> Result<Constraint> {
    let not = || anyhow!("{text}: a view's key is PRIMARY KEY, UNIQUE or FOREIGN KEY, said NOT ENFORCED");
    let (view, ops) = alter_table(&format!("{name} ADD {text}")).ok_or_else(not)?;
    match &ops[..] {
        [AlterTableOperation::AddConstraint { constraint, .. }] => fact(constraint, &view),
        _ => Err(not()),
    }
}

/// `text` read as `ALTER TABLE text` (the parser takes a table's ADD and DROP CONSTRAINT): the
/// name it gives and its operations.
fn alter_table(text: &str) -> Option<(String, Vec<AlterTableOperation>)> {
    match Parser::parse_sql(&GenericDialect {}, &format!("ALTER TABLE {text}")).ok()?.pop()? {
        Statement::AlterTable(a) => Some((object(&a.name), a.operations)),
        _ => None,
    }
}

/// A table constraint as a view keeps it: NOT ENFORCED, or refused by name with the one to write.
fn fact(k: &TableConstraint, view: &str) -> Result<Constraint> {
    if let TableConstraint::Check(_) = k {
        bail!("a view's CHECK: a materialized view's checks are its expectations (CREATE MATERIALIZED VIEW v (CONSTRAINT c CHECK (…)) AS …)");
    }
    let informal = match k {
        TableConstraint::PrimaryKey(p) => informational(&p.characteristics),
        TableConstraint::Unique(u) => informational(&u.characteristics),
        _ => true, // (a FOREIGN KEY: `constraints::of` says what it lacks)
    };
    ensure!(informal, "{k} on a view: its rows are its sources', so nothing can check them: write {k} NOT ENFORCED");
    let short = view.rsplit('.').next().unwrap_or(view); // (default names: `{view}_pkey`, …)
    crate::constraints::of(k, short)?.ok_or_else(|| anyhow!("{k}: a view takes PRIMARY KEY, UNIQUE and FOREIGN KEY, said NOT ENFORCED"))
}

/// The statement the rest of the list made, with the facts in its view's definition.
fn kept_in(stmt: Option<Stmt>, facts: &[Constraint]) -> Stmt {
    match stmt {
        Some(Stmt::Ddl(mut d)) => match d.len() == 1 && keep(&mut d[0], facts) {
            true => Stmt::Ddl(d),
            false => Stmt::Invalid(format!("a view's facts go in its own statement: {WANT_CREATE}")),
        },
        Some(Stmt::Invalid(e)) => Stmt::Invalid(e),
        _ => Stmt::Invalid(format!("a view's facts go in its own statement: {WANT_CREATE}")),
    }
}
const WANT_CREATE: &str = "CREATE VIEW v (PRIMARY KEY (id) NOT ENFORCED) AS SELECT …";

/// Puts the facts in a `CREATE VIEW` or `CREATE MATERIALIZED VIEW` (through its IF NOT EXISTS and OR
/// REPLACE). A materialized view's go in its options as JSON, for `taken`.
fn keep(d: &mut Ddl, facts: &[Constraint]) -> bool {
    match d {
        Ddl::CreateView { facts: kept, .. } => {
            *kept = Some(facts.to_vec());
            true
        }
        Ddl::CreateMaterialized { options, .. } => {
            options.insert("facts".into(), serde_json::to_string(facts).unwrap_or_default());
            true
        }
        Ddl::Unless { then, .. } | Ddl::Replacing { then, .. } => keep(then, facts),
        _ => false,
    }
}

/// The facts a view's definition is given as it is kept: each column named is one of the view's
/// `columns` (SQL's names), at most one PRIMARY KEY, no two of one name. Never enforced, whatever the
/// caller asks: a view's rows aren't checked.
pub fn fit(view: &str, facts: Vec<Constraint>, columns: &[String]) -> Result<Vec<Constraint>> {
    let facts: Vec<Constraint> = facts.into_iter().map(|c| Constraint { enforced: false, ..c }).collect();
    for c in &facts {
        for n in &c.columns {
            ensure!(columns.contains(n), "column \"{n}\" of relation \"{view}\" does not exist");
        }
    }
    ensure!(facts.iter().filter(|c| c.kind == Kind::PrimaryKey).count() <= 1, "multiple primary keys for view \"{view}\" are not allowed");
    let mut names = HashSet::new();
    for c in &facts {
        ensure!(names.insert(c.name.as_str()), "constraint \"{}\" for relation \"{view}\" already exists", c.name);
    }
    Ok(facts)
}

/// A query's columns as a view shows them: its schema's names, but a measure is no column.
fn shown_columns(sql: &str, names: impl Iterator<Item = String>) -> Vec<String> {
    let measures: Vec<String> = crate::measures::listed(sql).into_iter().map(|(n, _)| n).collect();
    names.filter(|n| !measures.contains(n)).collect()
}

/// The facts a made (or replaced) stored view keeps. `given`: the list its CREATE wrote, checked
/// against its columns. Without one, a replaced view keeps its old facts while their columns are
/// there (else the change is refused, and nothing is made); a new view has none.
pub async fn settled(lake: &Lake, name: &str, sql: &str, schema: &DFSchema, given: Option<Vec<Constraint>>, replace: bool) -> Result<Vec<Constraint>> {
    let columns = shown_columns(sql, schema.fields().iter().map(|f| f.name().clone()));
    if let Some(list) = given {
        return fit(name, list, &columns);
    }
    let old = match replace {
        true => lake.cat.get::<StoredView>(&query_key(name)).await?.map(|v| v.facts).unwrap_or_default(),
        false => vec![],
    };
    for c in &old {
        if let Some(gone) = c.columns.iter().find(|n| !columns.contains(n)) {
            bail!("constraint \"{}\" of view \"{name}\" names column \"{gone}\", which the new query doesn't have: ALTER VIEW {name} DROP CONSTRAINT {} first", c.name, c.name);
        }
    }
    Ok(old)
}

/// The facts a change leaves, or None when it changes nothing: a constraint of that name is there
/// (IF NOT EXISTS said so), or a GROUP BY view's key is asked for (its groups are that key already).
/// `columns` and `key` are SQL's names; `key` is empty for a stored view. `expect` is a materialized
/// view's expectations' names: a fact may not share one (SHOW CREATE would list both).
fn made(view: &str, facts: &[Constraint], change: &Change, columns: &[String], key: &[String], expect: &[String]) -> Result<Option<Vec<Constraint>>> {
    match change {
        Change::Add(c, if_not_exists) => {
            if expect.contains(&c.name) {
                bail!("constraint \"{}\" for relation \"{view}\" already exists (an expectation of its own)", c.name);
            }
            if facts.iter().any(|f| f.name == c.name) {
                ensure!(*if_not_exists, "constraint \"{}\" for relation \"{view}\" already exists", c.name);
                return Ok(None);
            }
            if c.kind == Kind::PrimaryKey && !key.is_empty() {
                ensure!(c.columns == key, "{view} is keyed by its GROUP BY ({}): its PRIMARY KEY is that key, not another", key.join(", "));
                return Ok(None);
            }
            let mut all = facts.to_vec();
            all.push(c.clone());
            Ok(Some(fit(view, all, columns)?))
        }
        Change::Drop { name, if_exists } => {
            if !facts.iter().any(|f| f.name == *name) {
                ensure!(*if_exists, "constraint \"{name}\" of relation \"{view}\" does not exist");
                return Ok(None);
            }
            Ok(Some(facts.iter().filter(|f| f.name != *name).cloned().collect()))
        }
        Change::AddCheck(name, _) => bail!("constraint \"{name}\" of relation \"{view}\": a view's CHECK is its expectations (CREATE MATERIALIZED VIEW)"),
    }
}

/// Is `view` a materialized view (its rows kept, or its windows still open: `once`)?
async fn is_materialized(lake: &Lake, view: &str) -> Result<bool> {
    Ok(lake.cat.get::<Value>(&crate::views::view_key(view)).await?.is_some() || lake.cat.get::<Value>(&crate::views::view_key(&crate::once::open(view))).await?.is_some())
}

/// The names of a materialized view's expectations (its `v/` entry), or none for a view that has none.
async fn expectation_names(lake: &Lake, view: &str) -> Result<Vec<String>> {
    let entry = match lake.cat.get::<crate::views::View>(&crate::views::view_key(view)).await? {
        Some(v) => Some(v),
        None => lake.cat.get::<crate::views::View>(&crate::views::view_key(&crate::once::open(view))).await?,
    };
    Ok(entry.map(|v| v.expect.into_iter().map(|e| e.name).collect()).unwrap_or_default())
}

/// Is `view` a stored view or a materialized view?
async fn is_view(lake: &Lake, view: &str) -> Result<bool> {
    Ok(lake.cat.get::<StoredView>(&query_key(view)).await?.is_some() || is_materialized(lake, view).await?)
}

/// `ALTER TABLE t …` given a view's name: refused, saying what to write (SQLSTATE 42809).
pub async fn not_a_view(lake: &Lake, table: &str) -> Result<()> {
    if is_view(lake, table).await? {
        return Err(coded("42809", format!("{table} is a view: ALTER VIEW {table} ADD CONSTRAINT … NOT ENFORCED")));
    }
    Ok(())
}

/// Leader, under the lake's lock: one `ALTER [MATERIALIZED] VIEW [IF EXISTS] v ADD | DROP CONSTRAINT`
/// of a view's facts. With IF EXISTS a view that isn't there is nothing to change.
pub async fn apply(lake: &Lake, view: &str, if_exists: bool, change: Change) -> Result<Value> {
    if if_exists && !is_view(lake, view).await? && lake.cat.get::<TableMeta>(&table_key(view)).await?.is_none() {
        return Ok(j!({"view": view, "exists": false, "notice": format!("relation \"{view}\" does not exist, skipping")}));
    }
    let name = match &change {
        Change::Add(c, _) => c.name.clone(),
        Change::Drop { name, .. } | Change::AddCheck(name, _) => name.clone(),
    };
    let dropped = matches!(change, Change::Drop { .. });
    let changed = if let Some(v) = lake.cat.get::<StoredView>(&query_key(view)).await? {
        let columns = stored_columns(lake, view, &v).await?;
        match made(view, &v.facts, &change, &columns, &[], &[])? {
            Some(facts) => {
                lake.cat.commit(vec![(query_key(view), json(&StoredView { facts, ..v }))], &[]).await?;
                true
            }
            None => false,
        }
    } else if is_materialized(lake, view).await? {
        let mut k = kept(lake, view).await?;
        let expect = expectation_names(lake, view).await?;
        match made(view, &k.facts, &change, &k.columns, &k.key, &expect)? {
            Some(facts) => {
                keep_in_table(&mut k.meta, facts);
                lake.cat.commit(vec![(table_key(view), json(&k.meta))], &[]).await?;
                true
            }
            None => false,
        }
    } else if lake.cat.get::<TableMeta>(&table_key(view)).await?.is_some() {
        bail!("{view} is a table: ALTER TABLE {view} ADD CONSTRAINT … NOT ENFORCED");
    } else {
        bail!("relation \"{view}\" does not exist");
    };
    Ok(match (changed, dropped) {
        (true, true) => j!({"view": view, "constraint": name, "dropped": true}),
        (true, false) => j!({"view": view, "constraint": name}),
        (false, true) => j!({"view": view, "constraint": name, "exists": false}),
        (false, false) => j!({"view": view, "constraint": name, "exists": true, "notice": format!("relation \"{name}\" already exists, skipping")}),
    })
}

/// A stored view's columns, as its query names them now (planned, not run).
async fn stored_columns(lake: &Lake, view: &str, v: &StoredView) -> Result<Vec<String>> {
    let sql = format!("SELECT * FROM {} LIMIT 0", crate::write::sql_name(view));
    let ctx = crate::query::session(lake, &sql, "").await?;
    let df = crate::query::sql(&ctx, &sql).await?;
    Ok(shown_columns(&v.sql, df.schema().fields().iter().map(|f| f.name().clone())))
}

/// A materialized view's table, with its facts and columns as SQL names (`described`).
struct Kept {
    meta: TableMeta,
    columns: Vec<String>,
    key: Vec<String>,
    facts: Vec<Constraint>,
}

async fn kept(lake: &Lake, view: &str) -> Result<Kept> {
    let meta: TableMeta = lake.cat.get(&table_key(view)).await?.ok_or_else(|| anyhow!("relation \"{view}\" does not exist"))?;
    let d = meta.described();
    Ok(Kept {
        columns: d.columns.iter().map(|(n, _)| n.clone()).filter(|n| !d.marker(n)).collect(),
        key: d.key.clone(),
        facts: d.constraints.clone(),
        meta,
    })
}

/// Writes a materialized view's facts into its table's entry, under the stored names of their columns.
fn keep_in_table(meta: &mut TableMeta, facts: Vec<Constraint>) {
    let stored: Vec<Constraint> = facts
        .into_iter()
        .map(|c| {
            let columns = c.columns.iter().map(|n| meta.stored(n).unwrap_or(n).to_string()).collect();
            Constraint { columns, ..c }
        })
        .collect();
    meta.constraints = stored;
}

/// The facts a materialized view was made with, kept in its table's entry in one commit. The view is
/// made first: the caller drops it when one of them is refused.
pub async fn materialized(lake: &Lake, name: &str, facts: Vec<Constraint>) -> Result<()> {
    if facts.is_empty() {
        return Ok(());
    }
    let mut k = kept(lake, name).await?;
    let expect = expectation_names(lake, name).await?; // (the view is made before this runs: its entry is there)
    for c in facts {
        if let Some(now) = made(name, &k.facts, &Change::Add(c, false), &k.columns, &k.key, &expect)? {
            k.facts = now;
        }
    }
    keep_in_table(&mut k.meta, k.facts);
    lake.cat.commit(vec![(table_key(name), json(&k.meta))], &[]).await?;
    Ok(())
}

/// The facts a materialized view's options carry (`parse` puts them there), taken out of the options
/// so the view's own options never see them.
pub fn taken(options: &mut BTreeMap<String, String>) -> Result<Vec<Constraint>> {
    let Some(text) = options.remove("facts") else { return Ok(vec![]) };
    serde_json::from_str(&text).context("a view's facts")
}

/// The list SHOW CREATE and export write after a view's name: `""` for none, else its facts as
/// `CONSTRAINT n …` items, which `parse` reads back.
pub fn in_list(facts: &[Constraint]) -> String {
    if facts.is_empty() {
        return String::new();
    }
    let items: Vec<String> = facts.iter().map(|c| c.sql(&crate::objects::ident)).collect();
    format!(" (\n  {}\n)", items.join(",\n  "))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The one statement `sql` makes, as `write::parse` reads it.
    fn ddl(sql: &str) -> Ddl {
        match crate::write::parse(sql) {
            Some(Stmt::Ddl(mut d)) if d.len() == 1 => d.remove(0),
            _ => panic!("{sql}: not one DDL statement"),
        }
    }

    /// The message a statement is refused with.
    fn refused(sql: &str) -> String {
        match crate::write::parse(sql) {
            Some(Stmt::Invalid(e)) => e,
            _ => panic!("{sql}: not refused"),
        }
    }

    fn key_fact(name: &str, kind: Kind, columns: &[&str]) -> Constraint {
        Constraint { name: name.into(), kind, columns: columns.iter().map(|c| c.to_string()).collect(), references: None, enforced: false }
    }

    #[test]
    fn a_view_keeps_its_facts_and_its_query() {
        let d = ddl("CREATE VIEW v (PRIMARY KEY (id) NOT ENFORCED, CONSTRAINT v_region_fkey FOREIGN KEY (region) REFERENCES regions (code) NOT ENFORCED) AS SELECT id, region FROM t");
        let Ddl::CreateView { name, sql, facts, .. } = d else { panic!("a view") };
        assert_eq!(name, "v");
        assert!(sql.ends_with("FROM t"), "{sql}");
        let facts = facts.expect("the facts");
        assert_eq!(facts.iter().map(|c| c.name.as_str()).collect::<Vec<_>>(), ["v_pkey", "v_region_fkey"]);
        assert_eq!(facts[0].kind, Kind::PrimaryKey);
        assert_eq!(facts[1].references, Some(("regions".into(), vec!["code".into()])));
        assert!(facts.iter().all(|c| !c.enforced));
    }

    #[test]
    fn or_replace_and_if_not_exists_wrap_the_facts() {
        let Ddl::CreateView { replace, facts, .. } = ddl("CREATE OR REPLACE VIEW v (UNIQUE (name) NOT ENFORCED) AS SELECT name FROM t") else { panic!("a view") };
        assert!(replace);
        assert_eq!(facts.map(|f| f.len()), Some(1));
        let Ddl::Unless { then, .. } = ddl("CREATE VIEW IF NOT EXISTS v (UNIQUE (name) NOT ENFORCED) AS SELECT name FROM t") else { panic!("IF NOT EXISTS") };
        assert!(matches!(*then, Ddl::CreateView { facts: Some(ref f), .. } if f.len() == 1));
    }

    #[test]
    fn a_materialized_view_keeps_its_facts_beside_its_expectations() {
        // (not OR REPLACE: `write::parse` attaches the expectations only to a bare CREATE, a pre-existing gap)
        let sql = "CREATE MATERIALIZED VIEW m (PRIMARY KEY (region) NOT ENFORCED, CONSTRAINT m_big CHECK (total > 0) ON VIOLATION DROP ROW) AS SELECT region, sum(x) AS total FROM t GROUP BY region";
        let Ddl::CreateMaterialized { mut options, .. } = ddl(sql) else { panic!("a materialized view") };
        assert!(options.contains_key("expect"));
        assert_eq!(taken(&mut options).expect("the facts").len(), 1);
        assert!(!options.contains_key("facts"));
    }

    #[test]
    fn or_replace_wraps_a_materialized_views_facts() {
        let Ddl::Replacing { then, .. } = ddl("CREATE OR REPLACE MATERIALIZED VIEW m (UNIQUE (total) NOT ENFORCED) AS SELECT sum(x) AS total FROM t") else { panic!("OR REPLACE") };
        assert!(matches!(*then, Ddl::CreateMaterialized { ref options, .. } if options.contains_key("facts")));
    }

    #[test]
    fn alter_adds_and_drops_one_fact() {
        let Ddl::ViewFacts { view, if_exists, change } = ddl("ALTER MATERIALIZED VIEW m ADD CONSTRAINT IF NOT EXISTS u UNIQUE (total) NOT ENFORCED") else { panic!("ALTER") };
        assert_eq!(view, "m");
        assert!(matches!(change, Change::Add(ref c, true) if c.name == "u" && c.kind == Kind::Unique));
        assert!(!if_exists);
        let Ddl::ViewFacts { change, if_exists, .. } = ddl("ALTER VIEW v DROP CONSTRAINT IF EXISTS u") else { panic!("ALTER") };
        assert!(!if_exists);
        assert!(matches!(change, Change::Drop { ref name, if_exists: true } if name == "u"));
    }

    #[test]
    fn refusals_say_what_to_write() {
        assert!(refused("CREATE VIEW v (PRIMARY KEY (id)) AS SELECT id FROM t").contains("write PRIMARY KEY (id) NOT ENFORCED"));
        assert!(refused("CREATE VIEW v (UNIQUE (id)) AS SELECT id FROM t").contains("write UNIQUE (id) NOT ENFORCED"));
        assert!(refused("CREATE VIEW v (CONSTRAINT c CHECK (id > 0)) AS SELECT id FROM t").contains("a view's CHECK"));
        assert!(refused("CREATE TEMP VIEW v (PRIMARY KEY (id) NOT ENFORCED) AS SELECT id FROM t").contains("temporary view"));
        assert!(refused("ALTER VIEW v ADD CONSTRAINT c CHECK (id > 0)").contains("a view's CHECK"));
        assert!(refused("ALTER VIEW v ADD COLUMN x INT").contains("PRIMARY KEY"));
        assert!(matches!(ddl("ALTER VIEW IF EXISTS v DROP CONSTRAINT c"), Ddl::ViewFacts { if_exists: true, .. }));
    }

    #[test]
    fn other_statements_are_left_alone() {
        for sql in ["CREATE VIEW v AS SELECT 1", "ALTER VIEW v RENAME TO w", "ALTER MATERIALIZED VIEW v DETACH", "SELECT 1", "CREATE TABLE views (id INT)"] {
            assert!(parse(sql).is_none(), "{sql}");
        }
    }

    #[test]
    fn fit_refuses_what_cannot_hold() {
        let cols: Vec<String> = ["id", "name"].iter().map(|c| c.to_string()).collect();
        let refusal = |facts: Vec<Constraint>| fit("v", facts, &cols).err().map(|e| e.to_string()).unwrap_or_default();
        assert!(refusal(vec![key_fact("v_x_key", Kind::Unique, &["x"])]).contains("column \"x\" of relation \"v\" does not exist"));
        assert!(refusal(vec![key_fact("a", Kind::PrimaryKey, &["id"]), key_fact("b", Kind::PrimaryKey, &["name"])]).contains("multiple primary keys for view \"v\""));
        assert!(refusal(vec![key_fact("a", Kind::Unique, &["id"]), key_fact("a", Kind::Unique, &["name"])]).contains("constraint \"a\" for relation \"v\" already exists"));
        assert!(fit("v", vec![key_fact("v_pkey", Kind::PrimaryKey, &["id"])], &cols).is_ok());
    }

    #[test]
    fn in_list_writes_the_facts_after_the_name() {
        assert_eq!(in_list(&[]), "");
        assert_eq!(in_list(&[key_fact("v_pkey", Kind::PrimaryKey, &["id"])]), " (\n  CONSTRAINT v_pkey PRIMARY KEY (id) NOT ENFORCED\n)");
    }
}
