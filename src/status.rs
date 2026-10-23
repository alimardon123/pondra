//! What differs between a project's files and a database (ADR-060 §7). `pondra status` lists what an
//! apply would change, what was changed in the database outside an apply, and what no project declares;
//! `pondra capture` writes the chosen ones into the project's files. Both read the plan an apply would
//! make and the database as it is, so they agree with what an apply does. Who changed an object, and
//! when, comes from `pondra.history`; when that can't be read (history is off, or not made yet) the
//! status still answers, without who and when.

use crate::apply::{self, Current, Declared, Project, Shape};
use crate::objects::{ident, name_sql as quoted};
use crate::routines::{Outcome, Who};
use crate::server::App;
use crate::store::json;
use anyhow::{ensure, Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::json as j;
use std::collections::{BTreeMap, HashMap};

/// One line of `status`: an object an apply would change (`apply`), one changed outside an apply
/// (`changed`), or one in the database that no project declares (`untracked`).
#[derive(Serialize)]
pub struct Line {
    state: &'static str,
    mark: String,
    kind: String,
    name: String,
    what: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    file: Option<String>, // the project file that declares it
    #[serde(skip_serializing_if = "Option::is_none")]
    to: Option<String>, // a renamed table's new name
    #[serde(skip_serializing_if = "Option::is_none")]
    who: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    at: Option<String>,
    #[serde(skip)]
    cols: Columns, // a table's columns changed outside an apply
    #[serde(skip)]
    statement: Option<String>, // the newest statement that named it, as its author wrote it
}

impl Line {
    fn new(p: &Project, state: &'static str, mark: &str, key: &str, what: String) -> Line {
        let (kind, name) = apply::split_key(key);
        let file = p.objects.iter().find(|d| d.key() == key).map(|d| d.file.clone());
        Line { state, mark: mark.into(), kind: kind.into(), name: name.into(), what, file, to: None, who: None, at: None, cols: Columns::default(), statement: None }
    }

    fn key(&self) -> String { format!("{} {}", self.kind, self.name) }
}

/// The columns of a table that were renamed, dropped or added outside an apply, by their SQL names.
#[derive(Default)]
struct Columns {
    renamed: Vec<(String, String)>, // (was, now)
    dropped: Vec<String>,
    added: Vec<String>,
}

impl Columns {
    fn is_empty(&self) -> bool { self.renamed.is_empty() && self.dropped.is_empty() && self.added.is_empty() }

    /// What a line says about them, each change in turn.
    fn text(&self) -> String {
        let parts: Vec<String> = self.renamed.iter().map(|(a, b)| format!("renamed {a} → {b}"))
            .chain(self.dropped.iter().map(|c| format!("dropped {c}")))
            .chain(self.added.iter().map(|c| format!("added {c}")))
            .collect();
        format!("columns changed outside an apply: {}", parts.join("; "))
    }
}

/// The column changes from `was` (the shape an apply recorded) to `now`. Columns are matched by their stored
/// names, which a rename keeps: the same stored name with another SQL name was renamed, one gone was dropped.
fn changes(was: &Shape, now: &Shape) -> Columns {
    let mut c = Columns::default();
    for (stored, old) in &was.columns {
        match now.columns.iter().find(|(s, _)| s == stored) {
            Some((_, new)) if new != old => c.renamed.push((old.clone(), new.clone())),
            Some(_) => {}
            None => c.dropped.push(old.clone()),
        }
    }
    c.added = now.columns.iter().filter(|(s, _)| !was.columns.iter().any(|(w, _)| w == s)).map(|(_, n)| n.clone()).collect();
    c
}

/// What `status` and `capture` read: the project, the lines, what the plan refuses, the database as it is.
struct View {
    p: Project,
    after: u64,
    now: BTreeMap<String, Current>,
    lines: Vec<Line>,
    refused: Vec<String>,
}

/// The name of a key (`table sales.orders` is `sales.orders`).
fn name_of(key: &str) -> &str { apply::split_key(key).1 }

/// The lines for the project against the database, with who changed what and when.
async fn view(app: &App, ask: &apply::Ask, who: Who) -> Result<View> {
    let p = apply::project(ask)?;
    let branch = app.lake.cat.get_raw(crate::branch::BASES).await?.is_some();
    let plan = apply::plan(&app.lake, &p, ask.prune, branch).await?;
    let all = apply::records(&app.lake).await?;
    let last = apply::last_finished(&all);
    let now = apply::current(&app.lake).await?;
    let mine = last.get(&p.name).copied();
    let objects = mine.map(|r| r.objects.clone()).unwrap_or_default();
    let shapes = mine.map(|r| r.shapes.clone()).unwrap_or_default();
    let declared: Vec<String> = p.objects.iter().map(Declared::key).collect();
    // Untracked: in a schema the project's objects are in, declared by no project now, and in no project's last apply.
    let listed = |k: &str| last.values().any(|r| r.objects.contains_key(k));
    let untracked: Vec<String> = now.keys().filter(|k| p.holds(k.as_str()) && !declared.contains(*k) && !listed(k.as_str())).cloned().collect();
    // A table gone from its name, with a new table in its folder: renamed outside an apply (a folder is kept
    // through a rename, so no other table has it).
    let mut renamed: Vec<(String, String)> = vec![];
    for a in objects.keys().filter(|a| apply::split_key(a.as_str()).0 == "table" && !now.contains_key(a.as_str())) {
        let Some(folder) = shapes.get(a).map(|s| s.folder.clone()) else { continue };
        let found: Vec<&String> = untracked.iter().filter(|b| apply::split_key(b.as_str()).0 == "table" && now.get(b.as_str()).and_then(|c| c.shape.as_ref()).is_some_and(|s| s.folder == folder)).collect();
        if let [b] = found.as_slice() {
            renamed.push((a.clone(), b.to_string()));
        }
    }
    let from: Vec<String> = renamed.iter().map(|(a, _)| a.clone()).collect();
    let mut lines: Vec<Line> = vec![];
    let mut refused = plan.refused.clone();
    for s in &plan.steps {
        let key = format!("{} {}", s.kind, s.name);
        if s.drift && from.contains(&key) {
            continue; // (the rename's own line says it)
        }
        lines.push(Line::new(&p, if s.drift { "changed" } else { "apply" }, s.mark, &key, s.what.clone()));
    }
    for (a, b) in &renamed {
        let cols = match (shapes.get(a), now.get(b).and_then(|c| c.shape.as_ref())) {
            (Some(was), Some(here)) => changes(was, here),
            _ => Columns::default(),
        };
        let mut what = format!("renamed to {} outside an apply", name_of(b));
        if !cols.is_empty() {
            what = format!("{what}; {}", cols.text());
        }
        let mut line = Line::new(&p, "changed", "↷", a, what);
        line.to = Some(name_of(b).to_string());
        line.cols = cols;
        lines.push(line);
    }
    // A table under its own name whose columns changed outside an apply: the plan refuses it, and this line says
    // why instead. Once the files say what the table has (captured), the plan neither refuses nor changes it.
    let mut own: Vec<String> = vec![];
    for (k, was) in &shapes {
        let Some(here) = now.get(k).and_then(|c| c.shape.as_ref()) else { continue };
        let cols = changes(was, here);
        let planned = plan.steps.iter().any(|s| format!("{} {}", s.kind, s.name) == *k) || refused.iter().any(|r| r.starts_with(&format!("table {} (", name_of(k))));
        if !declared.contains(k) || cols.is_empty() || !planned {
            continue;
        }
        let mut line = Line::new(&p, "changed", "~", k, cols.text());
        line.cols = cols;
        lines.push(line);
        own.push(k.clone());
    }
    refused.retain(|r| !own.iter().any(|k| r.starts_with(&format!("table {} (", name_of(k)))));
    for k in untracked.iter().filter(|k| !renamed.iter().any(|(_, b)| b == *k)) {
        lines.push(Line::new(&p, "untracked", "?", k, "made outside an apply: in no project's files".into()));
    }
    // Who and when: the DDL rows since the project's last apply, oldest first, so the newest that names an object wins.
    let rows = history(app, mine.map_or(0, |r| r.started_ms), who).await;
    let all_names: Vec<String> = lines.iter().filter(|l| l.state != "apply").flat_map(|l| std::iter::once(l.name.clone()).chain(l.to.clone())).collect();
    for row in &rows {
        let named = crate::protect::named(&row.statement);
        for l in lines.iter_mut().filter(|l| l.state != "apply") {
            let targets: Vec<String> = std::iter::once(l.name.clone()).chain(l.to.clone()).collect();
            if targets.iter().any(|t| hits(&named, t, &all_names)) {
                l.who = Some(row.who.clone()).filter(|w| !w.is_empty()); // (an open node's statements have no user)
                l.at = Some(clock(&row.at));
                l.statement = Some(row.statement.clone());
            }
        }
    }
    Ok(View { p, after: plan.after, now, lines, refused })
}

/// Whether a statement names `full`: by its full name, or unqualified by its short name when only one line
/// is that short name (`ALTER TABLE memos` in a project's schema means `sales.memos`, when nothing else is).
fn hits(named: &[String], full: &str, all: &[String]) -> bool {
    let short = crate::text::split(full).1;
    named.iter().any(|n| n == full || (n == short && !n.contains('.') && all.iter().filter(|x| crate::text::split(x.as_str()).1 == short).count() == 1))
}

#[derive(Deserialize)]
struct Row {
    at: String,
    who: String,
    statement: String,
}

/// The DDL that succeeded since `since`, oldest first, from `pondra.history`, as the caller may read it. Any
/// failure is no rows: a status is never refused for its history.
async fn history(app: &App, since: u64, who: Who) -> Vec<Row> {
    let sql = format!("SELECT CAST(at AS VARCHAR) AS at, \"user\" AS who, statement FROM pondra.history WHERE class = 'ddl' AND outcome = 'ok' AND at >= to_timestamp_millis({since}) ORDER BY at");
    let Ok(Outcome::Rows(rows)) = crate::routines::script(app, &sql, &HashMap::new(), &HashMap::new(), who, None).await else { return vec![] };
    apply::rows_json(&rows, usize::MAX).unwrap_or_default().into_iter().filter_map(|v| serde_json::from_value(v).ok()).collect()
}

/// A history time to the second: `2026-10-11 04:12:03`.
fn clock(at: &str) -> String { at.replace('T', " ").chars().take(19).collect() }

/// `POST /status` (`CALL status`): what differs between the project's files and the database, as lines.
pub async fn status(app: &App, ask: &apply::Ask, who: Who) -> Result<serde_json::Value> {
    let v = view(app, ask, who).await?;
    Ok(j!({"status": {"after": v.after, "lines": v.lines, "refused": v.refused}, "project": v.p.name, "ok": v.refused.is_empty()}))
}

/// `POST /capture` (`pondra capture NAME…`): the files that write the chosen changed and untracked objects
/// into the project, as the database has them (the author's words kept where they still mean it). Nothing is
/// written here: the answer holds each file's text (null: removed), and the command line writes them. A renamed
/// table or column gets a migration, recorded as run in this database, where it already is.
pub async fn capture(app: &App, ask: &apply::Ask, who: Who) -> Result<serde_json::Value> {
    let applier = apply::applier();
    apply::authorize(app, who, applier).await?;
    let v = view(app, ask, who).await?;
    // A name chooses the lines it is the name of (a renamed table's old or new name).
    let named = |l: &Line, n: &str| l.name == n || l.to.as_deref() == Some(n);
    let changes: Vec<&Line> = v.lines.iter().filter(|l| l.state != "apply").collect();
    for n in &ask.names {
        ensure!(changes.iter().any(|l| named(l, n.as_str())), "{n}: no object changed outside an apply or untracked under that name: pondra status lists what can be captured");
    }
    let mut work = ask.files.clone();
    let mut out: BTreeMap<String, Option<String>> = BTreeMap::new();
    let (mut refused, mut captured, mut migrations) = (vec![], vec![], vec![]);
    let mut done: Vec<String> = vec![];
    for l in changes {
        if !ask.names.is_empty() && !ask.names.iter().any(|n| named(l, n.as_str())) || done.contains(&l.key()) {
            continue;
        }
        done.push(l.key());
        if let Some(why) = refusal(&v.p, l) {
            refused.push(why);
            continue;
        }
        if let Some(file) = write_line(app, &v, l, &mut work, &mut out)? {
            migrations.push(file);
        }
        captured.push(l);
    }
    if !migrations.is_empty() {
        let record = async {
            for file in &migrations {
                let entry = j!({"migration": apply::migration_key(&v.p.name, file)});
                crate::write::on_node_as(app, crate::write::Stmt::Ddl(vec![crate::ddl::Ddl::Apply { claim: false, after: 0, record: json(&entry) }]), None, false).await?;
            }
            Ok::<(), anyhow::Error>(())
        };
        // (as an apply's statements run: an APPLY holder below admin records them as an admin, under its own name)
        match apply::admin_for(who, applier) {
            Some(me) => crate::auth::WHO.scope(me, record).await?,
            None => record.await?,
        }
    }
    Ok(j!({"files": out, "captured": captured, "migrations": migrations, "refused": refused, "ok": refused.is_empty()}))
}

/// Why a line can't be written: its declared statement uses one of the environment's values, which the database
/// has bound, so the database can't give the statement back as the project means it.
fn refusal(p: &Project, l: &Line) -> Option<String> {
    let d = p.objects.iter().find(|d| d.key() == l.key())?;
    let used = p.values.keys().find(|k| uses(&d.sql, k.as_str()))?;
    Some(format!("{}: its statement uses ${used}, a value: change it in {} by hand", l.name, d.file))
}

/// Whether a statement uses the value `key` (`$key`, as a whole word: `$big` is not `$bigger`).
fn uses(sql: &str, key: &str) -> bool {
    let pat = format!("${key}");
    sql.match_indices(&pat).any(|(i, _)| !sql[i + pat.len()..].starts_with(|c: char| c.is_alphanumeric() || c == '_'))
}

/// Writes one line into the working copy of the files. Returns the migration's file name when it wrote one.
fn write_line(app: &App, v: &View, l: &Line, work: &mut BTreeMap<String, String>, out: &mut BTreeMap<String, Option<String>>) -> Result<Option<String>> {
    let key = l.key();
    if l.to.is_none() && !v.now.contains_key(&key) {
        // Dropped outside an apply: its declaration goes.
        let file = l.file.clone().context("a dropped object is declared in a file")?;
        edit(work, out, &file, &key, None)?;
        return Ok(None);
    }
    let now_key = match &l.to {
        Some(to) => format!("{} {to}", l.kind),
        None => key.clone(),
    };
    let cur = v.now.get(&now_key).with_context(|| format!("{now_key} isn't in the database"))?;
    if l.state == "untracked" {
        append(work, out, &apply::path_of(&l.kind, &l.name), &format!("{};\n", statement(l, cur)));
        return Ok(None);
    }
    let file = l.file.clone().context("a changed object is declared in a file")?;
    let needs = l.to.is_some() || !l.cols.renamed.is_empty() || !l.cols.dropped.is_empty();
    if l.to.is_none() && l.cols.is_empty() {
        edit(work, out, &file, &key, Some(statement(l, cur)))?;
        return Ok(None);
    }
    // A table renamed, or with columns changed: the table as it is now, and where a name or column went, a migration.
    edit(work, out, &file, &key, Some(cur.sql.clone()))?;
    if !needs {
        return Ok(None);
    }
    let to = l.to.clone().unwrap_or_else(|| l.name.clone());
    let names: Vec<&str> = work.keys().filter_map(|p| p.strip_prefix("migrations/")).collect();
    let name = format!("{}-rename-{}.sql", number(&names), crate::text::split(&to).1);
    let text = migration(app, v, l, &to);
    work.insert(format!("migrations/{name}"), text.clone());
    out.insert(format!("migrations/{name}"), Some(text));
    Ok(Some(name))
}

/// The statement an object is written as: the newest statement that named it, when that is a CREATE of the same
/// object meaning what the database has (the author's words, comments and layout kept); else the database's.
fn statement(l: &Line, cur: &Current) -> String {
    let fp = |s: &str| apply::fingerprint(s, &HashMap::new(), &BTreeMap::new());
    let key = l.key();
    match l.statement.as_deref() {
        Some(s) if matches!(apply::head(s), Ok(Some((k, n))) if apply::key(k, &n) == key) && fp(s) == fp(&cur.sql) => s.trim().trim_end_matches(';').trim_end().to_string(),
        _ => cur.sql.clone(),
    }
}

/// `text` in place of the statement `key` names in `file`, or the statement taken out (`None`, with its `;`).
/// A file left with nothing but whitespace and comments is removed: its entry is null.
fn edit(work: &mut BTreeMap<String, String>, out: &mut BTreeMap<String, Option<String>>, file: &str, key: &str, text: Option<String>) -> Result<()> {
    let old = work.get(file).with_context(|| format!("{file} isn't in the project's files"))?;
    let new = replaced(old, key, text.as_deref());
    if crate::routines::split(&new).is_empty() {
        work.remove(file);
        out.insert(file.into(), None);
    } else {
        work.insert(file.into(), new.clone());
        out.insert(file.into(), Some(new));
    }
    Ok(())
}

/// `text` added to the end of `file`, after a blank line (a file of its own when there is none).
fn append(work: &mut BTreeMap<String, String>, out: &mut BTreeMap<String, Option<String>>, file: &str, text: &str) {
    let mut all = work.get(file).map(|t| t.trim_end().to_string()).unwrap_or_default();
    if !all.is_empty() {
        all.push_str("\n\n");
    }
    all.push_str(text);
    work.insert(file.into(), all.clone());
    out.insert(file.into(), Some(all));
}

/// The text of `sql` with the code of the statement `key` names replaced by `text`, or taken out with its `;`.
/// The statements are found in the order `routines::split` reads them; the others, and every comment, stay as
/// they were. A statement not found leaves the text as it is.
fn replaced(sql: &str, key: &str, text: Option<&str>) -> String {
    let mut from = 0; // (where the next statement starts: after the last `;`)
    for piece in crate::routines::split(sql) {
        let Some(found) = sql[from..].find(&piece) else { continue };
        let at = from + found;
        let end = at + piece.len();
        let semi = if sql[end..].starts_with(';') { end + 1 } else { end };
        from = semi;
        let code_at = at + code_start(&piece);
        let code = sql[code_at..end].trim_end();
        if !matches!(apply::head(code), Ok(Some((k, n))) if apply::key(k, &n) == key) {
            continue;
        }
        let stop = code_at + code.len();
        return match text {
            Some(t) => format!("{}{t}{}", &sql[..code_at], &sql[stop..]),
            None => format!("{}{}", &sql[..code_at], &sql[semi..]),
        };
    }
    sql.to_string()
}

/// Where a statement's code begins: past its whitespace and the comments in front of it (`--` lines, `/* */`).
fn code_start(piece: &str) -> usize {
    let mut i = 0;
    loop {
        let rest = &piece[i..];
        i += rest.len() - rest.trim_start().len();
        let rest = &piece[i..];
        if rest.starts_with("--") {
            i += rest.find('\n').map_or(rest.len(), |n| n + 1);
        } else if rest.starts_with("/*") {
            i += rest.find("*/").map_or(rest.len(), |n| n + 2);
        } else {
            return i;
        }
    }
}

/// The migration a renamed table, or columns renamed or dropped, need for the databases that still have the old
/// names: the views and tasks that read the table are dropped first (the apply makes them again after the
/// migrations), then the rename, then each column's rename and drop, on the new name.
fn migration(app: &App, v: &View, l: &Line, to: &str) -> String {
    let from = l.name.as_str();
    let by = match (&l.who, &l.at) {
        (Some(w), Some(a)) => format!(", by {w} ({a})"),
        _ => String::new(),
    };
    let head = match l.to {
        Some(_) => format!("{from} renamed outside an apply"),
        None => format!("{from} columns changed outside an apply"),
    };
    let mut readers: Vec<&Declared> = vec![];
    let mut names = vec![from.to_string()];
    loop {
        let found: Vec<&Declared> = v.p.objects.iter().filter(|d| matches!(d.kind.word(), "view" | "materialized view" | "task") && !readers.iter().any(|r| r.key() == d.key()) && names.iter().any(|n| apply::mentions(&d.sql, n))).collect();
        if found.is_empty() {
            break;
        }
        names.extend(found.iter().map(|d| d.name.clone()));
        readers.extend(found);
    }
    readers.reverse(); // (what reads another goes first)
    let mut sql = format!("-- pondra capture: {head} in {}{by}\n", crate::ddl::lake_name(&app.lake));
    for d in &readers {
        sql.push_str(&format!("DROP {} IF EXISTS {};\n", d.kind.word().to_uppercase(), quoted(&d.name)));
    }
    if from != to {
        let rename = match crate::text::split(from).0 == crate::text::split(to).0 {
            true => crate::text::split(to).1, // (in the same schema: its short name)
            false => to,
        };
        sql.push_str(&format!("ALTER TABLE {} RENAME TO {rename};\n", quoted(from)));
    }
    for (old, new) in &l.cols.renamed {
        sql.push_str(&format!("ALTER TABLE {} RENAME COLUMN {} TO {};\n", quoted(to), ident(old), ident(new)));
    }
    for c in &l.cols.dropped {
        sql.push_str(&format!("ALTER TABLE {} DROP COLUMN {};\n", quoted(to), ident(c)));
    }
    sql
}

/// The number the next migration takes: one more than the largest any name starts with, as wide as that name,
/// and at least three digits (`003`).
fn number(names: &[&str]) -> String {
    let (n, w) = names.iter().filter_map(|f| {
        let digits: String = f.chars().take_while(char::is_ascii_digit).collect();
        Some((digits.parse::<u64>().ok()?, digits.len()))
    }).max().unwrap_or((0, 3));
    format!("{:0w$}", n + 1, w = w.max(3))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_migration_takes_the_next_number() {
        assert_eq!(number(&["001-a.sql", "002-rename.sql"]), "003");
        assert_eq!(number(&["0009-x.sql"]), "0010");
        assert_eq!(number(&[]), "001");
    }

    #[test]
    fn a_statement_starts_after_its_comments() {
        let piece = "-- the orders\n  /* a */ CREATE TABLE t (a INT)";
        assert_eq!(&piece[code_start(piece)..], "CREATE TABLE t (a INT)");
    }

    #[test]
    fn column_changes_are_read_by_stored_name() {
        let shape = |cols: &[(&str, &str)]| Shape { folder: "notes".into(), columns: cols.iter().map(|(s, n)| (s.to_string(), n.to_string())).collect() };
        let was = shape(&[("id", "id"), ("body", "body"), ("amount", "amount"), ("note", "note")]);
        let now = shape(&[("id", "id"), ("body", "text"), ("amount", "total"), ("added", "added")]);
        let c = changes(&was, &now);
        assert_eq!(c.renamed, [("body".to_string(), "text".to_string()), ("amount".to_string(), "total".to_string())]);
        assert_eq!(c.dropped, ["note"]);
        assert_eq!(c.added, ["added"]);
        assert_eq!(c.text(), "columns changed outside an apply: renamed body → text; renamed amount → total; dropped note; added added");
        assert!(changes(&was, &was).is_empty());
    }

    #[test]
    fn a_statement_is_replaced_or_taken_out_and_the_others_stay() {
        let sql = "-- first\nCREATE TABLE a.x (id INT);\n-- second\nCREATE VIEW a.v AS SELECT 1;\nCREATE VIEW a.w AS SELECT 2;\n";
        assert_eq!(replaced(sql, "view a.v", Some("CREATE VIEW a.v AS SELECT 3")), "-- first\nCREATE TABLE a.x (id INT);\n-- second\nCREATE VIEW a.v AS SELECT 3;\nCREATE VIEW a.w AS SELECT 2;\n");
        assert_eq!(replaced(sql, "view a.v", None), "-- first\nCREATE TABLE a.x (id INT);\n-- second\n\nCREATE VIEW a.w AS SELECT 2;\n");
        assert!(crate::routines::split("-- kept\n\n").is_empty()); // (a file of comments is empty: removed)
    }

    #[test]
    fn a_value_is_named_as_a_whole_word() {
        assert!(uses("SELECT * FROM t WHERE x > $big", "big"));
        assert!(!uses("SELECT * FROM t WHERE x > $bigger", "big"));
    }

    #[test]
    fn a_row_names_an_object_by_its_name_or_its_short_one_when_alone() {
        let all = ["sales.notes".to_string(), "sales.memos".to_string()];
        assert!(hits(&["sales.notes".to_string()], "sales.notes", &all));
        assert!(hits(&["memos".to_string()], "sales.memos", &all));
        assert!(!hits(&["sales.notes".to_string()], "sales.memos", &all));
    }

    #[test]
    fn a_time_is_read_to_the_second() {
        assert_eq!(clock("2026-10-11T04:12:03.123456"), "2026-10-11 04:12:03");
    }
}
