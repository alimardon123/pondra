//! The core's statements in the registry's statements section (step 8, PR 8: l6/step8-pr8-plan.md): each family the
//! core reads by its words, with one reader a family, found by its leading words as an extension's statement is. Their
//! run and right are the stand-ins `Run::Stmt` and `Right::FromStmt` (the `write::Stmt` read carries itself out, and
//! `rights::asks` decides its right) until each family has its own. A family generic reads every form of is read from
//! sqlparser's tree instead (PR 8c), and the rest of `write::parse`'s readers join in 8b-8d.
use crate::ddl::Ddl;
use crate::statements::{Read, Right, Run, Statement, Statements, CORE};
use crate::words::{Words, W};
use crate::write::Stmt;
use anyhow::{bail, Result};

/// The core's entries, as an extension adds its own.
pub static CORE_STATEMENTS: crate::registry::BuiltIn = crate::registry::BuiltIn {
    name: CORE,
    requires: &[],
    register: |r| {
        let s = r.section_mut::<Statements>();
        ENTRIES.iter().for_each(|e| s.add(e));
    },
};

const fn entry(words: &'static [&'static str], read: fn(&mut Words) -> Result<Stmt>, about: &'static str) -> Statement {
    Statement { words, read: Read::Core(read), run: Run::Stmt, right: Right::FromStmt, needs: &[], about, extension: CORE }
}

/// An entry of a family read from sqlparser's tree (`Read::Tree`).
const fn tree(words: &'static [&'static str], read: fn(&datafusion::sql::sqlparser::ast::Statement) -> Result<Stmt>, about: &'static str) -> Statement {
    Statement { words, read: Read::Tree(read), run: Run::Stmt, right: Right::FromStmt, needs: &[], about, extension: CORE }
}

/// Each family's words. A form whose telling word comes after a name (`CREATE TABLE c CLONE t`) is an entry with a name
/// slot beside its family, which sqlparser's tree reads (rows 35-59 of `write::parse`); a family no other reader takes is
/// one entry (`ALTER DATABASE`).
static ENTRIES: &[Statement] = &[
    entry(&["CREATE", "OR", "REPLACE", "SCHEMA"], no_replace, "refused: replacing a schema would drop everything in it"),
    entry(&["CREATE", "OR", "REPLACE", "DATABASE"], no_replace, "refused: replacing a database would drop everything in it"),
    entry(&["CREATE", "OR", "REPLACE", "USER"], no_replace, "refused: replacing a user would drop the rights given to it"),
    entry(&["CREATE", "OR", "REPLACE", "ROLE"], no_replace, "refused: replacing a role would drop the rights given to it"),
    entry(&["ALTER", "VIEW", "*", "RENAME"], rename_view, "renames a view (dbt's ALTER VIEW v RENAME TO w)"),
    entry(&["ALTER", "VIEW", "IF", "EXISTS", "*", "RENAME"], rename_view, "renames a view"),
    entry(&["ALTER", "MATERIALIZED", "VIEW", "*", "RENAME"], rename_view, "renames a materialized view"),
    entry(&["ALTER", "MATERIALIZED", "VIEW", "IF", "EXISTS", "*", "RENAME"], rename_view, "renames a materialized view"),
    entry(&["ALTER", "MATERIALIZED", "VIEW", "*", "DETACH"], detach, "a materialized view's rows stop following, and stay as a table of its name"),
    entry(&["CREATE", "TABLE", "*", "CLONE"], clone_table, "a table of another's files as they are now, copying none (ADR-043)"),
    entry(&["CREATE", "TABLE", "*", "SHALLOW", "CLONE"], clone_table, "a table of another's files as they are now, copying none"),
    entry(&["CREATE", "TABLE", "IF", "NOT", "EXISTS", "*", "CLONE"], clone_table, "a clone of a table, unless one of its name is there"),
    entry(&["CREATE", "TABLE", "IF", "NOT", "EXISTS", "*", "SHALLOW", "CLONE"], clone_table, "a clone of a table, unless one of its name is there"),
    entry(&["CREATE", "DATABASE", "*", "CLONE"], create_database, "a branch of a database, copying none of its files (ADR-047)"),
    entry(&["CREATE", "DATABASE", "*", "LOCATION"], create_database, "a database in a folder of its own, or a branch there"),
    entry(&["CREATE", "DATABASE", "IF", "NOT", "EXISTS", "*", "CLONE"], create_database, "a branch, unless a database of its name is there"),
    entry(&["CREATE", "DATABASE", "IF", "NOT", "EXISTS", "*", "LOCATION"], create_database, "a database in a folder, unless one of its name is there"),
    entry(&["ALTER", "DATABASE"], alter_database, "a branch refreshed from its base, a lake in memory moved to a folder, a database's protection"),
    entry(&["BACKUP", "DATABASE"], backup, "a database's backup, a lake of its own (ADR-070)"),
    entry(&["RESTORE", "DATABASE"], restore, "a database brought back from a backup (ADR-070)"),
    entry(&["EXPORT", "DATABASE"], portable, "refused by name: BACKUP DATABASE and COPY are the ways"),
    entry(&["IMPORT", "DATABASE"], portable, "refused by name: RESTORE DATABASE and COPY are the ways"),
    entry(&["CREATE", "USER"], crate::users::read, "a user, who signs in (ADR-035)"),
    entry(&["CREATE", "ROLE"], crate::users::read, "a role: a user who can't sign in, whose grants its members have"),
    entry(&["ALTER", "USER"], crate::users::read, "a user's password, superuser, sign-in and limits"),
    entry(&["ALTER", "ROLE"], crate::users::read, "a role's superuser, sign-in and limits"),
    entry(&["DROP", "USER"], crate::users::read, "drops a user"),
    entry(&["DROP", "ROLE"], crate::users::read, "drops a role"),
    entry(&["CREATE", "TOKEN"], crate::users::read, "a token a user signs in with"),
    entry(&["ALTER", "TOKEN"], crate::users::read, "a token, read as DROP TOKEN is"),
    entry(&["DROP", "TOKEN"], crate::users::read, "drops a token"),
    entry(&["CREATE", "SEQUENCE"], crate::seq::read, "a sequence: numbers in order, nextval('s') (Postgres's)"),
    entry(&["CREATE", "OR", "REPLACE", "SEQUENCE"], crate::seq::read, "a sequence, in place of one of its name"),
    entry(&["CREATE", "TEMP", "SEQUENCE"], crate::seq::read, "refused: a sequence is the lake's"),
    entry(&["CREATE", "TEMPORARY", "SEQUENCE"], crate::seq::read, "refused: a sequence is the lake's"),
    entry(&["CREATE", "OR", "REPLACE", "TEMP", "SEQUENCE"], crate::seq::read, "refused: a sequence is the lake's"),
    entry(&["CREATE", "OR", "REPLACE", "TEMPORARY", "SEQUENCE"], crate::seq::read, "refused: a sequence is the lake's"),
    entry(&["ALTER", "SEQUENCE"], crate::seq::read, "a sequence's options, its next value (RESTART) or its name"),
    entry(&["DROP", "SEQUENCE"], crate::seq::read, "drops sequences"),
    entry(&["COMMENT", "ON"], comment, "a comment on an object (COMMENT ON TABLE t IS 'text'), or none (IS NULL)"),
    entry(&["COMMENT", "IF", "EXISTS", "ON"], comment, "a comment on an object, where it is there"),
    entry(&["CREATE", "OR", "ALTER", "*"], or_alter, "a table made or altered to the columns given, a view made anew"),
    tree(&["CREATE", "INDEX"], crate::index::created, "an index, kept as it is written (nothing is built for it)"),
    tree(&["CREATE", "UNIQUE", "INDEX"], crate::index::created, "a UNIQUE constraint on a table's columns, written as an index"),
    entry(&["ALTER", "INDEX"], crate::index::read, "renames an index"),
    entry(&["DROP", "INDEX"], crate::index::read, "drops indexes"),
    entry(&["CREATE", "TYPE"], crate::types::read, "an enum type (CREATE TYPE mood AS ENUM ('sad', 'ok'))"),
    entry(&["CREATE", "OR", "REPLACE", "TYPE"], crate::types::read, "refused: a type's columns keep their labels"),
    entry(&["ALTER", "TYPE"], crate::types::read, "an enum's new label (ADD VALUE) or its name"),
    entry(&["DROP", "TYPE"], crate::types::read, "drops types"),
    entry(&["CREATE", "SECRET"], secret, "a secret: what reading a place takes (CREATE SECRET s (TYPE s3, KEY_ID '…', SECRET '…'))"),
    entry(&["CREATE", "OR", "REPLACE", "SECRET"], secret, "a secret, in place of one of its name"),
    entry(&["CREATE", "PERSISTENT", "SECRET"], secret, "a secret, the lake's (as every secret not TEMP is)"),
    entry(&["CREATE", "OR", "REPLACE", "PERSISTENT", "SECRET"], secret, "a secret, in place of one of its name"),
    entry(&["CREATE", "TEMP", "SECRET"], secret, "a secret of the session's, in memory"),
    entry(&["CREATE", "TEMPORARY", "SECRET"], secret, "a secret of the session's, in memory"),
    entry(&["CREATE", "OR", "REPLACE", "TEMP", "SECRET"], secret, "a secret of the session's, in place of one of its name"),
    entry(&["CREATE", "OR", "REPLACE", "TEMPORARY", "SECRET"], secret, "a secret of the session's, in place of one of its name"),
    entry(&["DROP", "SECRET"], secret, "drops a secret"),
    entry(&["DROP", "PERSISTENT", "SECRET"], secret, "drops a secret"),
    entry(&["DROP", "TEMPORARY", "SECRET"], secret, "drops a secret"),
    entry(&["CREATE", "FUNCTION"], routine, "a function: SQL, or Python (LANGUAGE python)"),
    entry(&["CREATE", "OR", "REPLACE", "FUNCTION"], routine, "a function, in place of one of its name"),
    entry(&["CREATE", "TEMP", "FUNCTION"], routine, "a function of the session's"),
    entry(&["CREATE", "TEMPORARY", "FUNCTION"], routine, "a function of the session's"),
    entry(&["CREATE", "OR", "REPLACE", "TEMP", "FUNCTION"], routine, "a function of the session's, in place of one of its name"),
    entry(&["CREATE", "OR", "REPLACE", "TEMPORARY", "FUNCTION"], routine, "a function of the session's, in place of one of its name"),
    entry(&["CREATE", "PROCEDURE"], routine, "a procedure: statements run in order, or Python"),
    entry(&["CREATE", "OR", "REPLACE", "PROCEDURE"], routine, "a procedure, in place of one of its name"),
    entry(&["CREATE", "TASK"], routine, "a task: a statement run on a schedule (ADR-045)"),
    entry(&["CREATE", "OR", "REPLACE", "TASK"], routine, "a task, in place of one of its name"),
    entry(&["ALTER", "TASK"], routine, "suspends or resumes a task"),
    entry(&["DROP", "TASK"], routine, "drops a task"),
    entry(&["DROP", "MACRO"], routine, "drops a macro (DuckDB's DROP MACRO [TABLE])"),
    // (where SQL lists a column: Databricks' and MySQL's words, which sqlparser doesn't read in an ALTER COLUMN, so two
    // names come before the telling word; ALTER TABLE's other forms are the tree's)
    entry(&["ALTER", "TABLE", "*", "ALTER", "COLUMN", "*", "FIRST"], placed, "puts a column first in SQL's order"),
    entry(&["ALTER", "TABLE", "*", "ALTER", "COLUMN", "*", "AFTER"], placed, "puts a column after another in SQL's order"),
    entry(&["ALTER", "TABLE", "*", "ALTER", "*", "FIRST"], placed, "puts a column first in SQL's order"),
    entry(&["ALTER", "TABLE", "*", "ALTER", "*", "AFTER"], placed, "puts a column after another in SQL's order"),
    entry(&["ALTER", "VIEW", "*", "ALTER", "COLUMN", "*", "FIRST"], placed, "puts a view's column first in SQL's order"),
    entry(&["ALTER", "VIEW", "*", "ALTER", "COLUMN", "*", "AFTER"], placed, "puts a view's column after another"),
    entry(&["ALTER", "VIEW", "*", "ALTER", "*", "FIRST"], placed, "puts a view's column first in SQL's order"),
    entry(&["ALTER", "VIEW", "*", "ALTER", "*", "AFTER"], placed, "puts a view's column after another"),
    entry(&["ALTER", "MATERIALIZED", "VIEW", "*", "ALTER", "COLUMN", "*", "FIRST"], placed, "puts a view's column first in SQL's order"),
    entry(&["ALTER", "MATERIALIZED", "VIEW", "*", "ALTER", "COLUMN", "*", "AFTER"], placed, "puts a view's column after another"),
    entry(&["ALTER", "MATERIALIZED", "VIEW", "*", "ALTER", "*", "FIRST"], placed, "puts a view's column first in SQL's order"),
    entry(&["ALTER", "MATERIALIZED", "VIEW", "*", "ALTER", "*", "AFTER"], placed, "puts a view's column after another"),
    entry(&["ATTACH"], attach, "another lake's tables, or a Delta, Iceberg or shared table's, or a connection's, by name"),
    entry(&["COPY"], copy, "a query's rows to files (COPY (SELECT …) TO 's3://…' (FORMAT parquet))"),
    entry(&["COPY", "INTO"], copy_into, "loads files into a table, each once (COPY INTO t FROM 's3://…/*.csv')"),
    entry(&["CREATE", "EXTERNAL", "TABLE"], external, "a stored view of files (DataFusion's words)"),
    entry(&["CREATE", "OR", "REPLACE", "EXTERNAL", "TABLE"], external, "a stored view of files, in place of one of its name"),
    entry(&["CREATE", "UNBOUNDED", "EXTERNAL", "TABLE"], external, "a stored view of files that grow"),
    entry(&["CREATE", "OR", "REPLACE", "UNBOUNDED", "EXTERNAL", "TABLE"], external, "a stored view of files that grow, in place of one of its name"),
    entry(&["CREATE", "TEMP", "EXTERNAL", "TABLE"], temp_external, "refused: a temporary view of the files is the session's"),
    entry(&["CREATE", "TEMPORARY", "EXTERNAL", "TABLE"], temp_external, "refused: a temporary view of the files is the session's"),
    entry(&["CREATE", "OR", "REPLACE", "TEMP", "EXTERNAL", "TABLE"], temp_external, "refused: a temporary view of the files is the session's"),
    entry(&["CREATE", "OR", "REPLACE", "TEMPORARY", "EXTERNAL", "TABLE"], temp_external, "refused: a temporary view of the files is the session's"),
];

/// A reader's own words for a form written wrong, after what it found: what a statement of the form is.
fn formed(form: &str, w: &mut Words, read: fn(&mut Words) -> Result<Stmt>) -> Result<Stmt> {
    read(w).map_err(|e| anyhow::anyhow!("{e} ({form})"))
}

fn if_not_exists(w: &mut Words) -> Result<bool> {
    if !w.is("if") {
        return Ok(false);
    }
    w.expect_all(&["not", "exists"])?;
    Ok(true)
}

/// A database's name: one part (`b`, `"B"`), unquoted in lower case, as `text::object_of` reads one, or written with `-`
/// as a lake's folder names it (`pondra-ab12`: `Words::joined`).
fn database(w: &mut Words) -> Result<String> { w.joined().map_err(|_| anyhow::anyhow!("expected a database's name {}", w.near())) }

/// `CREATE OR REPLACE SCHEMA | DATABASE | USER | ROLE`, where replacing one would lose what it holds: refused, saying why
/// (and SHARE and RECIPIENT, the sharing extension's: `shares::ENTRIES`).
fn no_replace(w: &mut Words) -> Result<Stmt> {
    w.expect_all(&["create", "or", "replace"])?;
    let kind = w.next().map(|k| k.word()).unwrap_or_default();
    Ok(replacing(&kind, if matches!(kind.as_str(), "user" | "role") { "the rights given to it" } else { "everything in it" }))
}

/// The refusal of a `CREATE OR REPLACE kind`, which would lose what one holds (`lost`).
pub(crate) fn replacing(kind: &str, lost: &str) -> Stmt {
    let kind = kind.to_uppercase();
    Stmt::Invalid(format!("CREATE OR REPLACE {kind}: replacing one would drop {lost}; CREATE {kind} IF NOT EXISTS leaves one that is there as it is"))
}

/// `ALTER [MATERIALIZED] VIEW [IF EXISTS] v RENAME TO w` (dbt's): the parser takes only ALTER VIEW … AS. (A view that
/// isn't there is the rename's refusal, IF EXISTS or not.)
fn rename_view(w: &mut Words) -> Result<Stmt> {
    formed("ALTER [MATERIALIZED] VIEW v RENAME TO w", w, |w| {
        w.expect("alter")?;
        w.is("materialized");
        w.expect("view")?;
        if_exists(w)?;
        let name = w.name()?;
        w.expect_all(&["rename", "to"])?;
        let to = w.name()?;
        w.done()?;
        Ok(Stmt::Ddl(vec![Ddl::RenameTable { name, to }]))
    })
}

fn if_exists(w: &mut Words) -> Result<()> {
    if w.is("if") {
        w.expect("exists")?;
    }
    Ok(())
}

/// `ALTER MATERIALIZED VIEW v DETACH`: its rows stop following, and stay as a table of that name.
fn detach(w: &mut Words) -> Result<Stmt> {
    formed("ALTER MATERIALIZED VIEW v DETACH", w, |w| {
        w.expect_all(&["alter", "materialized", "view"])?;
        let name = w.name()?;
        w.expect("detach")?;
        w.done()?;
        Ok(Stmt::Ddl(vec![Ddl::DetachView { name }]))
    })
}

/// `CREATE TABLE [IF NOT EXISTS] c [SHALLOW] CLONE t` (Snowflake's, Databricks'): a table of t's files as they are now,
/// copying none (ADR-043).
fn clone_table(w: &mut Words) -> Result<Stmt> {
    formed("CREATE TABLE c [SHALLOW] CLONE t", w, |w| {
        w.expect_all(&["create", "table"])?;
        let quiet = if_not_exists(w)?;
        let name = w.name()?;
        w.is("shallow");
        w.expect("clone")?;
        let from = w.name()?;
        w.done()?;
        let d = Ddl::Clone { name: name.clone(), from };
        Ok(Stmt::Ddl(vec![crate::write::unless(quiet, &name, "relation", d)]))
    })
}

/// `CREATE DATABASE [IF NOT EXISTS] b [LOCATION 'dir'] CLONE a [WITH (schemas = (x, y))] [WITH NO DATA]`: a branch of a,
/// copying none of its files (ADR-047); and `CREATE DATABASE b LOCATION 'dir'`, a database in that folder. (The parser
/// takes `CLONE a` alone, and a CREATE DATABASE with neither word.)
fn create_database(w: &mut Words) -> Result<Stmt> {
    formed("CREATE DATABASE b [LOCATION 'dir'] [CLONE a [WITH (schemas = (x, y))] [WITH NO DATA]]", w, |w| {
        w.expect_all(&["create", "database"])?;
        let if_not_exists = if_not_exists(w)?;
        let name = database(w)?;
        let dir = if w.is("location") { Some(w.string()?) } else { None };
        if !w.is("clone") {
            w.done()?;
            // (as sqlparser's tree reads a CREATE DATABASE without CLONE: its name in lower case, quoted or not)
            return Ok(Stmt::Ddl(vec![Ddl::CreateDatabase { name: name.to_lowercase(), if_not_exists, dir, clone: None }]));
        }
        let from = database(w)?;
        let (mut schemas, mut data) = (None, true);
        while w.is("with") {
            if w.is("no") {
                w.expect("data")?;
                data = false;
                continue;
            }
            w.expect_sym('(')?;
            w.expect("schemas")?;
            w.expect_sym('=')?;
            w.expect_sym('(')?;
            let named = if w.sym(')') {
                vec![]
            } else {
                let named = w.names()?;
                w.expect_sym(')')?;
                named
            };
            w.expect_sym(')')?;
            if schemas.is_none() {
                schemas = Some(named); // (the first list, where two are written)
            }
        }
        w.done()?;
        let clone = crate::branch::CloneOf { from, schemas: schemas.unwrap_or_default(), data, owner: None };
        Ok(Stmt::Ddl(vec![Ddl::CreateDatabase { name, if_not_exists, dir, clone: Some(clone) }]))
    })
}

/// `ALTER DATABASE b REFRESH [t, …]`: a branch's tables (all it took from its base, or those named: a view stands for the
/// tables it reads), and what follows them, as its base has them now (ADR-047). `ALTER DATABASE b SET LOCATION 'dir'`: a
/// lake held in memory moves there, and goes on there (`memory.rs`). `ALTER DATABASE b SET (protected = true | false)`:
/// a database's protection (ADR-058: `protect.rs`).
fn alter_database(w: &mut Words) -> Result<Stmt> {
    formed("ALTER DATABASE b REFRESH [t, …], SET LOCATION 'dir' or SET (protected = true | false)", w, |w| {
        w.expect_all(&["alter", "database"])?;
        let database = database(w)?;
        if w.is("refresh") {
            let tables = if w.0.is_empty() { vec![] } else { w.names()? };
            w.done()?;
            return Ok(Stmt::Ddl(vec![Ddl::Refresh { database: Some(database), tables }]));
        }
        w.expect("set")?;
        if w.is("location") {
            let to = w.string()?;
            w.done()?;
            return Ok(Stmt::Ddl(vec![Ddl::SetLocation { database, to }]));
        }
        w.expect_sym('(')?;
        w.expect("protected")?;
        w.expect_sym('=')?;
        let on = match w.next().map(|v| v.word()).as_deref() {
            Some("true") => true,
            Some("false") => false,
            _ => bail!("protected is true or false"),
        };
        w.expect_sym(')')?;
        w.done()?;
        Ok(Stmt::Ddl(vec![Ddl::Protect { database, on, by: String::new() }]))
    })
}

const BACKUP_FORM: &str = "a backup is written as BACKUP DATABASE [name] TO 'place' [WITH (retention_days = 7)], and restored as RESTORE DATABASE name FROM 'place' [LOCATION 'place'] [AT (TIMESTAMP => '2026-10-12 03:00:00')]";

/// `BACKUP DATABASE [b] TO 'url' [WITH (retention_days = n)]` (ADR-070: a backup is a lake, `backup.rs`).
fn backup(w: &mut Words) -> Result<Stmt> {
    fn read(w: &mut Words) -> Result<(Option<String>, String, Option<String>)> {
        w.expect_all(&["backup", "database"])?;
        let database = if w.peek_word().as_deref() == Some("to") { None } else { Some(database(w)?) };
        w.expect("to")?;
        let to = w.string()?;
        let mut days = None;
        if w.is("with") {
            w.expect_sym('(')?;
            w.expect("retention_days")?;
            w.expect_sym('=')?;
            days = match w.next() {
                Some(W::Num(n)) => Some(n),
                _ => bail!("retention_days is a number"),
            };
            w.expect_sym(')')?;
        }
        w.done()?;
        Ok((database, to, days))
    }
    let Ok((database, to, days)) = read(w) else { return Ok(Stmt::Invalid(BACKUP_FORM.into())) };
    let Ok(retention_days) = days.map(|d| d.parse::<u64>()).transpose() else { return Ok(Stmt::Invalid("retention_days is a number of days".into())) };
    Ok(Stmt::Ddl(vec![Ddl::BackupDatabase { database, to, retention_days }]))
}

/// `RESTORE DATABASE b FROM 'url' [LOCATION 'dir'] [AT (TIMESTAMP => '…')]` (ADR-070).
fn restore(w: &mut Words) -> Result<Stmt> {
    fn read(w: &mut Words) -> Result<(String, String, Option<String>, Option<String>)> {
        w.expect_all(&["restore", "database"])?;
        let name = database(w)?;
        w.expect("from")?;
        let from = w.string()?;
        let dir = if w.is("location") { Some(w.string()?) } else { None };
        let mut at = None;
        if w.is("at") {
            w.expect_sym('(')?;
            w.expect("timestamp")?;
            w.expect_sym('⇒')?;
            at = Some(w.string()?);
            w.expect_sym(')')?;
        }
        w.done()?;
        Ok((name, from, dir, at))
    }
    let Ok((name, from, dir, at)) = read(w) else { return Ok(Stmt::Invalid(BACKUP_FORM.into())) };
    let at = match at.map(|t| crate::backup::instant(&t)).transpose() {
        Ok(at) => at,
        Err(e) => return Ok(Stmt::Invalid(format!("{e:#}"))),
    };
    Ok(Stmt::Ddl(vec![Ddl::RestoreDatabase { name, from, dir, at }]))
}

/// DuckDB's `EXPORT DATABASE 'dir'` and `IMPORT DATABASE 'dir'` (a folder of a database's files): not Pondra's yet, so
/// refused by name with the ways it has, not with the parser's words.
fn portable(_: &mut Words) -> Result<Stmt> {
    Ok(Stmt::Invalid("EXPORT DATABASE and IMPORT DATABASE are not supported yet: BACKUP DATABASE TO 'place' copies a database (RESTORE DATABASE name FROM 'place' brings it back), and COPY t TO 'dir/t.parquet' (FORMAT parquet) writes a table's rows".into()))
}

/// `COMMENT [IF EXISTS] ON <kind> name IS 'text' | NULL`: the kind is the longest of the kinds' words that the next
/// words are, and its name and text are read as SQL (`objects::comment_of`), which the leader carries out (`objects::apply`).
fn comment(w: &mut Words) -> Result<Stmt> {
    w.expect("comment")?;
    let if_exists = w.is("if") && {
        w.expect("exists")?;
        true
    };
    w.expect("on")?;
    let mut kinds: Vec<&str> = crate::objects::all_kinds().map(|k| k.name).chain(["column"]).collect();
    kinds.sort_by_key(|k| std::cmp::Reverse(k.len()));
    let Some(kind) = kinds.into_iter().find(|k| w.begins(&k.split(' ').collect::<Vec<_>>())) else {
        return Ok(Stmt::Invalid(format!("COMMENT ON <kind> name IS 'text' | NULL: expected a kind of object (TABLE, COLUMN, VIEW, …) {}", w.near())));
    };
    for _ in kind.split(' ') {
        w.next();
    }
    Ok(match crate::objects::comment_of(w.rest()) {
        Ok((name, text)) => Stmt::Ddl(vec![Ddl::Object(crate::objects::Op::Comment { kind: kind.to_string(), name, text, if_exists })]),
        Err(e) => Stmt::Invalid(format!("COMMENT ON {} name IS 'text' | NULL: {}", kind.to_uppercase(), crate::ext::said(&e))),
    })
}

/// `CREATE OR ALTER TABLE | VIEW …`: a view is made anew, as it holds nothing to keep; a table is made or altered to the
/// columns given (`Op::OrAlterTable`, the statement as `CREATE TABLE`); another kind is refused.
fn or_alter(w: &mut Words) -> Result<Stmt> {
    w.expect("create")?;
    let or = w.at();
    w.expect_all(&["or", "alter"])?;
    let (sql, at) = (w.sql(), w.at());
    let (create, rest) = (crate::text::first_word(&sql[..or]).to_string(), sql[at..].to_string()); // (`CREATE ` as written, and what follows OR ALTER)
    let mut what = match w.next() {
        Some(W::Word(x, _)) => x.to_uppercase(),
        _ => bail!("CREATE OR ALTER TABLE | VIEW name …"),
    };
    if what != "VIEW" && w.is("view") {
        what.push_str(" VIEW"); // (`MATERIALIZED VIEW`)
    }
    Ok(match what.as_str() {
        "VIEW" => {
            let anew = format!("{create}OR REPLACE {rest}");
            crate::write::parse(&anew).unwrap_or_else(|| Stmt::Invalid(crate::parsed::generic(&anew).err().map_or("CREATE OR ALTER VIEW name AS SELECT …".into(), |e| e.to_string())))
        }
        "TABLE" => {
            let plain = format!("{create}{rest}");
            match crate::write::parse(&plain) {
                Some(Stmt::Create(t)) if t.query.is_none() && t.like.is_none() && !t.or_replace && !t.if_not_exists && !t.temporary => {
                    Stmt::Ddl(vec![Ddl::Object(crate::objects::Op::OrAlterTable { name: crate::text::object(&t.name), sql: plain })])
                }
                Some(Stmt::Invalid(e)) => Stmt::Invalid(e),
                Some(Stmt::Refused(c, e)) => Stmt::Refused(c, e),
                _ => Stmt::Invalid("CREATE OR ALTER TABLE takes the table's columns (… AS SELECT makes it anew: CREATE OR REPLACE TABLE)".into()),
            }
        }
        kind => Stmt::Refused("0A000", format!("CREATE OR ALTER takes a table or a view; a {} is made anew with CREATE OR REPLACE {kind}", kind.to_lowercase())),
    })
}

/// SECRET's statements, by ext's walk of their text (`ext::secret`), which reads each value as a string, a word or a number.
fn secret(w: &mut Words) -> Result<Stmt> {
    match crate::ext::secret(w.sql()) {
        Some(s) => Ok(s),
        None => bail!("{}", unread(w.sql(), "CREATE SECRET name (TYPE s3, KEY_ID '…', SECRET '…', SCOPE 's3://bucket') or DROP SECRET name")),
    }
}

/// The routines' and the tasks' statements, by routines' walk of their text (`routines::statement`): a function's or
/// procedure's body is read as Postgres and DuckDB write it.
fn routine(w: &mut Words) -> Result<Stmt> {
    match crate::routines::statement(w.sql()) {
        Some(s) => Ok(s),
        None if w.peek_word().as_deref() == Some("alter") => bail!("ALTER TASK name SUSPEND | RESUME"),
        None => bail!("{}", unread(w.sql(), "CREATE [OR REPLACE] FUNCTION | PROCEDURE | TASK name …, DROP TASK | MACRO name")),
    }
}

/// `ALTER TABLE | VIEW | MATERIALIZED VIEW t ALTER [COLUMN] c FIRST | AFTER b`, by its walk of the text (`write::placed`).
/// sqlparser has MySQL's column place for ADD and MODIFY but not here: when it reads this, the tree does and the entry
/// holds one name (an upstream proposal, the counted list).
fn placed(w: &mut Words) -> Result<Stmt> {
    match crate::write::placed(w.sql()) {
        Some(s) => Ok(s),
        None => bail!("{}", unread(w.sql(), "ALTER TABLE t ALTER [COLUMN] c FIRST | AFTER b")),
    }
}

/// ATTACH, every form, by ext's walk and then sqlparser's tree (`ext::read_attach`).
fn attach(w: &mut Words) -> Result<Stmt> { crate::ext::read_attach(w.sql()) }

/// COPY to files, by ext's walk (`ext::copy`); a form it doesn't place is DataFusion's own, planned as before.
fn copy(w: &mut Words) -> Result<Stmt> { crate::ext::copy(w.sql()) }

/// COPY INTO, by ext's walk (`ext::copy_into`).
fn copy_into(w: &mut Words) -> Result<Stmt> { crate::ext::copy_into(w.sql()) }

/// `CREATE [OR REPLACE] [UNBOUNDED] EXTERNAL TABLE`, DataFusion's own (a stored view of files: `ext::external`), read by
/// DataFusion's parser and in its words.
fn external(w: &mut Words) -> Result<Stmt> {
    use datafusion::sql::parser::{DFParserBuilder, Statement as DF};
    Ok(match DFParserBuilder::new(w.sql()).build().and_then(|mut p| p.parse_statements()) {
        Ok(mut all) if all.len() == 1 => match all.pop_front() {
            Some(DF::CreateExternalTable(c)) => crate::ext::external(&c).map(|d| Stmt::Ddl(vec![d])).unwrap_or_else(|e| Stmt::Invalid(format!("{e:#}"))),
            _ => Stmt::Invalid("CREATE EXTERNAL TABLE t … STORED AS … LOCATION '…'".into()),
        },
        Ok(all) => Stmt::Invalid(format!("CREATE EXTERNAL TABLE and {} more: send one statement at a time here (or as a script, over HTTP or in the shell)", all.len().saturating_sub(1))),
        Err(e) => Stmt::Invalid(format!("{e}")),
    })
}

/// A TEMP EXTERNAL TABLE, refused by name: the session's view of files is a TEMP VIEW.
fn temp_external(_: &mut Words) -> Result<Stmt> {
    Ok(Stmt::Invalid("CREATE TEMPORARY EXTERNAL TABLE: make a temporary view of the files instead (CREATE TEMP VIEW t AS SELECT * FROM read_parquet('…'))".into()))
}

/// Why a walk found no statement in a text its entry's words begin: the parser's words, or the form.
fn unread(sql: &str, form: &str) -> String { crate::parsed::generic(sql).err().map_or_else(|| form.to_string(), |e| e.to_string()) }

#[cfg(test)]
mod tests {
    use super::*;

    /// Every core entry stands in for its run and right until its family has its own: the count grows as PR 8 moves the
    /// core's families in, and falls as each gets them (l6/step8-pr8-plan.md). An entry's texts: `write::was`'s test.
    #[test]
    fn the_core_entries_on_stand_ins_are_counted() {
        let s = crate::registry::get().section::<Statements>();
        assert_eq!((ENTRIES.len(), s.stand_ins()), (97, 109), "the core's 97, the sharing extension's 8 and the identity providers' 4");
    }

    /// A column named first or after is a name in ALTER COLUMN's other forms, which are the tree's: no place entry takes
    /// ALTER COLUMN's COLUMN as the column's name (`statements::shadowed`).
    #[test]
    fn a_column_named_first_is_the_trees() {
        for sql in ["ALTER TABLE t ALTER COLUMN first SET NOT NULL", "alter table s.t alter column after type bigint", "ALTER TABLE t ALTER COLUMN first SET DEFAULT 0", "ALTER VIEW v ALTER COLUMN first SET DEFAULT 1"] {
            assert!(crate::statements::matched(sql).is_none(), "{sql}: the tree's");
            assert!(!matches!(crate::write::parse(sql), Some(Stmt::Invalid(e)) if e.contains("FIRST | AFTER")), "{sql}");
        }
        for sql in ["ALTER TABLE t ALTER COLUMN first FIRST", "ALTER TABLE t ALTER first AFTER after", "ALTER TABLE t ALTER COLUMN \"column\" FIRST", "alter materialized view m alter column after first"] {
            assert!(matches!(crate::write::parse(sql), Some(Stmt::Ddl(_))), "{sql}: the place's");
        }
    }

    /// COMMENT ON and CREATE OR ALTER, as `write::parse` reads them (each was `objects::statement`'s).
    #[test]
    fn comments_and_or_alter() {
        use serde_json::json as j;
        let ddl = |s: &str| match crate::write::parse(s) {
            Some(Stmt::Ddl(d)) => serde_json::to_value(&d[0]).unwrap(),
            Some(Stmt::Invalid(e) | Stmt::Refused(_, e)) => j!({"error": e}),
            _ => j!(null),
        };
        assert_eq!(ddl("COMMENT ON TABLE sales.Orders IS 'one row per order'")["text"], "one row per order");
        assert_eq!(ddl("comment on materialized view v is null")["kind"], "materialized view");
        assert_eq!((ddl("COMMENT ON SHARE acme IS 'for Acme'")["kind"].clone(), ddl("comment on recipient acme_corp is null")["kind"].clone()), (j!("share"), j!("recipient")));
        assert_eq!(ddl("COMMENT IF EXISTS ON COLUMN t.\"Amount\" IS $$in euros$$")["name"], "t.Amount");
        assert_eq!(ddl("COMMENT ON TASK nightly IS ''")["text"], j!(null));
        assert!(ddl("COMMENT ON TABLE t IS 42")["error"].is_string());
        assert_eq!(ddl("COMMENT ON SECRETS s IS 'x'")["error"], "COMMENT ON <kind> name IS 'text' | NULL: expected a kind of object (TABLE, COLUMN, VIEW, …) near secrets");
        assert_eq!(ddl("CREATE OR ALTER TABLE t (a BIGINT) CLUSTER BY (a)")["do"], "or_alter_table");
        assert!(ddl("CREATE OR ALTER TABLE t AS SELECT 1")["error"].is_string());
        assert!(ddl("CREATE OR ALTER MATERIALIZED VIEW v AS SELECT 1")["error"].as_str().unwrap().contains("CREATE OR REPLACE MATERIALIZED VIEW"));
        assert!(ddl("CREATE OR ALTER VIEW v AS SELEC")["error"].is_string(), "the parser's words for the view made anew");
        assert_eq!(ddl("SELECT 1"), j!(null));
    }

    /// A lake on disk is named after its folder (`pondra-ab12`), and ALTER DATABASE, CLONE, BACKUP and RESTORE take the
    /// name so, as their regexes did: written with `-` and no space or comment beside it. A `-` with one ends the name.
    #[test]
    fn a_database_is_named_as_its_folder() {
        let name = |sql: &str| Words::of(sql).and_then(|mut w| w.joined().ok().map(|n| (n, w.near())));
        let read = |n: &str, near: &str| Some((n.to_string(), near.to_string()));
        assert_eq!(name("pondra-ab12 SET"), read("pondra-ab12", "near set"));
        assert_eq!(name("pondra-1a2b SET"), read("pondra-1a2b", "near set"), "a number then a word, as the tokenizer splits them");
        assert_eq!(name("My-Lake_2-9"), read("my-lake_2-9", "at the end"));
        assert_eq!(name("\"a-B\" SET"), read("a-B", "near set"));
        for sql in ["a - b", "a -b", "a- b", "a/* c */-b", "a-"] {
            assert_eq!(name(sql), read("a", "near -"), "{sql}");
        }
        assert_eq!(name("1-a"), None);
        match crate::write::parse("ALTER DATABASE pondra-ab12 SET LOCATION '/x'") {
            Some(Stmt::Ddl(d)) => assert!(matches!(&d[..], [Ddl::SetLocation { database, to }] if database == "pondra-ab12" && to == "/x")),
            _ => panic!("ALTER DATABASE pondra-ab12 SET LOCATION: read"),
        }
        // (a name slot takes one written so too: the entry found is CLONE's, before its reader reads the name)
        assert!(Words::of("dev-2 CLONE").is_some_and(|w| w.begins(&["*", "CLONE"])) && Words::of("s.pondra-ab12 X").is_some_and(|w| w.begins(&["*", "X"])));
        assert!(!Words::of("dev - 2 CLONE").is_some_and(|w| w.begins(&["*", "CLONE"])));
        match crate::write::parse("CREATE DATABASE dev-2 CLONE pondra-ab12") {
            Some(Stmt::Ddl(d)) => assert!(matches!(&d[..], [Ddl::CreateDatabase { name, clone: Some(_), .. }] if name == "dev-2")),
            _ => panic!("CREATE DATABASE dev-2 CLONE pondra-ab12: read"),
        }
    }
}
