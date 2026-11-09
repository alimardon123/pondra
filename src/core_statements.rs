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

/// Each family's words. A form whose telling word comes after a name (`CREATE TABLE c CLONE t`) is an entry with a name
/// slot beside its family, which sqlparser's tree reads (rows 35-59 of `write::parse`); a family no other reader takes is
/// one entry (`ALTER DATABASE`).
static ENTRIES: &[Statement] = &[
    entry(&["CREATE", "OR", "REPLACE", "SCHEMA"], no_replace, "refused: replacing a schema would drop everything in it"),
    entry(&["CREATE", "OR", "REPLACE", "DATABASE"], no_replace, "refused: replacing a database would drop everything in it"),
    entry(&["CREATE", "OR", "REPLACE", "USER"], no_replace, "refused: replacing a user would drop the rights given to it"),
    entry(&["CREATE", "OR", "REPLACE", "ROLE"], no_replace, "refused: replacing a role would drop the rights given to it"),
    entry(&["CREATE", "OR", "REPLACE", "SHARE"], no_replace, "refused: replacing a share would drop its tables and grants"),
    entry(&["CREATE", "OR", "REPLACE", "RECIPIENT"], no_replace, "refused: replacing a recipient would drop its token and shares"),
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

/// A database's name: one part (`b`, `"B"`), unquoted in lower case, as `text::object_of` reads one.
fn database(w: &mut Words) -> Result<String> { w.part().map_err(|_| anyhow::anyhow!("expected a database's name {}", w.near())) }

/// `CREATE OR REPLACE SCHEMA | DATABASE | USER | ROLE | SHARE | RECIPIENT`, where replacing one would lose what it holds:
/// refused, saying why.
fn no_replace(w: &mut Words) -> Result<Stmt> {
    w.expect_all(&["create", "or", "replace"])?;
    let kind = w.next().map(|k| k.word().to_uppercase()).unwrap_or_default();
    let lost = match kind.as_str() {
        "USER" | "ROLE" => "the rights given to it",
        "SHARE" => "its tables and grants",
        "RECIPIENT" => "its token and the shares granted to it",
        _ => "everything in it",
    };
    Ok(Stmt::Invalid(format!("CREATE OR REPLACE {kind}: replacing one would drop {lost}; CREATE {kind} IF NOT EXISTS leaves one that is there as it is")))
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

#[cfg(test)]
mod tests {
    use super::*;

    /// Every core entry stands in for its run and right until its family has its own: the count grows as PR 8 moves the
    /// core's families in, and falls as each gets them (l6/step8-pr8-plan.md). An entry's texts: `write::was`'s test.
    #[test]
    fn the_core_entries_on_stand_ins_are_counted() {
        let s = crate::registry::get().section::<Statements>();
        assert_eq!((s.stand_ins(), ENTRIES.len()), (24, 24));
    }
}
