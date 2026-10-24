//! DuckDB's extension statements (`INSTALL`, `FORCE INSTALL`, `LOAD`) for the extensions Pondra has built in (ADR-075).
//! A script that loads one of them runs unchanged: the statement is answered with a notice saying nothing had to be done.
//! Any other name is refused by name, never answered as if it were done (principle 10): Pondra's own extensions come in
//! 1.1. The node's doors answer these through `settings.rs`, before anything expands the statement (so the notice reaches
//! the client as the command tag and the notice header or NOTICE does). A host that only runs SQL through the front (a
//! browser page, `pondra sql`) asks `row` instead, and answers one row naming what happened.

use crate::codes::coded;
use anyhow::Result;
use datafusion::sql::sqlparser::{dialect::GenericDialect, tokenizer::{Token, Tokenizer}};

/// What a statement comes to: its command tag, and the notice the client is told.
pub struct Said {
    pub tag: &'static str,
    pub notice: String,
}

/// DuckDB's extensions Pondra has built in, each with a few words on what in Pondra does its job. A name goes in when a
/// DuckDB script that loads it and uses its main function or a feature of it runs here unchanged (checked by hand, on the
/// binary). Each description says what does not run: a JSON array file, `timezone()`, an http(s) read with no secret.
const BUILT_IN: &[(&str, &str)] = &[
    ("httpfs", "reads s3:// files, and http(s):// files a secret covers"),
    ("parquet", "reads Parquet files"),
    ("json", "reads newline-delimited JSON files"),
    ("icu", "time zones: SET TimeZone and AT TIME ZONE"),
    ("iceberg", "reads Iceberg tables"),
    ("delta", "reads Delta tables"),
    ("aws", "reads s3:// files with the AWS credential chain"),
];

/// The statement as a row naming what happened, for a host that runs SQL through the front and has no door of its own
/// (a browser page, `pondra sql`): `SELECT 'httpfs is built into Pondra: …' AS notice`. None for any other statement.
pub fn row(sql: &str) -> Option<Result<String>> {
    Some(statement(sql)?.map(|said| format!("SELECT '{}' AS notice", said.notice.replace('\'', "''"))))
}

/// The answer to `sql` if it is one of DuckDB's extension statements; None for any other statement, which goes on as it did.
/// The words are read from the tokens: `[FORCE] INSTALL name [FROM repo] [VERSION 'v']` and `LOAD name`, the name bare,
/// quoted or a string. `LOAD DATA …` is another statement, and so is anything with more after the name.
pub fn statement(sql: &str) -> Option<Result<Said>> {
    if !matches!(crate::text::head(sql).as_str(), "install" | "force" | "load") {
        return None;
    }
    let tokens = Tokenizer::new(&GenericDialect {}, sql).tokenize().ok()?;
    Some(answer(&read(&tokens)?))
}

/// A statement's command (`INSTALL` or `LOAD`) and the name it gives.
struct Ask {
    install: bool,
    name: String,
}

/// The form the tokens are, or None when they are not one of DuckDB's extension statements.
fn read(tokens: &[Token]) -> Option<Ask> {
    let mut words: Vec<&Token> = tokens.iter().filter(|t| !matches!(t, Token::Whitespace(_) | Token::EOF)).collect();
    if matches!(words.last(), Some(Token::SemiColon)) {
        words.pop();
    }
    let at = |i: usize| words.get(i).copied();
    let mut i = 0;
    let forced = keyword(at(0), "force");
    if forced {
        i += 1;
    }
    let install = keyword(at(i), "install");
    if !install && (forced || !keyword(at(i), "load")) {
        return None; // (FORCE LOAD, or a word that is neither)
    }
    i += 1;
    let name = name_of(at(i))?;
    i += 1;
    if install {
        if keyword(at(i), "from") {
            name_of(at(i + 1))?; // (the repository: a word or a string)
            i += 2;
        }
        if keyword(at(i), "version") {
            name_of(at(i + 1))?; // (the version: a string)
            i += 2;
        }
    }
    (i == words.len()).then_some(Ask { install, name })
}

/// Is this token the keyword `word` (and not a quoted identifier of that name)?
fn keyword(t: Option<&Token>, word: &str) -> bool {
    matches!(t, Some(Token::Word(w)) if w.quote_style.is_none() && w.value.eq_ignore_ascii_case(word))
}

/// A name as a token: a word (bare or quoted) or a string.
fn name_of(t: Option<&Token>) -> Option<String> {
    match t? {
        Token::Word(w) => Some(w.value.clone()),
        Token::SingleQuotedString(s) | Token::DoubleQuotedString(s) => Some(s.clone()),
        _ => None,
    }
}

/// The notice for a built-in name, or the refusal by name for any other.
fn answer(ask: &Ask) -> Result<Said> {
    let tag = if ask.install { "INSTALL" } else { "LOAD" };
    if ask.name.contains('/') || ask.name.ends_with(".duckdb_extension") {
        return Err(coded("0A000", format!("{}: a DuckDB extension file is DuckDB's own native code, which Pondra doesn't run", ask.name)));
    }
    let name = ask.name.to_lowercase();
    if BUILT_IN.iter().any(|(built, _)| *built == name) {
        let step = if ask.install { "install" } else { "load" };
        return Ok(Said { tag, notice: format!("{name} is built into Pondra: nothing to {step}") });
    }
    let built: Vec<&str> = BUILT_IN.iter().map(|(n, _)| *n).collect();
    Err(coded("0A000", format!("{name} isn't built into Pondra: Pondra's own extensions come in 1.1 (ADR-075), and the DuckDB extensions it has built in now are {}", built.join(", "))))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn answered(sql: &str) -> Result<Said> {
        statement(sql).unwrap_or_else(|| panic!("{sql}: not an extension statement"))
    }

    #[test]
    fn every_form_is_read() {
        let forms = [
            ("INSTALL httpfs", "INSTALL"),
            ("install httpfs;", "INSTALL"),
            ("INSTALL 'httpfs'", "INSTALL"),
            ("INSTALL \"httpfs\"", "INSTALL"),
            ("FORCE INSTALL httpfs", "INSTALL"),
            ("force install HTTPFS", "INSTALL"),
            ("INSTALL httpfs FROM community", "INSTALL"),
            ("INSTALL httpfs FROM 'community' VERSION 'v1.0'", "INSTALL"),
            ("-- a note\nINSTALL httpfs", "INSTALL"),
            ("LOAD httpfs", "LOAD"),
            ("load 'httpfs'", "LOAD"),
            ("LOAD \"httpfs\";", "LOAD"),
        ];
        for (sql, tag) in forms {
            let said = answered(sql).unwrap_or_else(|e| panic!("{sql}: {e:#}"));
            assert_eq!(said.tag, tag, "{sql}");
            assert!(said.notice.starts_with("httpfs is built into Pondra: nothing to "), "{sql}: {}", said.notice);
        }
    }

    #[test]
    fn a_built_in_name_is_told_nothing_had_to_be_done() {
        assert_eq!(answered("INSTALL httpfs").unwrap().notice, "httpfs is built into Pondra: nothing to install");
        assert_eq!(answered("LOAD httpfs").unwrap().notice, "httpfs is built into Pondra: nothing to load");
    }

    #[test]
    fn any_other_name_is_refused_by_name() {
        for sql in ["INSTALL spatial", "LOAD spatial", "FORCE INSTALL 'spatial'", "INSTALL spatial FROM community"] {
            let e = answered(sql).err().unwrap_or_else(|| panic!("{sql}: answered"));
            assert_eq!(crate::codes::of(&e), "0A000", "{sql}");
            let words = format!("{e:#}");
            assert!(words.starts_with("spatial isn't built into Pondra") && words.contains("1.1 (ADR-075)") && words.contains("httpfs"), "{words}");
        }
    }

    #[test]
    fn a_duckdb_extension_file_is_refused() {
        for sql in ["LOAD '/x/y.duckdb_extension'", "LOAD 'y.duckdb_extension'", "INSTALL 'x/httpfs'"] {
            let e = answered(sql).err().unwrap_or_else(|| panic!("{sql}: answered"));
            assert_eq!(crate::codes::of(&e), "0A000", "{sql}");
        }
    }

    #[test]
    fn the_rest_is_not_an_extension_statement() {
        for sql in [
            "LOAD DATA LOCAL INPATH 'x.csv' INTO TABLE t",
            "SELECT 1",
            "INSTALL",
            "INSTALL httpfs; SELECT 1",
            "FORCE LOAD httpfs",
            "INSTALL httpfs EXTRA",
            "LOAD httpfs, parquet",
            "LOAD_DATA x",
            "SET x = 1",
        ] {
            assert!(statement(sql).is_none(), "{sql}");
        }
    }

    #[test]
    fn a_row_names_it_for_a_host_with_no_door_of_its_own() {
        assert_eq!(row("INSTALL httpfs").unwrap().unwrap(), "SELECT 'httpfs is built into Pondra: nothing to install' AS notice");
        assert_eq!(row("LOAD aws").unwrap().unwrap(), "SELECT 'aws is built into Pondra: nothing to load' AS notice");
        assert_eq!(crate::codes::of(&row("INSTALL spatial").unwrap().unwrap_err()), "0A000");
        assert!(row("SELECT 1").is_none());
    }
}
