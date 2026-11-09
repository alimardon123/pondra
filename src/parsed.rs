//! A statement's text parsed once for the front's looks at it. `write::parse` (what kind of statement it is), `front::expand`
//! (what to expand in it) and `expander::named_apart` (its answer's column names) each parsed the same text with sqlparser's
//! generic dialect: about 0.4M instructions each for a TPC-H query, where a copy of what one parse made is a fifteenth of that.
//! So the last `KEPT` texts are kept with what they parsed to, and each look takes its own copy, to change as it likes. A
//! text's tokens are kept the same way (`text::tokens`), by `kept`.
use datafusion::sql::sqlparser::ast::Statement;
use datafusion::sql::sqlparser::dialect::GenericDialect;
use datafusion::sql::sqlparser::parser::{Parser, ParserError};
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

/// The texts kept: a statement's looks come one after the other, so the last few are what the next look asks for.
const KEPT: usize = 4;
/// A longer text (a bulk INSERT's rows) is parsed each time it is asked for: keeping its tree would hold its rows twice.
pub(crate) const LONGEST: usize = 64 << 10;

type Parsed = Arc<Result<Vec<Statement>, ParserError>>;

// (the last texts and what they parsed to: a per-node static, so a node's stop lets them go. An interim, as `text::TOKENS`
// is: step 8's statement context carries the parse instead)
crate::per_node!(static LAST: Mutex<VecDeque<(String, Parsed)>> = Mutex::new(VecDeque::new()));

/// `sql` parsed as `Parser::parse_sql(&GenericDialect {}, sql)` parses it, the same statements or the same error.
pub(crate) fn generic(sql: &str) -> Result<Vec<Statement>, ParserError> {
    if sql.len() > LONGEST {
        return Parser::parse_sql(&GenericDialect {}, sql);
    }
    let parsed = kept(&LAST, sql, |sql| Arc::new(Parser::parse_sql(&GenericDialect {}, sql)));
    (*parsed).clone() // (copied outside the lock: another statement's look waits for no copy)
}

/// `sql` read as the front reads it: as `generic` parses it, and a text generic refuses parsed again with the fallback
/// dialect's hooks (`dialect.rs`), which read only texts generic refuses. When both refuse, the error of the one that
/// read further into the text (a typo inside a hook's form points there), generic's on a tie. Not kept: generic's own
/// answer for the text is its refusal.
pub(crate) fn read(sql: &str) -> Result<Vec<Statement>, ParserError> {
    let refused = match generic(sql) {
        Ok(stmts) => return Ok(stmts),
        Err(e) => e,
    };
    let (theirs, used) = crate::dialect::parse(sql);
    if crate::passes::checked() && theirs.is_ok() {
        // (a panic: a request's is answered as an error, so a gate's run names the hook whose trigger is short)
        if let Err(e) = crate::passes::get().hooks_fired(sql, &used) {
            panic!("{e}");
        }
    }
    match theirs {
        Ok(stmts) => Ok(stmts),
        Err(e) if reached(&e) > reached(&refused) => Err(e),
        Err(_) => Err(refused),
    }
}

/// Where in the text a parse error stood, as sqlparser says it (`at Line: l, Column: c`; the text's end for `found:
/// EOF`), to compare two errors: (0, 0) when it says nowhere.
fn reached(e: &ParserError) -> (u64, u64) {
    let said = e.to_string();
    if said.contains("found: EOF") {
        return (u64::MAX, u64::MAX);
    }
    let at = |key: &str| said.rsplit_once(key).and_then(|(_, rest)| rest.split(|c: char| !c.is_ascii_digit()).next()?.parse().ok());
    match (at("at Line: "), at(", Column: ")) {
        (Some(l), Some(c)) => (l, c),
        _ => (0, 0),
    }
}

/// `sql`'s parse when one of the last few looks made it (`generic`), and None when none did: nothing is parsed here. The
/// engine plans a query from it rather than parse the text again (`engine::planned`).
pub(crate) fn already(sql: &str) -> Option<Parsed> {
    LAST.lock().unwrap_or_else(|e| e.into_inner()).iter().find(|(text, _)| text == sql).map(|(_, parsed)| parsed.clone())
}

/// `stmts` kept as `sql`'s parse, for a tree that `sql` is its text of (`text::text_kept`): a look or the engine that then
/// asks for `sql`'s parse takes them, and the text is not parsed again. It parses back to the same statements (sqlparser's
/// `Display`). Under the one-parse check (`checked`) each tree kept is held to the text's own parse here, and the one the
/// engine plans from to DataFusion's parse of the text too (`engine::datafusions_parse`).
pub(crate) fn keep(sql: &str, stmts: Vec<Statement>) {
    if sql.len() > LONGEST {
        return;
    }
    if checked() {
        let theirs = Parser::parse_sql(&GenericDialect {}, sql);
        // (a panic: a request's is answered as an error, so a gate's run names the text; keep says nothing to its caller)
        assert!(theirs.as_ref().is_ok_and(|t| *t == stmts), "one parse: a tree kept is not its text's parse, for {sql}\nthe tree: {stmts:?}\nthe text's: {theirs:?}");
    }
    kept(&LAST, sql, |_| Arc::new(Ok(stmts)));
}

/// Whether the front's trees are checked against the parse of their text (`keep`, `engine::datafusions_parse`):
/// `env::front_checked`.
pub(crate) fn checked() -> bool { crate::env::front_checked() }

/// What `make` makes of `sql`, kept with it among the last `KEPT` texts of `last` (the caller leaves out a text past
/// `LONGEST`): made once while the text is among them. The lock is held for the look and the keeping, never for `make`.
pub(crate) fn kept<T: Clone>(last: &Mutex<VecDeque<(String, T)>>, sql: &str, make: impl FnOnce(&str) -> T) -> T {
    if let Some(made) = last.lock().unwrap_or_else(|e| e.into_inner()).iter().find(|(text, _)| text == sql).map(|(_, made)| made.clone()) {
        return made;
    }
    let made = make(sql);
    let mut last = last.lock().unwrap_or_else(|e| e.into_inner());
    if last.len() == KEPT {
        last.pop_front();
    }
    last.push_back((sql.to_string(), made.clone()));
    made
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_text_parses_as_sqlparser_parses_it_again_and_again() {
        for sql in ["SELECT a, b FROM t WHERE c = 'x'", "SELECT 1; SELECT 2", "SELECT FROM WHERE", "INSERT INTO t VALUES (1)"] {
            let want = Parser::parse_sql(&GenericDialect {}, sql);
            for _ in 0..3 {
                assert_eq!(generic(sql), want, "{sql}");
            }
        }
        // (more texts than are kept: each still parses as it does, the first one too, parsed again)
        for i in 0..KEPT + 2 {
            let sql = format!("SELECT {i}");
            assert_eq!(generic(&sql), Parser::parse_sql(&GenericDialect {}, &sql));
        }
        assert_eq!(generic("SELECT 0"), Parser::parse_sql(&GenericDialect {}, "SELECT 0"));
        let long = format!("SELECT '{}'", "x".repeat(LONGEST));
        assert_eq!(generic(&long), Parser::parse_sql(&GenericDialect {}, &long), "a text past LONGEST is parsed, not kept");
    }
}
