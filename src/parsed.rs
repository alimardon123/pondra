//! A statement's text parsed once for the front's looks at it. `write::parse` (what kind of statement it is), `front::expand`
//! (what to expand in it) and `expander::named_apart` (its answer's column names) each parsed the same text with sqlparser's
//! generic dialect: about 0.4M instructions each for a TPC-H query, where a copy of what one parse made is a fifteenth of that.
//! So the last `KEPT` texts are kept with what they parsed to, and each look takes its own copy, to change as it likes. A
//! text's tokens are kept the same way (`text::tokens`), by `kept`.
use datafusion::sql::sqlparser::ast::{self, Expr, Statement};
use datafusion::sql::sqlparser::dialect::GenericDialect;
use datafusion::sql::sqlparser::parser::{Parser, ParserError};
use std::collections::VecDeque;
use std::ops::ControlFlow;
use std::sync::{Arc, Mutex};

/// The texts kept: a statement's looks come one after the other, so the last few are what the next look asks for.
const KEPT: usize = 4;
/// A longer text (a bulk INSERT's rows) is parsed each time it is asked for: keeping its tree would hold its rows twice.
pub(crate) const LONGEST: usize = 64 << 10;

type Parsed = Arc<Result<Vec<Statement>, ParserError>>;

// (the last texts and what they parsed to: a function of the text (a tree kept, `keep`, is its text's parse), so the
// process's, the same for any lake here. An interim, as `text::TOKENS` is: step 8's statement context carries the parse instead)
static LAST: std::sync::LazyLock<Mutex<VecDeque<(String, Parsed)>>> = std::sync::LazyLock::new(Default::default);

/// `sql` parsed as `Parser::parse_sql(&GenericDialect {}, sql)` parses it, the same statements or the same error, but
/// for `signed`'s parentheses. Under the one-parse check (`checked`) each query is held to its print: it reads back as
/// it means (`reads_back`).
pub(crate) fn generic(sql: &str) -> Result<Vec<Statement>, ParserError> {
    if sql.len() > LONGEST {
        return Parser::parse_sql(&GenericDialect {}, sql).map(signed_all);
    }
    let parsed = kept(&LAST, sql, |sql| {
        let parsed = Parser::parse_sql(&GenericDialect {}, sql).map(signed_all);
        if let (Ok(stmts), true) = (&parsed, checked()) {
            // (a panic: a request's is answered as an error, so a gate's run names the text)
            for s in stmts.iter().filter(|s| matches!(s, Statement::Query(_))) {
                assert!(reads_back(s), "one parse: a query prints as a text that reads otherwise, for {sql}\nits print: {s}");
            }
        }
        Arc::new(parsed)
    });
    (*parsed).clone() // (copied outside the lock: another statement's look waits for no copy)
}

/// A minus before anything printed with a minus first (`- -3`, or `- $x` once -3 is bound to `$x`) with that operand in
/// parentheses, `-(-3)`: sqlparser prints it `--3`, which reads back as a comment to the line's end (`INSERT INTO w
/// SELECT id, - -k FROM t` kept its query as `SELECT id,`). Each parse of a text has it done (`generic`, `read`, a
/// session's dialect's in `write::parse`, Spark's), so any print of a tree a text parsed to reads back as it means; and
/// `text::sql`, for a tree built since. Whether any was.
pub(crate) fn signed(s: &mut Statement) -> bool {
    let mut any = false;
    let _ = crate::walk::visit_expressions_mut(s, |e| {
        if let Expr::UnaryOp { op: ast::UnaryOperator::Minus, expr } = e {
            // (after its children: `- - -1` is `-(-(-1))`)
            if !matches!(**expr, Expr::Identifier(_) | Expr::CompoundIdentifier(_) | Expr::Nested(_) | Expr::Function(_)) && expr.to_string().starts_with('-') {
                **expr = Expr::Nested(Box::new(std::mem::replace(&mut **expr, Expr::Value(ast::Value::Null.into()))));
                any = true;
            }
        }
        ControlFlow::<()>::Continue(())
    });
    any
}

/// `signed` on each statement.
pub(crate) fn signed_all(mut stmts: Vec<Statement>) -> Vec<Statement> {
    stmts.iter_mut().for_each(|s| {
        signed(s);
    });
    stmts
}

/// Whether `s` prints as a text that reads back as it means: one statement, `s` again (as `generic` parses it), or one
/// that prints as the same text (a print sqlparser reads back to another tree of the same text). A print that a comment
/// or a quote cut short reads back as less, or not at all.
pub(crate) fn reads_back(s: &Statement) -> bool {
    let printed = s.to_string();
    // (parsed here, not by `generic`: its check would ask this of the print again)
    match Parser::parse_sql(&GenericDialect {}, &printed).map(signed_all).as_deref() {
        Ok([back]) => back == s || back.to_string() == printed,
        _ => false,
    }
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
        Ok(stmts) => Ok(signed_all(stmts)),
        Err(e) if reached(&e) > reached(&refused) => Err(e),
        Err(_) => Err(refused),
    }
}

/// Pondra's SQL, a language of the registry's (`languages.rs`): the core's, read as the front reads a statement (`pondra`).
pub(crate) static LANGUAGE: crate::languages::Language = crate::languages::Language { name: "pondra", names: &[], parse: statements, inline: None, about: "Pondra's SQL" };

fn statements(sql: &str) -> anyhow::Result<Vec<Statement>> {
    Ok(pondra(sql)?)
}

/// `sql` as Pondra's SQL: as `read` reads it, and DuckDB's FROM first with clauses after it (`FROM t WHERE …`) as
/// `SELECT * FROM t WHERE …`; `read`'s error when neither reads it.
pub(crate) fn pondra(sql: &str) -> Result<Vec<Statement>, ParserError> {
    static LEADING_FROM: std::sync::LazyLock<regex::Regex> =
        std::sync::LazyLock::new(|| regex::Regex::new(r"(?is)^\s*(?:(?:--[^\n]*\n|/\*.*?\*/)\s*)*from\b").expect("a regex"));
    read(sql).or_else(|e| match LEADING_FROM.is_match(sql) {
        true => {
            let all = format!("SELECT * {sql}");
            Parser::parse_sql(&GenericDialect {}, &all).or_else(|_| crate::dialect::parse(&all).0).map_err(|_| e)
        }
        false => Err(e),
    })
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
        let theirs = Parser::parse_sql(&GenericDialect {}, sql).map(signed_all);
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

    /// A minus before a minus (`- -k`) is printed `-(-k)`, not `--k`, a comment to the line's end, by every tree a text
    /// parses to (the statements the core keeps as text: `write::tests::a_minus_before_a_minus_is_kept_as_it_means`).
    #[test]
    fn a_minus_before_a_minus_prints_as_it_means() {
        let one = |sql: &str| generic(sql).unwrap().remove(0);
        let q = one("SELECT - -k AS m, - - -1 AS n, 2 - -k AS o, -1 AS p, -k AS r FROM t");
        assert_eq!(q.to_string(), "SELECT -(-k) AS m, -(-(-1)) AS n, 2 - -k AS o, -1 AS p, -k AS r FROM t");
        assert!(reads_back(&q));
        assert_eq!(generic(&q.to_string()).unwrap(), [q.clone()], "the print parses to the tree");
        let plain = "SELECT -1 AS p, -k AS r, 2 - -k AS o FROM t WHERE x > -3";
        assert_eq!(generic(plain), Parser::parse_sql(&GenericDialect {}, plain), "no minus before a minus: sqlparser's own tree");
        // (sqlparser's own tree prints the comment: what the check holds a query to)
        let raw = Parser::parse_sql(&GenericDialect {}, "SELECT 1 AS id, - -k AS m FROM t").unwrap();
        assert!(raw[0].to_string().contains("--k") && !reads_back(&raw[0]), "{}", raw[0]);
        // (the fallback dialect's hooks: a list comprehension is read as array_transform)
        assert_eq!(read("SELECT [- -x FOR x IN [1, 2]] AS l").unwrap()[0].to_string(), "SELECT array_transform(([1, 2]), x -> (-(-x))) AS l");
    }

    /// The random queries' texts (`tools/random_sql.py --texts`, the nightly run's generator: its `(- -3)` was one) each
    /// print as a text that reads back as they mean (`reads_back`), a few thousand a run (`PONDRA_RANDOM_TEXTS`, seed
    /// `PONDRA_RANDOM_SEED`). The nightly run's nodes hold every query they read to the same (`generic`, `checked`).
    #[test]
    fn random_queries_print_as_they_mean() {
        let n = std::env::var("PONDRA_RANDOM_TEXTS").unwrap_or_else(|_| "3000".into());
        let seed = std::env::var("PONDRA_RANDOM_SEED").unwrap_or_else(|_| "1".into());
        let out = std::process::Command::new("python3").args(["tools/random_sql.py", "--texts", &n, "--seed", &seed]).output().expect("python3 runs tools/random_sql.py");
        assert!(out.status.success(), "random_sql.py --texts: {}", String::from_utf8_lossy(&out.stderr));
        let texts: Vec<String> = String::from_utf8(out.stdout).unwrap().lines().map(|l| serde_json::from_str(l).expect("a text a line, as JSON")).collect();
        assert_eq!(texts.len(), n.parse::<usize>().unwrap());
        let (mut minus, mut differ) = (0, vec![]);
        for sql in &texts {
            let stmts = Parser::parse_sql(&GenericDialect {}, sql).unwrap_or_else(|e| panic!("{sql}: {e}"));
            minus += stmts.iter().filter(|s| !reads_back(s)).count(); // (sqlparser's own tree: `(- -3)` prints `(--3)`)
            for s in signed_all(stmts) {
                if !reads_back(&s) {
                    differ.push(format!("{sql}\n  prints {s}"));
                }
            }
        }
        assert!(differ.is_empty(), "{} of {} print as a text that reads otherwise:\n{}", differ.len(), texts.len(), differ[..differ.len().min(5)].join("\n"));
        assert!(minus > 0 || texts.len() < 1000, "the texts have a minus before a minus, as the generator writes: none among {}", texts.len());
    }
}
