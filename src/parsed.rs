//! A statement's text parsed once for the front's looks at it. `write::parse` (what kind of statement it is), `front::expand`
//! (what to expand in it) and `expander::named_apart` (its answer's column names) each parsed the same text with sqlparser's
//! generic dialect: about 0.4M instructions each for a TPC-H query, where a copy of what one parse made is a fifteenth of that.
//! So the last `KEPT` texts are kept with what they parsed to, and each look takes its own copy, to change as it likes.
use datafusion::sql::sqlparser::ast::Statement;
use datafusion::sql::sqlparser::dialect::GenericDialect;
use datafusion::sql::sqlparser::parser::{Parser, ParserError};
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

/// The texts kept: a statement's looks come one after the other, so the last few are what the next look asks for.
const KEPT: usize = 4;
/// A longer text (a bulk INSERT's rows) is parsed each time it is asked for: keeping its tree would hold its rows twice.
const LONGEST: usize = 64 << 10;

type Parsed = Arc<Result<Vec<Statement>, ParserError>>;

// (the last texts and what they parsed to: a per-node static, so a node's stop lets them go)
crate::per_node!(static LAST: Mutex<VecDeque<(String, Parsed)>> = Mutex::new(VecDeque::new()));

/// `sql` parsed as `Parser::parse_sql(&GenericDialect {}, sql)` parses it, the same statements or the same error.
pub(crate) fn generic(sql: &str) -> Result<Vec<Statement>, ParserError> {
    if sql.len() > LONGEST {
        return Parser::parse_sql(&GenericDialect {}, sql);
    }
    let kept = LAST.lock().unwrap_or_else(|e| e.into_inner()).iter().find(|(text, _)| text == sql).map(|(_, parsed)| parsed.clone());
    let parsed = match kept {
        Some(parsed) => parsed,
        None => {
            let parsed: Parsed = Arc::new(Parser::parse_sql(&GenericDialect {}, sql));
            let mut last = LAST.lock().unwrap_or_else(|e| e.into_inner());
            if last.len() == KEPT {
                last.pop_front();
            }
            last.push_back((sql.to_string(), parsed.clone()));
            parsed
        }
    };
    (*parsed).clone() // (copied outside the lock: another statement's look waits for no copy)
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
