//! What a transaction's statements mean, and the words its refusals say, in every form (ADR-067): a node keeps its
//! transactions in `txn.rs` and a page in `page::txn`, each reading BEGIN's modes, a savepoint's name and a write's word
//! here, so the two forms take the same statements and say the same thing.
use anyhow::Result;
use datafusion::sql::sqlparser::ast::{Statement, TransactionAccessMode, TransactionIsolationLevel, TransactionMode};
use datafusion::sql::sqlparser::{dialect::GenericDialect, parser::Parser};

pub const SERIALIZABLE: &str = "SERIALIZABLE isn't kept here: REPEATABLE READ is snapshot isolation, which allows write skew; ask for it with SET TRANSACTION ISOLATION LEVEL REPEATABLE READ";

/// A BEGIN's warning when a transaction is open already (the BEGIN is taken, and changes nothing).
pub const ALREADY: &str = "there is already a transaction in progress";

/// A COMMIT's or a ROLLBACK's warning when no transaction is open.
pub const NONE_OPEN: &str = "there is no transaction in progress";

/// READ COMMITTED or REPEATABLE READ (ADR-067 §1). READ UNCOMMITTED is READ COMMITTED, as in Postgres.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Level {
    Committed,
    Repeatable,
}

impl Level {
    pub fn name(self) -> &'static str {
        match self {
            Level::Committed => "read committed",
            Level::Repeatable => "repeatable read",
        }
    }
}

/// The modes of a `BEGIN` (or `START TRANSACTION`) statement, as sqlparser reads them.
pub fn begin_modes(sql: &str) -> Result<Vec<TransactionMode>> {
    Ok(match Parser::parse_sql(&GenericDialect {}, sql)?.pop() {
        Some(Statement::StartTransaction { modes, .. }) => modes,
        _ => vec![],
    })
}

/// The level and access mode the modes name (None: not named). SERIALIZABLE, and the isolation levels that aren't built,
/// are refused by name (0A000); a mode named twice is refused (42601).
pub fn plan(modes: &[TransactionMode]) -> Result<(Option<Level>, Option<bool>)> {
    let (mut level, mut read_only) = (None, None);
    for m in modes {
        match m {
            TransactionMode::IsolationLevel(l) => {
                if level.is_some() {
                    return Err(crate::codes::coded("42601", "conflicting or redundant isolation levels"));
                }
                level = Some(match l {
                    TransactionIsolationLevel::ReadUncommitted | TransactionIsolationLevel::ReadCommitted => Level::Committed,
                    TransactionIsolationLevel::RepeatableRead => Level::Repeatable,
                    TransactionIsolationLevel::Serializable => return Err(crate::codes::coded("0A000", SERIALIZABLE)),
                    _ => return Err(crate::codes::coded("0A000", "that isolation level isn't built here: READ COMMITTED and REPEATABLE READ are")),
                });
            }
            TransactionMode::AccessMode(a) => {
                if read_only.is_some() {
                    return Err(crate::codes::coded("42601", "conflicting or redundant access modes"));
                }
                read_only = Some(matches!(a, TransactionAccessMode::ReadOnly));
            }
        }
    }
    Ok((level, read_only))
}

/// The name a savepoint statement gives: an unquoted one lower-cased, as Postgres folds it.
pub fn savepoint_name(sql: &str) -> Result<String> {
    let ident = match Parser::parse_sql(&GenericDialect {}, sql)?.pop() {
        Some(Statement::Savepoint { name }) | Some(Statement::ReleaseSavepoint { name }) => name,
        Some(Statement::Rollback { savepoint: Some(name), .. }) => name,
        _ => return Err(crate::codes::coded("42601", "a savepoint statement needs a savepoint's name")),
    };
    Ok(match ident.quote_style {
        Some(_) => ident.value,
        None => ident.value.to_lowercase(),
    })
}

/// The newest of `names` (oldest first) that is `name`, or the words of its refusal (3B001: `no_savepoint`), which fail
/// the transaction, as in Postgres.
pub fn mark_of<'a>(names: impl Iterator<Item = &'a str>, name: &str) -> Result<usize, String> {
    names.collect::<Vec<_>>().iter().rposition(|n| *n == name).ok_or_else(|| format!("savepoint \"{name}\" does not exist"))
}

/// A savepoint's refusal, with the words `mark_of` gave (3B001).
pub fn no_savepoint(why: &str) -> anyhow::Error {
    crate::codes::coded("3B001", why.to_string())
}

/// A savepoint word outside a transaction (25P01): `what` is the statement, as Postgres names it.
pub fn outside(what: &str) -> anyhow::Error {
    crate::codes::coded("25P01", format!("{what} can only be used in transaction blocks"))
}

/// The error for a statement in a failed transaction (25P02): until its end, only ROLLBACK (or COMMIT) goes on.
pub fn aborted(why: &str) -> anyhow::Error {
    crate::codes::coded("25P02", format!("current transaction is aborted, commands ignored until end of transaction block (it failed: {why})"))
}

/// The error for a write in a read-only transaction (25006), as Postgres words it.
pub fn read_only(word: &str) -> anyhow::Error {
    crate::codes::coded("25006", format!("cannot execute {word} in a read-only transaction"))
}

/// DDL in a REPEATABLE READ transaction (0A000): it runs at once, which the transaction's snapshot can't see.
pub fn ddl_repeatable() -> anyhow::Error {
    crate::codes::coded("0A000", "DDL in a REPEATABLE READ transaction: its snapshot can't see what the DDL makes. Run it before BEGIN or after COMMIT, or use READ COMMITTED (the default)")
}

/// DDL on a table this transaction has written (0A000): the writes were made against the shape it would change.
pub fn ddl_pending(name: &str) -> anyhow::Error {
    crate::codes::coded("0A000", format!("{name} has writes pending in this transaction: DDL on it would change the shape they were written against. COMMIT first, then run the DDL"))
}

/// The notice DDL in a transaction sends once it has run (ADR-067 §8): it ran as a commit of its own.
pub fn ran_at_once(word: &str) -> String {
    format!("{word} ran at once: DDL isn't part of a transaction here, and ROLLBACK won't undo it")
}

/// The words Postgres names a write by in 25006: None for a statement that reads. (Its text, before the parse.)
pub fn write_word(sql: &str) -> Option<String> {
    let words: Vec<String> = crate::text::first_word(sql).trim_end_matches(';').split_whitespace().take(2).map(|w| w.to_uppercase()).collect();
    let first = words.first()?.as_str();
    match first {
        "INSERT" | "UPDATE" | "DELETE" | "MERGE" | "TRUNCATE" | "GRANT" | "REVOKE" | "COMMENT" => Some(first.to_string()),
        "CREATE" | "DROP" | "ALTER" => Some(words.join(" ")),
        "COPY" => (!sql.to_uppercase().contains(" TO ")).then(|| "COPY FROM".to_string()),
        "WITH" => with_write(sql), // (its words don't say what it does: its parse does)
        _ => None,
    }
}

/// The write a `WITH` statement makes (`WITH … INSERT`, `… UPDATE`, `… DELETE`, `… MERGE`), by its parse; None for one
/// that only reads, or doesn't parse (the door reports that).
fn with_write(sql: &str) -> Option<String> {
    let Statement::Query(q) = Parser::parse_sql(&GenericDialect {}, sql).ok()?.pop()? else { return None };
    crate::text::write_of(&q).map(str::to_string)
}
