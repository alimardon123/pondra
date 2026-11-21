//! A statement's context (redesign step 8, PR 9 (4)): what one statement was read as, made once and carried with it from
//! its door through the binder, the front and the planner, and gone with it. Each text the statement is read as (as sent,
//! its language's print, the front's print) is parsed once (`parsed::generic`, or a tree kept as a text's parse,
//! `parsed::keep`) and tokenized once (`text::tokens`) while the statement runs, and the engine plans from the front's
//! parse (`parsed::already`). Nothing in it is another statement's: before it, a process's last few texts were kept for
//! whichever statement asked next (`parsed::LAST`, `text::TOKENS`), so two statements running at once pushed each other's
//! out, and a text was kept past its statement's end.
//!
//! It goes with the statement's task, as the request's other state does (`temp::SESSION`, `rights`): each door scopes
//! one for each statement it runs (`scope`: `statement::run`, the Postgres door's messages, a script's or a procedure's
//! statements in `routines::one`, each finding what the request around it read), and work a statement hands to another
//! task takes it along (`carried`). A text read where no statement runs (a test, a background writer) is parsed each time
//! it is asked for.
use datafusion::sql::sqlparser::ast::Statement;
use datafusion::sql::sqlparser::parser::ParserError;
use std::collections::VecDeque;
use std::future::Future;
use std::sync::{Arc, Mutex};

/// A text's parse, as `Parser::parse_sql` gives it (`parsed::generic`).
pub(crate) type Parsed = Arc<Result<Vec<Statement>, ParserError>>;
/// A text's tokens, None where the tokenizer refuses it (`text::tokens`).
pub(crate) type Tokened = Option<Arc<crate::text::Tokens>>;

/// The texts one statement keeps: a statement's looks come one after the other (its door, the binder, the front's passes,
/// the looks after it, the planner), and each reads the text the one before it printed, so the last few are what the
/// next look asks for. More than the process's four were: a statement that names views and macros reads their bodies too.
const KEPT: usize = 8;

tokio::task_local! {
    /// The statement this task runs, if any (`scope`).
    static THIS: Arc<Context>;
}

/// One statement's context. What it holds next (step 8, l6/step8-pr9-plan.md): the text as sent and its language, the
/// front's facts (`front::Facts`), the statement's kind and its point lookup, so the door, the binder and the point read
/// them here rather than look at the text again; and for places (local-cloud C1, design only) each object it reads or
/// writes with its place and the version read there, and the places the request has passed.
#[derive(Default)]
pub(crate) struct Context {
    /// The statement it runs inside (a script's or a procedure's statement inside the request that sent it): what that one
    /// read, this one finds too, and what this one reads stays its own. A one-statement request run as a script reads its
    /// text once, not once at its door and again as the script's statement.
    within: Option<Arc<Context>>,
    /// Each text it was parsed as, with its parse: the most recent last.
    parses: Ring<Parsed>,
    /// Each text it was tokenized as, with its tokens.
    tokens: Ring<Tokened>,
}

impl Context {
    /// A context for a statement about to run, on its own (a prepared statement's).
    pub(crate) fn new() -> Arc<Context> { Arc::default() }

    /// A context for a statement about to run inside the one running here, if any (`scope`).
    fn within_current() -> Arc<Context> { Arc::new(Context { within: current(), ..Default::default() }) }

    /// This statement's context, then the ones it runs inside.
    fn chain(&self) -> impl Iterator<Item = &Context> { std::iter::successors(Some(self), |c| c.within.as_deref()) }
}

/// `go`, run as one statement: what it reads, it reads in a context of its own (a statement inside it, a script's or a
/// procedure's, in its own again).
pub(crate) fn scope<F: Future>(go: F) -> impl Future<Output = F::Output> { THIS.scope(Context::within_current(), go) }

/// `go` run in `context`: a statement's, made before (a prepared statement's, for each message that works on it), or the
/// one running here (`current`), for a task it hands work to.
pub(crate) fn carried<F: Future>(context: Arc<Context>, go: F) -> impl Future<Output = F::Output> { THIS.scope(context, go) }

/// `f` run as one statement, for a caller that isn't async (a test's look at a text).
#[cfg(test)]
pub(crate) fn sync_scope<R>(f: impl FnOnce() -> R) -> R { THIS.sync_scope(Context::within_current(), f) }

/// The context of the statement this task runs, if one does.
pub(crate) fn current() -> Option<Arc<Context>> { THIS.try_with(Arc::clone).ok() }

/// `sql`'s parse: the one the running statement made of it, or `make`'s, kept for the statement's later looks. Where no
/// statement runs, `make`'s, kept nowhere.
pub(crate) fn parse(sql: &str, make: impl FnOnce(&str) -> Parsed) -> Parsed {
    match current() {
        Some(c) => kept(&c, |c| &c.parses, sql, make),
        None => make(sql),
    }
}

/// `sql`'s tokens, as `parse` does for its parse.
pub(crate) fn tokens(sql: &str, make: impl FnOnce(&str) -> Tokened) -> Tokened {
    match current() {
        Some(c) => kept(&c, |c| &c.tokens, sql, make),
        None => make(sql),
    }
}

/// `sql`'s parse when the running statement made one (`parse`, `keep`), and None when it didn't: nothing is parsed here.
pub(crate) fn already(sql: &str) -> Option<Parsed> {
    THIS.try_with(|c| found(c, |c| &c.parses, sql)).ok().flatten()
}

/// `stmts` kept as `sql`'s parse in the running statement's context (none: nothing is kept).
pub(crate) fn keep(sql: &str, stmts: Vec<Statement>) {
    let _ = THIS.try_with(|c| kept(c, |c| &c.parses, sql, |_| Arc::new(Ok(stmts))));
}

type Ring<T> = Mutex<VecDeque<(String, T)>>;

/// What `sql` was made into among the last texts of `context` (`ring` of it) or of a statement it runs inside.
fn found<T: Clone>(context: &Context, ring: impl Fn(&Context) -> &Ring<T>, sql: &str) -> Option<T> {
    context.chain().find_map(|c| ring(c).lock().unwrap_or_else(|e| e.into_inner()).iter().rev().find(|(text, _)| text == sql).map(|(_, made)| made.clone()))
}

/// `make(sql)`, or what it made for `sql` before (`found`), kept among the last `KEPT` texts of `context`'s `ring`.
fn kept<T: Clone>(context: &Context, ring: impl Fn(&Context) -> &Ring<T>, sql: &str, make: impl FnOnce(&str) -> T) -> T {
    if let Some(made) = found(context, &ring, sql) {
        return made;
    }
    let made = make(sql); // (outside the lock: a look's parse holds up none of the statement's other looks)
    let mut last = ring(context).lock().unwrap_or_else(|e| e.into_inner());
    if last.len() == KEPT {
        last.pop_front();
    }
    last.push_back((sql.to_string(), made.clone()));
    made
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parsed(n: &std::sync::atomic::AtomicUsize) -> impl FnOnce(&str) -> Parsed + '_ {
        move |sql| {
            n.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            Arc::new(crate::parsed::read(sql))
        }
    }

    #[test]
    fn a_statement_parses_a_text_once_and_keeps_it_to_itself() {
        let n = std::sync::atomic::AtomicUsize::new(0);
        let count = || n.load(std::sync::atomic::Ordering::Relaxed);
        sync_scope(|| {
            parse("SELECT 1", parsed(&n));
            parse("SELECT 1", parsed(&n));
            assert_eq!(count(), 1, "one parse in a statement");
            assert!(already("SELECT 1").is_some());
            sync_scope(|| {
                assert!(already("SELECT 1").is_some(), "a statement inside it reads what it read");
                parse("SELECT 4", parsed(&n));
            });
            assert!(already("SELECT 4").is_none(), "and keeps what it reads to itself");
            keep("SELECT 2", crate::parsed::read("SELECT 2").unwrap());
            assert!(already("SELECT 2").is_some(), "a tree kept is its text's parse");
        });
        assert!(already("SELECT 1").is_none(), "gone with its statement");
        sync_scope(|| assert!(already("SELECT 1").is_none(), "another statement's context has none of it"));
        parse("SELECT 1", parsed(&n));
        parse("SELECT 1", parsed(&n));
        assert_eq!(count(), 4, "where no statement runs, a text is parsed each time");
        keep("SELECT 3", vec![]);
        assert!(already("SELECT 3").is_none(), "and nothing is kept");
    }

    #[test]
    fn a_statement_keeps_its_last_texts() {
        let n = std::sync::atomic::AtomicUsize::new(0);
        sync_scope(|| {
            for i in 0..=KEPT {
                parse(&format!("SELECT {i}"), parsed(&n));
            }
            assert!(already("SELECT 0").is_none() && already(&format!("SELECT {KEPT}")).is_some(), "the oldest goes first");
        });
    }
}
