//! Runs one statement for any door: the steps every door used to run before the engine, in one place. A door reads
//! its wire into a `Request`, calls `run` with how it asks and what its encoding needs, and writes the `Answer` back
//! in its own encoding (HTTP's is in server.rs). A plain query is answered by a key lookup, a remembered answer or a
//! run; anything else is a script.

use crate::audit;
use crate::routines::{self, Outcome, Request, Who};
use crate::app::{App, Kept};
use crate::temp;
use anyhow::Result;
use bytes::Bytes;
use datafusion::arrow::record_batch::RecordBatch;
use serde_json::Value;
use std::borrow::Cow;
use std::sync::Arc;
use std::time::Duration;

/// How a door asks, beyond the text: a write's `job`, where it may run, and how fresh it must be.
pub struct How<'a> {
    pub job: Option<&'a str>,    // writes: a retry with the same job id is applied once
    pub spread: Option<&'a str>, // "1": run across the cluster even for small tables; "0": only here
    pub after: Option<u64>,      // read-your-writes: first wait until this node has seen segment `after` (from an ack)
    pub stale: Option<Duration>, // accept a cached result up to this old (see `Results`)
}

/// What a door's wire needs from the core's path, and nothing more.
pub trait Wire: Send + Sync {
    /// What this door's own caller takes: HTTP's `Rendered`, Postgres's and Flight's batches, MCP's JSON.
    type Own: Send;
    /// The results cache's name for this encoding (the key's start); None keeps nothing, and `remembers` isn't asked.
    fn kept_as(&self) -> Option<Cow<'_, str>>;
    /// This caller's answer from the rows, and the bytes to keep and share (None: not kept), as `App::remembered`'s
    /// run gives them. It runs inside `audit::statement`, so a refusal while writing the answer is the statement's.
    fn encode(&self, rows: Vec<RecordBatch>) -> Result<(Option<Bytes>, Self::Own)>;
    /// Does a key lookup's JSON answer this wire as it is (`serve::point_sql`: HTTP's JSON)?
    fn takes_point_json(&self) -> bool {
        false
    }
    /// The format a query's answer is handed over in, in pieces, as its plan makes them (`query::arm`); None: collected.
    fn pieces(&self, _query: &str) -> Option<Option<&str>> {
        None
    }
}

/// What `run` gives its door: the wire's own bytes (a key lookup's, or another run's remembered answer), this run's
/// answer as the wire's `encode` made it, the rows for the door to write (a script's last answer), a statement's
/// outcome that isn't rows, or `Sent`: the answer went to the door in pieces as its plan made them (`query::handed`).
pub enum Answer<O> {
    Kept(Bytes),
    Own(O),
    Rows(Vec<RecordBatch>),
    Done(Value),
    Sent,
}

/// Runs one statement a door was sent, for `who`, as `how` asks, and gives its answer as `wire` encodes it. The lake it was
/// sent to is in scope while it runs (`ext::rooted`), so every door stores a client's relative LOCATION as the same file.
pub async fn run<W: Wire>(app: &App, req: Request, how: How<'_>, who: Who, wire: &W) -> Result<Answer<W::Own>> {
    crate::ext::rooted(&app.lake.url, one_statement(app, req, how, who, wire)).await
}

async fn one_statement<W: Wire>(app: &App, req: Request, how: How<'_>, who: Who, wire: &W) -> Result<Answer<W::Own>> {
    if let Some(seg) = how.after {
        let mut hwm = app.lake.hwm.subscribe();
        let _ = tokio::time::timeout(Duration::from_secs(30), async { while app.lake.visible() < seg { hwm.changed().await.ok()?; } Some(()) }).await;
    }
    if req.params.is_empty() && req.tables.is_empty() && req.views.is_empty() {
        if let Some((sent, one)) = plain_of(app, &req.sql).await? {
            if !crate::txn::open() {
                crate::query::mark("request read and checked");
                return audit::statement(app, &sent, query(app, &one, &how, who.files, wire)).await; // (the audit log and history keep it as it was sent)
            }
        }
    }
    // A script sent without a session is one of its own while it runs: its SET, PREPARE, BEGIN
    // and temporary tables last to its end.
    let own = (temp::current().is_none() && routines::split(&req.sql).len() > 1).then(temp::of_script);
    let tables = Arc::new(req.tables);
    let script = crate::query::SENT.scope(tables, routines::script(app, &req.sql, &req.params, &req.views, who, how.job.map(str::to_string)));
    let out = match &own {
        Some(s) => temp::SESSION.scope(Some(s.clone()), script).await,
        None => script.await,
    };
    own.inspect(|s| _ = temp::end(s));
    Ok(match out? {
        Outcome::Rows(batches) => Answer::Rows(batches),
        Outcome::Done(v) => Answer::Done(v),
    })
}

/// The text as sent, when it is one statement that is no variable's change and no script: the first half of the test
/// `run` makes of a request for a plain query. `pondra sql` makes it of its argument too (cli.rs).
pub(crate) fn single_statement(sql: &str) -> Option<String> {
    let [one] = &routines::split(sql)[..] else { return None };
    (crate::vars::change(sql).is_none() && !crate::script::is(sql)).then(|| one.clone())
}

/// The second half, on the statement once its `$name`s and functions are expanded: none of a checkpoint, a procedure's
/// CALL, a write, transaction control, a setting or several statements. Such a statement is answered from a read, and a
/// `pondra sql` process leaves it to a guest that reads (`node::Guest::Reads`).
pub(crate) fn plain_statement(one: &str) -> bool {
    !crate::write::checkpoint(one) && !routines::runs_procedure(one) && crate::write::parse(one).is_none() && !routines::several(one) && crate::text::control(one).is_none() && !crate::settings::is(one)
}

/// A plain query's text as sent, and the statement it runs as (its `$name`s and macros expanded, `vars::bound` and
/// `expand`): the pair when `sql` is one plain statement (`single_statement`, `plain_statement`); None for a script, a
/// change or a write, which `run` runs whole. Flight plans a query from the second half without running it.
pub(crate) async fn plain_of(app: &App, sql: &str) -> Result<Option<(String, String)>> {
    let Some(sent) = single_statement(sql) else { return Ok(None) };
    let one = routines::expand(&app.lake, &crate::vars::bound(&sent)?).await?; // (`$name`: the session's variables)
    Ok(plain_statement(&one).then_some((sent, one)))
}

/// Changes data, the lake or a transaction (INSERT, CREATE, CALL, EXECUTE TASK, CHECKPOINT, BEGIN…) in any of `sql`'s
/// statements: anything but a query, or a setting or variable of the session's own. A read door (MCP's query tool, a
/// Flight ticket) refuses it by name, as Postgres's read-only transaction does. (What it takes is listed, not what it
/// refuses, so a statement kind added later is refused until it is called a read.)
pub(crate) fn is_write(sql: &str) -> bool {
    routines::split(sql).iter().any(|one| !(plain_statement(one) || crate::settings::is(one) || crate::vars::change(one).is_some()))
}

/// Step 2's inside, on a statement already expanded (`run` makes it, inside `audit::statement`): the key lookup, the
/// remembered answer or the run. Postgres's door enters here too, with its own text and its own audit.
pub(crate) async fn query<W: Wire>(app: &App, one: &str, how: &How<'_>, files: bool, wire: &W) -> Result<Answer<W::Own>> {
    let limited = crate::auth::limited().is_some(); // (a user granted some tables: planned as it may read them, never answered from what another read, nor kept: its grants may change)
    if wire.takes_point_json() && !crate::temp::mentioned(one) && !limited {
        if let Some(body) = crate::serve::point_sql(&app.lake, one).await? {
            return Ok(Answer::Kept(body.into())); // a key lookup: no planning
        }
    }
    // The program that started the node (`files`) has its answers remembered apart, run with its
    // rights: a file here it reads is named in the statement as expanded, or in a view it reads, and
    // so never remembered.
    let remembered = match wire.kept_as() {
        Some(kept) => app.remembers(one).await.map(|version| (kept, version)),
        None => None,
    };
    let Some((kept, version)) = remembered else {
        crate::query::mark("answer kept?");
        return Ok(run_once(app, how, one, files, wire).await?.1);
    };
    let key = format!("{kept}|{}|{}|{one}", how.spread.unwrap_or(""), if files { "owner" } else { "" });
    let stale = how.stale.filter(|_| how.after.is_none()); // (read-your-writes wins)
    Ok(match app.remembered(key, one, version, stale, || run_once(app, how, one, files, wire)).await? {
        Kept::Bytes(body) => Answer::Kept(body),
        Kept::Own(answer) => answer,
    })
}

/// One run of the query, as `remembered` runs it for everyone asking at the same moment: the answer encoded for the wire
/// (inside the statement's audit), with the bytes to keep when it has them; `Sent` when the plan already handed the
/// answer to the door.
async fn run_once<W: Wire>(app: &App, how: &How<'_>, one: &str, files: bool, wire: &W) -> Result<(Option<Bytes>, Answer<W::Own>)> {
    let batches = {
        // (an answer in a format that streams may be sent to the door from its plan: `query::collect_or_send`)
        let _armed = wire.pieces(one).map(crate::query::arm);
        match files {
            true => app.query_as(&crate::sys::planned(one)?, Some("0"), true).await?, // (a file here: this node only)
            false => app.query(one, how.spread).await?,
        }
    };
    if crate::query::handed().is_some() {
        return Ok((None, Answer::Sent)); // (its answer went to the door in pieces as its plan made them)
    }
    let (keep, own) = wire.encode(batches)?;
    crate::query::mark("answer rendered");
    Ok((keep, Answer::Own(own)))
}
