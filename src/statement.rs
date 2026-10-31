//! Runs one statement for any door: the steps every door used to run before the engine, in one place. A door reads
//! its wire into a `Request`, calls `run` with how it asks and what its encoding needs, and writes the `Answer` back
//! in its own encoding (HTTP's, `HttpWire`, is here, as `sql` is). A plain query is answered by a key lookup, a remembered answer or a
//! run; anything else is a script. The steps every `/sql` takes are here too (`sql`), for every door that reads one.

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

/// Runs one statement a door was sent, for `who`, as `how` asks, and gives its answer as `wire` encodes it.
pub async fn run<W: Wire>(app: &App, req: Request, how: How<'_>, who: Who, wire: &W) -> Result<Answer<W::Own>> {
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

/// `POST /sql`'s options, as the HTTP door reads them from its query string (`axum`'s `Query`): what each one asks is its comment.
#[derive(serde::Deserialize, Default)]
pub struct SqlParams {
    pub format: Option<String>, // json (default), table (text), arrow (Arrow IPC stream), typed (columns and types, then rows: the console's)
    pub after: Option<u64>,     // read-your-writes: first wait until this node has seen segment `after` (from an ack)
    pub spread: Option<String>, // "1": run across the cluster even for small tables; "0": only here
    pub stale_ms: Option<u64>,  // accept a cached result up to this old (see `Results`)
    pub job: Option<String>,    // writes: a retry with the same job id is applied once
    pub rows: Option<usize>,    // typed: the rows of the first page (the console's rows a page; `SHOWN` if not said)
    pub catalog: Option<String>, // "postgres": Postgres's catalog, as the Postgres port answers it (the DB-API, SQLAlchemy: `pg_catalog`)
}

/// Is this request from whoever started the node here (the shell's or `local()`'s `PONDRA_OWNER_KEY`, a
/// library's host's key: the node's `Auth::owned_by`)? Then its SQL may read files on this machine, as
/// `pondra sql` may (invariant 21: nobody else's).
pub(crate) fn owner(auth: &crate::auth::Auth, headers: &http::HeaderMap) -> bool {
    let token = headers.get("authorization").and_then(|v| v.to_str().ok()).and_then(|v| v.strip_prefix("Bearer "));
    crate::auth::lent(token).is_some_and(|l| l.1) // (a Python procedure the owner called)
        || headers.get("x-pondra-owner").and_then(|h| h.to_str().ok()).is_some_and(|h| auth.is_owner(h))
}

/// The header a `/sql` answer carries while its session holds temporary tables (`temp::holds`): the client keeps to this
/// node meanwhile. Each door adds it (HTTP's `held_session`, a page's `lake_sql`).
pub const HELD_SESSION: (&str, &str) = ("x-pondra-session", "held");

/// What the statement's procedures printed, for the caller: a JSON list of lines (all of it in ASCII, the first 32 KB of
/// it; the run log has the rest), as the text of the `x-pondra-notices` header. None when nothing was said.
pub fn notices(heard: &[String]) -> Option<String> {
    if heard.is_empty() {
        return None;
    }
    let (mut kept, mut size) = (vec![], 0);
    for n in heard {
        size += n.len() + 4;
        if size > 32 << 10 {
            kept.push("…".to_string());
            break;
        }
        kept.push(n.clone());
    }
    let text = serde_json::to_string(&kept).ok()?;
    let ascii: String = text.chars().map(|c| if c.is_ascii() && !c.is_ascii_control() { c.to_string() } else { c.encode_utf16(&mut [0; 2]).iter().map(|u| format!("\\u{u:04x}")).collect() }).collect();
    Some(ascii)
}

/// What one `/sql` request answers, by any door that reads one (the HTTP door, a page): its reply, the notices its
/// statements said, and its session, which the door says it holds while it has temporary tables (`temp::holds`).
pub struct Said {
    pub reply: anyhow::Result<Reply>,
    pub heard: Vec<String>,
    pub session: Option<String>,
}

/// A `/sql` answer: rows as its wire renders them, or a statement's outcome that isn't rows (JSON).
pub enum Reply {
    Rendered(Rendered),
    Done(Value),
}

/// An answer as the HTTP door sends it: rendered whole (what `Results` keeps and the others asking at
/// the same moment share), or as a body in pieces, for an answer too big to render as one, or
/// `Sent`: its answer went to the door in pieces as its plan made them (`query::collect_or_send`), and the
/// door already has its body (`sql`).
pub enum Rendered {
    Whole(Bytes),
    /// An answer already collected, sent in pieces of about 1 MB of Arrow, each written as `render` writes its rows
    /// (`query::hand_out`), so the pieces together are the answer's bytes. The first piece is written before the body
    /// starts, so a format that can't write the answer's columns (a list in CSV) is refused as `render` refuses it,
    /// with its message; a failure later cuts the body short. (The door sends it: `server::pieces`.)
    Pieces(crate::query::Handed),
    Sent,
}

/// One `/sql` request (its query, its headers, its body), for `role`, with every step a `/sql` takes around
/// `statement::run` (the session, the owner's files, …): server.rs's `sql`, moved.
pub async fn sql(app: App, p: SqlParams, role: crate::auth::Role, headers: http::HeaderMap, body: Bytes) -> Said {
    let files = owner(&app.auth, &headers); // (the program that started this node: its files, and URLs no secret covers)
    let session = match crate::temp::of(&headers) { // (its temporary tables: `temp.rs`)
        Ok(session) => session,
        Err(e) => return Said { reply: Err(e), heard: vec![], session: None }, // (a session name that isn't one: refused, saying what one is)
    };
    let postgres = match p.catalog.as_deref() {
        None => false,
        Some("postgres") => true,
        Some(other) => return Said { reply: Err(crate::codes::coded("22023", format!("catalog={other}: the catalog a request may ask for is postgres (Postgres's, as the Postgres port answers it)"))), heard: vec![], session: None },
    };
    let token = headers.get("authorization").and_then(|v| v.to_str().ok()).and_then(|v| v.strip_prefix("Bearer "));
    let vars = crate::auth::lent_vars(token); // (Python code a run lent a connection to: the run's variables)
    let (hand, mut handed) = tokio::sync::oneshot::channel(); // (the door's end of an answer in pieces: `query::Pieces`)
    let statement = crate::query::PIECES.scope(crate::query::Pieces::new(hand), sql_as(app, p, role, headers, body));
    // (the answer's memory is counted here until its body is sent: `query::collect`, `answer`)
    let go = crate::query::HELD.scope(Default::default(), crate::vars::within(vars.clone(), crate::temp::SESSION.scope(session.clone(), crate::ext::scope(files, crate::pg_catalog::speaking(postgres, Box::pin(statement))))));
    // (`guard` gave this request its principal and its apply flag, outside this handler: a request whose answer is
    // handed over runs on after it, and its history and audit still need who asked, so `auth::carried` takes WHO here)
    let applying = crate::protect::APPLYING.try_with(|_| ()).is_ok();
    let applied = async move { if applying { crate::protect::APPLYING.scope((), go).await } else { go.await } };
    let mut go = Box::pin(crate::query::traced(crate::auth::carried(crate::routines::with_notices(crate::python::holder(vars.is_some(), applied))))); // (a lent token's code holds a slot: what it calls takes none; boxed: a statement's future is big; traced with PONDRA_TRACE_QUERY)
    let (out, heard) = tokio::select! {
        biased;
        done = &mut go => done,
        Ok(h) = &mut handed => {
            // Its answer goes in pieces as its plan makes them (`query::send_rest`): the rest of the request, the
            // pieces and the statement's end, is polled on the queries' runtime while its client reads; the plan's
            // partition tasks stay where the prefix spawned them. The node may still stop it for memory (`oom.rs`).
            crate::panics::aside(crate::panics::work(true, go));
            return Said { reply: Ok(Reply::Rendered(Rendered::Pieces(h))), heard: vec![], session };
        }
    };
    match handed.try_recv() {
        Ok(h) => Said { reply: Ok(Reply::Rendered(Rendered::Pieces(h))), heard, session }, // (handed over as the request ended: its rows still come in pieces)
        Err(_) => Said { reply: out, heard, session },
    }
}

/// `sql`'s statement, as HTTP asks it: its text as its content type says (`routines::Request`), how it asks (`How`), and who.
async fn sql_as(app: App, p: SqlParams, role: crate::auth::Role, headers: http::HeaderMap, body: Bytes) -> anyhow::Result<Reply> {
    let kind = headers.get("content-type").and_then(|v| v.to_str().ok()).unwrap_or_default();
    let req = crate::routines::Request::read(kind, &body)?;
    // (a Python procedure's own calls, through the token lent to it; anyone else's is a first call)
    let lent = crate::auth::lent(headers.get("authorization").and_then(|v| v.to_str().ok()).and_then(|v| v.strip_prefix("Bearer "))).is_some();
    let depth = headers.get("x-pondra-depth").filter(|_| lent).and_then(|v| v.to_str().ok()?.parse().ok()).unwrap_or(0);
    let who = Who { role, files: owner(&app.auth, &headers), depth };
    let how = How { job: p.job.as_deref(), spread: p.spread.as_deref(), after: p.after, stale: p.stale_ms.map(Duration::from_millis) };
    Ok(match crate::statement::run(&app, req, how, who, &HttpWire(&p)).await? {
        Answer::Kept(body) => Reply::Rendered(Rendered::Whole(body)),
        Answer::Own(r) => Reply::Rendered(r),
        Answer::Rows(batches) => Reply::Rendered(answer(&batches, &p)?),
        Answer::Done(v) => Reply::Done(v),
        Answer::Sent => Reply::Rendered(Rendered::Sent),
    })
}

/// HTTP's encoding of an answer, for `statement::run`: JSON (or the format `?format=` names), with the console's typed
/// first page of `?rows=` rows.
struct HttpWire<'a>(&'a SqlParams);

impl Wire for HttpWire<'_> {
    type Own = Rendered;
    fn kept_as(&self) -> Option<Cow<'_, str>> {
        // (the key starts with the format, JSON when none is given, then a typed first page's rows)
        let format = self.0.format.as_deref().unwrap_or("json");
        Some(match self.0.rows {
            None => Cow::Borrowed(format),
            Some(n) => Cow::Owned(format!("{format}:{n}")),
        })
    }
    fn encode(&self, rows: Vec<RecordBatch>) -> Result<(Option<Bytes>, Rendered)> {
        let r = answer(&rows, self.0)?;
        let keep = match &r {
            Rendered::Whole(b) => Some(b.clone()),
            Rendered::Pieces(_) | Rendered::Sent => None, // (a big answer: sent to its own caller only)
        };
        Ok((keep, r))
    }
    fn takes_point_json(&self) -> bool {
        self.0.format.as_deref().unwrap_or("json") == "json"
    }
    fn pieces(&self, query: &str) -> Option<Option<&str>> {
        (crate::answer::streams(self.0.format.as_deref()) && !crate::explain::asked(query)).then_some(self.0.format.as_deref())
    }
}

/// Rows as `p` asks: `render`'s, a typed answer's first page as many rows as `?rows=` says, or, from
/// `PONDRA_STREAM_MB` of Arrow up, in pieces (`query::hand_out`).
fn answer(batches: &[RecordBatch], p: &SqlParams) -> Result<Rendered> {
    match (p.format.as_deref(), p.rows) {
        (Some("typed"), Some(n)) => Ok(Rendered::Whole(crate::answer::typed(batches, n.clamp(1, crate::answer::MOST), crate::pages::keep)?.into())),
        (format, _) if crate::answer::streams(format) && big(batches) => Ok(Rendered::Pieces(crate::query::hand_out(batches.to_vec(), format, crate::query::take_held())?)),
        (format, _) => Ok(Rendered::Whole(crate::answer::render(batches, format, crate::pages::keep)?)),
    }
}

/// Is an answer big enough to send in pieces (`query::stream_bytes`: `PONDRA_STREAM_MB` of Arrow)?
fn big(batches: &[RecordBatch]) -> bool {
    let mut counted = datafusion::common::utils::memory::RecordBatchMemoryCounter::new(); // (as `query::collect` counts it)
    batches.iter().map(|b| counted.count_batch(b)).sum::<usize>() >= crate::query::stream_bytes()
}
