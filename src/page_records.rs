//! A joined page's own statements, recorded by its node (audit 11-01 M8). A page that joined a node keeps no records of its own:
//! what it runs on its lake (its reads of the lake, and the INSERTs it stages) leaves its lines on the node it joined, under the
//! sign-in's name, as the door `page-reported` (`REPORTED`), from the request's address. `history::ended` hands each statement to `add`; a sender
//! started with the page's member (`node::open_joined`, `run`) posts what waits to `POST /cluster/records` once a second, and
//! `node::Paged::close` posts what is left once (`flush`). The node writes each line as its own (`record`): its audit line when
//! the class is written there, and its history row.
//!
//! A page's own INSERT has two lines: the page's (what the user typed) and the node's `/insert` (what the node ran).
//!
//! Bounds: a page holds `MAX_LINES` unsent lines, and past that the oldest go and are counted; a batch is at most `BATCH_LINES`
//! lines and `BATCH_BYTES`; a node records `CAP` lines a minute per sign-in and counts the rest. What is counted is written by the
//! node as one line a minute (`audit::page_note`), at the first batch that comes after the minute has ended.
#![cfg_attr(not(feature = "doors"), allow(dead_code))] // (the node's half is the doors' route, `server::records`: a page's build has none of it)
use crate::app::App;
use crate::auth::Principal;
use crate::dial::Dial;
use crate::{audit, history};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::time::Duration;

/// Unsent lines a page holds: past this the oldest go, and are counted.
pub const MAX_LINES: usize = 2000;
/// Lines in one batch at most, and bytes: the node takes a batch of 256 lines and up to 1 MB.
pub const BATCH_LINES: usize = 256;
const BATCH_BYTES: usize = 512 << 10;
/// Lines a node records a minute for one sign-in's page; the rest are counted, not written.
pub const CAP: usize = 1200;
const STATEMENT_BYTES: usize = 16 << 10;
const ERROR_BYTES: usize = 4 << 10;
const MAX_MS: i64 = 24 * 60 * 60 * 1000;
const EVERY: Duration = Duration::from_secs(1);

/// One statement a page ran: its text, its outcome, its error and its time in ms. Nothing that names the page's user, its door,
/// its address or the time: the node takes those itself.
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct Line {
    pub statement: String,
    pub outcome: String,
    #[serde(default)]
    pub error: Option<String>,
    #[serde(default)]
    pub ms: i64,
}

/// A batch as a page sends it: its lines, and how many lines it dropped unsent since its last batch.
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct Batch {
    #[serde(default)]
    pub lines: Vec<Line>,
    #[serde(default)]
    pub dropped: u64,
}

/// `text` at most `max` bytes, cut at a character's end.
fn cut(text: &str, max: usize) -> String {
    let mut at = text.len().min(max);
    while !text.is_char_boundary(at) {
        at -= 1;
    }
    text[..at].to_string()
}

/// A statement a page ran, as it is kept until its node has it: its text and error cut as they are kept.
fn line(sql: &str, outcome: &str, error: Option<String>, took: Duration) -> Line {
    Line { statement: cut(sql, STATEMENT_BYTES), outcome: outcome.to_string(), error: error.map(|e| cut(&e, ERROR_BYTES)), ms: (took.as_millis() as i64).min(MAX_MS) }
}

/// What a page holds unsent.
#[derive(Default)]
pub struct Buffer {
    lines: VecDeque<Line>,
    dropped: u64,
    /// The sign-in no longer reaches the node (`Sent::Ended`: a 401 or 403, as a page whose user is no superuser has at its first
    /// batch): nothing is kept from then on, since nothing will send it (11-02 audit M9).
    ended: bool,
}

impl Buffer {
    /// Keeps a line; past `MAX_LINES` the oldest go, counted. Once the sender has ended, nothing is kept.
    pub fn push(&mut self, line: Line) {
        if self.ended {
            return;
        }
        self.lines.push_back(line);
        while self.lines.len() > MAX_LINES {
            self.lines.pop_front();
            self.dropped += 1;
        }
    }

    /// The next batch: the oldest lines, up to `BATCH_LINES` and `BATCH_BYTES`, with the lines dropped since the last batch.
    /// None when nothing waits.
    pub fn take(&mut self) -> Option<Batch> {
        if self.lines.is_empty() && self.dropped == 0 {
            return None;
        }
        let (mut lines, mut bytes) = (vec![], 0);
        while let Some(size) = self.lines.front().map(|l| l.statement.len() + l.error.as_ref().map_or(0, String::len)) {
            if lines.len() == BATCH_LINES || (!lines.is_empty() && bytes + size > BATCH_BYTES) {
                break;
            }
            bytes += size;
            lines.extend(self.lines.pop_front());
        }
        Some(Batch { lines, dropped: std::mem::take(&mut self.dropped) })
    }

    /// A batch that didn't reach its node goes back to the front, for the next second.
    pub fn give_back(&mut self, batch: Batch) {
        self.dropped += batch.dropped;
        for line in batch.lines.into_iter().rev() {
            self.lines.push_front(line);
        }
        while self.lines.len() > MAX_LINES {
            self.lines.pop_front();
            self.dropped += 1;
        }
    }
}

// What this page holds unsent: a per-node static, so the page's close (`reset`) sets it back.
crate::per_node!(static BUFFER: std::sync::Mutex<Buffer> = std::sync::Mutex::new(Buffer::default()));

fn buffer() -> std::sync::MutexGuard<'static, Buffer> {
    BUFFER.lock().unwrap_or_else(|e| e.into_inner())
}

/// A statement a joined page ran has ended: kept for its node (`history::ended` on a page). Nothing is sent here.
pub(crate) fn add(sql: &str, outcome: &str, error: Option<String>, took: Duration) {
    buffer().push(line(sql, outcome, error, took));
}

/// What became of a batch sent to the node.
enum Sent {
    /// The node took it, or refused it as it is (its lines are counted as dropped, and not sent again).
    Took,
    /// No answer came, or the node failed (a 5xx): the batch waits for the next second.
    Kept(Batch),
    /// The sign-in no longer reaches the node (a 401 or 403): the page stops sending.
    Ended,
}

async fn send(node: &str, batch: Batch) -> Sent {
    let Ok(call) = crate::dial::Call::json("/cluster/records", &batch) else { return Sent::Kept(batch) };
    // (`dial::Paged` and not `dial::call`: a build with doors dials with the nodes' key, which the node refuses here)
    match crate::dial::Paged.call(node, call.timeout(Duration::from_secs(5))).await {
        Ok(reply) => match reply.status {
            200..=299 => Sent::Took,
            401 | 403 => Sent::Ended,
            // (a 4xx: a size or a shape the node won't take, as a 413 is; sent again, it would be refused again and hold back the rest)
            400..=499 => {
                buffer().dropped += batch.lines.len() as u64;
                Sent::Took
            }
            _ => Sent::Kept(batch),
        },
        Err(_) => Sent::Kept(batch),
    }
}

/// Sends what waits to the node the page joined, batch after batch. A batch that doesn't reach the node is kept and the sweep
/// ends; false when the sign-in ended.
async fn sweep() -> bool {
    let Some((node, _)) = crate::dial::page_node() else { return true };
    loop {
        let next = buffer().take();
        let Some(batch) = next else { return true };
        match send(&node, batch).await {
            Sent::Took => {}
            Sent::Kept(batch) => {
                buffer().give_back(batch);
                return true;
            }
            Sent::Ended => {
                let mut b = buffer();
                (b.ended, b.lines, b.dropped) = (true, VecDeque::new(), 0);
                return false;
            }
        }
    }
}

/// The page's sender, started with its member (`node::open_joined`): each second, what waits goes to the node. It ends with the
/// member (`abort_all`), or when the sign-in ends.
pub(crate) async fn run() {
    let mut ticks = tokio::time::interval(EVERY);
    ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        ticks.tick().await;
        if !sweep().await {
            return;
        }
    }
}

/// Sends what waits once, as the page closes (`node::Paged::close`, which allows it 2 s): no retry.
pub(crate) async fn flush() {
    let _ = sweep().await;
}

/// Why a sign-in may not send a page's lines to this node, if it may not: only a superuser's own sign-in does. Not the nodes' key
/// or a token of the nodes', a user granted some rights, or a procedure's lent token.
pub fn refusal(who: &Principal, token: Option<&str>) -> Option<&'static str> {
    if who.operator {
        return Some("the nodes' own key and their tokens send no page's lines: a joined page sends its own sign-in's");
    }
    if !who.superuser() {
        return Some("a joined page's lines are sent by a superuser's sign-in only");
    }
    if crate::auth::lent(token).is_some() {
        return Some("a procedure's lent token sends no page's lines");
    }
    None
}

/// The door a joined page's lines are written as: they are what the page says it ran, which no node saw run (a changed page, or
/// any superuser's sign-in calling `/cluster/records`, may leave lines out or send lines for statements that never ran).
pub const REPORTED: &str = "page-reported";

/// The principal a page's lines are written as: the sign-in's name and its address, as the door `REPORTED`.
fn as_page(who: &Principal) -> Principal {
    who.clone().at(REPORTED, who.from)
}

/// Lines recorded a minute for one sign-in's page: when the minute began, the lines recorded, the lines past `CAP` and the
/// lines the page dropped unsent.
#[derive(Clone, Copy, Default)]
struct Minute {
    since: u64,
    recorded: usize,
    capped: usize,
    lost: usize,
}

/// The minutes of each sign-in's page that the node counts (`record`).
#[derive(Default)]
pub struct Cap {
    minutes: HashMap<String, Minute>,
}

/// What a batch from a sign-in's page admits: how many of its lines the node records, and the minutes that ended with lines not
/// recorded.
pub struct Admit {
    pub take: usize,
    pub ended: Vec<Ended>,
}

/// A minute that ended with lines of a sign-in's page not recorded: past `CAP`, and dropped unsent by the page.
#[derive(Debug, PartialEq)]
pub struct Ended {
    pub user: String,
    pub capped: usize,
    pub lost: usize,
}

impl Cap {
    /// `lines` of `user`'s page arrive at `now` (ms), and `lost` were dropped unsent before them.
    pub fn admit(&mut self, user: &str, now: u64, lines: usize, lost: usize) -> Admit {
        let mut ended = vec![];
        self.minutes.retain(|name, m| {
            let over = now.saturating_sub(m.since) >= 60_000;
            if over && (m.capped > 0 || m.lost > 0) {
                ended.push(Ended { user: name.clone(), capped: m.capped, lost: m.lost });
            }
            !over
        });
        let m = self.minutes.entry(user.to_string()).or_insert(Minute { since: now, ..Default::default() });
        let take = lines.min(CAP.saturating_sub(m.recorded));
        m.recorded += take;
        m.capped = m.capped.saturating_add(lines - take);
        m.lost = m.lost.saturating_add(lost); // (the page's own count: a sender's u64 that wraps nothing here)
        Admit { take, ended }
    }
}

// The node's count of each sign-in's page: a per-node static, set back when the node stops.
crate::per_node!(static COUNTS: std::sync::Mutex<Cap> = std::sync::Mutex::new(Cap::default()));

/// A statement as the node records it: its class (the node's own `audit::class`, not the page's word for it), its outcome (one
/// of the four words, else `failed`), its text and error cut as a page's are, and its time, clamped to a day.
#[derive(Debug, PartialEq)]
struct Recorded {
    class: &'static str,
    statement: String,
    outcome: &'static str,
    error: Option<String>,
    ms: i64,
}

fn recorded(line: Line) -> Recorded {
    let statement = cut(&line.statement, STATEMENT_BYTES);
    Recorded {
        class: audit::class(&statement),
        statement,
        outcome: match line.outcome.as_str() {
            "ok" => "ok",
            "stopped" => "stopped",
            "refused" => "refused",
            _ => "failed",
        },
        error: line.error.map(|e| cut(&e, ERROR_BYTES)),
        ms: line.ms.clamp(0, MAX_MS),
    }
}

/// Writes a batch a joined page sent (`POST /cluster/records`, a superuser's sign-in, `refusal`): each statement as its own audit
/// line (when its class is written here, or it was refused) and its history row, under `who`'s name as the door `REPORTED`.
/// Lines past the sign-in's `CAP` a minute are not written; the minutes that ended with lines not written are noted by the node.
pub async fn record(app: &App, who: &Principal, batch: Batch) {
    let admit = COUNTS.lock().unwrap_or_else(|e| e.into_inner()).admit(&who.name, crate::log::now_ms(), batch.lines.len(), usize::try_from(batch.dropped).unwrap_or(usize::MAX));
    for e in admit.ended {
        if e.capped > 0 {
            audit::page_note(app, format!("{} statements of {}'s page were not recorded (more than {CAP} a minute)", e.capped, e.user));
        }
        if e.lost > 0 {
            audit::page_note(app, format!("{} statements of {}'s page were not recorded (the page could not send them)", e.lost, e.user));
        }
    }
    let lines: Vec<Recorded> = batch.lines.into_iter().take(admit.take).map(recorded).collect();
    crate::auth::WHO
        .scope(as_page(who), async move {
            for r in lines {
                audit::page_statement(app, r.class, &r.statement, r.outcome, r.error.clone(), r.ms);
                history::page_ended(app, r.class, &r.statement, r.outcome, r.error, Duration::from_millis(r.ms as u64));
            }
        })
        .await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::Role;
    use std::net::SocketAddr;

    fn text(n: usize) -> String {
        "x".repeat(n)
    }

    #[test]
    fn a_line_is_cut_as_it_is_kept_and_its_time_clamped() {
        let l = line(&text(20 << 10), "ok", Some(text(9 << 10)), Duration::from_millis(7));
        assert_eq!(l.statement.len(), 16 << 10, "the statement cut to 16 KB");
        assert_eq!(l.error.as_ref().map(String::len), Some(4 << 10), "the error cut to 4 KB");
        assert_eq!(l.ms, 7);
        let café = format!("{}é", "a".repeat(STATEMENT_BYTES - 1)); // (the é is two bytes: the cut stops before it)
        assert_eq!(line(&café, "ok", None, Duration::ZERO).statement.len(), STATEMENT_BYTES - 1, "cut at a character's end");
        assert_eq!(line("SELECT 1", "ok", None, Duration::from_secs(90_000)).ms, MAX_MS, "a time past a day is a day");
    }

    #[test]
    fn a_buffer_past_2000_lines_drops_its_oldest_and_counts_them() {
        let mut b = Buffer::default();
        for i in 0..MAX_LINES + 5 {
            b.push(Line { statement: i.to_string(), outcome: "ok".into(), error: None, ms: 0 });
        }
        assert_eq!(b.lines.len(), MAX_LINES);
        assert_eq!(b.lines.front().map(|l| l.statement.as_str()), Some("5"), "the five oldest went");
        let batch = b.take().expect("a batch");
        assert_eq!(batch.dropped, 5, "the five dropped are sent with the next batch");
        assert_eq!(b.dropped, 0, "and counted once");
    }

    #[test]
    fn a_batch_is_at_most_256_lines_and_512_kb_and_a_batch_that_fails_goes_back_first() {
        let mut b = Buffer::default();
        for _ in 0..300 {
            b.push(Line { statement: "SELECT 1".into(), outcome: "ok".into(), error: None, ms: 0 });
        }
        let first = b.take().expect("a batch");
        assert_eq!(first.lines.len(), BATCH_LINES);
        b.push(Line { statement: "later".into(), outcome: "ok".into(), error: None, ms: 0 });
        b.give_back(first);
        let again = b.take().expect("the batch again");
        assert_eq!(again.lines.len(), BATCH_LINES, "the batch that failed is the next one");
        assert_eq!(b.lines.back().map(|l| l.statement.as_str()), Some("later"));
        let mut big = Buffer::default();
        for _ in 0..4 {
            big.push(Line { statement: "y".repeat(200 << 10), outcome: "ok".into(), error: None, ms: 0 });
        }
        assert_eq!(big.take().expect("a batch").lines.len(), 2, "two lines of 200 KB fit in 512 KB, a third doesn't");
    }

    #[test]
    fn the_node_gives_a_line_its_own_class_and_the_four_words_for_its_outcome() {
        let r = recorded(Line { statement: "CREATE USER x PASSWORD 'p'".into(), outcome: "maybe".into(), error: Some(text(5000)), ms: -4 });
        assert_eq!(r.class, "role", "the class is the node's, from the statement");
        assert_eq!(r.outcome, "failed", "an outcome that isn't one of the four words is failed");
        assert_eq!(r.ms, 0, "a time below zero is zero");
        assert_eq!(r.error.map(|e| e.len()), Some(4 << 10));
        let r = recorded(Line { statement: "SELECT 1".into(), outcome: "stopped".into(), error: None, ms: MAX_MS * 3 });
        assert_eq!((r.class, r.outcome, r.ms), ("read", "stopped", MAX_MS));
    }

    #[test]
    fn a_page_writes_as_the_sign_ins_name_and_the_door_page_reported_from_its_address() {
        let from: SocketAddr = "203.0.113.9:4000".parse().unwrap();
        let boss = Principal { name: "boss".into(), ..Principal::of(Role::Admin) }.at("http", Some(from));
        let page = as_page(&boss);
        assert_eq!((page.name.as_str(), page.door, page.from), ("boss", "page-reported", Some(from)), "marked as what the page says it ran");
        assert_eq!(page.role, Role::Admin);
    }

    #[test]
    fn only_a_superusers_own_sign_in_sends_a_page_lines() {
        let boss = Principal { name: "boss".into(), ..Principal::of(Role::Admin) };
        assert_eq!(refusal(&boss, None), None);
        assert!(refusal(&Principal::token(Role::Admin), None).is_some(), "the nodes' key or a token of the nodes'");
        assert!(refusal(&Principal { name: "rd".into(), ..Principal::of(Role::Read) }, None).is_some(), "a reader");
        let granted = Principal { name: "bob".into(), scope: crate::rights::Scope::granted(Default::default()), ..Principal::of(Role::Admin) };
        assert!(refusal(&granted, None).is_some(), "a sign-in with grants is not a superuser's");
    }

    #[test]
    fn a_sign_in_has_1200_lines_a_minute_and_the_1201st_is_dropped_and_counted() {
        let mut cap = Cap::default();
        let first = cap.admit("boss", 1_000, CAP, 0);
        assert_eq!(first.take, CAP, "1200 lines recorded in the minute");
        let over = cap.admit("boss", 1_500, 1, 0);
        assert_eq!(over.take, 0, "line 1201 is not recorded");
        assert!(over.ended.is_empty(), "the minute is not over");
        let next = cap.admit("boss", 61_000, 2, 0);
        assert_eq!(next.take, 2, "a new minute records again");
        assert_eq!(next.ended, vec![Ended { user: "boss".into(), capped: 1, lost: 0 }], "the minute that ended says what it didn't record");
    }

    #[test]
    fn a_sign_in_counts_apart_and_what_the_page_dropped_is_counted_in_its_minute() {
        let mut cap = Cap::default();
        cap.admit("boss", 1_000, CAP, 0);
        assert_eq!(cap.admit("ann", 1_000, 3, 0).take, 3, "another sign-in has its own 1200");
        let lost = cap.admit("boss", 1_000, 0, 7);
        assert_eq!(lost.take, 0);
        let after = cap.admit("ann", 61_000, 0, 0);
        assert_eq!(after.ended, vec![Ended { user: "boss".into(), capped: 0, lost: 7 }], "boss's minute ends with the 7 the page dropped");
    }

    #[test]
    fn a_count_of_lines_the_page_dropped_never_wraps() {
        let mut cap = Cap::default();
        cap.admit("boss", 1_000, 0, usize::MAX);
        cap.admit("boss", 1_000, 0, usize::MAX); // (the page's own count, sent twice: 11-02 audit M9)
        assert_eq!(cap.admit("ann", 61_000, 0, 0).ended, vec![Ended { user: "boss".into(), capped: 0, lost: usize::MAX }]);
    }

    #[test]
    fn a_buffer_whose_sender_ended_keeps_nothing() {
        let mut b = Buffer::default();
        b.ended = true; // (as `sweep` leaves it at a 401 or 403: a page whose user is no superuser)
        for _ in 0..10 {
            b.push(Line { statement: "SELECT 1".into(), outcome: "ok".into(), error: None, ms: 0 });
        }
        assert!(b.take().is_none(), "nothing kept: nothing will send it");
    }
}
