//! A node stops the query that grows it past its memory line, rather than be killed for memory.
//!
//! Some operators grow without asking the query memory pool: `array_agg`, `median` and `string_agg`
//! over huge groups, and the last steps that build their answers. The pool can't refuse what it
//! isn't asked for, so on a machine or in a container with too little memory the OS killed the whole
//! node: `array_agg` of 30 M values in 3 groups peaked at 1,941 MB resident. A cap that looked only
//! when something asked the pool found 1.8–1.9 GB already resident when it refused, no better than
//! none. So a watch reads the process's resident memory on a timer, whatever asked for it, and when
//! it passes the node's line it stops the oldest request that has grown since it started: its future
//! is dropped, so its memory is let go, and it answers 53200. The node stays up and answers the rest.
//! The one watch lets the hot columns (`hot.rs`) go first, past seven tenths of the line, since a column
//! is decoded again when it is read; it stops a query only past the line itself.
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering::Relaxed};
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};

/// What a stopped request answers (SQLSTATE 53200: `codes::by_words` reads "out of memory").
pub const OOM: &str = "out of memory: the node was nearly out of memory and stopped this query, the one that grew it (PONDRA_MEMORY_MAX_GB); a write in it may or may not have been applied";

/// How long a stop holds the watch: the node's allocator gives freed pages back over about ten
/// seconds (`main.rs`), so resident memory falls after a stop, and a second stop would be for memory
/// the first one already let go. The same time holds the credit for what the hot columns gave back
/// (`felt`).
const HOLD: Duration = Duration::from_secs(10);

/// How long a request must have run to be stopped: one that has only just started did not grow the
/// memory the watch sees, and its own cost is still arriving. (A second let `array_agg` of 30M values
/// reach 1.75 GB on a 1 GB line on CI's runner, where it grows faster.)
const FIRST: Duration = Duration::from_millis(200);

/// How fast the watch takes a query to grow, at most: it looks again before memory growing at this
/// rate could pass the line, every 20 ms near it and every second at most far below it.
const RATE: f64 = 8.0 * (1u64 << 30) as f64;

crate::per_node!(static SEEN: AtomicUsize = AtomicUsize::new(0));
// (the watch is waiting: a request that starts then has no reading until the watch's next look gives it one)
crate::per_node!(static IDLE: AtomicBool = AtomicBool::new(false));
// (the caches the watch may give memory back from: a lake opened in this node registers its hot columns)
crate::per_node!(static GIVERS: std::sync::Mutex<Vec<Weak<crate::hot::Hot>>> = Default::default());
// (a wake-up holding no data: a request started, or the hot columns grew, so the watch looks again now)
crate::per_node!(static WAKE: tokio::sync::Notify = tokio::sync::Notify::new(); clear = |_| ());

/// The node's memory line in bytes: `PONDRA_MEMORY_MAX_GB`, else 85% of the machine's or container's
/// memory. None when it is 0 (off), or in a host's process, where the memory is the host's.
pub fn line() -> Option<usize> {
    if crate::library::on() {
        return None;
    }
    let bytes = match crate::env::parse::<f64>("PONDRA_MEMORY_MAX_GB") {
        Some(gb) => gb * (1u64 << 30) as f64,
        None => crate::store::ram()? as f64 * 0.85,
    };
    (bytes >= 1.0).then_some(bytes as usize)
}

/// The resident memory the watch read last (0 before its first look). While the watch waits it is
/// `usize::MAX`, not read yet: a request that starts then takes the watch's next reading as its own
/// (`panics::note`), and `pick` passes it over until then.
pub fn seen() -> usize {
    if IDLE.load(Relaxed) {
        usize::MAX
    } else {
        SEEN.load(Relaxed)
    }
}

/// The resident memory past which the hot columns are given back: seven tenths of the line, so the
/// cache goes before a query is stopped. With no line (a host's process, or `PONDRA_MEMORY_MAX_GB=0`),
/// three fifths of the machine's memory, as the cache had it before this watch: the host's memory isn't
/// ours to stop queries for, but our own cache is ours to drop.
pub fn trim_line() -> Option<usize> {
    line().map(of_line).or_else(|| Some(crate::store::ram()? / 5 * 3))
}

/// Seven tenths of a line: 85% of RAM by default, which is 60% of RAM, where the hot columns were
/// trimmed before this watch took them over.
fn of_line(line: usize) -> usize {
    line / 10 * 7
}

/// Lets `hot`'s decoded columns be given back when the node needs memory: the watch (`start`) trims
/// them past [`trim_line`], before it stops any query. A lake opened outside a node (`pondra sql`,
/// `pondra catalog`, `pondra backup`) is held to the cache's own budget alone, since no watch runs in
/// its process.
pub fn gives_back(hot: &Arc<crate::hot::Hot>) {
    if hot.on() {
        GIVERS.lock().unwrap_or_else(|e| e.into_inner()).push(Arc::downgrade(hot));
        wake();
    }
}

/// Wakes the watch, so it looks again now. `notify_one` keeps a wake sent between a look and the wait
/// after it, so none is lost.
pub fn wake() {
    WAKE.notify_one()
}

/// The caches still alive, upgraded for the caller to hold a moment (dead entries are dropped here: a
/// closed lake's columns go with its last `Arc`, and the caller must not keep one over a wait).
fn givers() -> Vec<Arc<crate::hot::Hot>> {
    let mut list = GIVERS.lock().unwrap_or_else(|e| e.into_inner());
    list.retain(|w| w.strong_count() > 0);
    list.iter().filter_map(Weak::upgrade).collect()
}

/// Memory the hot columns let go of at one look: what it freed, the resident memory then, and the least seen since.
struct Given {
    at: Instant,
    bytes: usize,
    from: usize,
    low: usize,
}

/// The memory the watch goes by: resident, less what was let go in the last [`HOLD`] and hasn't left the
/// process yet (freed pages go back to the OS over seconds, `HOLD`'s comment). Each credit shrinks as
/// resident falls below where it was taken, so memory that came back is never counted twice.
fn felt(resident: usize, given: &mut Vec<Given>, now: Instant) -> usize {
    given.retain(|g| now.saturating_duration_since(g.at) < HOLD);
    let mut credit = 0;
    for g in given.iter_mut() {
        g.low = g.low.min(resident);
        credit += g.bytes.saturating_sub(g.from.saturating_sub(g.low));
    }
    resident.saturating_sub(credit)
}

/// Starts the watch, when the process can say how much memory it holds and the hot columns can be given
/// back (`trim_line`). The line is checked only where there is one (a node's own process). It runs on a thread of
/// its own (`panics::alone`), which a node stopped ends, as its other loops end.
pub fn start() {
    let Some(trim) = trim_line() else { return };
    if crate::store::resident().is_none() {
        return;
    }
    let line = line(); // (None in a host's process: there the watch only gives the cache back)
    crate::panics::alone(async move {
        let mut given: Vec<Given> = vec![]; // what the hot columns let go of, held by `felt` for `HOLD`
        let mut last_stop = None; // when the last stop was made: it holds the watch (`pending`)
        let mut said = false; // the line for this stretch over the line, said once
        loop {
            let resident = crate::store::resident().unwrap_or(0);
            SEEN.store(resident, Relaxed); // (resident, never felt: `noted` and `pick` compare what the OS says)
            crate::panics::note(resident); // (a request that started unread takes this look's reading, before any stop)
            let now = Instant::now();
            let mut felt = felt(resident, &mut given, now);
            if felt > trim {
                // The cache goes first: a column is decoded again when it is read, a query can't be.
                let need = felt - trim;
                let holders = givers();
                let mut freed = 0;
                for hot in &holders {
                    if freed >= need {
                        break;
                    }
                    freed += hot.trim(need - freed);
                }
                drop(holders); // (held over a wait, a closed lake's columns would stay in memory)
                if freed > 0 {
                    given.push(Given { at: now, bytes: freed, from: resident, low: resident });
                    felt = felt.saturating_sub(freed);
                }
            }
            if let Some(line) = line {
                if felt < line {
                    said = false;
                }
                if due(last_stop, felt, line, now) {
                    let mb = |bytes: usize| bytes >> 20;
                    if crate::panics::stop_one(now, line / 10) {
                        crate::metrics::add(&crate::metrics::STOPPED_FOR_MEMORY, 1);
                        eprintln!("stopped a query: the node held {} MB, its line {} MB (PONDRA_MEMORY_MAX_GB)", mb(felt), mb(line));
                        last_stop = Some(now);
                    } else if !std::mem::replace(&mut said, true) {
                        // (nothing to stop yet: a request old enough that grew the node is stopped at a later look,
                        // so no hold starts here; the line is said once per stretch over the line)
                        eprintln!("the node holds {} MB, past its line {} MB, and no query grew it", mb(felt), mb(line));
                    }
                }
            }
            // The wait: as often as the trim and the stop need while a request runs (a plain sleep, since a wake on every
            // request would look on every statement); otherwise until a request starts, the cache changes, or
            // the timer ends: 2 s while the cache holds columns (Flight, Kafka and tiering grow the node outside
            // `work`, and the cache is what can answer them), else 30 s.
            match line {
                Some(line) if crate::panics::running() => tokio::time::sleep(every(felt, if felt < trim { trim } else { line })).await,
                _ => {
                    let timer = if givers().iter().any(|h| h.usage().0 > 0) { Duration::from_secs(2) } else { Duration::from_secs(30) };
                    IDLE.store(true, Relaxed);
                    let _ = tokio::time::timeout(timer, WAKE.notified()).await;
                    IDLE.store(false, Relaxed);
                }
            }
        }
    });
}

/// When to look again: before memory growing at [`RATE`] could pass the next mark from where it is now, the trim
/// line while under it (the cache must go before the line comes), else the line. (Looking every second while under
/// half the line, a query that grew from there in that second passed a 1 GB line by 0.75 GB before the watch saw it.)
fn every(resident: usize, mark: usize) -> Duration {
    Duration::from_secs_f64(mark.saturating_sub(resident) as f64 / RATE).clamp(Duration::from_millis(20), Duration::from_secs(1))
}

/// Is a request to be stopped now: over the line, and no stop still holding.
fn due(last_stop: Option<Instant>, resident: usize, line: usize, now: Instant) -> bool {
    resident >= line && !pending(last_stop, resident, line, now)
}

/// Is a stop still holding: the node is over its line, and the last stop was less than [`HOLD`] ago. (A
/// look that finds nothing to stop holds nothing: a request old enough by the next look is stopped then.)
fn pending(last_stop: Option<Instant>, resident: usize, line: usize, now: Instant) -> bool {
    resident >= line && last_stop.is_some_and(|at| now.saturating_duration_since(at) < HOLD)
}

/// A request running through `panics::work`, as the choice of which to stop sees it (`panics::RUNNING`,
/// by id).
pub struct Running {
    /// When it started.
    pub since: Instant,
    /// The resident memory the watch read when it started.
    pub noted: usize,
    /// How to stop it, when it may be stopped (the node has a line: `panics::work`).
    pub stop: Option<futures::future::AbortHandle>,
    pub stopped: bool,
}

/// The request to stop: the oldest that can be stopped, has run for [`FIRST`], and started at least
/// `margin` bytes below the memory now, so it is one that grew it. None when no request qualifies. Of
/// requests that started together, the lower id (the map's first) is the older.
pub fn pick(running: &std::collections::BTreeMap<u64, Running>, now: Instant, resident: usize, margin: usize) -> Option<u64> {
    running
        .iter()
        .filter(|(_, r)| r.stop.is_some() && !r.stopped)
        .filter(|(_, r)| now.saturating_duration_since(r.since) >= FIRST)
        .filter(|(_, r)| r.noted.saturating_add(margin) <= resident)
        .min_by_key(|(_, r)| r.since)
        .map(|(id, _)| *id)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    const LINE: usize = 1 << 30;
    const MARGIN: usize = LINE / 10;
    const MB: usize = 1 << 20;

    fn at(secs: u64) -> Instant {
        Instant::now() + Duration::from_secs(secs)
    }

    fn req(since: Instant, noted: usize) -> Running {
        Running { since, noted, stop: Some(futures::future::AbortHandle::new_pair().0), stopped: false }
    }

    fn runs(list: Vec<(u64, Running)>) -> BTreeMap<u64, Running> {
        list.into_iter().collect()
    }

    #[test]
    fn the_watch_looks_sooner_the_nearer_the_line() {
        assert_eq!(every(0, 64 << 30), Duration::from_secs(1), "far below a big line: a look a second");
        assert_eq!(every(LINE - (1 << 20), LINE), Duration::from_millis(20), "near it: every 20 ms");
        assert_eq!(every(2 * LINE, LINE), Duration::from_millis(20), "past it too");
        let half = every(LINE / 2, LINE);
        assert!(half > Duration::from_millis(50) && half < Duration::from_millis(70), "half a 1 GB line: before 8 GB/s could pass it ({half:?})");
    }

    #[test]
    fn under_the_line_nothing_is_stopped() {
        let now = at(60);
        assert!(!due(None, LINE - 1, LINE, now));
        assert!(due(None, LINE, LINE, now)); // (at the line: over it)
        assert!(!due(Some(now), LINE - 1, LINE, now));
    }

    #[test]
    fn over_the_line_the_oldest_grown_request_is_stopped() {
        let start = Instant::now();
        let running = runs(vec![(7, req(start + Duration::from_secs(2), 0)), (3, req(start, 0)), (9, req(start, 0))]);
        // (3 and 9 started together: the lower id is the older in the map, and both qualify; 3 comes first)
        assert_eq!(pick(&running, start + Duration::from_secs(5), LINE, MARGIN), Some(3));
    }

    #[test]
    fn younger_stopped_unstoppable_and_fresh_requests_are_passed_over() {
        let start = Instant::now();
        let now = start + Duration::from_secs(5);
        let young = req(now - Duration::from_millis(150), 0);
        assert_eq!(pick(&runs(vec![(1, young)]), now, LINE, MARGIN), None, "just started");
        let stopped = Running { stopped: true, ..req(start, 0) };
        assert_eq!(pick(&runs(vec![(2, stopped)]), now, LINE, MARGIN), None, "already stopped");
        let not = Running { stop: None, ..req(start, 0) };
        assert_eq!(pick(&runs(vec![(3, not)]), now, LINE, MARGIN), None, "not stoppable");
        let near = || (4, req(start, LINE - MARGIN / 2));
        assert_eq!(pick(&runs(vec![near()]), now, LINE, MARGIN), None, "started near today's memory: it didn't grow it");
        let grown = (5, req(start, LINE - 2 * MARGIN));
        assert_eq!(pick(&runs(vec![near(), grown]), now, LINE, MARGIN), Some(5), "the one that grew is chosen past the others");
        let unread = Running { noted: usize::MAX, ..req(start, 0) };
        assert_eq!(pick(&runs(vec![(6, unread)]), now, LINE, MARGIN), None, "no reading yet (it started while the watch waited): passed over");
    }

    #[test]
    fn a_stop_holds_for_ten_seconds_or_until_the_memory_is_under_the_line() {
        let now = at(100);
        let half_a_second_ago = now - Duration::from_millis(500);
        let ten_and_a_half_ago = now - Duration::from_millis(10_500);
        assert!(pending(Some(half_a_second_ago), LINE, LINE, now));
        assert!(!pending(Some(ten_and_a_half_ago), LINE, LINE, now));
        assert!(!pending(Some(half_a_second_ago), LINE - 1, LINE, now), "under the line: nothing holds");
        assert!(!pending(None, LINE, LINE, now));
        assert!(!due(Some(half_a_second_ago), LINE, LINE, now), "held: not again");
        assert!(due(Some(ten_and_a_half_ago), LINE, LINE, now), "held past ten seconds: again");
    }

    #[test]
    fn the_watch_gives_the_cache_seven_tenths_of_the_line() {
        assert_eq!(of_line(10 << 30), 7 << 30);
        assert_eq!(of_line(LINE), LINE / 10 * 7);
    }

    #[test]
    fn what_the_cache_gave_back_is_credited_until_it_leaves_the_process_and_never_twice() {
        let start = Instant::now();
        let at = |secs: u64| start + Duration::from_secs(secs);
        // Resident 1000 MB, and the cache lets 300 MB go at once.
        let mut given = vec![Given { at: start, bytes: 300 * MB, from: 1000 * MB, low: 1000 * MB }];
        assert_eq!(felt(1000 * MB, &mut given, at(0)), 700 * MB, "just trimmed: the pages haven't left yet");
        assert_eq!(felt(700 * MB, &mut given, at(1)), 700 * MB, "resident fell by what was freed: felt is resident");
        assert_eq!(felt(800 * MB, &mut given, at(2)), 800 * MB, "then it grew: felt follows it");
        assert_eq!(felt(800 * MB, &mut given, at(11)), 800 * MB, "past HOLD: no credit, and the entry is gone");
        assert!(given.is_empty());
    }
}
