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
use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};
use std::time::{Duration, Instant};

/// What a stopped request answers (SQLSTATE 53200: `codes::by_words` reads "out of memory").
pub const OOM: &str = "out of memory: the node was nearly out of memory and stopped this query, the one that grew it (PONDRA_MEMORY_MAX_GB); a write in it may or may not have been applied";

/// How long a stop holds the watch: the node's allocator gives freed pages back over about ten
/// seconds (`main.rs`), so resident memory falls after a stop, and a second stop would be for memory
/// the first one already let go.
const HOLD: Duration = Duration::from_secs(10);

/// How long a request must have run to be stopped: one that has only just started did not grow the
/// memory the watch sees, and its own cost is still arriving.
const FIRST: Duration = Duration::from_secs(1);

crate::per_node!(static SEEN: AtomicUsize = AtomicUsize::new(0));

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

/// The resident memory the watch read last (0 before its first look). A request notes it as it starts.
pub fn seen() -> usize {
    SEEN.load(Relaxed)
}

/// Starts the watch, when the node has a line and the process can say how much memory it holds. A
/// node stopped ends it (`panics::task`), as its other loops end.
pub fn start() {
    let Some(line) = line() else { return };
    if crate::store::resident().is_none() {
        return;
    }
    crate::panics::task(async move {
        let mut last_stop = None; // when the last stop was made: it holds the watch (`pending`)
        let mut said = false; // the line for this stretch over the line, said once
        loop {
            let resident = crate::store::resident().unwrap_or(0);
            SEEN.store(resident, Relaxed);
            let now = Instant::now();
            if resident < line {
                said = false;
            }
            if due(last_stop, resident, line, now) {
                let mb = |bytes: usize| bytes >> 20;
                if crate::panics::stop_one(now, line / 10) {
                    crate::metrics::add(&crate::metrics::STOPPED_FOR_MEMORY, 1);
                    eprintln!("stopped a query: the node held {} MB, its line {} MB (PONDRA_MEMORY_MAX_GB)", mb(resident), mb(line));
                    last_stop = Some(now);
                } else if !std::mem::replace(&mut said, true) {
                    // (nothing to stop yet: a request a second old that grew the node is stopped at a later look,
                    // so no hold starts here; the line is said once per stretch over the line)
                    eprintln!("the node holds {} MB, past its line {} MB, and no query grew it", mb(resident), mb(line));
                }
            }
            // (a look every second while the node is well under its line, every 50 ms above half of it)
            let every = if resident < line / 2 { Duration::from_secs(1) } else { Duration::from_millis(50) };
            tokio::time::sleep(every).await;
        }
    });
}

/// Is a request to be stopped now: over the line, and no stop still holding.
fn due(last_stop: Option<Instant>, resident: usize, line: usize, now: Instant) -> bool {
    resident >= line && !pending(last_stop, resident, line, now)
}

/// Is a stop still holding: the node is over its line, and the last stop was less than [`HOLD`] ago. (A
/// look that finds nothing to stop holds nothing: a request that is a second old by the next look is stopped then.)
fn pending(last_stop: Option<Instant>, resident: usize, line: usize, now: Instant) -> bool {
    resident >= line && last_stop.is_some_and(|at| now.saturating_duration_since(at) < HOLD)
}

/// A request running through `panics::work`, as the choice of which to stop sees it.
pub struct Running {
    pub id: u64,
    pub since: Instant,
    /// The resident memory the watch read when it started.
    pub noted: usize,
    pub stoppable: bool,
    pub stopped: bool,
}

/// The request to stop: the oldest that can be stopped, has run for a second, and started at least
/// `margin` bytes below the memory now, so it is one that grew it. None when no request qualifies.
pub fn pick(running: &[Running], now: Instant, resident: usize, margin: usize) -> Option<u64> {
    running
        .iter()
        .filter(|r| r.stoppable && !r.stopped)
        .filter(|r| now.saturating_duration_since(r.since) >= FIRST)
        .filter(|r| r.noted.saturating_add(margin) <= resident)
        .min_by_key(|r| r.since)
        .map(|r| r.id)
}

#[cfg(test)]
mod tests {
    use super::*;

    const LINE: usize = 1 << 30;
    const MARGIN: usize = LINE / 10;

    fn at(secs: u64) -> Instant {
        Instant::now() + Duration::from_secs(secs)
    }

    fn req(id: u64, since: Instant, noted: usize) -> Running {
        Running { id, since, noted, stoppable: true, stopped: false }
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
        let running = [req(7, start + Duration::from_secs(2), 0), req(3, start, 0), req(9, start, 0)];
        // (3 and 9 started together: the lower id is the older in the map, and both qualify; 3 comes first)
        assert_eq!(pick(&running, start + Duration::from_secs(5), LINE, MARGIN), Some(3));
    }

    #[test]
    fn younger_stopped_unstoppable_and_fresh_requests_are_passed_over() {
        let start = Instant::now();
        let now = start + Duration::from_secs(5);
        let young = req(1, now - Duration::from_millis(900), 0);
        assert_eq!(pick(&[young], now, LINE, MARGIN), None, "under a second old");
        let stopped = Running { stopped: true, ..req(2, start, 0) };
        assert_eq!(pick(&[stopped], now, LINE, MARGIN), None, "already stopped");
        let not = Running { stoppable: false, ..req(3, start, 0) };
        assert_eq!(pick(&[not], now, LINE, MARGIN), None, "not stoppable");
        let near = || req(4, start, LINE - MARGIN / 2);
        assert_eq!(pick(&[near()], now, LINE, MARGIN), None, "started near today's memory: it didn't grow it");
        let grown = req(5, start, LINE - 2 * MARGIN);
        assert_eq!(pick(&[near(), grown], now, LINE, MARGIN), Some(5), "the one that grew is chosen past the others");
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

}
