//! The one clock (redesign step 3, piece 3a: l7-wip/step3-sim-plan.md): what time it is, and how long since. The sim runs
//! a cluster on tokio's paused time, where a sleep of a minute ends at once, so every time the core reads must be that
//! time too: a lease that runs out, a member not heard from, a lock's wait, a mark's age. Read any other way, the paused
//! sleeps would pass while the clock stood still, and no leader would ever be seen gone.
//! - [`Instant`] is tokio's: on a node the same as std's and read as cheaply; in a test that pauses time, that time; on a
//!   page, the page's (`web/tokio`).
//! - [`wall_us`] and [`wall_ms`]: the machine's clock (the browser's on a page). In a test build and the sim's (`cfg(test)`,
//!   feature `sim`) it also runs ahead with paused time: the later of the machine's time and its start plus the time
//!   paused sleeps passed, so it never goes back. A release build reads the machine's clock alone.

/// A point on the core's clock: tokio's, which a test's paused time moves.
pub use tokio::time::Instant;

/// The time now, on the clock leases, waits and timeouts are measured on.
pub fn now() -> Instant {
    Instant::now()
}

/// Milliseconds since the Unix epoch: the machine's clock on a node, the browser's on a page.
pub fn wall_ms() -> u64 {
    wall_us() / 1000
}

/// Microseconds since the Unix epoch, ahead of the machine's by what paused time passed (`ahead`).
#[cfg(not(target_arch = "wasm32"))]
pub fn wall_us() -> u64 {
    let machine = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default();
    (machine + ahead()).as_micros() as u64
}

/// (a page's clock counts whole milliseconds, and a page's tests pause no time)
#[cfg(target_arch = "wasm32")]
pub fn wall_us() -> u64 {
    tokio::clock::now_ms() * 1000
}

/// How far paused time has run past the machine's own: what its sleeps skipped, less the time they took. Tokio's clock
/// is read first, so time that isn't paused is never ahead (it reads no later than std's read after it).
#[cfg(all(not(target_arch = "wasm32"), any(test, feature = "sim")))]
fn ahead() -> std::time::Duration {
    let paused = Instant::now().into_std();
    paused.saturating_duration_since(std::time::Instant::now())
}

/// (a release build pauses no time)
#[cfg(all(not(target_arch = "wasm32"), not(any(test, feature = "sim"))))]
fn ahead() -> std::time::Duration {
    std::time::Duration::ZERO
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    /// With time paused, a minute's sleep ends at once, and the clock says a minute passed: on `Instant` and on the wall.
    #[tokio::test(start_paused = true)]
    async fn paused_time_moves_the_clock() {
        let (at, wall, real) = (now(), wall_ms(), std::time::Instant::now());
        tokio::time::sleep(Duration::from_secs(60)).await;
        assert!(real.elapsed() < Duration::from_secs(5), "the sleep didn't wait");
        assert_eq!(at.elapsed(), Duration::from_secs(60));
        let passed = wall_ms() - wall;
        assert!((60_000..65_000).contains(&passed), "the wall clock ran a minute ahead too: {passed} ms");
    }

    /// Time that isn't paused: the wall clock is the machine's, and never goes back.
    #[tokio::test]
    async fn unpaused_the_wall_is_the_machines() {
        let machine = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() as u64;
        let (a, b) = (wall_ms(), wall_ms());
        assert!(a.abs_diff(machine) < 1_000 && b >= a);
    }
}
