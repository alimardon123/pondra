//! What a sim member's calls meet (redesign step 3, piece 3d: l7-wip/step3-sim-plan.md): seeded dice, and for each kind
//! of call (to the bucket, or to another member) the chance that it fails, that it is done and its answer lost, and how slow it may be. A member
//! can also be frozen (its calls wait until it is thawed: the sim's SIGSTOP), cut off (its calls fail at once), or stopped
//! at a named point (`store::maybe_crash`'s points, the harness's `PONDRA_CRASH`). Each fault met is written down, so a
//! failing seed says what it met.
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering::SeqCst};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

/// A kind of call to the bucket.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Op {
    /// A read of an object, or of its head.
    Get,
    /// A write that may replace what is there (a put, an upload's part or its end, a copy over).
    Put,
    /// A write that only makes what isn't there (a put or a copy `if not exists`): a term's claim.
    Create,
    /// A listing.
    List,
    /// A delete.
    Delete,
    /// A call to another member, as it leaves this one or reaches it (`net.rs`).
    Call,
}

impl Op {
    /// Every kind.
    pub const ALL: [Op; 6] = [Op::Get, Op::Put, Op::Create, Op::List, Op::Delete, Op::Call];
}

/// What may happen to one kind of call: each chance between 0 and 1.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct Odds {
    /// It fails before it reaches the bucket.
    pub fail: f64,
    /// It reaches the bucket, which does it, and its answer is lost: the caller sees it fail.
    pub lost: f64,
    /// It waits up to this long before it goes (any time from none to this, as the dice say).
    pub slow: Duration,
}

/// What a call meets, once it may go (`Faults::meet`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Verdict {
    /// It goes, and its answer comes back.
    Go,
    /// It fails before it reaches the bucket.
    Fail,
    /// It goes, and its answer is lost.
    Lose,
}

/// One member's faults.
pub struct Faults {
    dice: Mutex<Dice>,
    odds: Mutex<HashMap<Op, Odds>>,
    /// Its calls wait while this says so.
    frozen: tokio::sync::watch::Sender<bool>,
    /// Its calls fail at once.
    cut: AtomicBool,
    /// The points it stops at, each with its chance.
    points: Mutex<HashMap<String, f64>>,
    /// What it met, in order.
    met: Mutex<Vec<String>>,
}

impl std::fmt::Debug for Faults {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { f.write_str("Faults") }
}

impl Faults {
    /// A member's faults, its dice thrown from `seed`: none until set.
    pub fn new(seed: u64) -> Arc<Faults> {
        Arc::new(Faults {
            dice: Mutex::new(Dice(seed)),
            odds: Default::default(),
            frozen: tokio::sync::watch::Sender::new(false),
            cut: AtomicBool::new(false),
            points: Default::default(),
            met: Default::default(),
        })
    }

    /// Calls of kind `op` meet `odds` from now on.
    pub fn set(&self, op: Op, odds: Odds) { lock(&self.odds).insert(op, odds); }

    /// Every kind of call meets `odds` from now on.
    pub fn everywhere(&self, odds: Odds) { Op::ALL.iter().for_each(|op| self.set(*op, odds)); }

    /// No call meets any fault from now on: no odds, nothing frozen or cut, no point to stop at.
    pub fn calm(&self) {
        lock(&self.odds).clear();
        lock(&self.points).clear();
        self.cut.store(false, SeqCst);
        self.frozen.send_replace(false);
    }

    /// Its calls wait from now on, each where it is, until `thaw`.
    pub fn freeze(&self) { self.frozen.send_replace(true); }

    /// Its calls go on.
    pub fn thaw(&self) { self.frozen.send_replace(false); }

    /// Its calls fail at once from now on (`true`: cut off from the bucket), or go again (`false`).
    pub fn cut(&self, cut: bool) { self.cut.store(cut, SeqCst); }

    /// It stops at `point` (`store::maybe_crash`'s names), with chance `chance` each time it passes it.
    pub fn stop_at(&self, point: &str, chance: f64) { lock(&self.points).insert(point.to_string(), chance); }

    /// Does it stop at `point` this time?
    pub fn stops_at(&self, point: &str) -> bool {
        let Some(chance) = lock(&self.points).get(point).copied() else { return false };
        let stops = lock(&self.dice).chance(chance);
        if stops {
            self.note(format!("stopped at {point}"));
        }
        stops
    }

    /// What a call of kind `op` (to `what`) meets: it waits while the member is frozen, then as long as the dice say up to
    /// its odds' `slow`; then it goes, fails, or goes and loses its answer.
    pub async fn meet(&self, op: Op, what: &(dyn std::fmt::Display + Sync)) -> Verdict {
        let mut frozen = self.frozen.subscribe();
        let _ = frozen.wait_for(|f| !f).await; // (the sender lives as long as these faults)
        if self.cut.load(SeqCst) {
            self.note(format!("{op:?} {what}: cut off"));
            return Verdict::Fail;
        }
        let Some(odds) = lock(&self.odds).get(&op).copied() else { return Verdict::Go };
        let (wait, verdict) = {
            let mut dice = lock(&self.dice);
            let wait = dice.upto(odds.slow);
            let verdict = if dice.chance(odds.fail) {
                Verdict::Fail
            } else if dice.chance(odds.lost) {
                Verdict::Lose
            } else {
                Verdict::Go
            };
            (wait, verdict)
        };
        if !wait.is_zero() {
            tokio::time::sleep(wait).await;
        }
        if verdict != Verdict::Go {
            self.note(format!("{op:?} {what}: {verdict:?}"));
        }
        verdict
    }

    /// What it met, in order.
    pub fn met(&self) -> Vec<String> { lock(&self.met).clone() }

    fn note(&self, what: String) { lock(&self.met).push(what); }
}

/// Dice from a seed: splitmix64, the same numbers for the same seed on every machine.
struct Dice(u64);

impl Dice {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    /// True with chance `p`.
    fn chance(&mut self, p: f64) -> bool { p > 0.0 && ((self.next() >> 11) as f64 / (1u64 << 53) as f64) < p }

    /// A time from none to `most`.
    fn upto(&mut self, most: Duration) -> Duration {
        match most.as_micros() as u64 {
            0 => Duration::ZERO,
            m => Duration::from_micros(self.next() % (m + 1)),
        }
    }
}

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> { m.lock().unwrap_or_else(PoisonError::into_inner) }

#[cfg(test)]
mod tests {
    use super::*;

    /// The same seed throws the same faults; a frozen member's call waits until it is thawed; a cut one's fails at once.
    #[tokio::test(start_paused = true)]
    async fn faults_are_seeded_and_a_frozen_call_waits_for_its_thaw() {
        let thrown = |seed| async move {
            let f = Faults::new(seed);
            f.everywhere(Odds { fail: 0.3, lost: 0.3, slow: Duration::from_millis(100) });
            let mut out = vec![];
            for _ in 0..50 {
                out.push(f.meet(Op::Put, &"x").await);
            }
            out
        };
        let (a, b, c) = (thrown(7).await, thrown(7).await, thrown(8).await);
        assert_eq!(a, b, "one seed, one throw");
        assert_ne!(a, c, "another seed, another throw");
        assert!(a.contains(&Verdict::Fail) && a.contains(&Verdict::Lose) && a.contains(&Verdict::Go));

        let f = Faults::new(1);
        f.freeze();
        let waiting = tokio::spawn({
            let f = f.clone();
            async move { f.meet(Op::Get, &"y").await }
        });
        tokio::time::sleep(Duration::from_secs(60)).await;
        assert!(!waiting.is_finished(), "a frozen member's call waits");
        f.thaw();
        assert_eq!(waiting.await.unwrap(), Verdict::Go);
        f.cut(true);
        assert_eq!(f.meet(Op::List, &"z").await, Verdict::Fail);
        assert!(f.met().iter().any(|m| m.contains("cut off")));
        f.calm();
        assert_eq!(f.meet(Op::List, &"z").await, Verdict::Go);
        f.stop_at("after_commit", 1.0);
        assert!(f.stops_at("after_commit") && !f.stops_at("after_seg_put"));
    }
}
