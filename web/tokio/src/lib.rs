//! tokio's time and task calls for a page, with the same names and signatures. The page's build takes `tokio` from
//! this crate on wasm32 (the root `Cargo.toml` renames it), so the modules that sleep, time out and spawn build for
//! the page unchanged. The node keeps tokio itself.
//!
//! What is tokio's own, re-exported: `sync`, `io`, and the macros `select!`, `join!`, `pin!` and `task_local!`. What is
//! here instead is the clock (`time`, `clock`) and the tasks (`task`), over the browser's timers and event loop. A page
//! has one thread and no runtime: a task is a future that `spawn_local` drives, and a sleep is a JavaScript timer. Where
//! tokio's types are `Send`, these are too. Nothing for `net`, `fs`, `process`, `signal` or `runtime`: those stay the
//! node's.

pub use task::spawn;
pub use tokio::{io, join, pin, select, sync, task_local};

/// A wall clock. `SystemTime::now()` panics on wasm32-unknown-unknown, so the page reads `Date.now()`.
pub mod clock {
    /// Milliseconds since the Unix epoch.
    pub fn now_ms() -> u64 {
        js_sys::Date::now() as u64
    }
}

/// Sleeps, timeouts and intervals over the browser's clock (`performance.now()`) and timers (`setTimeout`), called
/// through the global object so they work in a window, a worker and Node alike.
pub mod time {
    use futures::task::AtomicWaker;
    use js_sys::{Function, Reflect};
    use std::cell::{Cell, RefCell};
    use std::collections::HashMap;
    use std::future::Future;
    use std::ops::{Add, AddAssign, Sub, SubAssign};
    use std::pin::Pin;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    use std::task::{Context, Poll, Waker};
    use wasm_bindgen::prelude::Closure;
    use wasm_bindgen::{JsCast, JsValue};

    pub use std::time::Duration;

    /// The most a `setTimeout` waits: its delay is a signed 32-bit count of milliseconds.
    const MAX_DELAY_MS: f64 = i32::MAX as f64;

    /// Where the page's clock starts. `performance.now()` is 0 when the page opens, while a node's clock counts from the
    /// machine's boot, so an instant a while before now (`Instant::now() - Duration`) is on a node's clock and, with
    /// this start, on a page's too in its first minutes.
    const ORIGIN: Duration = Duration::from_secs(1 << 32);

    /// A point in time on the page's clock: the time since the page's time origin.
    #[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
    pub struct Instant {
        at: Duration,
    }

    impl Instant {
        /// The time now.
        pub fn now() -> Instant {
            Instant { at: ORIGIN + from_ms(monotonic_ms()) }
        }

        /// The time since this instant.
        pub fn elapsed(&self) -> Duration {
            Instant::now().duration_since(*self)
        }

        /// The time from `earlier` to this instant, or zero when `earlier` is the later.
        pub fn duration_since(&self, earlier: Instant) -> Duration {
            self.at.saturating_sub(earlier.at)
        }

        /// This instant moved `duration` later, or `None` if that is past the clock's range.
        pub fn checked_add(&self, duration: Duration) -> Option<Instant> {
            self.at.checked_add(duration).map(|at| Instant { at })
        }

        /// This instant moved `duration` earlier, or `None` if that is before the time origin.
        pub fn checked_sub(&self, duration: Duration) -> Option<Instant> {
            self.at.checked_sub(duration).map(|at| Instant { at })
        }
    }

    impl Add<Duration> for Instant {
        type Output = Instant;
        fn add(self, duration: Duration) -> Instant {
            Instant { at: self.at + duration }
        }
    }

    impl AddAssign<Duration> for Instant {
        fn add_assign(&mut self, duration: Duration) {
            self.at += duration;
        }
    }

    impl Sub<Duration> for Instant {
        type Output = Instant;
        fn sub(self, duration: Duration) -> Instant {
            Instant { at: self.at - duration }
        }
    }

    impl SubAssign<Duration> for Instant {
        fn sub_assign(&mut self, duration: Duration) {
            self.at -= duration;
        }
    }

    impl Sub<Instant> for Instant {
        type Output = Duration;
        fn sub(self, earlier: Instant) -> Duration {
            self.duration_since(earlier)
        }
    }

    /// Milliseconds since the page's time origin, from `performance.now()`. Where there is no `performance`, the
    /// fallback is `Date.now()`: a page has one or the other, so the clock is the same all along.
    fn monotonic_ms() -> f64 {
        let global: JsValue = js_sys::global().into();
        let performance = Reflect::get(&global, &"performance".into()).unwrap_or(JsValue::UNDEFINED);
        function(&performance, "now")
            .and_then(|now| now.call0(&performance).ok())
            .and_then(|ms| ms.as_f64())
            .unwrap_or_else(js_sys::Date::now)
    }

    /// A count of milliseconds as a `Duration`. A clock that gives no finite time reads as the origin.
    fn from_ms(ms: f64) -> Duration {
        let secs = ms / 1000.0;
        if secs.is_finite() && secs > 0.0 {
            Duration::from_secs_f64(secs)
        } else {
            Duration::ZERO
        }
    }

    /// The function `name` of `this`, if `this` has one.
    fn function(this: &JsValue, name: &str) -> Option<Function> {
        Reflect::get(this, &name.into()).ok()?.dyn_into::<Function>().ok()
    }

    /// Calls `callback` in `ms` milliseconds, through the global `setTimeout`. The handle is what `clearTimeout` takes.
    fn set_timeout(callback: &Closure<dyn FnMut()>, ms: f64) -> JsValue {
        let global: JsValue = js_sys::global().into();
        let set = function(&global, "setTimeout").expect("the page has no setTimeout");
        set.call2(&global, callback.as_ref(), &JsValue::from_f64(ms)).expect("setTimeout refused the delay")
    }

    /// Cancels a timer made by `set_timeout`. A timer that has already fired is left alone.
    fn clear_timeout(handle: &JsValue) {
        let global: JsValue = js_sys::global().into();
        if let Some(clear) = function(&global, "clearTimeout") {
            let _ = clear.call1(&global, handle);
        }
    }

    /// A timer that is armed: its callback and handle. They are `!Send` (a closure, and an object in Node), so they
    /// live here, in the thread's map by key, and a `Sleep` holds only the key. A page has one thread.
    struct Armed {
        _callback: Closure<dyn FnMut()>,
        handle: JsValue,
    }

    thread_local! {
        static ARMED: RefCell<HashMap<u64, Armed>> = RefCell::new(HashMap::new());
        static NEXT_KEY: Cell<u64> = const { Cell::new(0) };
    }

    /// A timer's flag, set by its callback, and the waker of the sleep waiting on it.
    struct Fired {
        done: AtomicBool,
        waker: AtomicWaker,
    }

    /// The timer a sleep has armed: its key in `ARMED`, and what its callback sets.
    struct Arm {
        key: u64,
        fired: Arc<Fired>,
    }

    /// A future that resolves at its deadline. It arms a JavaScript timer on its first poll. A timer that fires before
    /// the deadline (a clock's rounding, or a wait longer than `setTimeout` takes) is armed again for the time left.
    /// Dropping it clears its timer.
    pub struct Sleep {
        deadline: Instant,
        armed: Option<Arm>,
    }

    /// A sleep for `duration`.
    pub fn sleep(duration: Duration) -> Sleep {
        sleep_until(after(duration))
    }

    /// A sleep until `deadline`, which is now or later.
    pub fn sleep_until(deadline: Instant) -> Sleep {
        Sleep { deadline, armed: None }
    }

    /// The instant `duration` from now. A duration past the clock's range is the end of time.
    fn after(duration: Duration) -> Instant {
        Instant::now().checked_add(duration).unwrap_or(Instant { at: Duration::MAX })
    }

    impl Sleep {
        /// Arms a timer for the time left, waking `waker` when it fires.
        fn arm(&mut self, waker: &Waker) {
            let left = self.deadline.duration_since(Instant::now());
            let ms = (left.as_secs_f64() * 1000.0).ceil().min(MAX_DELAY_MS);
            let fired = Arc::new(Fired { done: AtomicBool::new(false), waker: AtomicWaker::new() });
            fired.waker.register(waker);
            let on_fire = {
                let fired = fired.clone();
                Closure::<dyn FnMut()>::new(move || {
                    fired.done.store(true, Ordering::Release);
                    fired.waker.wake();
                })
            };
            let handle = set_timeout(&on_fire, ms);
            let key = NEXT_KEY.with(|next| {
                let key = next.get();
                next.set(key.wrapping_add(1));
                key
            });
            ARMED.with(|armed| {
                armed.borrow_mut().insert(key, Armed { _callback: on_fire, handle });
            });
            self.armed = Some(Arm { key, fired });
        }

        /// Clears the timer, if one is armed: its callback is freed, and the JavaScript timer cancelled.
        fn disarm(&mut self) {
            let Some(arm) = self.armed.take() else { return };
            let armed = ARMED.try_with(|map| map.borrow_mut().remove(&arm.key)).ok().flatten();
            if let Some(armed) = armed {
                clear_timeout(&armed.handle);
            }
        }
    }

    impl Future for Sleep {
        type Output = ();

        fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
            let this = self.get_mut();
            if Instant::now() >= this.deadline {
                this.disarm();
                return Poll::Ready(());
            }
            if let Some(arm) = &this.armed {
                if !arm.fired.done.load(Ordering::Acquire) {
                    arm.fired.waker.register(cx.waker());
                    return Poll::Pending;
                }
            }
            // Not armed yet, or the timer fired before the deadline: arm the time left.
            this.disarm();
            this.arm(cx.waker());
            Poll::Pending
        }
    }

    impl Drop for Sleep {
        fn drop(&mut self) {
            self.disarm();
        }
    }

    /// The output of a future, or `Elapsed` when `duration` passes first.
    pub fn timeout<F: Future>(duration: Duration, future: F) -> Timeout<F> {
        timeout_at(after(duration), future)
    }

    /// The output of a future, or `Elapsed` when `deadline` comes first.
    pub fn timeout_at<F: Future>(deadline: Instant, future: F) -> Timeout<F> {
        Timeout { future, sleep: sleep_until(deadline) }
    }

    /// A future with a deadline, from `timeout` or `timeout_at`.
    pub struct Timeout<F> {
        future: F,
        sleep: Sleep,
    }

    impl<F: Future> Future for Timeout<F> {
        type Output = Result<F::Output, error::Elapsed>;

        fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            // SAFETY: `future` is only reached through this pin and never moved out, and `Timeout` has no `Drop`.
            let this = unsafe { self.get_unchecked_mut() };
            let future = unsafe { Pin::new_unchecked(&mut this.future) };
            if let Poll::Ready(output) = future.poll(cx) {
                return Poll::Ready(Ok(output));
            }
            Pin::new(&mut this.sleep).poll(cx).map(|()| Err(error::Elapsed::new()))
        }
    }

    /// An interval of `period`, from `interval` or `interval_at`: each `tick` completes when its time is due.
    pub struct Interval {
        next: Instant,
        period: Duration,
        behavior: MissedTickBehavior,
    }

    /// An interval whose first tick is now.
    pub fn interval(period: Duration) -> Interval {
        interval_at(Instant::now(), period)
    }

    /// An interval whose first tick is at `start`. Panics if `period` is zero.
    pub fn interval_at(start: Instant, period: Duration) -> Interval {
        assert!(period > Duration::ZERO, "`period` must be non-zero.");
        Interval { next: start, period, behavior: MissedTickBehavior::default() }
    }

    impl Interval {
        /// Waits for the next tick and gives the instant it was due. A tick that was missed is handled as the
        /// behavior says.
        pub async fn tick(&mut self) -> Instant {
            let due = self.next;
            sleep_until(due).await;
            let now = Instant::now();
            self.next = match self.behavior {
                MissedTickBehavior::Burst => due + self.period,
                MissedTickBehavior::Delay => now + self.period,
                MissedTickBehavior::Skip => {
                    // The first tick after now: the periods that have passed, and one more.
                    let passed = now.duration_since(due).as_nanos() / self.period.as_nanos() + 1;
                    let passed = u32::try_from(passed).unwrap_or(u32::MAX);
                    due + self.period.saturating_mul(passed)
                }
            };
            due
        }

        /// Sets what a tick does when it is late.
        pub fn set_missed_tick_behavior(&mut self, behavior: MissedTickBehavior) {
            self.behavior = behavior;
        }
    }

    /// What an interval does with the ticks it misses.
    #[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
    pub enum MissedTickBehavior {
        /// Each tick that is late comes at once, one after another, until the interval is caught up.
        #[default]
        Burst,
        /// The next tick comes one period after the late one ran.
        Delay,
        /// The missed ticks are dropped: the next tick is the first period edge after now.
        Skip,
    }

    pub mod error {
        use std::fmt;

        /// A `timeout` whose deadline came before its future was ready.
        #[derive(Clone, Copy, Debug, PartialEq, Eq)]
        pub struct Elapsed(());

        impl Elapsed {
            pub(crate) fn new() -> Elapsed {
                Elapsed(())
            }
        }

        impl fmt::Display for Elapsed {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("deadline has elapsed")
            }
        }

        impl std::error::Error for Elapsed {}
    }
}

/// The runtime that runs a task. A page has none, so `Handle::try_current` is always an error: that is the answer the core
/// asks for on a page (`optimize.rs`, which reads ahead only where a runtime runs the plan).
pub mod runtime {
    use std::fmt;

    /// A handle to the runtime running the current task. A page has no runtime, so none is ever given.
    #[derive(Clone, Debug)]
    pub struct Handle(());

    impl Handle {
        /// The handle of the runtime running the current task: always an error on a page.
        pub fn try_current() -> Result<Handle, TryCurrentError> {
            Err(TryCurrentError(()))
        }
    }

    /// No runtime is running the current task.
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub struct TryCurrentError(());

    impl fmt::Display for TryCurrentError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("there is no runtime on the page")
        }
    }

    impl std::error::Error for TryCurrentError {}
}

/// Tasks on the page's event loop. A task is a future that `spawn_local` drives, on the page's one thread, so it need
/// not be `Send`. Nothing here blocks: `spawn_blocking` runs its closure at once.
pub mod task {
    use futures::future::{AbortHandle as FutureAbort, Abortable, Aborted};
    use std::fmt;
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    use std::task::{Context, Poll};

    pub use tokio::task::LocalKey;

    /// Starts `future` on the page's thread and gives its handle. The task ends when the future does, or when it is
    /// aborted. Dropping the handle leaves the task running.
    pub fn spawn<F>(future: F) -> JoinHandle<F::Output>
    where
        F: Future + 'static,
        F::Output: 'static,
    {
        let (abort, registration) = FutureAbort::new_pair();
        let done = Arc::new(AtomicBool::new(false));
        let (sender, receiver) = tokio::sync::oneshot::channel();
        let finished = done.clone();
        wasm_bindgen_futures::spawn_local(async move {
            let output = Abortable::new(future, registration).await;
            finished.store(true, Ordering::Release);
            let _ = sender.send(output);
        });
        JoinHandle { receiver, handle: AbortHandle { abort, done } }
    }

    /// Runs `f` at once, on the page's thread, and gives a handle that is already finished.
    pub fn spawn_blocking<F, R>(f: F) -> JoinHandle<R>
    where
        F: FnOnce() -> R + 'static,
        R: 'static,
    {
        let (sender, receiver) = tokio::sync::oneshot::channel();
        let output: Result<R, Aborted> = Ok(f());
        let _ = sender.send(output);
        let (abort, _registration) = FutureAbort::new_pair();
        JoinHandle { receiver, handle: AbortHandle { abort, done: Arc::new(AtomicBool::new(true)) } }
    }

    /// Lets the other tasks run: the first poll wakes the task and waits, and the next poll completes.
    pub async fn yield_now() {
        YieldNow { yielded: false }.await
    }

    struct YieldNow {
        yielded: bool,
    }

    impl Future for YieldNow {
        type Output = ();

        fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
            if self.yielded {
                Poll::Ready(())
            } else {
                self.yielded = true;
                cx.waker().wake_by_ref();
                Poll::Pending
            }
        }
    }

    /// A task's identifier. The page's tasks have none, so `try_id` is always `None`.
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
    pub struct Id(u64);

    /// The running task's identifier. A page's tasks have none, so this is always `None`.
    pub fn try_id() -> Option<Id> {
        None
    }

    /// A task's handle. Awaiting it gives the task's output, or a `JoinError` if the task was aborted.
    pub struct JoinHandle<T> {
        receiver: tokio::sync::oneshot::Receiver<Result<T, Aborted>>,
        handle: AbortHandle,
    }

    impl<T> JoinHandle<T> {
        /// Aborts the task. It stops at its next poll.
        pub fn abort(&self) {
            self.handle.abort()
        }

        /// Whether the task has ended, with its output or aborted.
        pub fn is_finished(&self) -> bool {
            self.handle.is_finished()
        }

        /// A handle that can abort the task without awaiting it.
        pub fn abort_handle(&self) -> AbortHandle {
            self.handle.clone()
        }
    }

    impl<T> Future for JoinHandle<T> {
        type Output = Result<T, JoinError>;

        fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            match Pin::new(&mut self.receiver).poll(cx) {
                Poll::Ready(Ok(Ok(output))) => Poll::Ready(Ok(output)),
                Poll::Ready(Ok(Err(Aborted))) | Poll::Ready(Err(_)) => Poll::Ready(Err(JoinError(()))),
                Poll::Pending => Poll::Pending,
            }
        }
    }

    /// Why a task gave no output. A task here ends without one only when it is aborted.
    #[derive(Debug)]
    pub struct JoinError(());

    impl JoinError {
        /// Always true: a task here gives no output only when it is aborted.
        pub fn is_cancelled(&self) -> bool {
            true
        }

        /// Always false: a task here cannot panic into its handle.
        pub fn is_panic(&self) -> bool {
            false
        }
    }

    impl fmt::Display for JoinError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("task was cancelled")
        }
    }

    impl std::error::Error for JoinError {}

    /// Aborts a task from elsewhere. The task is woken and stops at that poll.
    #[derive(Clone, Debug)]
    pub struct AbortHandle {
        abort: FutureAbort,
        done: Arc<AtomicBool>,
    }

    impl AbortHandle {
        /// Aborts the task, unless it has ended.
        pub fn abort(&self) {
            self.abort.abort()
        }

        /// Whether the task has ended, with its output or aborted.
        pub fn is_finished(&self) -> bool {
            self.done.load(Ordering::Acquire)
        }
    }
}
