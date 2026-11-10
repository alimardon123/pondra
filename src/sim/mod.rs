//! The sim (redesign step 3: l7-wip/step3-sim-plan.md, redesign.md §8): a whole cluster in one test. Its members are
//! nodes in this process (`cluster.rs`) on the test's runtime, whose time a test pauses (the core's clock follows it:
//! `clock.rs`); their lake is one bucket each reaches through its own faults (`store.rs`, `faults.rs`), and they reach
//! each other over the sim's network (`net.rs`). A failing seed mostly replays: DataFusion, SlateDB and hyper spawn tasks
//! of their own, so full determinism isn't promised, and a failure says the faults it met.
pub mod cluster;
pub mod faults;
pub mod net;
pub mod store;

#[cfg(test)]
mod tests;
