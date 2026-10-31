//! A kept table's identity counter, as its kept file's note has it (`pondra.counters`): the value a sequence's next `nextval`
//! gave, which an import restarts the lake's sequence at (`restored`).

use serde::Deserialize;

/// One sequence's counter as a kept file wrote it: the value its next `nextval` gave, None once it was past its end. The note's
/// other fields (the step and the bounds) are not read: an import restarts the lake's sequence at this value alone.
#[derive(Debug, Deserialize)]
pub struct Counter {
    next: Option<i64>,
}

impl Counter {
    /// The value its next `nextval` gives, or None once it is past its end: what an import restarts its sequence at.
    pub(crate) fn next(&self) -> Option<i64> {
        self.next
    }
}
