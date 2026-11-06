//! A value made once from what it sits beside, and kept with it: a table entry's rules (`TableMeta::rules`) and column
//! list (`TableMeta::cols`) are each made from one parse of the entry, which can't change under its `Arc`. A clone may be
//! changed, so a clone starts empty and makes its own.

use std::sync::{Arc, OnceLock};

pub struct Memo<T>(OnceLock<Arc<T>>);

impl<T> Default for Memo<T> {
    fn default() -> Self { Memo(OnceLock::new()) }
}

impl<T> Clone for Memo<T> {
    fn clone(&self) -> Self { Memo::default() }
}

impl<T> Memo<T> {
    /// The value, made by `make` the first time.
    pub fn get_or_init(&self, make: impl FnOnce() -> T) -> Arc<T> { self.0.get_or_init(|| Arc::new(make())).clone() }

    /// The value, made by `make` the first time it succeeds. Two callers at once may both make it; the first kept wins.
    pub fn get_or_try_init<E>(&self, make: impl FnOnce() -> Result<T, E>) -> Result<Arc<T>, E> {
        if let Some(v) = self.0.get() {
            return Ok(v.clone());
        }
        let v = Arc::new(make()?);
        Ok(self.0.get_or_init(|| v).clone())
    }

    /// The value, made by `make` the first time, borrowed from the memo.
    pub fn get_ref(&self, make: impl FnOnce() -> T) -> &T { self.0.get_or_init(|| Arc::new(make())) }
    /// The value, if it was made.
    pub fn get(&self) -> Option<&Arc<T>> { self.0.get() }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Made once, a failure keeps nothing, and a clone starts empty.
    #[test]
    fn a_memo_is_made_once_and_a_clone_starts_empty() {
        let m: Memo<u32> = Memo::default();
        assert!(m.get_or_try_init(|| Err::<u32, &str>("not yet")).is_err() && m.get().is_none());
        let a = m.get_or_init(|| 7);
        assert!(Arc::ptr_eq(&a, &m.get_or_try_init(|| Ok::<u32, &str>(8)).unwrap()), "made once");
        assert!(m.clone().get().is_none());
    }
}
