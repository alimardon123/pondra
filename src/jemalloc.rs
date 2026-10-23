//! jemalloc's controls (`mallctl`), for what reads the node's allocator: its statistics on `/metrics`
//! (`metrics::allocator`), and the memory watch's thread counters and purge (`oom.rs`). jemalloc is the node's
//! allocator on Linux with glibc (`main.rs`).
use std::ffi::{c_void, CStr};
use std::ptr::null_mut;

/// The value jemalloc keeps under `name`, or None where it keeps none.
pub fn read<T: Copy>(name: &CStr) -> Option<T> {
    let mut value = std::mem::MaybeUninit::<T>::uninit();
    let mut len = std::mem::size_of::<T>();
    // SAFETY: jemalloc writes at most `len` bytes, a `T`'s size, into `value`, and is given no new value.
    let rc = unsafe { tikv_jemalloc_sys::mallctl(name.as_ptr(), value.as_mut_ptr() as *mut c_void, &mut len, null_mut(), 0) };
    // SAFETY: on success jemalloc wrote the whole `T` (`len` is still its size).
    (rc == 0 && len == std::mem::size_of::<T>()).then(|| unsafe { value.assume_init() })
}

/// Gives jemalloc `value` under `name`, or runs the command `name` when `value` is `()`; true if it took it.
pub fn write<T>(name: &CStr, mut value: T) -> bool {
    let len = std::mem::size_of::<T>();
    let new = if len == 0 { null_mut() } else { &mut value as *mut T as *mut c_void };
    // SAFETY: jemalloc reads `len` bytes of `value` (none for a command) and writes nothing back.
    unsafe { tikv_jemalloc_sys::mallctl(name.as_ptr(), null_mut(), null_mut(), new, len) == 0 }
}
