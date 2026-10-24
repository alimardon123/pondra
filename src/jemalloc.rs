//! jemalloc's controls (`mallctl`), for what reads the node's allocator: its statistics on `/metrics`
//! (`metrics::allocator`), and the memory watch's thread counters and purge (`oom.rs`). jemalloc is the node's
//! allocator on Linux with glibc (`main.rs`).
use std::ffi::{c_char, c_void, CStr};
use std::ptr::null_mut;

/// jemalloc's options, for the program and the library to give it as `malloc_conf` (`main.rs`,
/// `capi/src/lib.rs`: the crate sets no allocator). jemalloc reads them before its first allocation, and
/// `_RJEM_MALLOC_CONF` (a heap leg's profiling) adds to them.
///
/// `thp:never`: its pages stay off transparent huge pages. Where the system makes every page huge (THP
/// `always`, as on GitHub's runners), the kernel joined each 2 MB range that jemalloc had given back all
/// but a page of into one huge page again. Over five hours a node held 120–250 MB more than jemalloc counted
/// (`reviews/soak-heap-33e136c.md`).
pub const CONF: Conf = Conf(c"thp:never".as_ptr());

/// A C string jemalloc reads at its start (`malloc_conf` is a `const char *`).
#[repr(transparent)]
pub struct Conf(*const c_char);
// SAFETY: it points at a static string, which nothing writes.
unsafe impl Sync for Conf {}

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
