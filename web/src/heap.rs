//! The page's heap: std's allocator (dlmalloc) over the module's memory, grown in steps that grow with the memory.
//!
//! Each `memory.grow` gives JavaScript a new ArrayBuffer over the whole memory, and V8 counts all of it as new outside
//! memory, so each grow costs a full garbage collection. std's allocator grows the memory by what one allocation lacks:
//! loading TPC-H SF1's lineitem into a page took about 2,500 collections, three quarters of the load. A step of an
//! eighth of the memory (at least 16 MiB) reaches 2 GB in about 40. A grown page nothing has written to holds no
//! physical memory, so a step costs address space, not memory.

use core::arch::wasm32;
use std::alloc::{GlobalAlloc, Layout};
use std::cell::UnsafeCell;
use std::ptr;

#[cfg(target_feature = "atomics")]
compile_error!("the page's heap has no lock: it is for a module on one thread");

const PAGE: usize = 64 * 1024; // (a wasm page)
const LEAST_STEP: usize = 16 * 1024 * 1024 / PAGE; // (in pages)

/// The memory dlmalloc is given: grown by the larger of what it asks for and a step, or by just what it asks for when
/// a step would pass the memory's limit.
struct Steps;

unsafe impl dlmalloc::Allocator for Steps {
    fn alloc(&self, size: usize) -> (*mut u8, usize, u32) {
        let asked = size.div_ceil(PAGE);
        let step = asked.max(wasm32::memory_size(0) / 8).max(LEAST_STEP);
        for pages in [step, asked] {
            let before = wasm32::memory_grow(0, pages);
            if before != usize::MAX {
                return ((before * PAGE) as *mut u8, pages * PAGE, 0);
            }
        }
        (ptr::null_mut(), 0, 0)
    }

    // (a wasm memory never shrinks: nothing given back, nothing moved)
    fn remap(&self, _ptr: *mut u8, _old: usize, _new: usize, _can_move: bool) -> *mut u8 {
        ptr::null_mut()
    }

    fn free_part(&self, _ptr: *mut u8, _old: usize, _new: usize) -> bool {
        false
    }

    fn free(&self, _ptr: *mut u8, _size: usize) -> bool {
        false
    }

    fn can_release_part(&self, _flags: u32) -> bool {
        false
    }

    fn allocates_zeros(&self) -> bool {
        true // (grown memory is zeros)
    }

    fn page_size(&self) -> usize {
        PAGE
    }
}

struct Heap(UnsafeCell<dlmalloc::Dlmalloc<Steps>>);

unsafe impl Sync for Heap {} // (one thread: the module is built without atomics, as checked above)

#[global_allocator]
static HEAP: Heap = Heap(UnsafeCell::new(dlmalloc::Dlmalloc::new_with_allocator(Steps)));

unsafe impl GlobalAlloc for Heap {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        (*self.0.get()).malloc(layout.size(), layout.align())
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        (*self.0.get()).calloc(layout.size(), layout.align())
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        (*self.0.get()).free(ptr, layout.size(), layout.align())
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        (*self.0.get()).realloc(ptr, layout.size(), layout.align(), new_size)
    }
}
