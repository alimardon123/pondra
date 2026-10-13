//! pondra_lib: a Pondra node in any language's process (ADR-061 §6, `designs/l2-step4-open.md`).
//! Eight functions over `pondra::library::Opened`; Python binds them with ctypes, and the packages'
//! small `pondra` launcher calls `pondra_main`. Every entry catches a panic (unwinding across
//! `extern "C"` is undefined) and says what failed through `pondra_last_error`. The library is never
//! unloaded, and a handle is a number, never a pointer: a closed or unknown one is refused by name.
//! Node, Bun and Deno load the same library as a Node-API module (`napi.rs`).
mod napi;

use pondra::library::Opened;
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::ffi::{c_char, CStr, CString};
use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicU64, Ordering::SeqCst};
use std::sync::{Arc, Mutex, PoisonError};

// The library's allocator, the program's (`main.rs`; the crate sets none): jemalloc on Linux, which gives
// back what a node has freed, mimalloc elsewhere. The packages' `pondra` runs its node through this library,
// and a host's (Python's, Node's) through `connect()`. Their thread-locals are dlopen's kind (Cargo.toml),
// so any process can load the library, and jemalloc's symbols are prefixed: a host's own malloc stays its.
#[cfg(all(target_os = "linux", target_env = "gnu"))]
#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;
#[cfg(not(all(target_os = "linux", target_env = "gnu")))]
#[global_allocator]
static ALLOC: mimalloc::MiMalloc = mimalloc::MiMalloc;

/// The nodes open, by handle (one at a time in a process: `Opened::open` refuses a second).
static NODES: Mutex<BTreeMap<u64, Arc<Opened>>> = Mutex::new(BTreeMap::new());
static NEXT: AtomicU64 = AtomicU64::new(1);

thread_local! {
    /// This thread's last error, kept until its next call that fails.
    static LAST: RefCell<CString> = RefCell::new(CString::default());
}

/// `f`, with its error or panic kept for `pondra_last_error` and `failed` returned instead.
fn caught<T>(failed: T, f: impl FnOnce() -> anyhow::Result<T>) -> T {
    let why = match std::panic::catch_unwind(AssertUnwindSafe(f)) {
        Ok(Ok(v)) => return v,
        Ok(Err(e)) => format!("{e:#}"),
        Err(p) => p.downcast_ref::<&str>().map(|s| s.to_string()).or_else(|| p.downcast_ref::<String>().cloned()).unwrap_or_else(|| "a panic".into()),
    };
    LAST.with(|l| *l.borrow_mut() = CString::new(why.replace('\0', " ")).unwrap_or_default());
    failed
}

/// A C string the caller gave, as text (NULL is none).
unsafe fn text<'a>(p: *const c_char) -> anyhow::Result<Option<&'a str>> {
    if p.is_null() {
        return Ok(None);
    }
    Ok(Some(CStr::from_ptr(p).to_str().map_err(|_| anyhow::anyhow!("a string given to Pondra's library isn't UTF-8"))?))
}

fn node(handle: u64) -> anyhow::Result<Arc<Opened>> {
    NODES.lock().unwrap_or_else(PoisonError::into_inner).get(&handle).cloned()
        .ok_or_else(|| anyhow::anyhow!("no node is open with handle {handle} (it was closed, or never opened): open the lake again"))
}

/// Opens a node in this process from a JSON object of options (`lake`, `addr`, `owner`, `python`,
/// `attach`, and settings by name): its handle once it serves, or 0.
#[no_mangle]
pub unsafe extern "C" fn pondra_open(options_json: *const c_char) -> u64 {
    caught(0, || open_node(text(options_json)?.ok_or_else(|| anyhow::anyhow!("pondra_open takes a JSON object of options"))?))
}

/// A node opened from its options, kept under a new handle (the C ABI's and Node-API's alike).
fn open_node(options: &str) -> anyhow::Result<u64> {
    let opened = Opened::open_json(options)?;
    let handle = NEXT.fetch_add(1, SeqCst);
    NODES.lock().unwrap_or_else(PoisonError::into_inner).insert(handle, Arc::new(opened));
    Ok(handle)
}

/// Closes the node under `handle`: drained, its term given back (the C ABI's and Node-API's alike).
fn close_node(handle: u64) -> anyhow::Result<()> {
    let opened = NODES.lock().unwrap_or_else(PoisonError::into_inner).remove(&handle)
        .ok_or_else(|| anyhow::anyhow!("no node is open with handle {handle}"))?;
    opened.close();
    Ok(())
}

/// A request's headers, given as a JSON object (NULL or none: no headers): the C ABI's and Node-API's alike.
fn headers_from(json: Option<&str>) -> anyhow::Result<Vec<(String, String)>> {
    let Some(json) = json else { return Ok(vec![]) };
    let map: BTreeMap<String, serde_json::Value> = serde_json::from_str(json).map_err(|e| anyhow::anyhow!("a request's headers are a JSON object: {e}"))?;
    Ok(map.into_iter().map(|(k, v)| (k, v.as_str().map_or_else(|| v.to_string(), str::to_string))).collect())
}

/// An answer's headers as a JSON object.
fn headers_json(headers: Vec<(String, String)>) -> String {
    serde_json::Value::Object(headers.into_iter().map(|(k, v)| (k, v.into())).collect()).to_string()
}

/// How long a call may wait: milliseconds, or as long as it takes when negative.
fn within(wait_ms: i64) -> Option<std::time::Duration> {
    u64::try_from(wait_ms).ok().map(std::time::Duration::from_millis)
}

/// A piece of an answer's body, where the library holds it until the next `pondra_read` of its answer
/// or `pondra_free`.
#[repr(C)]
pub struct PondraPiece {
    pub data: *const u8,
    pub len: usize,
}

/// An answer: its status (0 until `pondra_wait` has its head) and its headers as a JSON object (NULL until
/// then). Its body is read a piece at a time with `pondra_read`, as the node writes it. Freed with
/// `pondra_free`. One thread at a time waits on, reads or frees an answer; requests on several threads at
/// once each have their own.
#[repr(C)]
pub struct PondraAnswer {
    pub status: u16,
    pub headers_json: *mut c_char,
}

/// What a `PondraAnswer` pointer points into: the head, the request while its head is to come, then the
/// body still to read and the piece last read. `answer` is first, so a pointer to it is a pointer to the
/// whole.
#[repr(C)]
struct Held {
    answer: PondraAnswer,
    asked: Option<pondra::library::Asked>,
    body: Option<pondra::library::Body>,
    piece: bytes::Bytes,
}

/// One request to the node, as the HTTP API takes it (`method`, `path` with its query, headers as a
/// JSON object or NULL, the body), started at once: its answer, whose head `pondra_wait` waits for, or
/// NULL.
#[no_mangle]
pub unsafe extern "C" fn pondra_request(handle: u64, method: *const c_char, path: *const c_char, headers_json: *const c_char, body: *const u8, len: usize) -> *mut PondraAnswer {
    caught(std::ptr::null_mut(), || {
        let method = text(method)?.unwrap_or("GET");
        let path = text(path)?.ok_or_else(|| anyhow::anyhow!("pondra_request needs a path"))?;
        let headers = headers_from(text(headers_json)?)?;
        let body = if body.is_null() || len == 0 { vec![] } else { std::slice::from_raw_parts(body, len).to_vec() };
        let asked = node(handle)?.ask(method, path, &headers, body)?;
        let answer = PondraAnswer { status: 0, headers_json: std::ptr::null_mut() };
        Ok(Box::into_raw(Box::new(Held { answer, asked: Some(asked), body: None, piece: bytes::Bytes::new() })) as *mut PondraAnswer)
    })
}

/// Waits for an answer's head, `wait_ms` at most (negative: as long as it takes): 1 once its `status` and
/// `headers_json` are there, 2 if not yet (a host waits in slices, so it sees its own signals: Ctrl-C), or
/// -1 when the request failed (`pondra_last_error` says why).
#[no_mangle]
pub unsafe extern "C" fn pondra_wait(answer: *mut PondraAnswer, wait_ms: i64) -> i32 {
    caught(-1, || {
        anyhow::ensure!(!answer.is_null(), "pondra_wait takes an answer");
        let held = &mut *(answer as *mut Held);
        let Some(asked) = held.asked.as_mut() else {
            anyhow::ensure!(held.body.is_some(), "this answer's request failed");
            return Ok(1);
        };
        let Some(got) = pondra::library::wait(asked, within(wait_ms)) else { return Ok(2) };
        held.asked = None;
        let pondra::library::Answer { status, headers, body } = got?;
        held.answer = PondraAnswer { status, headers_json: CString::new(headers_json(headers))?.into_raw() };
        held.body = Some(body);
        Ok(1)
    })
}

/// The next piece of an answer's body, `wait_ms` at most for the node to write it (negative: as long as
/// it takes): 1 with the piece in `out` (the library's until the next read of this answer or
/// `pondra_free`), 0 at the body's end, 2 if not written yet, or -1 when its work failed
/// (`pondra_last_error` says why). Freeing an answer before its end ends its work.
#[no_mangle]
pub unsafe extern "C" fn pondra_read(answer: *mut PondraAnswer, out: *mut PondraPiece, wait_ms: i64) -> i32 {
    caught(-1, || {
        anyhow::ensure!(!answer.is_null() && !out.is_null(), "pondra_read takes an answer and a piece to fill");
        let held = &mut *(answer as *mut Held);
        held.piece = bytes::Bytes::new(); // (the last piece goes before the next is waited for)
        let body = held.body.as_mut().ok_or_else(|| anyhow::anyhow!("pondra_read: wait for the answer's head first (pondra_wait)"))?;
        match pondra::library::wait(body.recv(), within(wait_ms)) {
            None => Ok(2),
            Some(None) => Ok(0),
            Some(Some(piece)) => {
                held.piece = piece?;
                *out = PondraPiece { data: held.piece.as_ptr(), len: held.piece.len() };
                Ok(1)
            }
        }
    })
}

/// Frees an answer `pondra_request` gave (NULL is nothing). Before its head or its body's end, its work
/// stops, as a client's does when it goes.
#[no_mangle]
pub unsafe extern "C" fn pondra_free(answer: *mut PondraAnswer) {
    caught((), || {
        if !answer.is_null() {
            let held = Box::from_raw(answer as *mut Held);
            if !held.answer.headers_json.is_null() {
                drop(CString::from_raw(held.answer.headers_json));
            }
        }
        Ok(())
    })
}

/// Closes a node: drained (its requests in flight answered), its term given back, its statics set
/// back. 0, or -1.
#[no_mangle]
pub unsafe extern "C" fn pondra_close(handle: u64) -> i32 {
    caught(-1, || close_node(handle).map(|()| 0))
}

/// This thread's last error, until its next call that fails (owned by the library: not freed).
#[no_mangle]
pub extern "C" fn pondra_last_error() -> *const c_char {
    LAST.with(|l| l.borrow().as_ptr())
}

/// The program (`pondra …`), for the packages' launcher, which calls this with its own arguments:
/// the command line reads the process's arguments as the program does.
#[no_mangle]
pub extern "C" fn pondra_main(_argc: i32, _argv: *const *const c_char) -> i32 {
    caught(101, || {
        pondra::cli::main();
        Ok(0)
    })
}
