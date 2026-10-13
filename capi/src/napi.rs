//! The library as a Node-API module: Node, Bun and Deno load it with `process.dlopen`. Node-API's
//! functions are looked up in the host process when the module is registered, so the library links
//! to nothing of Node's, and Python and the launcher load it as before and never call them.
//!
//! `open` and `close` run on libuv's pool (they block while a node starts or drains). A request and a
//! read of its body run on the library's runtime instead, never holding a pool thread while a live
//! query waits for its next answer, and come back to JavaScript's thread through one thread-safe
//! function per env (`Way`), held only while a call waits, so an idle process exits as it would.

use futures::FutureExt;
use pondra::library::Body;
use std::any::Any;
use std::collections::BTreeMap;
use std::ffi::{c_char, c_void, CStr, CString};
use std::panic::{self, AssertUnwindSafe};
use std::ptr::{null, null_mut};
use std::sync::atomic::{AtomicU64, Ordering::SeqCst};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock};

type Env = *mut c_void;
type Value = *mut c_void;
type Info = *mut c_void;
type Deferred = *mut c_void;
type Work = *mut c_void;
type Tsfn = *mut c_void; // napi_threadsafe_function
type Status = i32; // napi_status: 0 is napi_ok

// The callbacks Node-API takes.
type Callback = unsafe extern "C" fn(Env, Info) -> Value;
type Execute = unsafe extern "C" fn(Env, *mut c_void);
type Complete = unsafe extern "C" fn(Env, Status, *mut c_void);
type Hook = unsafe extern "C" fn(*mut c_void);
type CallJs = unsafe extern "C" fn(Env, Value, *mut c_void, *mut c_void);
type Finalize = unsafe extern "C" fn(Env, *mut c_void, *mut c_void);

/// The Node-API functions this module calls. Each field has its symbol's name, which `symbols!` relies on.
struct Api {
    napi_create_function: unsafe extern "C" fn(Env, *const c_char, usize, Callback, *mut c_void, *mut Value) -> Status,
    napi_set_named_property: unsafe extern "C" fn(Env, Value, *const c_char, Value) -> Status,
    napi_get_cb_info: unsafe extern "C" fn(Env, Info, *mut usize, *mut Value, *mut Value, *mut *mut c_void) -> Status,
    napi_get_value_string_utf8: unsafe extern "C" fn(Env, Value, *mut c_char, usize, *mut usize) -> Status,
    napi_get_value_double: unsafe extern "C" fn(Env, Value, *mut f64) -> Status,
    napi_create_double: unsafe extern "C" fn(Env, f64, *mut Value) -> Status,
    napi_create_string_utf8: unsafe extern "C" fn(Env, *const c_char, usize, *mut Value) -> Status,
    napi_create_error: unsafe extern "C" fn(Env, Value, Value, *mut Value) -> Status,
    napi_create_promise: unsafe extern "C" fn(Env, *mut Deferred, *mut Value) -> Status,
    napi_resolve_deferred: unsafe extern "C" fn(Env, Deferred, Value) -> Status,
    napi_reject_deferred: unsafe extern "C" fn(Env, Deferred, Value) -> Status,
    napi_create_async_work: unsafe extern "C" fn(Env, Value, Value, Execute, Complete, *mut c_void, *mut Work) -> Status,
    napi_queue_async_work: unsafe extern "C" fn(Env, Work) -> Status,
    napi_delete_async_work: unsafe extern "C" fn(Env, Work) -> Status,
    napi_get_undefined: unsafe extern "C" fn(Env, *mut Value) -> Status,
    napi_throw_error: unsafe extern "C" fn(Env, *const c_char, *const c_char) -> Status,
    napi_add_env_cleanup_hook: unsafe extern "C" fn(Env, Hook, *mut c_void) -> Status,
    napi_get_null: unsafe extern "C" fn(Env, *mut Value) -> Status,
    napi_typeof: unsafe extern "C" fn(Env, Value, *mut i32) -> Status,
    napi_is_typedarray: unsafe extern "C" fn(Env, Value, *mut bool) -> Status,
    napi_get_typedarray_info: unsafe extern "C" fn(Env, Value, *mut i32, *mut usize, *mut *mut c_void, *mut Value, *mut usize) -> Status,
    napi_create_buffer_copy: unsafe extern "C" fn(Env, usize, *const c_void, *mut *mut c_void, *mut Value) -> Status,
    napi_create_array_with_length: unsafe extern "C" fn(Env, usize, *mut Value) -> Status,
    napi_set_element: unsafe extern "C" fn(Env, Value, u32, Value) -> Status,
    napi_create_threadsafe_function: unsafe extern "C" fn(Env, Value, Value, Value, usize, usize, *mut c_void, Option<Finalize>, *mut c_void, CallJs, *mut Tsfn) -> Status,
    napi_call_threadsafe_function: unsafe extern "C" fn(Tsfn, *mut c_void, i32) -> Status,
    napi_ref_threadsafe_function: unsafe extern "C" fn(Env, Tsfn) -> Status,
    napi_unref_threadsafe_function: unsafe extern "C" fn(Env, Tsfn) -> Status,
}

static API: OnceLock<Api> = OnceLock::new();

/// The functions, looked up once and kept. A missing one is an error that names it.
fn api() -> Result<&'static Api, String> {
    if let Some(a) = API.get() {
        return Ok(a);
    }
    let found = load()?;
    Ok(API.get_or_init(|| found))
}

/// Builds `Api` from the symbols of the same names; each field's type says what the function is.
macro_rules! symbols {
    ($($name:ident),* $(,)?) => {
        Api { $($name: unsafe { std::mem::transmute(find(stringify!($name))?) }),* }
    };
}

fn load() -> Result<Api, String> {
    Ok(symbols!(
        napi_create_function,
        napi_set_named_property,
        napi_get_cb_info,
        napi_get_value_string_utf8,
        napi_get_value_double,
        napi_create_double,
        napi_create_string_utf8,
        napi_create_error,
        napi_create_promise,
        napi_resolve_deferred,
        napi_reject_deferred,
        napi_create_async_work,
        napi_queue_async_work,
        napi_delete_async_work,
        napi_get_undefined,
        napi_throw_error,
        napi_add_env_cleanup_hook,
        napi_get_null,
        napi_typeof,
        napi_is_typedarray,
        napi_get_typedarray_info,
        napi_create_buffer_copy,
        napi_create_array_with_length,
        napi_set_element,
        napi_create_threadsafe_function,
        napi_call_threadsafe_function,
        napi_ref_threadsafe_function,
        napi_unref_threadsafe_function,
    ))
}

/// The address the host process has under `name`: on Unix in the global scope, on Windows in the executable.
fn find(name: &str) -> Result<*mut c_void, String> {
    let c = CString::new(name).map_err(|e| e.to_string())?;
    #[cfg(unix)]
    let p = unsafe { libc::dlsym(libc::RTLD_DEFAULT, c.as_ptr()) };
    #[cfg(windows)]
    let p = unsafe { GetProcAddress(GetModuleHandleW(null()), c.as_ptr().cast()) };
    if p.is_null() {
        return Err(format!("this process has no Node-API function {name}: load the library with Node, Bun or Deno"));
    }
    Ok(p)
}

// Windows: the two calls `find` needs, declared by hand so the module needs no crate for them.
#[cfg(windows)]
#[allow(non_snake_case)]
#[link(name = "kernel32")]
extern "system" {
    fn GetModuleHandleW(name: *const u16) -> *mut c_void;
    fn GetProcAddress(module: *mut c_void, name: *const u8) -> *mut c_void;
}

/// A call's status as a Result, naming the call that failed.
fn ok(status: Status, what: &str) -> Result<(), String> {
    if status == 0 {
        Ok(())
    } else {
        Err(format!("{what} failed (Node-API status {status})"))
    }
}

/// Prints a failure that nothing else can report.
fn report(r: Result<(), String>) {
    if let Err(e) = r {
        eprintln!("pondra: {e}");
    }
}

/// A panic's payload as an error message.
fn panicked(p: Box<dyn Any + Send>) -> String {
    let text = p.downcast_ref::<&str>().map(|s| s.to_string()).or_else(|| p.downcast_ref::<String>().cloned());
    format!("a panic: {}", text.unwrap_or_else(|| "unknown".into()))
}

/// Takes a lock even after a panic poisoned it: the lists it guards stay good.
fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|p| p.into_inner())
}

/// (env address, handle) for each node an env opened and has not closed; the cleanup hook closes the rest.
static OPENED: Mutex<Vec<(usize, u64)>> = Mutex::new(Vec::new());
/// The env addresses with a cleanup hook. Node aborts if the same hook is added twice to one env.
static HOOKED: Mutex<Vec<usize>> = Mutex::new(Vec::new());
const RESOURCE: &str = "pondra"; // the async resource's name, as async hooks see it

enum Task {
    Open(String),
    Close(u64),
}

/// One call from JavaScript: boxed, given to libuv's pool as its data, and freed in `complete`.
struct Job {
    task: Task,
    deferred: Deferred,
    work: Work,
    env: usize,
    outcome: Option<Result<u64, String>>,
}

/// Node calls this when it loads the module. Nothing can be thrown without the functions, so a failure
/// goes to stderr and the exports come back unchanged.
#[no_mangle]
pub unsafe extern "C" fn napi_register_module_v1(env: Env, exports: Value) -> Value {
    let r = panic::catch_unwind(AssertUnwindSafe(|| register(env, exports)));
    report(r.unwrap_or_else(|p| Err(panicked(p))));
    exports
}

unsafe fn register(env: Env, exports: Value) -> Result<(), String> {
    export(env, exports, c"open", open)?;
    export(env, exports, c"close", close)?;
    export(env, exports, c"closeSync", close_sync)?;
    export(env, exports, c"request", request)?;
    export(env, exports, c"read", read)?;
    export(env, exports, c"free", free)?;
    // Each env gets the hook once: adding it twice to one env aborts Node.
    let mut hooked = lock(&HOOKED);
    if !hooked.contains(&(env as usize)) {
        ok((api()?.napi_add_env_cleanup_hook)(env, cleanup, env), "napi_add_env_cleanup_hook")?;
        hooked.push(env as usize);
    }
    Ok(())
}

/// Sets `exports[name]` to a function that calls `cb`.
unsafe fn export(env: Env, exports: Value, name: &CStr, cb: Callback) -> Result<(), String> {
    let a = api()?;
    let mut f: Value = null_mut();
    ok((a.napi_create_function)(env, name.as_ptr(), name.to_bytes().len(), cb, null_mut(), &mut f), "napi_create_function")?;
    ok((a.napi_set_named_property)(env, exports, name.as_ptr(), f), "napi_set_named_property")
}

/// `open(optionsJson)`: a promise of the new node's handle, made on libuv's pool.
unsafe extern "C" fn open(env: Env, info: Info) -> Value {
    js(env, || {
        let options = string_of(env, arg(env, info)?).ok_or("open takes a JSON string of options")?;
        queue(env, Task::Open(options))
    })
}

/// `close(handle)`: a promise that settles once the node is closed.
unsafe extern "C" fn close(env: Env, info: Info) -> Value {
    js(env, || {
        let handle = number_of(env, arg(env, info)?)
            .filter(|n| *n >= 0.0 && n.fract() == 0.0)
            .ok_or("close takes a node's handle")?;
        queue(env, Task::Close(handle as u64))
    })
}

/// `closeSync(handle)`: closes the node on this thread, for the process's `exit` event, where nothing
/// asynchronous runs any more (`process.exit()` ends Node without its cleanup hooks).
unsafe extern "C" fn close_sync(env: Env, info: Info) -> Value {
    js(env, || {
        let handle = number_of(env, arg(env, info)?)
            .filter(|n| *n >= 0.0 && n.fract() == 0.0)
            .ok_or("closeSync takes a node's handle")? as u64;
        lock(&OPENED).retain(|&(_, h)| h != handle);
        crate::close_node(handle).map_err(|e| format!("{e:#}"))?;
        let mut undefined: Value = null_mut();
        ok((api()?.napi_get_undefined)(env, &mut undefined), "napi_get_undefined")?;
        Ok(undefined)
    })
}

/// Runs what JavaScript called. An error it returns, or a panic it hits, is thrown: nothing may unwind into Node.
unsafe fn js(env: Env, body: impl FnOnce() -> Result<Value, String>) -> Value {
    let r = panic::catch_unwind(AssertUnwindSafe(body));
    match r.unwrap_or_else(|p| Err(panicked(p))) {
        Ok(value) => value,
        Err(msg) => {
            throw(env, &msg);
            null_mut()
        }
    }
}

/// Throws an Error into JavaScript. If Node-API refuses, the reason goes to stderr: nothing else can carry it.
unsafe fn throw(env: Env, msg: &str) {
    let text = CString::new(msg.replace('\0', " ")).unwrap_or_default();
    report(api().and_then(|a| ok((a.napi_throw_error)(env, null(), text.as_ptr()), "napi_throw_error")));
}

/// The first argument of a call (undefined when none was given).
unsafe fn arg(env: Env, info: Info) -> Result<Value, String> {
    Ok(args::<1>(env, info)?[0])
}

/// A call's first `N` arguments (undefined for those not given).
unsafe fn args<const N: usize>(env: Env, info: Info) -> Result<[Value; N], String> {
    let a = api()?;
    let mut argc = N;
    let mut argv: [Value; N] = [null_mut(); N];
    ok((a.napi_get_cb_info)(env, info, &mut argc, argv.as_mut_ptr(), null_mut(), null_mut()), "napi_get_cb_info")?;
    Ok(argv)
}

/// The text of a JavaScript string, or None for anything else. Node-API reports that by status, without throwing.
unsafe fn string_of(env: Env, v: Value) -> Option<String> {
    let a = api().ok()?;
    let mut len = 0usize;
    if (a.napi_get_value_string_utf8)(env, v, null_mut(), 0, &mut len) != 0 {
        return None;
    }
    let mut buf = vec![0u8; len + 1];
    let mut n = 0usize;
    if (a.napi_get_value_string_utf8)(env, v, buf.as_mut_ptr().cast(), buf.len(), &mut n) != 0 {
        return None;
    }
    buf.truncate(n);
    String::from_utf8(buf).ok()
}

/// The number a value is, or None when it is not one.
unsafe fn number_of(env: Env, v: Value) -> Option<f64> {
    let a = api().ok()?;
    let mut n = 0.0;
    ((a.napi_get_value_double)(env, v, &mut n) == 0).then_some(n)
}

/// Makes the promise and starts the task on libuv's pool. If starting fails, the promise rejects with why,
/// and it is returned all the same: nothing is left to throw.
unsafe fn queue(env: Env, task: Task) -> Result<Value, String> {
    let a = api()?;
    let mut deferred: Deferred = null_mut();
    let mut promise: Value = null_mut();
    ok((a.napi_create_promise)(env, &mut deferred, &mut promise), "napi_create_promise")?;
    let closing = match task {
        Task::Close(h) => Some(h),
        Task::Open(_) => None,
    };
    let job = Box::into_raw(Box::new(Job { task, deferred, work: null_mut(), env: env as usize, outcome: None }));
    match schedule(env, job) {
        Ok(()) => {
            // A queued close's handle is no longer this env's to close at exit.
            if let Some(h) = closing {
                lock(&OPENED).retain(|&(_, x)| x != h);
            }
        }
        Err(e) => {
            drop(Box::from_raw(job));
            report(reject(env, deferred, &e));
        }
    }
    Ok(promise)
}

/// Makes the async work for `job` and queues it. On failure the work is freed; the job stays the caller's.
unsafe fn schedule(env: Env, job: *mut Job) -> Result<(), String> {
    let a = api()?;
    let mut name: Value = null_mut();
    ok((a.napi_create_string_utf8)(env, RESOURCE.as_ptr().cast(), RESOURCE.len(), &mut name), "napi_create_string_utf8")?;
    let mut work: Work = null_mut();
    ok((a.napi_create_async_work)(env, null_mut(), name, execute, complete, job.cast(), &mut work), "napi_create_async_work")?;
    (*job).work = work;
    let queued = ok((a.napi_queue_async_work)(env, work), "napi_queue_async_work");
    if queued.is_err() {
        report(ok((a.napi_delete_async_work)(env, work), "napi_delete_async_work"));
    }
    queued
}

/// Runs on libuv's pool, so it calls no Node-API function: only the library's own work. A panic becomes the job's error.
unsafe extern "C" fn execute(_env: Env, data: *mut c_void) {
    let job = &mut *data.cast::<Job>();
    let ran = panic::catch_unwind(AssertUnwindSafe(|| match &job.task {
        Task::Open(options) => crate::open_node(options),
        Task::Close(handle) => crate::close_node(*handle).map(|()| 0),
    }));
    let outcome = ran.unwrap_or_else(|p| Err(anyhow::anyhow!(panicked(p)))).map_err(|e| format!("{e:#}"));
    if let (Ok(handle), Task::Open(_)) = (&outcome, &job.task) {
        lock(&OPENED).push((job.env, *handle)); // (here, not in `complete`: an env that ends before it runs still closes the node)
    }
    job.outcome = Some(outcome);
}

/// Runs on the main thread once the pool is done: settles the promise, then frees the work and the job.
unsafe extern "C" fn complete(env: Env, status: Status, data: *mut c_void) {
    let mut job = Box::from_raw(data.cast::<Job>());
    let settled = panic::catch_unwind(AssertUnwindSafe(|| {
        let outcome = job.outcome.take().unwrap_or_else(|| Err(format!("the work did not run (status {status})")));
        settle(env, &job, outcome)
    }));
    report(settled.unwrap_or_else(|p| Err(panicked(p))));
    report(api().and_then(|a| ok((a.napi_delete_async_work)(env, job.work), "napi_delete_async_work")));
}

/// Settles the promise: an open with the node's handle, a close with undefined.
unsafe fn settle(env: Env, job: &Job, outcome: Result<u64, String>) -> Result<(), String> {
    let a = api()?;
    let mut value: Value = null_mut();
    match (outcome, &job.task) {
        (Err(e), _) => return reject(env, job.deferred, &e),
        (Ok(handle), Task::Open(_)) => ok((a.napi_create_double)(env, handle as f64, &mut value), "napi_create_double")?,
        (Ok(_), Task::Close(_)) => ok((a.napi_get_undefined)(env, &mut value), "napi_get_undefined")?,
    }
    ok((a.napi_resolve_deferred)(env, job.deferred, value), "napi_resolve_deferred")
}

/// Rejects a promise with an Error carrying `msg`.
unsafe fn reject(env: Env, deferred: Deferred, msg: &str) -> Result<(), String> {
    let a = api()?;
    let mut text: Value = null_mut();
    ok((a.napi_create_string_utf8)(env, msg.as_ptr().cast(), msg.len(), &mut text), "napi_create_string_utf8")?;
    let mut error: Value = null_mut();
    ok((a.napi_create_error)(env, null_mut(), text, &mut error), "napi_create_error")?;
    ok((a.napi_reject_deferred)(env, deferred, error), "napi_reject_deferred")
}

/// Runs as the env ends (the process exits, or a worker thread ends). Nothing else runs then, so every handle the
/// env opened and never closed is closed here, one at a time.
unsafe extern "C" fn cleanup(data: *mut c_void) {
    let env = data as usize;
    let r = panic::catch_unwind(|| {
        lock(&HOOKED).retain(|&e| e != env);
        lock(&WAYS).retain(|w| w.env != env); // (what comes back for it after this is dropped)
        let answers: Vec<Arc<Reading>> = {
            let mut reading = lock(&READING);
            let ids: Vec<u64> = reading.iter().filter(|(_, r)| r.env == env).map(|(&id, _)| id).collect();
            ids.into_iter().filter_map(|id| reading.remove(&id)).collect()
        };
        for r in answers {
            r.freed.send_replace(true); // (their work stops before the nodes drain)
        }
        let mine: Vec<u64> = lock(&OPENED).iter().filter(|(e, _)| *e == env).map(|&(_, h)| h).collect();
        lock(&OPENED).retain(|(e, _)| *e != env);
        for h in mine {
            report(crate::close_node(h).map_err(|e| format!("closing the node at exit: {e:#}")));
        }
    });
    report(r.map_err(panicked));
}

// Requests and their bodies, on the library's runtime.

/// Each env's way back to its thread: a thread-safe function, held (ref'd) while `waiting` calls of the
/// env wait, so a pending request keeps Node running and an idle process exits as it would.
struct Way {
    env: usize,
    tsfn: usize,
    waiting: usize,
}

static WAYS: Mutex<Vec<Way>> = Mutex::new(Vec::new());

/// A body JavaScript reads, by id: the env that asked, the body (taken out while a read waits on it), and
/// what ends that wait when it is freed.
struct Reading {
    env: usize,
    body: Mutex<Option<Body>>,
    freed: tokio::sync::watch::Sender<bool>,
}

static READING: Mutex<BTreeMap<u64, Arc<Reading>>> = Mutex::new(BTreeMap::new());
static NEXT_ANSWER: AtomicU64 = AtomicU64::new(1);

/// What comes back to JavaScript's thread: the promise to settle (its deferred's address, carried by the
/// runtime and used on JavaScript's thread alone), and what settles it.
struct Done {
    deferred: usize,
    outcome: Outcome,
}

enum Outcome {
    /// A request's status, headers (JSON), what its body holds already, and the rest of it (`None`: it came whole).
    Head(Result<(u16, String, Vec<u8>, Option<Body>), String>),
    /// A body's next piece, or `None` at its end.
    Piece(u64, Result<Option<bytes::Bytes>, String>),
}

/// `request(handle, method, path, headersJson, body)`: a promise of `[status, headersJson, id, first]`, the
/// node's answer as soon as it has its head: `first` is what its body holds already (a Buffer, or null), and
/// `id` (0 when the body came whole) is read with `read(id)` and let go of with `free(id)`.
unsafe extern "C" fn request(env: Env, info: Info) -> Value {
    js(env, || {
        let [handle, method, path, headers, body] = args::<5>(env, info)?;
        let handle = number_of(env, handle).filter(|n| *n >= 0.0 && n.fract() == 0.0).ok_or("request takes a node's handle")? as u64;
        let method = string_of(env, method).ok_or("request takes a method")?;
        let path = string_of(env, path).ok_or("request takes a path")?;
        let headers = crate::headers_from(string_of(env, headers).as_deref()).map_err(|e| format!("{e:#}"))?;
        let body = bytes_of(env, body)?;
        let asked = crate::node(handle).and_then(|n| n.ask(&method, &path, &headers, body)).map_err(|e| format!("{e:#}"))?;
        let (promise, deferred) = pending(env)?;
        let (at, deferred) = (env as usize, deferred as usize);
        pondra::library::spawn(async move {
            let head = async {
                let mut answer = asked.await?;
                // (what the body holds already comes with the head, at most what its channel holds: most answers
                // are whole by then, and a long one's rest is read as it is written)
                let mut first = Vec::new();
                for _ in 0..3 {
                    match answer.body.recv().now_or_never() {
                        None => break,
                        Some(None) => return Ok((answer.status, crate::headers_json(answer.headers), first, None)),
                        Some(Some(piece)) => first.extend_from_slice(&piece?),
                    }
                }
                Ok((answer.status, crate::headers_json(answer.headers), first, Some(answer.body)))
            };
            let outcome = head.await.map_err(|e: anyhow::Error| format!("{e:#}"));
            send_back(at, Done { deferred, outcome: Outcome::Head(outcome) });
        });
        Ok(promise)
    })
}

/// `read(id)`: the body's next piece (a Buffer), or null at its end, at once when the node has written it,
/// else a promise of it. A body's failure is thrown (or rejects); either way its id is let go of.
unsafe extern "C" fn read(env: Env, info: Info) -> Value {
    js(env, || {
        let id = id_of(env, arg(env, info)?)?;
        let Some(reading) = lock(&READING).get(&id).cloned() else {
            return null_value(env); // (freed: its end)
        };
        let mut body = lock(&reading.body).take().ok_or("a read of this answer is waiting already")?;
        if let Some(piece) = body.recv().now_or_never() {
            *lock(&reading.body) = Some(body);
            return piece_value(env, id, piece.transpose().map_err(|e| format!("{e:#}")));
        }
        let (promise, deferred) = pending(env)?;
        let (at, deferred) = (env as usize, deferred as usize);
        let mut freed = reading.freed.subscribe();
        pondra::library::spawn(async move {
            let piece = tokio::select! {
                piece = body.recv() => piece.transpose().map_err(|e| format!("{e:#}")),
                _ = freed.wait_for(|f| *f) => Ok(None), // (freed while it waited: its end, and its work stops)
            };
            *lock(&reading.body) = Some(body);
            send_back(at, Done { deferred, outcome: Outcome::Piece(id, piece) });
        });
        Ok(promise)
    })
}

/// `free(id)`: lets go of a body before its end (a stream cancelled, or collected unread), which stops its
/// work; a read waiting on it ends with null. Again, or an unknown id, is nothing.
unsafe extern "C" fn free(env: Env, info: Info) -> Value {
    js(env, || {
        let id = id_of(env, arg(env, info)?)?;
        let freed = lock(&READING).remove(&id);
        if let Some(r) = freed {
            r.freed.send_replace(true);
        }
        let mut undefined: Value = null_mut();
        ok((api()?.napi_get_undefined)(env, &mut undefined), "napi_get_undefined")?;
        Ok(undefined)
    })
}

/// An answer's id, as JavaScript gives it.
unsafe fn id_of(env: Env, v: Value) -> Result<u64, String> {
    number_of(env, v).filter(|n| *n >= 1.0 && n.fract() == 0.0).map(|n| n as u64).ok_or_else(|| "read and free take an answer's id".into())
}

/// A request's body from JavaScript: a string, a Uint8Array (a Buffer), or none (undefined, null).
unsafe fn bytes_of(env: Env, v: Value) -> Result<Vec<u8>, String> {
    const UNDEFINED: i32 = 0;
    const NULL: i32 = 1;
    const STRING: i32 = 4;
    let a = api()?;
    let mut kind = 0;
    ok((a.napi_typeof)(env, v, &mut kind), "napi_typeof")?;
    match kind {
        UNDEFINED | NULL => return Ok(vec![]),
        STRING => return Ok(string_of(env, v).ok_or("a request's body isn't text")?.into_bytes()),
        _ => {}
    }
    let mut typed = false;
    ok((a.napi_is_typedarray)(env, v, &mut typed), "napi_is_typedarray")?;
    let (mut array, mut len, mut data) = (-1, 0usize, null_mut());
    if typed {
        ok((a.napi_get_typedarray_info)(env, v, &mut array, &mut len, &mut data, null_mut(), null_mut()), "napi_get_typedarray_info")?;
    }
    // (napi_int8_array, napi_uint8_array, napi_uint8_clamped_array: a byte an element)
    if !(0..=2).contains(&array) {
        return Err("a request's body is a string or a Uint8Array".into());
    }
    Ok(if len == 0 || data.is_null() { vec![] } else { std::slice::from_raw_parts(data.cast::<u8>(), len).to_vec() })
}

/// A promise, with this env's way back held until it settles. Nothing is held if it can't be made.
unsafe fn pending(env: Env) -> Result<(Value, Deferred), String> {
    begin(env)?;
    let made = (|| {
        let mut deferred: Deferred = null_mut();
        let mut promise: Value = null_mut();
        ok((api()?.napi_create_promise)(env, &mut deferred, &mut promise), "napi_create_promise")?;
        Ok((promise, deferred))
    })();
    if made.is_err() {
        end(env);
    }
    made
}

/// A call starts waiting: its env's way back is made the first time, and held while any call waits.
unsafe fn begin(env: Env) -> Result<(), String> {
    let a = api()?;
    let mut ways = lock(&WAYS);
    let i = match ways.iter().position(|w| w.env == env as usize) {
        Some(i) => i,
        None => {
            let mut name: Value = null_mut();
            ok((a.napi_create_string_utf8)(env, RESOURCE.as_ptr().cast(), RESOURCE.len(), &mut name), "napi_create_string_utf8")?;
            let mut tsfn: Tsfn = null_mut();
            // (no queue limit, one thread: the library's runtime, which never lets it go; the env's teardown does)
            ok((a.napi_create_threadsafe_function)(env, null_mut(), null_mut(), name, 0, 1, null_mut(), Some(finalized), env, back, &mut tsfn), "napi_create_threadsafe_function")?;
            ok((a.napi_unref_threadsafe_function)(env, tsfn), "napi_unref_threadsafe_function")?; // (held below, while calls wait)
            ways.push(Way { env: env as usize, tsfn: tsfn as usize, waiting: 0 });
            ways.len() - 1
        }
    };
    let way = &mut ways[i];
    if way.waiting == 0 {
        ok((a.napi_ref_threadsafe_function)(env, way.tsfn as Tsfn), "napi_ref_threadsafe_function")?;
    }
    way.waiting += 1;
    Ok(())
}

/// A call stops waiting: the way back is let go once none waits.
unsafe fn end(env: Env) {
    let mut ways = lock(&WAYS);
    if let Some(way) = ways.iter_mut().find(|w| w.env == env as usize) {
        way.waiting = way.waiting.saturating_sub(1);
        if way.waiting == 0 {
            report(api().and_then(|a| ok((a.napi_unref_threadsafe_function)(env, way.tsfn as Tsfn), "napi_unref_threadsafe_function")));
        }
    }
}

/// From the library's runtime: `done` goes to its env's thread, or, the env gone, is dropped here. The ways'
/// lock is held across the call, so the env's teardown (`finalized`) can't free the function under it.
fn send_back(env: usize, done: Done) {
    let data = Box::into_raw(Box::new(done));
    let ways = lock(&WAYS);
    let sent = match (ways.iter().find(|w| w.env == env), api()) {
        // (napi_tsfn_nonblocking: the queue has no limit, so it never waits)
        (Some(w), Ok(a)) => (unsafe { (a.napi_call_threadsafe_function)(w.tsfn as Tsfn, data.cast(), 0) }) == 0,
        _ => false,
    };
    drop(ways);
    if !sent {
        drop(unsafe { Box::from_raw(data) });
    }
}

/// On JavaScript's thread: settles what came back. With no env (its teardown empties the queue) there is
/// nothing to settle, and what came is dropped.
unsafe extern "C" fn back(env: Env, _js: Value, _context: *mut c_void, data: *mut c_void) {
    let done = Box::from_raw(data.cast::<Done>());
    if env.is_null() {
        return;
    }
    end(env);
    let settled = panic::catch_unwind(AssertUnwindSafe(|| {
        let Done { deferred, outcome } = *done;
        let value = match outcome {
            Outcome::Head(Ok((status, headers, first, rest))) => head_value(env, status, &headers, &first, rest),
            Outcome::Head(Err(e)) => Err(e),
            Outcome::Piece(id, piece) => piece_value(env, id, piece),
        };
        match value {
            Ok(v) => ok((api()?.napi_resolve_deferred)(env, deferred as Deferred, v), "napi_resolve_deferred"),
            Err(e) => reject(env, deferred as Deferred, &e),
        }
    }));
    report(settled.unwrap_or_else(|p| Err(panicked(p))));
}

/// The env's thread-safe function is going (its env torn down): nothing may call it after this.
unsafe extern "C" fn finalized(env: Env, _data: *mut c_void, _hint: *mut c_void) {
    lock(&WAYS).retain(|w| w.env != env as usize);
}

/// `[status, headersJson, id, first]`, the body kept under a new id unless it came whole (and kept only
/// once the value is made: otherwise it is dropped, and its work stops).
unsafe fn head_value(env: Env, status: u16, headers: &str, first: &[u8], rest: Option<Body>) -> Result<Value, String> {
    let a = api()?;
    let id = if rest.is_some() { NEXT_ANSWER.fetch_add(1, SeqCst) } else { 0 };
    let mut items = [null_mut(); 4];
    ok((a.napi_create_double)(env, f64::from(status), &mut items[0]), "napi_create_double")?;
    ok((a.napi_create_string_utf8)(env, headers.as_ptr().cast(), headers.len(), &mut items[1]), "napi_create_string_utf8")?;
    ok((a.napi_create_double)(env, id as f64, &mut items[2]), "napi_create_double")?;
    items[3] = if first.is_empty() { null_value(env)? } else { buffer(env, first)? };
    let mut array: Value = null_mut();
    ok((a.napi_create_array_with_length)(env, items.len(), &mut array), "napi_create_array_with_length")?;
    for (i, item) in items.into_iter().enumerate() {
        ok((a.napi_set_element)(env, array, i as u32, item), "napi_set_element")?;
    }
    if let Some(body) = rest {
        let freed = tokio::sync::watch::channel(false).0;
        lock(&READING).insert(id, Arc::new(Reading { env: env as usize, body: Mutex::new(Some(body)), freed }));
    }
    Ok(array)
}

/// A body's piece as JavaScript takes it: a Buffer, or null at the end. At the end, or on a failure, its id
/// is let go of.
unsafe fn piece_value(env: Env, id: u64, piece: Result<Option<bytes::Bytes>, String>) -> Result<Value, String> {
    match piece {
        Ok(Some(p)) => buffer(env, &p),
        ended => {
            lock(&READING).remove(&id);
            ended.and_then(|_| null_value(env))
        }
    }
}

/// A Buffer holding a copy of `bytes`.
unsafe fn buffer(env: Env, bytes: &[u8]) -> Result<Value, String> {
    let mut data: *mut c_void = null_mut();
    let mut value: Value = null_mut();
    ok((api()?.napi_create_buffer_copy)(env, bytes.len(), bytes.as_ptr().cast(), &mut data, &mut value), "napi_create_buffer_copy")?;
    Ok(value)
}

/// JavaScript's null.
unsafe fn null_value(env: Env) -> Result<Value, String> {
    let mut v: Value = null_mut();
    ok((api()?.napi_get_null)(env, &mut v), "napi_get_null")?;
    Ok(v)
}
