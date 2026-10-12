//! The library as a Node-API module: Node, Bun and Deno load it with `process.dlopen`. Node-API's
//! functions are looked up in the host process when the module is registered, so the library links
//! to nothing of Node's, and Python and the launcher load it as before and never call them.

use std::any::Any;
use std::ffi::{c_char, c_void, CStr, CString};
use std::panic::{self, AssertUnwindSafe};
use std::ptr::{null, null_mut};
use std::sync::{Mutex, MutexGuard, OnceLock};

type Env = *mut c_void;
type Value = *mut c_void;
type Info = *mut c_void;
type Deferred = *mut c_void;
type Work = *mut c_void;
type Status = i32; // napi_status: 0 is napi_ok

// The callbacks Node-API takes.
type Callback = unsafe extern "C" fn(Env, Info) -> Value;
type Execute = unsafe extern "C" fn(Env, *mut c_void);
type Complete = unsafe extern "C" fn(Env, Status, *mut c_void);
type Hook = unsafe extern "C" fn(*mut c_void);

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
    let a = api()?;
    let mut argc = 1usize;
    let mut argv: Value = null_mut();
    ok((a.napi_get_cb_info)(env, info, &mut argc, &mut argv, null_mut(), null_mut()), "napi_get_cb_info")?;
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
        let mine: Vec<u64> = lock(&OPENED).iter().filter(|(e, _)| *e == env).map(|&(_, h)| h).collect();
        lock(&OPENED).retain(|(e, _)| *e != env);
        for h in mine {
            report(crate::close_node(h).map_err(|e| format!("closing the node at exit: {e:#}")));
        }
    });
    report(r.map_err(panicked));
}
