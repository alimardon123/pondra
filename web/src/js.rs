//! What the page's two stores share (`fetch.rs` over `fetch`, `opfs.rs` over the origin's private file system): JavaScript
//! calls and properties, promises awaited, a failure's words, the task a store's work runs as, what a read of an object
//! asks for, and the Web Lock a kept folder is held by (`lock`). Both stores read an object the same way (an empty range,
//! and a HEAD, want the meta and no body).

use futures::channel::oneshot;
use object_store::{GetOptions, GetRange};
use std::cell::RefCell;
use std::future::Future;
use std::rc::Rc;
use wasm_bindgen::closure::Closure;
use wasm_bindgen::{JsCast, JsValue};
use wasm_bindgen_futures::{spawn_local, JsFuture};

/// Runs `work` as a task on the page's thread and gives its result back as a `Send` future: the stores' futures hold
/// JavaScript values, which are not `Send`, and an `ObjectStore`'s futures must be.
pub(crate) fn sent<T: Send + 'static>(
    work: impl Future<Output = T> + 'static,
) -> impl Future<Output = Result<T, oneshot::Canceled>> + Send {
    let (tx, rx) = oneshot::channel();
    spawn_local(async move {
        let _ = tx.send(work.await);
    });
    rx
}

/// A property of a JavaScript object.
pub(crate) fn get(object: &JsValue, name: &str) -> Result<JsValue, String> {
    js_sys::Reflect::get(object, &name.into()).map_err(js_words)
}

pub(crate) fn set(object: &JsValue, name: &str, value: impl Into<JsValue>) -> Result<(), String> {
    js_sys::Reflect::set(object, &name.into(), &value.into()).map(|_| ()).map_err(js_words)
}

pub(crate) fn function(object: &JsValue, name: &str) -> Result<js_sys::Function, String> {
    get(object, name)?.dyn_into::<js_sys::Function>().map_err(js_words)
}

pub(crate) fn call_method(object: &JsValue, name: &str) -> Result<JsValue, String> {
    function(object, name)?.call0(object).map_err(js_words)
}

/// Calls `object`'s method `name` with `args` and awaits what it answers. A method that answers a promise is awaited;
/// one that answers a value (`slice`) is awaited as that value, since `Promise.resolve` makes either a promise.
pub(crate) async fn awaited(object: &JsValue, name: &str, args: &[JsValue]) -> Result<JsValue, JsValue> {
    let method = js_sys::Reflect::get(object, &name.into())?.dyn_into::<js_sys::Function>()?;
    let args: js_sys::Array = args.iter().collect();
    let answer = method.apply(object, &args)?;
    JsFuture::from(js_sys::Promise::resolve(&answer)).await
}

/// A JavaScript failure's words: an Error's message, a thrown string, or none of those. (`JsString::from` on an object
/// throws, so an object is never turned into a string that way.)
pub(crate) fn js_words(e: JsValue) -> String {
    e.dyn_ref::<js_sys::Error>()
        .map(|e| String::from(e.to_string()))
        .or_else(|| e.as_string())
        .unwrap_or_else(|| "a JavaScript call failed".into())
}

/// What a read of an object asks for. A HEAD wants no body, and neither does an empty range, which has no bytes to ask
/// for: each is answered with the object's meta alone. `range` is the range the body is read from (None: the whole
/// object, or no body at all); `empty_at` is an empty range's start, which the answer's range is.
pub(crate) struct Read {
    pub(crate) body: bool,
    pub(crate) range: Option<GetRange>,
    pub(crate) empty_at: Option<u64>,
}

pub(crate) fn read_of(options: GetOptions) -> Read {
    let empty_at = match &options.range {
        Some(GetRange::Bounded(r)) if r.start == r.end => Some(r.start),
        _ => None,
    };
    let head = options.head || empty_at.is_some();
    let range = if head { None } else { options.range };
    Read { body: !head, range, empty_at }
}

/// A Web Lock a page holds (`lock`), until it is dropped: dropping it resolves the promise the lock's callback returned,
/// and the browser lets the lock go.
pub(crate) struct Held(js_sys::Function);

impl Drop for Held {
    fn drop(&mut self) {
        let _ = self.0.call0(&JsValue::UNDEFINED);
    }
}

/// How long `lock` waits for a lock another page holds before it says so.
const WAIT_MS: f64 = 1000.0;

/// The page's Web Lock `name`, exclusive, held until the `Held` it gives is dropped. Some(held) once it is taken, None
/// when another page holds it for the whole wait (`WAIT_MS`), and Err when the browser has no Web Locks, or refuses the
/// request. The wait is a timeout, not `ifAvailable`: a lock let go by `free()` or a reload reaches the browser apart
/// from the next request, so a page opened just after could still find the lock held.
pub(crate) async fn lock(name: &str) -> Result<Option<Held>, String> {
    let (locks, abort) = lock_api().ok_or_else(|| NO_LOCKS.to_string())?;
    let signal = function(&abort, "timeout")?.call1(&abort, &JsValue::from_f64(WAIT_MS)).map_err(js_words)?;
    let options = JsValue::from(js_sys::Object::new());
    set(&options, "mode", "exclusive")?;
    set(&options, "signal", signal)?;
    let (sender, answer) = oneshot::channel();
    let slot = Rc::new(RefCell::new(Some(sender)));
    let grant = {
        let slot = slot.clone();
        Closure::once_into_js(move || granted(&slot))
    };
    let request = function(&locks, "request")?.call3(&locks, &JsValue::from_str(name), &options, &grant).map_err(js_words)?;
    spawn_local(async move {
        // (a granted lock's promise settles only when the lock is let go, and then nothing is reported: only a refusal is)
        if let Err(e) = JsFuture::from(js_sys::Promise::from(request)).await {
            let refused = if is_timeout(&e) { Ok(None) } else { Err(js_words(e)) };
            if let Some(sender) = slot.borrow_mut().take() {
                let _ = sender.send(refused);
            }
        }
    });
    answer.await.unwrap_or_else(|_| Err("the browser dropped the lock request".into()))
}

/// The callback a granted lock calls: it hands its `Held` to the `lock` that waits, and returns a promise that settles
/// when that `Held` drops, so the browser keeps the lock until then. Nothing waits for it any more: it is let go at once.
fn granted(slot: &Rc<RefCell<Option<oneshot::Sender<Result<Option<Held>, String>>>>>) -> JsValue {
    let mut release = None;
    let promise = js_sys::Promise::new(&mut |resolve, _reject| release = Some(resolve));
    let held = Held(release.expect("a promise's executor runs when it is made"));
    match slot.borrow_mut().take() {
        Some(sender) => {
            let _ = sender.send(Ok(Some(held)));
        }
        None => drop(held),
    }
    JsValue::from(promise)
}

/// The message when the browser has no Web Locks, or no `AbortSignal.timeout` (which a lock's wait is bounded by).
const NO_LOCKS: &str = "this browser has no Web Locks (navigator.locks): Page.kept needs them so two tabs don't overwrite each other's tables";

/// `navigator.locks`, and `AbortSignal`, which has the `timeout` a lock's wait is made with. None when either is missing
/// (Node has neither).
fn lock_api() -> Option<(JsValue, JsValue)> {
    let global = JsValue::from(js_sys::global());
    let locks = get(&get(&global, "navigator").ok()?, "locks").ok()?;
    let abort = get(&global, "AbortSignal").ok()?;
    let has_timeout = get(&abort, "timeout").ok()?.is_function();
    (locks.is_object() && has_timeout).then_some((locks, abort))
}

/// Whether a rejection is the wait's timeout: the signal's `TimeoutError`.
fn is_timeout(e: &JsValue) -> bool {
    get(e, "name").ok().and_then(|name| name.as_string()).as_deref() == Some("TimeoutError")
}
