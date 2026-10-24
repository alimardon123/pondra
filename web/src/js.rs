//! What the page's two stores share (`fetch.rs` over `fetch`, `opfs.rs` over the origin's private file system): JavaScript
//! calls and properties, promises awaited, a failure's words, the task a store's work runs as, and what a read of an
//! object asks for. Both stores read an object the same way (an empty range, and a HEAD, want the meta and no body).

use futures::channel::oneshot;
use object_store::{GetOptions, GetRange};
use std::future::Future;
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
