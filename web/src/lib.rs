//! The page form (designs/l2-b4-web.md, designs/page-p3.md): a page is a member on the core's lake, through wasm-bindgen.
//! Its requests and answers are a node's (ADR-061's contract), so a client speaks to a page as it speaks to a node. A
//! module holds one page at a time: a second is refused by name until the first is closed or freed, or it runs in a
//! Worker of its own (a Worker is a module of its own).

use pondra::page::Node;
use std::sync::Arc;
use wasm_bindgen::prelude::*;

mod heap;
mod js;
mod node;
mod opfs;

#[wasm_bindgen]
extern "C" {
    #[wasm_bindgen(js_namespace = console)]
    fn error(text: &str);
}

/// A page: tables given to it, SQL read as a node reads it, answers in a node's bytes. It is the module's one page
/// (`new` and `kept` refuse a second until this one is closed or freed). A kept page holds its folder's Web Lock
/// (`kept`) until it is freed, so no other page writes the folder meanwhile.
#[wasm_bindgen]
// (the lock is never read: it is held for its `Drop`, which lets the folder go when the page is freed)
pub struct Page(pondra::page::Page, #[allow(dead_code)] Option<js::Held>);

#[wasm_bindgen]
impl Page {
    /// A page with no tables, kept in the page's memory. Its member is on the core's lake, which opens on the page's first
    /// request. A web address a statement names is read through the browser's `fetch`, so its server must allow the
    /// page's origin (CORS).
    ///
    /// `opts` is a plain object, or nothing: `node` (the node's address) and `token` (its token), both text, make the
    /// page's node (`node.rs`), which answers the tables the page doesn't have. Every other key is a setting the node
    /// reads (`spill_gb` or `PONDRA_SPILL_GB`), given as text, a number or a boolean. The settings are the module's, not
    /// one page's: the first page given any fixes them, and a later page given others is refused by name.
    ///
    /// A module has one page: while another lives here (`new` or `kept`, until it is `close()`d or `free()`d), this is
    /// refused by name.
    #[wasm_bindgen(constructor)]
    pub fn new(opts: JsValue) -> Result<Page, JsValue> {
        set_panic_hook();
        let given = Given::read(&opts)?;
        give(given.settings)?;
        let page = pondra::page::Page::claim(&options(None, given.node)).map_err(|e| JsValue::from_str(&format!("{e:#}")))?;
        Ok(Page(page, None))
    }

    /// A page kept in the origin's private file system (OPFS): its files and its tables are kept in the folder `folder`
    /// (`"pondra"` when none is given), so what it had is there again after a reload. A folder is one page's at a time:
    /// while a kept page has it, another page given the same folder (in another tab, or this one) is refused, and the
    /// folder is let go by `free()` on that page (once the page's member has closed), or by closing or reloading its tab.
    /// `new Page()` keeps nothing. Without OPFS (Node, or a browser without it) this rejects, before anything is read.
    /// `opts` is as `new`'s, and its settings are fixed only once the folder is this page's, so a refused call fixes
    /// nothing.
    #[wasm_bindgen(js_name = kept)]
    pub async fn kept(folder: Option<String>, opts: JsValue) -> Result<Page, JsValue> {
        set_panic_hook();
        let folder = folder.unwrap_or_else(|| "pondra".into());
        check_folder(&folder)?;
        if !opfs::available() {
            return Err(JsValue::from_str(
                "this page has no private file system (OPFS): Page.kept needs navigator.storage.getDirectory",
            ));
        }
        let given = Given::read(&opts)?;
        let Some(held) = js::lock(&format!("pondra:{folder}")).await.map_err(|e| JsValue::from_str(&e))? else {
            return Err(JsValue::from_str(&format!(
                "another page (another tab, or one in this tab) has the folder '{folder}' open: close it, or keep this page in another folder"
            )));
        };
        give(given.settings)?;
        let store: Arc<dyn object_store::ObjectStore> = Arc::new(opfs::Opfs::new(folder));
        // (if the page can't be kept, `held` drops with this return, and the folder is let go)
        let page = pondra::page::Page::open_lake(&options(Some(store), given.node)).await;
        Ok(Page(page.map_err(|e| JsValue::from_str(&format!("{e:#}")))?, Some(held)))
    }

    /// The kept files this page couldn't read, each as `{file, why}`: the file's path and the words of why. Empty when
    /// it read them all, and for a page that keeps nothing. The file is left as it is, so the page can say which tables
    /// it left out.
    pub fn unread(&self) -> JsValue {
        self.0.unread().iter().map(|(file, why)| unread_entry(file, why)).collect::<js_sys::Array>().into()
    }

    /// One request, as to a node (ADR-061's contract): `request('POST', '/sql?format=csv', {}, 'FROM t')`,
    /// `request('PUT', '/files/t.csv', {'If-Match': '"v"'}, bytes)`. `headers` is a plain object of the request's
    /// headers (or undefined or null for none); `body` is text, bytes (a Uint8Array or an ArrayBuffer) or nothing. The
    /// answer is `{status, headers, body}`: `headers` a plain object of the answer's (`content-type` on every one,
    /// `x-pondra-sqlstate` on an error with a SQLSTATE, `etag` on a file), `body` a Uint8Array.
    pub async fn request(&self, method: String, path: String, headers: JsValue, body: JsValue) -> Result<JsValue, JsValue> {
        let headers = header_pairs(&headers)?;
        let body: Vec<u8> = match body.as_string() {
            Some(text) => text.into_bytes(),
            None if body.is_undefined() || body.is_null() => Vec::new(),
            None => js_sys::Uint8Array::new(&body).to_vec(),
        };
        let answer = self.0.request(&method, &path, &headers, body.into()).await;
        let out = js_sys::Object::new();
        js_sys::Reflect::set(&out, &"status".into(), &answer.status.into())?;
        let answered = js_sys::Object::new();
        for (name, value) in &answer.headers {
            js_sys::Reflect::set(&answered, &(*name).into(), &value.as_str().into())?;
        }
        js_sys::Reflect::set(&out, &"headers".into(), &answered)?;
        js_sys::Reflect::set(&out, &"body".into(), &js_sys::Uint8Array::from(&answer.body[..]).into())?;
        Ok(out.into())
    }

    /// Ends the page's member on the core's lake: its loops, its catalog, its term and its lake in memory, so another page
    /// can open in this module. A page that is already closed has nothing to end. wasm-bindgen holds this page mutably
    /// while `close` runs, so a `request` that is still running panics: await the requests first.
    pub async fn close(&mut self) {
        self.0.close().await
    }
}

impl Drop for Page {
    /// Ends the page's member in the background, and lets the folder's Web Lock go only once that has ended: another tab
    /// can't take the folder while the catalog still flushes. A page with no member to end (never requested, or closed)
    /// lets its lock go at once.
    fn drop(&mut self) {
        let Some(ending) = self.0.ending() else { return };
        let held = self.1.take();
        wasm_bindgen_futures::spawn_local(async move {
            ending.await;
            drop(held);
        });
    }
}

/// One unread file as `{file, why}`. A fresh object takes its keys, so setting them cannot fail.
fn unread_entry(file: &str, why: &str) -> JsValue {
    let object = js_sys::Object::new();
    for (key, value) in [("file", file), ("why", why)] {
        js_sys::Reflect::set(&object, &key.into(), &value.into()).expect("a fresh object takes its keys");
    }
    object.into()
}

/// A request's headers from a plain object (`{'If-Match': '"v"'}`), none for undefined or null. A value that isn't a
/// string is refused by name.
fn header_pairs(headers: &JsValue) -> Result<Vec<(String, String)>, JsValue> {
    if headers.is_undefined() || headers.is_null() {
        return Ok(vec![]);
    }
    let object = js_sys::Object::from(headers.clone());
    let mut pairs = vec![];
    for key in js_sys::Object::keys(&object).iter() {
        let name = key.as_string().unwrap_or_default();
        let value = js_sys::Reflect::get(&object, &key)?
            .as_string()
            .ok_or_else(|| JsValue::from_str(&format!("the header {name} must be a string")))?;
        pairs.push((name, value));
    }
    Ok(pairs)
}

/// A page's options as JavaScript gives them (`new Page({node, token, …})`): its node, if it has one, and the settings
/// the node reads, each as its name and its value's text.
struct Given {
    node: Option<Node>,
    settings: Vec<(String, String)>,
}

impl Given {
    /// The options given. None (undefined or null) gives a page with no node and no settings. Each key other than `node`
    /// and `token` is a setting; a setting whose value is undefined is not given.
    fn read(opts: &JsValue) -> Result<Given, JsValue> {
        if opts.is_undefined() || opts.is_null() {
            return Ok(Given { node: None, settings: vec![] });
        }
        if !opts.is_object() {
            return Err(JsValue::from_str("a page's options are a plain object: { node, token, and the node's settings }"));
        }
        let object = js_sys::Object::from(opts.clone());
        let (mut address, mut token, mut settings) = (None, None, vec![]);
        for key in js_sys::Object::keys(&object).iter() {
            let name = key.as_string().unwrap_or_default();
            let value = js_sys::Reflect::get(&object, &key)?;
            match name.as_str() {
                "node" => address = text_option(&name, &value)?,
                "token" => token = text_option(&name, &value)?,
                _ if value.is_undefined() => {}
                _ => settings.push((name.clone(), setting_text(&name, &value)?)),
            }
        }
        Ok(Given { node: address.map(|url| node::node(url, token)), settings })
    }
}

/// A text option (`node`, `token`): its text, or None for undefined, null or empty (an empty value is no value, as an
/// environment's is). Any other value is refused by name.
fn text_option(name: &str, value: &JsValue) -> Result<Option<String>, JsValue> {
    if value.is_undefined() || value.is_null() {
        return Ok(None);
    }
    let text = value.as_string().ok_or_else(|| JsValue::from_str(&format!("{name} must be text")))?;
    Ok((!text.is_empty()).then_some(text))
}

/// A setting's value as text: text as it is, and a number or a boolean as its text.
fn setting_text(name: &str, value: &JsValue) -> Result<String, JsValue> {
    value
        .as_string()
        .or_else(|| value.as_f64().map(|n| n.to_string()))
        .or_else(|| value.as_bool().map(|b| b.to_string()))
        .ok_or_else(|| JsValue::from_str(&format!("{name}'s value must be text, a number or a boolean")))
}

/// Gives the node its settings (`pondra::env::give`), and throws its error as JavaScript's. Nothing is given for none.
fn give(settings: Vec<(String, String)>) -> Result<(), JsValue> {
    if settings.is_empty() {
        return Ok(());
    }
    pondra::env::give(settings).map_err(|e| JsValue::from_str(&format!("{e:#}")))
}

/// A page's options: its member's lake is kept in `lake_store` (a kept page's folder; None is memory, as `new` has it),
/// `node` (`node.rs`) answers the tables the page doesn't have, and the bytes its allocator holds (`heap::live`) are
/// watched against the line its device has (`device_line`).
fn options(lake_store: Option<Arc<dyn object_store::ObjectStore>>, node: Option<Node>) -> pondra::page::Options {
    pondra::page::Options { lake_store, node, memory: Some(Arc::new(heap::live)), line: device_line(), ..Default::default() }
}

const GIB: f64 = (1u64 << 30) as f64;

/// The memory line a page has on this device (`page::Options::line`; `memory_max_gb` is taken over it), or None for the
/// page's own 3 GiB. A phone's browser refuses to grow a module's memory well before 3 GiB, and the page would trap there
/// instead of stopping the query that grew it (53200). A browser that says how much memory its device has
/// (`navigator.deviceMemory`, in GiB, which Chrome rounds down to a power of two and gives as 8 at most) gives three
/// eighths of it, so 8 keeps 3 GiB; one that doesn't say (Safari, Firefox) on a device whose pointer is a finger (`(pointer:
/// coarse)`: a phone or a tablet), or in a worker (no `matchMedia`) whose browser says it is a phone's or a tablet's
/// (`navigator.userAgent`), gives 1 GiB. Elsewhere (a desktop's Firefox or Safari, Node) the page keeps its own.
fn device_line() -> Option<usize> {
    let global = JsValue::from(js_sys::global());
    let said = js::get(&global, "navigator").and_then(|navigator| js::get(&navigator, "deviceMemory")).ok().and_then(|gib| gib.as_f64());
    let gib = match said {
        Some(gib) if gib > 0.0 => gib * 3.0 / 8.0,
        _ if coarse_pointer(&global) || mobile_agent(&global) => 1.0,
        _ => return None,
    };
    Some((gib.min(3.0) * GIB) as usize)
}

/// Whether the device's main pointer is a finger (`matchMedia('(pointer: coarse)')`). A worker has no `matchMedia`: false.
fn coarse_pointer(global: &JsValue) -> bool {
    let Ok(match_media) = js::function(global, "matchMedia") else { return false };
    let Ok(query) = match_media.call1(global, &"(pointer: coarse)".into()) else { return false };
    js::get(&query, "matches").ok().and_then(|matches| matches.as_bool()) == Some(true)
}

/// Whether the browser says it is a phone's or a tablet's (`navigator.userAgent`), which a worker can read where it has no
/// `matchMedia`. (An iPad's Safari says it is a desktop's.)
fn mobile_agent(global: &JsValue) -> bool {
    let agent = js::get(global, "navigator").and_then(|navigator| js::get(&navigator, "userAgent")).ok().and_then(|a| a.as_string()).unwrap_or_default();
    ["Mobi", "Android", "iPhone", "iPad"].iter().any(|m| agent.contains(m))
}

/// Sets the panic hook once: a panic says why, not just "unreachable".
fn set_panic_hook() {
    static HOOK: std::sync::Once = std::sync::Once::new();
    HOOK.call_once(|| std::panic::set_hook(Box::new(|p| error(&p.to_string()))));
}

/// A page's folder is one name in the origin's private file system: not empty, not `.` or `..`, and with no `/` or `\`
/// (a path would reach past the page's folder). Refused by name.
fn check_folder(folder: &str) -> Result<(), JsValue> {
    let bad = folder.is_empty() || folder == "." || folder == ".." || folder.contains(['/', '\\']);
    if bad {
        return Err(JsValue::from_str(&format!(
            "{folder:?} is no folder name: a page's folder is one name, not empty, '.' or '..', and with no '/' or '\\'"
        )));
    }
    Ok(())
}
