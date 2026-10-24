//! The page form (designs/l2-b4-web.md): the core's lake-less engine (`pondra::page::Page`) for a browser or Node,
//! through wasm-bindgen. Its requests and answers are a node's (ADR-061's contract), so a client speaks to a page as
//! it speaks to a node.

use pondra::page::Node;
use std::sync::Arc;
use wasm_bindgen::prelude::*;

mod fetch;
mod heap;
mod js;
mod node;
mod opfs;

#[wasm_bindgen]
extern "C" {
    #[wasm_bindgen(js_namespace = console)]
    fn error(text: &str);
}

/// A page: tables given to it, SQL read as a node reads it, answers in a node's bytes. A kept page holds its folder's
/// Web Lock (`kept`) until it is freed, so no other page writes the folder meanwhile.
#[wasm_bindgen]
// (the lock is never read: it is held for its `Drop`, which lets the folder go when the page is freed)
pub struct Page(pondra::page::Page, #[allow(dead_code)] Option<js::Held>);

#[wasm_bindgen]
impl Page {
    /// A page with no tables, on one partition (a page runs on one thread). It reads the web through `fetch` (`fetch.rs`):
    /// a web server must allow the page's origin (CORS), and expose `Content-Range` only if a read from the end of an
    /// object needs it (an `ETag` it hides is only left out of the object's meta).
    ///
    /// `opts` is a plain object, or nothing: `node` (the node's address) and `token` (its token), both text, make the
    /// page's node (`node.rs`), which answers the tables the page doesn't have. Every other key is a setting the node
    /// reads (`spill_gb` or `PONDRA_SPILL_GB`), given as text, a number or a boolean. The settings are the module's, not
    /// one page's: the first page given any fixes them, and a later page given others is refused by name.
    #[wasm_bindgen(constructor)]
    pub fn new(opts: JsValue) -> Result<Page, JsValue> {
        set_panic_hook();
        let given = Given::read(&opts)?;
        give(given.settings)?;
        Ok(Page(pondra::page::Page::open(&options(None, given.node)), None))
    }

    /// A page kept in the origin's private file system (OPFS): its files and its tables are kept in the folder `folder`
    /// (`"pondra"` when none is given), so what it had is there again after a reload. A folder is one page's at a time:
    /// while a kept page has it, another page given the same folder (in another tab, or this one) is refused, and the
    /// folder is let go by `free()` on that page, or by closing or reloading its tab. `new Page()` keeps nothing. Without
    /// OPFS (Node, or a browser without it) this rejects, before anything is read. `opts` is as `new`'s, and its settings
    /// are fixed only once the folder is this page's, so a refused call fixes nothing.
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
        let page = pondra::page::Page::kept(&options(Some(store), given.node)).await;
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

impl Default for Page {
    fn default() -> Self {
        set_panic_hook();
        Page(pondra::page::Page::open(&options(None, None)), None)
    }
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

/// A page's options: it reads the web through `fetch` (`fetch.rs`), asks `node` (`node.rs`) for the tables it doesn't
/// have, keeps its files and tables in `store` (none is memory, as `new` has it; OPFS is `kept`'s), and watches the bytes
/// its allocator holds (`heap::live`).
fn options(store: Option<Arc<dyn object_store::ObjectStore>>, node: Option<Node>) -> pondra::page::Options {
    pondra::page::Options {
        web: Some(Arc::new(|origin: &str| Arc::new(fetch::Fetch::new(origin)) as Arc<dyn object_store::ObjectStore>)),
        store,
        node,
        memory: Some(Arc::new(heap::live)),
        ..Default::default()
    }
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
