//! The page form (designs/l2-b4-web.md): the core's lake-less engine (`pondra::page::Page`) for a browser or Node,
//! through wasm-bindgen. Its requests and answers are a node's (ADR-061's contract), so a client speaks to a page as
//! it speaks to a node.

use wasm_bindgen::prelude::*;

#[wasm_bindgen]
extern "C" {
    #[wasm_bindgen(js_namespace = console)]
    fn error(text: &str);
}

/// A page: tables given to it, SQL read as a node reads it, answers in a node's bytes.
#[wasm_bindgen]
pub struct Page(pondra::page::Page);

#[wasm_bindgen]
impl Page {
    /// A page with no tables, on one partition (a page runs on one thread).
    #[wasm_bindgen(constructor)]
    pub fn new() -> Page {
        static HOOK: std::sync::Once = std::sync::Once::new();
        HOOK.call_once(|| std::panic::set_hook(Box::new(|p| error(&p.to_string())))); // (a panic says why, not just "unreachable")
        Page(pondra::page::Page::open(&pondra::page::Options::default()))
    }

    /// One request, as to a node: `request('POST', '/sql?format=csv', 'FROM t')`, `request('PUT', '/files/t.csv', bytes)`.
    /// The body is text, bytes (a Uint8Array or an ArrayBuffer) or nothing. The answer is `{status, contentType,
    /// sqlstate, body}`: `sqlstate` null unless it failed, `body` a Uint8Array.
    pub async fn request(&self, method: String, path: String, body: JsValue) -> Result<JsValue, JsValue> {
        let body: Vec<u8> = match body.as_string() {
            Some(text) => text.into_bytes(),
            None if body.is_undefined() || body.is_null() => Vec::new(),
            None => js_sys::Uint8Array::new(&body).to_vec(),
        };
        let answer = self.0.request(&method, &path, body.into()).await;
        let out = js_sys::Object::new();
        js_sys::Reflect::set(&out, &"status".into(), &answer.status.into())?;
        js_sys::Reflect::set(&out, &"contentType".into(), &answer.content_type.into())?;
        js_sys::Reflect::set(&out, &"sqlstate".into(), &answer.sqlstate.map(JsValue::from).unwrap_or(JsValue::NULL))?;
        js_sys::Reflect::set(&out, &"body".into(), &js_sys::Uint8Array::from(&answer.body[..]).into())?;
        Ok(out.into())
    }
}

impl Default for Page {
    fn default() -> Self {
        Page::new()
    }
}
