//! The console (ADR-030, ADR-032): a page at `/`, embedded in the binary, served by every node and by
//! `pondra serve` over a folder of lakes (its databases). SQL, Python and text cells, a live switch, notebooks kept in the lake as
//! `.ipynb`. No install, no CDN: it works offline, and no request leaves the node.
use axum::response::Html;

/// The console's files: a page that holds the header, and the module and stylesheet it loads, all
/// built into the binary, their mark and colours from `brand/` (ADR-032). Extensions a node was
/// given (`PONDRA_CONSOLE_EXTENSIONS`: scripts, separated as PATH is) load after the console, at
/// `/console/ext/{i}.js`: an enterprise build adds its own sections, panels and actions this way
/// (`window.pondra`).
const MARK: &str = include_str!("../brand/mark.svg");
const COLORS: &str = include_str!("../brand/colors.css");

static PAGE: std::sync::LazyLock<String> = std::sync::LazyLock::new(|| {
    let icon: String = MARK.bytes().map(|b| match b {
        b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b' ' | b'=' | b'/' | b':' | b'.' | b'-' | b'(' | b')' | b',' | b'"' => (b as char).to_string(),
        b => format!("%{b:02X}"), // (`#`, `<`, `>`, `{`, new lines: as a URL holds them)
    }).collect();
    let extensions: String = (0..extensions().len()).map(|i| format!("<script type=\"module\" src=\"/console/ext/{i}.js\"></script>\n")).collect();
    include_str!("console/index.html")
        .replacen("{{favicon}}", &format!("data:image/svg+xml,{}", icon.replace('"', "'")), 1)
        .replacen("{{mark}}", MARK.trim(), 1)
        .replacen("<!--{{extensions}}-->", &extensions, 1)
});
static CSS: std::sync::LazyLock<String> = std::sync::LazyLock::new(|| include_str!("console/console.css").replacen("/*{{colors}}*/", COLORS.trim(), 1));
const JS: &str = include_str!("console/console.js");

/// The scripts `PONDRA_CONSOLE_EXTENSIONS` names, read once.
fn extensions() -> &'static [String] {
    static ALL: std::sync::LazyLock<Vec<String>> = std::sync::LazyLock::new(|| {
        std::env::var_os("PONDRA_CONSOLE_EXTENSIONS").map(|v| std::env::split_paths(&v).filter_map(|p| match std::fs::read_to_string(&p) {
            Ok(s) => Some(s),
            Err(e) => {
                eprintln!("console extension {}: {e}", p.display());
                None
            }
        }).collect()).unwrap_or_default()
    });
    &ALL
}

/// A node's console: its lake.
pub fn page() -> Html<&'static str> { Html(PAGE.as_str()) }

/// The server's: its databases, each through `/db/{name}`.
pub fn server_page() -> Html<&'static str> {
    static SERVER: std::sync::LazyLock<String> = std::sync::LazyLock::new(|| PAGE.replacen(r#"data-mode="lake""#, r#"data-mode="lakes""#, 1));
    Html(SERVER.as_str())
}

/// `GET /console/{file}`: the module, the stylesheet, an extension. Browsers ask again each time
/// and get `304` while it is the same (its tag is its contents' hash).
pub async fn file(axum::extract::Path(name): axum::extract::Path<String>, headers: axum::http::HeaderMap) -> axum::response::Response {
    use axum::response::IntoResponse;
    let (body, kind): (&str, &str) = match name.as_str() {
        "console.js" => (JS, "text/javascript; charset=utf-8"),
        "console.css" => (CSS.as_str(), "text/css; charset=utf-8"),
        n => match n.strip_prefix("ext/").and_then(|n| n.strip_suffix(".js")).and_then(|i| i.parse::<usize>().ok()).and_then(|i| extensions().get(i)) {
            Some(s) => (s.as_str(), "text/javascript; charset=utf-8"),
            None => return (axum::http::StatusCode::NOT_FOUND, "no such file").into_response(),
        },
    };
    let tag = format!("\"{:x}\"", { use std::hash::{Hash, Hasher}; let mut h = std::hash::DefaultHasher::new(); body.hash(&mut h); h.finish() });
    if headers.get("if-none-match").is_some_and(|t| t.as_bytes() == tag.as_bytes()) {
        return axum::http::StatusCode::NOT_MODIFIED.into_response();
    }
    ([("content-type", kind), ("etag", tag.as_str()), ("cache-control", "no-cache")], body).into_response()
}

/// `GET /objects`: what the console's catalog and its details panel show of each table and view
/// of this lake and the lakes attached to it — its kind, key, layout, formats and size, and a
/// view's definition — read from the catalog alone, no query run (ADR-032); and where the lake's
/// own files are, for `read_csv('…/files/x.csv')`. Columns come from `information_schema`, as
/// every client reads them.
pub async fn objects(lake: &crate::store::Lake) -> anyhow::Result<serde_json::Value> {
    use crate::store::TableMeta;
    let mut all = vec![];
    let attached: Vec<(String, std::sync::Arc<crate::store::Lake>)> = lake.attached.read().unwrap().clone();
    let lakes = std::iter::once((crate::ddl::lake_name(lake), lake.arc())).chain(attached);
    for (catalog, l) in lakes {
        let materialized: std::collections::HashSet<String> = l.cat.scan::<serde_json::Value>("v/", "v0").await?.into_iter().map(|(k, _)| k[2..].to_string()).collect();
        for (k, m) in l.cat.scan::<TableMeta>("t/", "t0").await? {
            let name = &k[2..];
            if crate::sys::hidden(name) {
                continue;
            }
            let (schema, table) = crate::ddl::split(name);
            let m = m.logical();
            let sealed = m.sealed.clone().unwrap_or_default();
            let (rows, bytes) = m.files.iter().fold((sealed.rows, sealed.bytes), |(r, b), f| (r + f.rows, b + f.bytes));
            let kind = if materialized.contains(name) || materialized.contains(name.trim_end_matches("_final")) { "materialized view" } else { "table" };
            all.push(serde_json::json!({"catalog": catalog, "schema": schema, "name": table, "kind": kind, "rows": rows, "bytes": bytes,
                "files": m.files.len() as u64 + sealed.files, "key": m.key, "partition": m.partition, "cluster": m.cluster, "publish": m.publish,
                "not_null": m.not_null, "defaults": m.defaults, "ttl": m.ttl.map(|(c, s)| format!("{c}: {s} s"))}));
        }
        for (k, v) in l.cat.scan::<crate::ddl::StoredView>("q/", "q0").await? {
            let (schema, view) = crate::ddl::split(&k[2..]);
            all.push(serde_json::json!({"catalog": catalog, "schema": schema, "name": view, "kind": if v.external { "files" } else { "view" }, "sql": v.sql}));
        }
    }
    Ok(serde_json::json!({"objects": all, "files": format!("{}/files/", lake.url.trim_end_matches('/'))}))
}
