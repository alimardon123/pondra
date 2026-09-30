//! The console (ADR-030, ADR-032, ADR-034): a page at `/`, embedded in the binary, served by every
//! node and by `pondra serve` over a folder of lakes (its databases). Tabs of notebooks, SQL,
//! Python, data and text files; the lake's tables and files; answers in a grid. No install, no CDN:
//! it works offline, and no request leaves the node.
use axum::{http::{HeaderMap, StatusCode}, response::{IntoResponse, Response}};
use std::{collections::HashMap, sync::LazyLock};

/// The console's files (ADR-034): a page, its style sheet, its modules and its fonts, all built into
/// the binary, their mark and colours from `brand/` (ADR-032). Extensions a node was given
/// (`PONDRA_CONSOLE_EXTENSIONS`: scripts, separated as PATH is) load after the console, at
/// `/console/ext/{i}.js`: an enterprise build adds its own views, kinds of file and actions this way
/// (`window.pondra`).
const MARK: &str = include_str!("../brand/mark.svg");
const COLORS: &str = include_str!("../brand/colors.css");
const JS: &str = "text/javascript; charset=utf-8";

/// One file the console serves: its bytes, the same gzipped when it is text (worked out once), its
/// type, and a tag that is its contents' hash, so a browser asks again each time and gets `304`
/// while it is the same.
struct Asset { body: bytes::Bytes, gz: Option<bytes::Bytes>, kind: &'static str, tag: String }

impl Asset {
    fn new(body: impl Into<Vec<u8>>, kind: &'static str) -> Asset {
        use std::hash::{Hash, Hasher};
        let body: Vec<u8> = body.into();
        let gz = (!kind.starts_with("font/")).then(|| {
            let mut e = flate2::write::GzEncoder::new(vec![], flate2::Compression::best());
            std::io::Write::write_all(&mut e, &body).and_then(|_| e.finish()).map(bytes::Bytes::from).expect("gzip into memory")
        });
        let mut h = std::hash::DefaultHasher::new();
        body.hash(&mut h);
        Asset { body: body.into(), gz, kind, tag: format!("{:016x}", h.finish()) }
    }

    fn serve(&self, headers: &HeaderMap) -> Response {
        let takes_gzip = headers.get("accept-encoding").and_then(|v| v.to_str().ok()).is_some_and(|v| {
            v.split(',').map(|e| e.trim()).any(|e| e.split(';').next() == Some("gzip") && !e.replace(' ', "").ends_with("q=0"))
        });
        let (body, tag) = match &self.gz {
            Some(gz) if takes_gzip => (gz.clone(), format!("\"{}.gz\"", self.tag)),
            _ => (self.body.clone(), format!("\"{}\"", self.tag)),
        };
        if headers.get("if-none-match").is_some_and(|t| t.as_bytes() == tag.as_bytes()) {
            return StatusCode::NOT_MODIFIED.into_response();
        }
        let mut r = ([("content-type", self.kind), ("etag", tag.as_str()), ("cache-control", "no-cache"), ("vary", "accept-encoding")], body).into_response();
        if tag.ends_with(".gz\"") {
            r.headers_mut().insert("content-encoding", axum::http::HeaderValue::from_static("gzip"));
        }
        r
    }
}

/// The page: the header's mark, the favicon, the version and the extensions' scripts filled in.
fn page_text() -> String {
    let icon: String = MARK.bytes().map(|b| match b {
        b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b' ' | b'=' | b'/' | b':' | b'.' | b'-' | b'(' | b')' | b',' | b'"' => (b as char).to_string(),
        b => format!("%{b:02X}"), // (`#`, `<`, `>`, `{`, new lines: as a URL holds them)
    }).collect();
    let extensions: String = (0..extensions().len()).map(|i| format!("<script type=\"module\" src=\"/console/ext/{i}.js\"></script>\n")).collect();
    let html = from_dir("index.html").map(|a| String::from_utf8_lossy(&a.body).to_string());
    html.as_deref().unwrap_or(include_str!("console/index.html"))
        .replacen("{{favicon}}", &format!("data:image/svg+xml,{}", icon.replace('"', "'")), 1)
        .replacen("{{mark}}", MARK.trim(), 1)
        .replacen("{{version}}", env!("CARGO_PKG_VERSION"), 1)
        .replacen("<!--{{extensions}}-->", &extensions, 1)
}

/// The console's own script or style sheet as served: its comments, blank lines and indentation
/// left out (a tenth less to send, gzipped); the code of every other line as written. So its code
/// holds no string or template literal over several lines (console_check's parts run it as served).
fn lean(src: &str) -> String {
    let (mut out, mut comment) = (String::with_capacity(src.len()), false);
    for line in src.lines() {
        let mut t = line.trim();
        if comment {
            let Some(end) = t.find("*/") else { continue };
            (comment, t) = (false, t[end + 2..].trim());
        } else if t.starts_with("/*") && !t.starts_with("/*{{") {
            match t.find("*/") {
                Some(end) => t = t[end + 2..].trim(),
                None => (comment, t) = (true, ""),
            }
        }
        if let Some(at) = t.rfind(" // ").filter(|&at| !t[at..].contains(['\'', '"', '`']) && !t[at + 3..].contains('/')) {
            t = t[..at].trim_end(); // (a comment after the code: no string or regex can go on past it, as none spans lines)
        }
        if !t.is_empty() && !t.starts_with("//") {
            out.push_str(t);
            out.push('\n');
        }
    }
    out
}

/// Everything under `/console/`, by name.
static FILES: LazyLock<HashMap<String, Asset>> = LazyLock::new(|| {
    let mut all: HashMap<String, Asset> = [
        ("console.js", Asset::new(lean(include_str!("console/console.js")), JS)),
        ("core.js", Asset::new(lean(include_str!("console/core.js")), JS)),
        ("editor.js", Asset::new(lean(include_str!("console/editor.js")), JS)),
        ("grid.js", Asset::new(lean(include_str!("console/grid.js")), JS)),
        ("notebook.js", Asset::new(lean(include_str!("console/notebook.js")), JS)),
        ("files.js", Asset::new(lean(include_str!("console/files.js")), JS)),
        // (loaded when first shown, not with the page: its first load stays within ADR-034's budget)
        ("chart.js", Asset::new(lean(include_str!("console/chart.js")), JS)),
        ("plan.js", Asset::new(lean(include_str!("console/plan.js")), JS)),
        ("more.js", Asset::new(lean(include_str!("console/more.js")), JS)),
        ("data.js", Asset::new(lean(include_str!("console/data.js")), JS)),
        ("details.js", Asset::new(lean(include_str!("console/details.js")), JS)),
        ("more.css", Asset::new(lean(include_str!("console/more.css")), "text/css; charset=utf-8")),
        ("console.css", Asset::new(lean(&include_str!("console/console.css").replacen("/*{{colors}}*/", COLORS.trim(), 1)), "text/css; charset=utf-8")),
        ("fonts/Geist.woff2", Asset::new(&include_bytes!("../brand/fonts/Geist.woff2")[..], "font/woff2")),
        ("fonts/GeistMono.woff2", Asset::new(&include_bytes!("../brand/fonts/GeistMono.woff2")[..], "font/woff2")),
    ].into_iter().map(|(n, a)| (n.to_string(), a)).collect();
    for (i, s) in extensions().iter().enumerate() {
        all.insert(format!("ext/{i}.js"), Asset::new(s.as_str(), JS));
    }
    all
});

/// The scripts `PONDRA_CONSOLE_EXTENSIONS` names, read once.
fn extensions() -> &'static [String] {
    static ALL: LazyLock<Vec<String>> = LazyLock::new(|| {
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

/// `PONDRA_CONSOLE_DIR`, for working on the console: its files read from there on every request
/// (as served: lean), so a change shows on a reload, without building the binary again.
fn from_dir(name: &str) -> Option<Asset> {
    let dir = std::env::var_os("PONDRA_CONSOLE_DIR")?;
    let text = std::fs::read_to_string(std::path::Path::new(&dir).join(name)).ok()?;
    Some(match name.rsplit('.').next() {
        Some("js") => Asset::new(lean(&text), JS),
        Some("css") => Asset::new(lean(&text.replacen("/*{{colors}}*/", COLORS.trim(), 1)), "text/css; charset=utf-8"),
        _ => Asset::new(text, "text/html; charset=utf-8"),
    })
}

/// A node's console: its lake.
pub async fn page(headers: HeaderMap) -> Response {
    static PAGE: LazyLock<Asset> = LazyLock::new(|| Asset::new(page_text(), "text/html; charset=utf-8"));
    if std::env::var_os("PONDRA_CONSOLE_DIR").is_some() {
        return Asset::new(page_text(), "text/html; charset=utf-8").serve(&headers);
    }
    PAGE.serve(&headers)
}

/// The server's: its databases, each through `/db/{name}`.
pub async fn server_page(headers: HeaderMap) -> Response {
    static SERVER: LazyLock<Asset> = LazyLock::new(|| Asset::new(page_text().replacen(r#"data-mode="lake""#, r#"data-mode="lakes""#, 1), "text/html; charset=utf-8"));
    SERVER.serve(&headers)
}

/// `GET /console/{file}`: a module, the style sheet, a font, an extension.
pub async fn file(axum::extract::Path(name): axum::extract::Path<String>, headers: HeaderMap) -> Response {
    if let Some(a) = (!name.contains("..") && !name.starts_with("fonts/")).then(|| from_dir(&name)).flatten() {
        return a.serve(&headers);
    }
    match FILES.get(&name) {
        Some(a) => a.serve(&headers),
        None => (StatusCode::NOT_FOUND, "no such file").into_response(),
    }
}

/// Where the console's settings are kept on this machine, for every lake and every session:
/// `PONDRA_CONFIG_DIR`, else the system's place for a program's settings (Windows `%APPDATA%`,
/// macOS `~/Library/Application Support`, else `$XDG_CONFIG_HOME` or `~/.config`), in `pondra/`.
fn settings_file() -> Option<std::path::PathBuf> { settings_dir().map(|d| d.join("console.json")) }

/// This machine's folder for Pondra's settings (the console's, the Python chosen).
pub fn settings_dir() -> Option<std::path::PathBuf> {
    let var = |k: &str| std::env::var_os(k).filter(|v| !v.is_empty()).map(std::path::PathBuf::from);
    var("PONDRA_CONFIG_DIR").or_else(|| match () {
        _ if cfg!(windows) => var("APPDATA").map(|d| d.join("pondra")),
        _ if cfg!(target_os = "macos") => var("HOME").map(|d| d.join("Library/Application Support/pondra")),
        _ => var("XDG_CONFIG_HOME").or_else(|| var("HOME").map(|d| d.join(".config"))).map(|d| d.join("pondra")),
    })
}

/// `GET /console/settings`: the console's settings kept on this machine (`{}` if none yet): its
/// theme, colours, fonts and layout, the same for every lake and session opened here.
pub async fn settings() -> Response {
    let body = settings_file().and_then(|f| std::fs::read_to_string(f).ok()).filter(|s| serde_json::from_str::<serde_json::Value>(s).is_ok()).unwrap_or_else(|| "{}".into());
    ([("content-type", "application/json"), ("cache-control", "no-store")], body).into_response()
}

/// `PUT /console/settings`: keep them, for the page of someone on this machine (a request from
/// another one is refused: the file is this machine's user's). At most 64 KB of JSON.
pub async fn save_settings(axum::extract::ConnectInfo(from): axum::extract::ConnectInfo<std::net::SocketAddr>, body: bytes::Bytes) -> Response {
    if !from.ip().is_loopback() {
        return (StatusCode::FORBIDDEN, "the console's settings are kept by a page on this machine only").into_response();
    }
    if body.len() > 64 << 10 || serde_json::from_slice::<serde_json::Map<String, serde_json::Value>>(&body).is_err() {
        return (StatusCode::BAD_REQUEST, "settings are one JSON object, at most 64 KB").into_response();
    }
    let Some(file) = settings_file() else { return (StatusCode::NOT_FOUND, "no place for settings on this machine (set PONDRA_CONFIG_DIR)").into_response() };
    let wrote = file.parent().map_or(Ok(()), std::fs::create_dir_all).and_then(|_| {
        let tmp = file.with_extension("json.tmp"); // (whole or not at all: two pages saving at once)
        std::fs::write(&tmp, &body).and_then(|_| std::fs::rename(&tmp, &file))
    });
    match wrote {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, format!("{}: {e}", file.display())).into_response(),
    }
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
