//! The console (ADR-030): one page at `/`, embedded in the binary, served by every node and by
//! `pondra serve` over a folder of lakes (its databases). SQL, Python and text cells, a live switch, notebooks kept in the lake as
//! `.ipynb`. No install, no CDN: it works offline, and no request leaves the node.
use axum::response::Html;

/// The page, its mark and colours from their one source, `brand/` (ADR-032): the mark in the header
/// and as the tab's icon, the colours as the page's own.
static PAGE: std::sync::LazyLock<String> = std::sync::LazyLock::new(|| {
    const MARK: &str = include_str!("../brand/mark.svg");
    const COLORS: &str = include_str!("../brand/colors.css");
    let icon: String = MARK.bytes().map(|b| match b {
        b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b' ' | b'=' | b'/' | b':' | b'.' | b'-' | b'(' | b')' | b',' | b'"' => (b as char).to_string(),
        b => format!("%{b:02X}"), // (`#`, `<`, `>`, `{`, new lines: as a URL holds them)
    }).collect();
    include_str!("console.html")
        .replacen("{{favicon}}", &format!("data:image/svg+xml,{}", icon.replace('"', "'")), 1)
        .replacen("{{mark}}", MARK.trim(), 1)
        .replacen("/*{{colors}}*/", COLORS.trim(), 1)
});

/// A node's console: its lake.
pub fn page() -> Html<&'static str> { Html(PAGE.as_str()) }

/// The server's: its databases, each through `/db/{name}`.
pub fn server_page() -> Html<&'static str> {
    static SERVER: std::sync::LazyLock<String> = std::sync::LazyLock::new(|| PAGE.replacen(r#"data-mode="node""#, r#"data-mode="server""#, 1));
    Html(SERVER.as_str())
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
