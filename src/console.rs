//! The console (ADR-030): one page at `/`, embedded in the binary, served by every node and by
//! `pondra server`. SQL, Python and text cells, a live switch, notebooks kept in the lake as
//! `.ipynb`. No install, no CDN: it works offline, and no request leaves the node.
use axum::response::Html;

const PAGE: &str = include_str!("console.html");

/// A node's console: its lake.
pub fn page() -> Html<&'static str> { Html(PAGE) }

/// The server's: its databases, each through `/db/{name}`.
pub fn server_page() -> Html<&'static str> {
    static SERVER: std::sync::LazyLock<String> = std::sync::LazyLock::new(|| PAGE.replacen(r#"data-mode="node""#, r#"data-mode="server""#, 1));
    Html(SERVER.as_str())
}
