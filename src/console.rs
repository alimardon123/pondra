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
