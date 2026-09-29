//! The console at `/` (ADR-030): one page, in the binary, that every node and `pondra server`
//! serve. It needs no install and no network beyond this node.
use axum::response::Html;

pub fn page() -> Html<&'static str> { Html(include_str!("console.html")) }
