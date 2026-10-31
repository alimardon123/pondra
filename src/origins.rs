//! Pages of other origins call a node (`web/`, B5). A page in a browser sends its node `POST /sql` from its own
//! origin (`http://localhost:5173`, `https://app.example`), and the browser lets that request through only when the
//! node answers with CORS headers naming the page's origin. A node answers no CORS header unless its operator names
//! the origins it trusts (`PONDRA_WEB_ORIGINS`), so a node that names none is as it was. A named origin also passes
//! `auth::cross_site` on a node with no sign-in, because the operator chose it; `*` never does, since on such a node
//! it would let every site in the world act as an admin through its visitors' browsers.
use anyhow::{anyhow, Result};
use axum::extract::{Request, State};
use axum::http::{header, HeaderValue, Method, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use std::sync::{Arc, LazyLock};
use url::Url;

/// The origins a node's operator names: `*` (any), and the listed ones as a browser writes them.
#[derive(Clone, Default, Debug)]
pub struct Origins {
    any: bool,
    listed: Vec<String>,
}

impl Origins {
    /// The setting's text, read entry by entry. Each entry is `*` or an origin, and anything else stops the node at
    /// start, by name: a typo must not quietly open a node wider than its operator meant.
    pub fn parse(text: &str) -> Result<Origins> {
        let mut origins = Origins::default();
        for entry in text.split(',').map(str::trim).filter(|e| !e.is_empty()) {
            match origin_of(entry)? {
                Some(origin) => origins.listed.push(origin),
                None => origins.any = true,
            }
        }
        Ok(origins)
    }

    /// Does a page from `origin` (its `Origin` header) get CORS headers? `*` says yes to every origin, `null` (a
    /// sandboxed page, a file) included; a list says yes only to the origins it names.
    pub fn allows(&self, origin: &str) -> bool {
        self.any || opens(self, origin)
    }

    /// Does this node answer CORS headers at all: it names `*` or an origin.
    fn named(&self) -> bool {
        self.any || !self.listed.is_empty()
    }
}

/// One entry of the setting: `None` for `*`, else the origin it names. Trimmed and lowercased first, with one trailing
/// `/` taken off, so `https://App.Example/` is the origin `https://app.example`.
fn origin_of(entry: &str) -> Result<Option<String>> {
    let lower = entry.to_ascii_lowercase();
    let text = lower.strip_suffix('/').unwrap_or(&lower);
    if text == "*" {
        return Ok(None);
    }
    let url = Url::parse(text).ok();
    // (`Url` takes `*` in a host as a literal name that matches nothing: a wildcard would be silently useless)
    if url.as_ref().and_then(Url::host_str).is_some_and(|host| host.contains('*')) {
        anyhow::bail!("web_origins: \"{entry}\": name each origin; a wildcard host isn't taken");
    }
    match url.filter(is_origin_only) {
        // (`origin()` leaves out a default port, so `https://app.example:443` is the same entry as `https://app.example`)
        Some(url) => Ok(Some(url.origin().ascii_serialization())),
        None => Err(anyhow!("web_origins: \"{entry}\" isn't an origin (scheme://host[:port], or *)")),
    }
}

/// Is this URL an origin and nothing more: http or https, a host, and no path, query, fragment or sign-in.
fn is_origin_only(url: &Url) -> bool {
    matches!(url.scheme(), "http" | "https")
        && url.host_str().is_some()
        && url.path() == "/"
        && url.query().is_none()
        && url.fragment().is_none()
        && url.username().is_empty()
        && url.password().is_none()
}

/// The setting as the operator wrote it, `""` when unset.
fn setting() -> String {
    crate::env::get("PONDRA_WEB_ORIGINS").unwrap_or_default()
}

/// Stops a node at start when `PONDRA_WEB_ORIGINS` names something that isn't an origin (`node.rs`).
pub fn check() -> Result<()> {
    Origins::parse(&setting()).map(|_| ())
}

/// The setting, read once from the first use. A node reaches here only after `check` passed, so a parse error can't
/// be there; the default (no origin) covers it anyway.
pub(crate) static ORIGINS: LazyLock<Origins> = LazyLock::new(|| Origins::parse(&setting()).unwrap_or_default());

/// Is `origin` one the operator named, by its own name (never `*`)? `auth::cross_site` lets such a page past its checks.
pub fn listed(origin: &str) -> bool {
    opens(&ORIGINS, origin)
}

/// Is `origin` one of the listed origins? Browsers write origins in lower case, the entries are kept so, and `*` is
/// not a name, so it opens nothing here.
fn opens(origins: &Origins, origin: &str) -> bool {
    origins.listed.iter().any(|e| e.eq_ignore_ascii_case(origin))
}

/// The CORS door, the router's outermost layer. A listed page's request gets its answer with the headers that let the
/// page read it, and its preflight is answered here, with no route run (a preflight carries no token). With no origin
/// named, nothing is added to any answer. With some named, every answer gets `vary: origin`, so a cache never gives a
/// listed page an answer made without its headers, or the reverse.
pub async fn door(State(origins): State<Arc<Origins>>, req: Request, next: Next) -> Response {
    if !origins.named() {
        return next.run(req).await;
    }
    let Some(origin) = asked_by(&origins, &req) else {
        return vary(next.run(req).await);
    };
    if is_preflight(&req) {
        return vary(preflight(origin));
    }
    let mut answer = next.run(req).await;
    let headers = answer.headers_mut();
    headers.insert(header::ACCESS_CONTROL_ALLOW_ORIGIN, origin);
    // (what a page may read beyond the usual: the SQLSTATE of a refusal, and an answer's ETag for a cache, and the
    // span and size of a file's range, which a Parquet reader asks for)
    headers.insert(header::ACCESS_CONTROL_EXPOSE_HEADERS, HeaderValue::from_static("x-pondra-sqlstate, etag, content-range, content-length, accept-ranges"));
    vary(answer)
}

/// The request's `Origin`, as its browser wrote it, when `origins` lets its page through.
fn asked_by(origins: &Origins, req: &Request) -> Option<HeaderValue> {
    let origin = req.headers().get(header::ORIGIN)?;
    origins.allows(origin.to_str().ok()?).then(|| origin.clone())
}

/// A preflight: an `OPTIONS` asking which method the request will use.
fn is_preflight(req: &Request) -> bool {
    req.method() == Method::OPTIONS && req.headers().contains_key(header::ACCESS_CONTROL_REQUEST_METHOD)
}

/// The answer to a listed page's preflight: 204, with the methods and headers the page's requests use.
fn preflight(origin: HeaderValue) -> Response {
    let mut answer = StatusCode::NO_CONTENT.into_response();
    let headers = answer.headers_mut();
    headers.insert(header::ACCESS_CONTROL_ALLOW_ORIGIN, origin);
    headers.insert(header::ACCESS_CONTROL_ALLOW_METHODS, HeaderValue::from_static("GET, POST, PUT, DELETE, HEAD"));
    // (`range` is not a safelisted request header: a page's Parquet reads send it)
    headers.insert(header::ACCESS_CONTROL_ALLOW_HEADERS, HeaderValue::from_static("authorization, content-type, range"));
    // (ten minutes: a browser asks once for a page's requests, not before each one)
    headers.insert(header::ACCESS_CONTROL_MAX_AGE, HeaderValue::from_static("600"));
    answer
}

/// `vary: origin` added after any `vary` the answer has already: a cache must key on the page's origin.
fn vary(mut answer: Response) -> Response {
    answer.headers_mut().append(header::VARY, HeaderValue::from_static("origin"));
    answer
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::middleware;
    use axum::routing::post;
    use axum::Router;
    use tower::ServiceExt;

    /// A `POST /sql` route answering 200 with a `vary` of its own, behind `door` for these origins.
    fn app(origins: &str) -> Router {
        let origins = Arc::new(Origins::parse(origins).unwrap());
        Router::new()
            .route("/sql", post(|| async { ([(header::VARY, "accept-encoding")], "ok") }))
            .layer(middleware::from_fn_with_state(origins, door))
    }

    /// A `POST /sql` from `origin`, when given.
    fn ask(origin: Option<&str>) -> Request {
        let mut req = axum::http::Request::builder().method(Method::POST).uri("/sql");
        if let Some(origin) = origin {
            req = req.header(header::ORIGIN, origin);
        }
        req.body(Body::empty()).unwrap()
    }

    /// A browser's preflight of `POST /sql` from `origin`.
    fn preflight_of(origin: &str) -> Request {
        axum::http::Request::builder()
            .method(Method::OPTIONS)
            .uri("/sql")
            .header(header::ORIGIN, origin)
            .header(header::ACCESS_CONTROL_REQUEST_METHOD, "POST")
            .body(Body::empty())
            .unwrap()
    }

    /// Every value of a response header, as text.
    fn values(res: &Response, name: &str) -> Vec<String> {
        res.headers().get_all(name).iter().map(|v| v.to_str().unwrap().to_string()).collect()
    }

    /// No CORS header of any kind on the answer.
    fn no_cors(res: &Response) -> bool {
        res.headers().keys().all(|k| !k.as_str().starts_with("access-control-"))
    }

    #[test]
    fn entries_are_origins_or_refused_by_name() {
        assert_eq!(Origins::parse("https://App.Example/").unwrap().listed, ["https://app.example"]);
        assert_eq!(Origins::parse("https://app.example:443").unwrap().listed, ["https://app.example"]);
        assert_eq!(Origins::parse("http://localhost:5173, *").unwrap().listed, ["http://localhost:5173"]);
        assert!(Origins::parse("*").unwrap().any);
        for bad in ["app.example", "https://app.example/x", "ftp://a.b", "https://u:p@a.b", "https://app.example?q", "https://*.example.com"] {
            let err = Origins::parse(bad).unwrap_err().to_string();
            assert!(err.contains(bad), "{bad}: the error names the entry: {err}");
        }
        let wild = Origins::parse("https://*.example.com").unwrap_err().to_string();
        assert!(wild.contains("a wildcard host isn't taken"), "{wild}");
    }

    #[tokio::test]
    async fn a_listed_origins_preflight_is_answered_without_the_routes() {
        let res = app("https://app.example").oneshot(preflight_of("https://app.example")).await.unwrap();
        // (the route takes only POST, so a route run would answer 405)
        assert_eq!(res.status(), StatusCode::NO_CONTENT);
        assert_eq!(values(&res, "access-control-allow-origin"), ["https://app.example"]);
        assert_eq!(values(&res, "access-control-allow-methods"), ["GET, POST, PUT, DELETE, HEAD"]);
        assert_eq!(values(&res, "access-control-allow-headers"), ["authorization, content-type, range"]);
        assert_eq!(values(&res, "access-control-max-age"), ["600"]);
        assert!(values(&res, "vary").contains(&"origin".to_string()));
    }

    #[tokio::test]
    async fn a_listed_origins_answer_names_it() {
        let res = app("https://app.example").oneshot(ask(Some("https://app.example"))).await.unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        assert_eq!(values(&res, "access-control-allow-origin"), ["https://app.example"]);
        assert_eq!(values(&res, "access-control-expose-headers"), ["x-pondra-sqlstate, etag, content-range, content-length, accept-ranges"]);
        assert_eq!(values(&res, "vary"), ["accept-encoding", "origin"]);
        assert!(res.headers().get("access-control-allow-credentials").is_none(), "a page sends its own token, never the console's cookie");
    }

    #[tokio::test]
    async fn an_unlisted_origin_and_no_origin_get_no_headers() {
        for req in [ask(Some("https://evil.example")), ask(None)] {
            let res = app("https://app.example").oneshot(req).await.unwrap();
            assert!(no_cors(&res));
            assert_eq!(values(&res, "vary"), ["accept-encoding", "origin"]);
        }
        let res = app("https://app.example").oneshot(preflight_of("https://evil.example")).await.unwrap();
        assert_eq!(res.status(), StatusCode::METHOD_NOT_ALLOWED, "an unlisted preflight reaches the routes");
        assert!(no_cors(&res));
    }

    #[tokio::test]
    async fn with_no_origin_named_no_answer_gets_a_header_from_door() {
        let res = app("").oneshot(ask(Some("https://app.example"))).await.unwrap();
        assert_eq!(values(&res, "vary"), ["accept-encoding"]);
        assert!(no_cors(&res));
        let res = app("").oneshot(preflight_of("https://app.example")).await.unwrap();
        assert!(values(&res, "vary").is_empty());
        assert!(no_cors(&res));
    }

    #[tokio::test]
    async fn any_echoes_the_origin_and_null_only_by_any() {
        let res = app("*").oneshot(ask(Some("https://anyone.example"))).await.unwrap();
        assert_eq!(values(&res, "access-control-allow-origin"), ["https://anyone.example"], "the request's own origin, never `*`");
        let res = app("*").oneshot(ask(Some("null"))).await.unwrap();
        assert_eq!(values(&res, "access-control-allow-origin"), ["null"]);
        let res = app("https://app.example").oneshot(ask(Some("null"))).await.unwrap();
        assert!(no_cors(&res), "a list never lets `null` through");
    }

    #[test]
    fn listed_opens_cross_site_for_named_origins_only() {
        assert!(opens(&Origins::parse("https://a.example").unwrap(), "https://A.example"));
        assert!(!opens(&Origins::parse("*").unwrap(), "https://a.example"));
        let both = Origins::parse("*, https://a.example").unwrap();
        assert!(opens(&both, "https://a.example") && !opens(&both, "https://b.example"));
        assert!(!opens(&both, "null"));
    }
}
