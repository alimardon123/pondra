//! The browser's way in (ADR-074 §5, invariant 411): a sign-in the node finishes itself, so it works
//! with every provider (Google's needs a client secret) and no provider token is ever kept in a page.
//! The console sends the browser to `/sso/{provider}/start`; the provider sends it back to
//! `/sso/callback` with a code; the node exchanges the code (with PKCE's verifier, and the client's
//! secret if the provider names one), checks the ID token as every token is checked (`sso::checked`)
//! and its nonce, and hands the page a session in the URL's fragment, which no server sees.
//! `pondra login` starts the same flow towards its own loopback address (RFC 8252) and gets a code back
//! that only it can redeem (`/sso/token`, with a verifier of its own). Nothing is kept on a node: the
//! flow rides in a cookie signed with the lake's key, so any node of the lake finishes a flow another
//! started.
use crate::server::App;
use crate::sso::Claimed;
use crate::tls::Peer;
use anyhow::{bail, ensure, Context, Result};
use axum::body::Bytes;
use axum::extract::{ConnectInfo, Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use base64::{engine::general_purpose::URL_SAFE_NO_PAD as B64U, Engine};
use serde_json::{json as j, Value};
use std::collections::HashMap;
use std::time::Duration;

/// The cookie a flow rides in, from its start to its callback.
const COOKIE: &str = "pondra_sso";
/// How long a person has at the provider.
const FLOW_MS: u64 = 10 * 60_000;
/// How long the command line has to redeem its code.
const CODE_MS: u64 = 60_000;
/// What the lake's key signs a flow's cookie and a command line's code for (`users::sign_for`): neither
/// passes for the other, nor for a session.
const FLOW: &str = "sso-flow";
const CODE: &str = "sso-code";

/// `GET /sso/{provider}/start?to=…`: to the provider, with the flow in a cookie. `to` is a path of this
/// node's (the console's page), or for `pondra login` its loopback address, with `challenge` (S256 of
/// the verifier it will redeem its code with).
pub async fn start(State(app): State<App>, ConnectInfo(peer): ConnectInfo<Peer>, Path(name): Path<String>, headers: HeaderMap, uri: axum::http::Uri, Query(q): Query<HashMap<String, String>>) -> Response {
    match begin(&app, &peer, &name, &headers, &uri, &q).await {
        Ok((to, cookie)) => (StatusCode::FOUND, [("location", to), ("set-cookie", cookie), ("cache-control", "no-store".into())]).into_response(),
        Err(e) => {
            crate::audit::refused(&app, "", "http", Some(peer.addr), "sign in", &format!("identity provider {name}: {e:#}"));
            // (the page, or the command line that asked, hears why where the sign-in would have ended; a request
            // naming nowhere it may end, or a command line's without its challenge, gets the reason as the answer)
            let why = format!("not signed in through {name}: {}", crate::ext::said(&e));
            let place = match destination(q.get("to").map_or("/", String::as_str)) {
                Ok(to) if to.starts_with('/') => Some(format!("{to}#pondra_error={}", encode(&why))),
                Ok(to) if q.contains_key("challenge") => Some(with_query(&to, "error", &why)),
                _ => None,
            };
            match place {
                Some(place) => (StatusCode::FOUND, [("location", place), ("cache-control", "no-store".into())]).into_response(),
                None => (StatusCode::BAD_REQUEST, format!("signing in through {name}: {}", crate::ext::said(&e))).into_response(),
            }
        }
    }
}

async fn begin(app: &App, peer: &Peer, name: &str, headers: &HeaderMap, uri: &axum::http::Uri, q: &HashMap<String, String>) -> Result<(String, String)> {
    let all = crate::sso::all(&app.lake).await?;
    let (_, p) = all.iter().find(|(n, _)| n == name).with_context(|| format!("no identity provider {name}"))?;
    let to = destination(q.get("to").map_or("/", String::as_str))?;
    let challenge = match to.starts_with('/') {
        true => None,
        false => Some(q.get("challenge").filter(|c| c.len() == 43 && c.bytes().all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_'))).context("a sign-in that ends at the command line names its challenge (S256 of its verifier, RFC 7636)")?.clone()),
    };
    let doc = crate::sso::discovery(&p.issuer).await?;
    let authorize = doc["authorization_endpoint"].as_str().context("its discovery document has no authorization_endpoint")?;
    let token = doc["token_endpoint"].as_str().context("its discovery document has no token_endpoint")?;
    ensure!(crate::sso::reachable(authorize) && crate::sso::reachable(token), "its endpoints are https (http only on this machine): {authorize}, {token}");
    let redirect = format!("{}/sso/callback", peer.origin(headers, uri).context("the request names no host")?);
    let path = reqwest::Url::parse(&redirect)?.path().trim_end_matches("callback").to_string(); // (the cookie goes only to this database's /sso/)
    let (state, nonce, verifier) = (random(), random(), random());
    let flow = j!({"p": name, "s": state, "n": nonce, "v": verifier, "to": to, "ch": challenge, "r": redirect, "t": token, "c": path, "x": crate::log::now_ms() + FLOW_MS});
    let secure = if redirect.starts_with("https:") { "; Secure" } else { "" };
    let cookie = format!("{COOKIE}={}; Path={path}; Max-Age={}; HttpOnly; SameSite=Lax{secure}", signed(app, FLOW, &flow).await?, FLOW_MS / 1000);
    let scopes = match p.scopes.split_whitespace().any(|s| s == "openid") {
        true => p.scopes.clone(),
        false => format!("openid {}", p.scopes), // (an ID token comes only with `openid`)
    };
    let mut at = reqwest::Url::parse(authorize)?;
    at.query_pairs_mut()
        .append_pair("response_type", "code")
        .append_pair("client_id", &p.client_id)
        .append_pair("redirect_uri", &redirect)
        .append_pair("scope", &scopes)
        .append_pair("state", &state)
        .append_pair("nonce", &nonce)
        .append_pair("code_challenge", &s256(&verifier))
        .append_pair("code_challenge_method", "S256");
    Ok((at.to_string(), cookie))
}

/// Where a sign-in may end: a path of this node's (one `/`, so never another site: `//host`), or a loopback
/// address with a port, the command line's. Anything else would send a session to whoever named it.
fn destination(to: &str) -> Result<String> {
    let to = to.split('#').next().unwrap_or_default();
    if to.starts_with('/') && !to.starts_with("//") && !to.contains('\\') && !to.bytes().any(|b| b.is_ascii_control()) {
        return Ok(to.to_string());
    }
    let u = reqwest::Url::parse(to).ok().context("a sign-in ends at a path of this node's (/…) or at the command line's own address (http://127.0.0.1:port/)")?;
    let loopback = u.scheme() == "http" && matches!(u.host_str(), Some("127.0.0.1" | "localhost" | "[::1]")) && u.port().is_some();
    ensure!(loopback && u.username().is_empty() && u.password().is_none(), "a sign-in ends at a path of this node's (/…) or at the command line's own address (http://127.0.0.1:port/), not at {to}");
    Ok(u.to_string())
}

/// `GET /sso/callback?code=…&state=…`: the provider's answer. The page gets a session in its URL's
/// fragment (or `pondra_error`); the command line a code to redeem (or `error`).
pub async fn callback(State(app): State<App>, ConnectInfo(peer): ConnectInfo<Peer>, headers: HeaderMap, Query(q): Query<HashMap<String, String>>) -> Response {
    let Some(flow) = flow_of(&app, &headers).await else {
        let why = "this browser holds no sign-in of this node's, or it took longer than ten minutes: start it again";
        crate::audit::refused(&app, "", "http", Some(peer.addr), "sign in", why);
        return (StatusCode::BAD_REQUEST, why).into_response();
    };
    let to = flow["to"].as_str().unwrap_or("/");
    let clear = format!("{COOKIE}=; Path={}; Max-Age=0; HttpOnly; SameSite=Lax", flow["c"].as_str().unwrap_or("/sso/"));
    let name = flow["p"].as_str().unwrap_or_default();
    // (where the browser goes, and, for the command line, whom its code signs in)
    let ended = match finish(&app, &flow, &q).await {
        Ok(c) => match flow["ch"].as_str() {
            Some(challenge) => code_for(&app, &c, challenge).await.map(|code| (with_query(to, "code", &code), Some(c.user.clone()))),
            None => crate::users::session_of(&app.lake, &c).await.map(|(token, until)| {
                (format!("{to}#pondra_session={token}&pondra_user={}&pondra_until={until}", encode(&c.user)), None)
            }),
        },
        Err(e) => Err(e),
    };
    let (place, asks) = ended.unwrap_or_else(|e| {
        crate::audit::refused(&app, "", "http", Some(peer.addr), "sign in", &format!("identity provider {name}: {e:#}"));
        let why = format!("not signed in through {name}: {}", crate::ext::said(&e)); // (as the sign-in dialog and the command line say it, a door's words)
        match flow["ch"].is_string() {
            true => (with_query(to, "error", &why), None),
            false => (format!("{to}#pondra_error={}", encode(&why)), None),
        }
    });
    match asks {
        // (a code goes to the command line only once the person says so on this page: the link that started the
        // sign-in may be anyone's, naming a port of this machine and a challenge of theirs, and a person already
        // signed in at the provider would otherwise hand them a code with no question asked)
        Some(user) => (StatusCode::OK, [("content-type", "text/html; charset=utf-8".to_string()), ("set-cookie", clear), ("cache-control", "no-store".into()),
            ("referrer-policy", "no-referrer".into()), ("x-frame-options", "DENY".into()), ("content-security-policy", "default-src 'none'; style-src 'unsafe-inline'; frame-ancestors 'none'".into())],
            confirm(&place, &user, name)).into_response(),
        None => (StatusCode::FOUND, [("location", place), ("set-cookie", clear), ("cache-control", "no-store".into())]).into_response(),
    }
}

/// The page that asks before a code goes to the command line: whom it signs in, and the port it goes to.
fn confirm(place: &str, user: &str, provider: &str) -> String {
    let port = reqwest::Url::parse(place).ok().and_then(|u| u.port()).map_or_else(String::new, |p| p.to_string());
    format!(
        concat!(
            "<!doctype html><meta charset=utf-8><meta name=viewport content=\"width=device-width\"><title>Pondra: sign in the command line?</title>",
            "<body style=\"font: 16px/1.5 system-ui, sans-serif; max-width: 34em; margin: 4em auto; padding: 0 1em\">",
            "<h1 style=\"font-size: 1.25em\">Sign in the command line?</h1>",
            "<p>A program on this computer, listening on port {port}, asks to be signed in as <b>{user}</b> through {provider}.</p>",
            "<p>Continue only if you just ran <code>pondra login</code> here. If you didn't, close this page: whoever sent you the link would be signed in as you.</p>",
            "<p><a id=\"continue\" href=\"{place}\">Continue</a></p>"
        ),
        port = port,
        user = html(user),
        provider = html(provider),
        place = html(place)
    )
}

/// Text put in a page's HTML, its `&`, `<`, `>` and quotes escaped (this page, and `pondra login`'s own: `project.rs`).
pub(crate) fn html(s: &str) -> String {
    s.replace('&', "&amp;").replace('<', "&lt;").replace('>', "&gt;").replace('"', "&quot;").replace('\'', "&#39;")
}

/// The provider's code exchanged and its ID token checked: whom it signs in.
async fn finish(app: &App, flow: &Value, q: &HashMap<String, String>) -> Result<Claimed> {
    ensure!(q.get("state").map(String::as_str) == flow["s"].as_str(), "its answer isn't this sign-in's (state)");
    if let Some(e) = q.get("error") {
        bail!("it refused: {e}{}", q.get("error_description").map(|d| format!(" ({d})")).unwrap_or_default());
    }
    let code = q.get("code").context("it sent no code")?;
    let name = flow["p"].as_str().unwrap_or_default();
    let all = crate::sso::all(&app.lake).await?;
    let provider = all.iter().find(|(n, _)| n == name).cloned().with_context(|| format!("no identity provider {name} now"))?;
    let p = &provider.1;
    let form = [("grant_type", "authorization_code"), ("code", code), ("redirect_uri", flow["r"].as_str().unwrap_or_default()), ("client_id", &p.client_id), ("code_verifier", flow["v"].as_str().unwrap_or_default())];
    let form = url::form_urlencoded::Serializer::new(String::new()).extend_pairs(form).finish();
    let mut ask = crate::ext::web().post(flow["t"].as_str().unwrap_or_default()).timeout(Duration::from_secs(10)).header("content-type", "application/x-www-form-urlencoded").body(form);
    if let Some(s) = &p.secret {
        ask = ask.basic_auth(&p.client_id, Some(client_secret(app, s).await?)); // (client_secret_basic: OpenID Connect's default)
    }
    let mut r = ask.send().await.context("its token endpoint doesn't answer")?;
    let status = r.status();
    let mut body = vec![];
    while let Some(piece) = r.chunk().await? {
        body.extend_from_slice(&piece);
        ensure!(body.len() <= 1 << 20, "its token endpoint answered more than 1 MB");
    }
    let answer: Value = serde_json::from_slice(&body).unwrap_or_default();
    if !status.is_success() {
        bail!("its token endpoint answered {status}: {}", answer["error_description"].as_str().or(answer["error"].as_str()).unwrap_or_default());
    }
    let id = answer["id_token"].as_str().context("its token endpoint sent no ID token")?;
    let c = crate::sso::checked(std::slice::from_ref(&provider), id, crate::log::now_ms() as f64 / 1000.0).await?.remove(0); // (one provider: one, or an error)
    let claims: Value = id.split('.').nth(1).and_then(|p| B64U.decode(p).ok()).and_then(|b| serde_json::from_slice(&b).ok()).unwrap_or_default();
    ensure!(claims["nonce"].as_str() == flow["n"].as_str(), "its ID token isn't this sign-in's (nonce)");
    crate::users::claimed(&app.lake, &c).await?; // (whom it may sign in: invariant 410)
    Ok(c)
}

/// `POST /sso/token` `{"code", "verifier"}`: the command line's code redeemed for a session, as `/login`
/// answers (`{"token", "until", "user"}`). Only the verifier whose S256 the flow started with redeems it,
/// within a minute.
pub async fn redeem(State(app): State<App>, ConnectInfo(peer): ConnectInfo<Peer>, body: Bytes) -> Response {
    let b: Value = serde_json::from_slice(&body).unwrap_or_default();
    let session = async {
        let (payload, sig) = b["code"].as_str().and_then(|c| c.strip_prefix("pc_")?.split_once('.')).context("no code")?;
        let v = crate::users::signed_for(&app.lake, CODE, payload, sig).await.context("not a code of this lake's")?;
        ensure!(v["x"].as_u64().is_some_and(|x| x > crate::log::now_ms()), "the code is over a minute old: sign in again");
        let challenge = v["ch"].as_str().context("not a code of this lake's")?;
        let verifier = b["verifier"].as_str().context("no verifier: a code is redeemed with the verifier its sign-in started with")?;
        ensure!(s256(verifier) == challenge, "the verifier isn't the one this sign-in started with");
        let name = v["p"].as_str().unwrap_or_default();
        let all = crate::sso::all(&app.lake).await?;
        let (_, p) = all.iter().find(|(n, _)| n == name).with_context(|| format!("no identity provider {name} now"))?;
        let roles = v["r"].as_array().map(|r| r.iter().filter_map(|x| x.as_str().map(String::from)).collect()).unwrap_or_default();
        let c = Claimed { user: v["u"].as_str().unwrap_or_default().into(), roles, provider: name.into(), any: p.any, expires_ms: 0 };
        crate::users::claimed(&app.lake, &c).await?;
        let (token, until) = crate::users::session_of(&app.lake, &c).await?;
        Ok::<_, anyhow::Error>(j!({"token": token, "until": until, "user": c.user}))
    };
    match session.await {
        Ok(v) => Json(v).into_response(),
        Err(e) => {
            crate::audit::refused(&app, "", "http", Some(peer.addr), "sign in", &format!("a sign-in's code: {e:#}"));
            (StatusCode::UNAUTHORIZED, crate::ext::said(&e)).into_response()
        }
    }
}

/// The command line's code: whom the flow signed in and its challenge, signed with the lake's key, for a minute.
async fn code_for(app: &App, c: &Claimed, challenge: &str) -> Result<String> {
    let code = j!({"u": c.user, "p": c.provider, "r": c.roles, "ch": challenge, "x": crate::log::now_ms() + CODE_MS});
    Ok(format!("pc_{}", signed(app, CODE, &code).await?))
}

/// The flow this request's cookie carries, if the lake's key signed it and it hasn't run out.
async fn flow_of(app: &App, headers: &HeaderMap) -> Option<Value> {
    let cookies = headers.get_all("cookie").iter().filter_map(|v| v.to_str().ok()).flat_map(|v| v.split(';')).map(str::trim);
    let value = cookies.filter_map(|c| c.strip_prefix(COOKIE)?.strip_prefix('=')).next()?;
    let (payload, sig) = value.split_once('.')?;
    let flow = crate::users::signed_for(&app.lake, FLOW, payload, sig).await?;
    flow["x"].as_u64().is_some_and(|x| x > crate::log::now_ms()).then_some(flow)
}

/// `value` as `<base64url JSON>.<the lake's signature of it for purpose>`.
async fn signed(app: &App, purpose: &str, value: &Value) -> Result<String> {
    let payload = B64U.encode(serde_json::to_vec(value)?);
    let sig = crate::users::sign_for(&app.lake, purpose, payload.as_bytes()).await?;
    Ok(format!("{payload}.{sig}"))
}

/// The client's secret: `CLIENT_SECRET` of the secret the provider names (`CREATE SECRET … (TYPE generic,
/// CLIENT_SECRET '…')`), read as the node, since only an admin makes a provider.
async fn client_secret(app: &App, name: &str) -> Result<String> {
    let (n, s) = crate::ext::list(&app.lake).await?.into_iter().find(|(n, s)| n == name && !s.temporary).with_context(|| format!("no secret {name}"))?;
    let values = crate::ext::reveal_secret(&n, &s)?;
    values.into_iter().find(|(k, _)| k.eq_ignore_ascii_case("client_secret")).map(|(_, v)| v).with_context(|| format!("secret {name} has no CLIENT_SECRET"))
}

fn random() -> String {
    let mut b = [0u8; 32];
    crate::crypto::random(&mut b).expect("randomness");
    B64U.encode(b)
}

/// PKCE's S256: the base64url SHA-256 of a verifier.
fn s256(verifier: &str) -> String { B64U.encode(crate::crypto::sha256(verifier.as_bytes())) }

fn encode(s: &str) -> String { url::form_urlencoded::byte_serialize(s.as_bytes()).collect() }

fn with_query(to: &str, key: &str, value: &str) -> String {
    let mut u = reqwest::Url::parse(to).expect("a loopback address, checked at the start");
    u.query_pairs_mut().append_pair(key, value);
    u.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn destinations() {
        assert_eq!(destination("/").unwrap(), "/");
        assert_eq!(destination("/db/sales/?x=1#a").unwrap(), "/db/sales/?x=1");
        assert_eq!(destination("http://127.0.0.1:5123/").unwrap(), "http://127.0.0.1:5123/");
        assert!(destination("http://localhost:9/back").is_ok());
        for bad in ["//evil.com/", "/\\evil.com", "https://evil.com/", "http://127.0.0.1/", "http://u:p@127.0.0.1:9/", "http://127.0.0.1.evil.com:9/", "javascript:alert(1)", "/a\nb"] {
            assert!(destination(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn pkce() {
        // (RFC 7636's own example)
        assert_eq!(s256("dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk"), "E9Melhoa2OwvFrEMTJguCHaoeK1t8URWbuGJSstw-cM");
    }

    #[test]
    fn confirmed() {
        let page = confirm("http://127.0.0.1:5123/?code=pc_a&x=1", "a<b>@acme.com", "acme");
        assert!(page.contains("listening on port 5123"), "{page}");
        assert!(page.contains("<b>a&lt;b&gt;@acme.com</b> through acme"), "{page}");
        assert!(page.contains(r#"<a id="continue" href="http://127.0.0.1:5123/?code=pc_a&amp;x=1">"#), "{page}");
    }
}
