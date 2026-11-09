//! HTTP API. Every node ingests (`/append`), answers SQL and streams changes (`/watch`); metadata
//! writes (tables, views, tasks, bulk inserts, tiering) go to the leader (followers forward them).
use crate::app::App;
use crate::log::{decode_flush, Ack, Src};
use crate::query::{latest_sql, raw, tail};
use crate::store::*;
use crate::tasks::Task;
use axum::body::{Body, Bytes};
use axum::extract::{Path, Query, Request, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post, put};
use axum::{Json, Router};
use datafusion::arrow::ipc::reader::StreamReader;
use datafusion::arrow::{compute::concat_batches, record_batch::RecordBatch};
use futures::StreamExt;
use serde::Deserialize;
use serde_json::{json as j, Value};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use crate::statement::Rendered;

pub fn router(app: App) -> Router {
    let leader_only = Router::new()
        .route("/tables/{name}", post(create_table))
        .route("/views/{name}", post(create_view))
        .route("/tasks/{name}", post(create_task))
        .route("/functions/{name}", post(create_function).delete(drop_function))
        .route("/tier", post(tier_now))
        .route("/cluster/ddl", post(ddl))
        .route("/cluster/change", post(change))
        .route("/cluster/txn", post(|State(app): State<App>, Json(c): Json<crate::txn::Commit>| async move {
            let seq = app.seq.as_ref().ok_or_else(|| anyhow::anyhow!("not the leader"))?;
            Ok::<_, E>(Json(crate::txn::commit_here(&app.lake, seq, &app.lock, c).await?))
        }))
        .route("/cluster/checked", post(|State(app): State<App>, body: Bytes| async move {
            let seq = app.seq.clone().ok_or_else(|| anyhow::anyhow!("not the leader"))?;
            let (table, src, batch) = crate::constraints::decode(body)?;
            Ok::<_, E>(Json(crate::constraints::leader(app.lake.clone(), seq, table, src, batch).await?))
        }))
        .route("/cluster/lock", post(|State(app): State<App>, Json(r): Json<crate::locks::Req>| async move {
            Ok::<_, E>(Json(crate::locks::serve(&app.lake, r).await?))
        }))
        .route("/cluster/iceberg", post(|State(app): State<App>, Json(c): Json<Vec<crate::iceberg::Commit>>| async move { Ok::<_, E>(Json(app.record_iceberg(c).await?)) }))
        .route_layer(middleware::from_fn_with_state(app.clone(), to_leader));
    Router::new()
        .merge(leader_only)
        .route("/append/{name}", post(append))
        .route("/insert/{name}", post(insert))
        .route("/cluster/files", post(files))
        .route("/", get(crate::console::page))
        .route("/sql", post(sql))
        .route("/sql/pages/{id}", get(page))
        .route("/mcp", post(crate::mcp::handle).delete(crate::mcp::end))
        .route("/files/{*path}", put(put_file).get(get_file).delete(delete_file).post(restore_file))
        .route("/lookup/{name}/{key}", get(lookup))
        .route("/watch/{name}", get(watch))
        .route("/live", get(crate::live::live).post(crate::live::live))
        .route("/sessions/{id}", axum::routing::delete(|Path(id): Path<String>, who: Option<axum::Extension<crate::auth::Principal>>| async move { Json(j!({"ended": crate::temp::end(&crate::temp::id(&who.map(|w| w.0.name).unwrap_or_default(), &id))})) }))
        .route("/sessions/{id}/python", get(session_python).delete(session_python).post(session_python))
        .route("/python", get(list_pythons).put(choose_python))
        .route("/python/format", post(format_python))
        .route("/console/settings", get(crate::console::settings).put(crate::console::save_settings))
        .route("/console/{*file}", get(crate::console::file))
        .route("/functions", get(list_functions))
        .route("/kinds", get(|| async { Json(crate::objects::kinds()) }))
        .route("/routines", get(routines_shown))
        .route("/secrets/{name}", get(secret))
        .route("/stats", get(stats))
        .route("/healthz", get(|| async { "ok" })) // (the process answers: a liveness probe)
        .route("/ready", get(ready))
        .route("/login", post(login).layer(axum::extract::DefaultBodyLimit::max(64 << 10))) // (asked before any sign-in: a small body)
        .route("/whoami", get(whoami))
        .route("/sso", get(|State(app): State<App>| async move { Json(crate::sso::listed(&app.lake).await) }))
        .route("/sso/{provider}/start", get(crate::sso_flow::start)) // (the browser's way in: ADR-074 §5)
        .route("/sso/callback", get(crate::sso_flow::callback))
        .route("/sso/token", post(crate::sso_flow::redeem).layer(axum::extract::DefaultBodyLimit::max(64 << 10)))
        .route("/.well-known/oauth-protected-resource", get(protected_resource))
        .route("/plan", post(apply))
        .route("/apply", post(apply))
        .route("/test", post(apply))
        .route("/status", post(apply))
        .route("/capture", post(apply))
        .route("/export", get(|State(app): State<App>, Query(q): Query<HashMap<String, String>>| async move {
            Ok::<_, E>(Json(match q.get("database") {
                Some(name) => crate::apply::export_attached(&app.lake, name).await?,
                None => crate::apply::export_here(&app.lake).await?,
            }))
        }))
        .route("/objects", get(|State(app): State<App>, who: Option<axum::Extension<crate::auth::Principal>>| async move { Ok::<_, E>(Json(crate::objects::listed(&app.lake, &scope_of(who)).await?)) }))
        .route("/metrics", get(|State(app): State<App>| async move { crate::metrics::render(&app).await.map_err(E) }))
        .route("/metrics/tables", get(|State(app): State<App>| async move { crate::metrics::tables(&app).await.map_err(E) }))
        .route("/cluster/commit", post(commit))
        .route("/cluster/log", get(feed))
        .route("/cluster/kv", get(kv))
        .route("/cluster/records", post(records).layer(axum::extract::DefaultBodyLimit::max(1 << 20))) // (a joined page's own statements: 256 lines, a 1 MB body)
        .route("/cluster/stage", post(stage))
        .route("/cluster/copy", post(copy))
        .route("/cluster/insert", post(insert_share))
        .route("/cluster/shuffle", get(bucket))
        .route("/cluster/job", post(job))
        .route("/cluster/probe", get(|Query(p): Query<HashMap<String, usize>>| async move { crate::guard::probe(p.get("bytes").copied().unwrap_or(0)) }))
        .route("/cluster/beat", post(beat))
        .nest_service("/cluster/store", crate::served::door_for(app.lake.store.clone(), crate::cluster::of(&app).is_some_and(|c| c.serves_store))) // (a lake's store, for nodes on other machines: ADR-072 §8)
        .nest_service("/store", crate::served::files_door(app.lake.store.clone())) // (a page's reads of the lake's files and log: an admin's sign-in, `Auth::needed`)
        .route("/cluster/ack", post(ack))
        .route("/cluster/replica", get(replica))
        .route("/cluster/leader", get(|State(app): State<App>| async move { crate::cluster::of(&app).map(|c| Json(c.leader_status())).ok_or(StatusCode::NOT_FOUND) }))
        .route("/cluster/visible", get(|State(app): State<App>| async move { Json(app.lake.visible()) })) // (a follower's read-your-writes: `write::seen_here`)
        .route("/cluster/committed", get(|State(app): State<App>| async move { Json(app.lake.cat.committed()) })) // (the leader's last commit: `write::seen_here`, `cluster::catch_up`)
        .route("/cluster/kafka", get(|| async { Json(crate::kafka::me()) }))
        .merge(crate::iceberg_rest::rest())
        .merge(crate::sharing::routes()) // (recipients sign in there with their own tokens: `sharing.rs`)
        .layer(axum::extract::DefaultBodyLimit::max(1 << 30)) // batches up to 1 GiB
        .layer(middleware::from_fn_with_state(app.clone(), guard))
        .layer(middleware::from_fn_with_state(app.clone(), used))
        .layer(middleware::from_fn_with_state(Arc::new(crate::origins::ORIGINS.clone()), crate::origins::door)) // (outermost: a preflight is answered before guard, and guard's refusals carry the headers)
        .with_state(app)
}

/// `POST /functions/<name>`: a function this lake has, run by an Arrow Flight server of your own
/// (see `udf.rs`): `{"flight": "http://host:port", "args": ["Binary"], "returns": "Utf8"}`.
/// `DELETE /functions/<name>` takes it away; `GET /functions` lists them.
async fn create_function(State(app): State<App>, Path(name): Path<String>, body: Bytes) -> Result<Json<Value>, E> {
    if let Some(says) = crate::kept::function_named(&name) {
        return Err(E(crate::codes::coded("0A000", says))); // (a name kept free for 1.1, as SQL refuses it: `kept.rs`)
    }
    let udf: crate::udf::Udf = serde_json::from_slice(&body).map_err(|e| E(anyhow::anyhow!("{e}: {{\"flight\": \"http://host:port\", \"args\": [\"Binary\"], \"returns\": \"Utf8\"}}")))?;
    for t in udf.args.iter().chain([&udf.returns]) {
        crate::answer::dtype(t)?;
    }
    app.lake.cat.commit(vec![(crate::udf::key(&name), serde_json::to_vec(&udf)?)], &[]).await?;
    crate::udf::forget(&app.lake);
    Ok(Json(j!({"function": name})))
}

async fn drop_function(State(app): State<App>, Path(name): Path<String>) -> Result<Json<Value>, E> {
    app.lake.cat.commit(vec![], &[crate::udf::key(&name)]).await?;
    crate::udf::forget(&app.lake);
    Ok(Json(j!({"dropped": name})))
}

/// `PUT /files/<path>`: an object in the lake next to the tables — an image, a PDF, a model, a SQL
/// file — for `files('…')` to list and `file_read(path)` to read (see `files.rs`). A path that
/// exists is refused (409), unless `If-Match` names the version `GET` gave: then it is replaced,
/// if nobody replaced it meanwhile (412 if so; ADR-034). Each save is kept as a version too
/// (`files::keep`, ADR-035 §8).
async fn put_file(State(app): State<App>, Path(path): Path<String>, who: Option<axum::Extension<crate::auth::Principal>>, headers: HeaderMap, body: Bytes) -> Result<Response, E> {
    let if_match = headers.get("if-match").and_then(|v| v.to_str().ok());
    let role = who.map_or(crate::auth::Role::None, |w| w.0.role);
    Ok(filed(crate::files::put(&app.lake, &path, if_match, body, role).await?))
}

/// A `Filed` as the node's HTTP answer: its status, its content type and, when it is the file's bytes, its version as
/// `etag` (quoted) with `cache-control: no-store`.
fn filed(f: crate::lake_files::Filed) -> Response {
    let status = StatusCode::from_u16(f.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    match f.etag {
        Some(version) => {
            let etag = format!("\"{version}\"");
            (status, [("content-type", f.content_type), ("etag", etag.as_str()), ("cache-control", "no-store")], f.body).into_response()
        }
        None => (status, [("content-type", f.content_type)], f.body).into_response(),
    }
}

/// `GET /files/<path>`: that object's bytes, its version (`etag`, for `If-Match`) and a content
/// type by its name. `?versions`: the versions it keeps, newest first; `?version=<id>`: one's bytes.
async fn get_file(State(app): State<App>, Path(path): Path<String>, Query(q): Query<HashMap<String, String>>) -> Result<Response, E> {
    Ok(filed(crate::files::get(&app.lake, &path, &q).await?))
}

/// `POST /files/<path>?restore=<id>`: that version made the file again (itself kept as the newest).
async fn restore_file(State(app): State<App>, Path(path): Path<String>, Query(q): Query<HashMap<String, String>>) -> Result<Response, E> {
    Ok(filed(crate::files::restore(&app.lake, &path, &q).await?))
}

/// `DELETE /files/<path>`: that object gone (a writer's, as `PUT`); its versions stay. A path no file is at: 404.
async fn delete_file(State(app): State<App>, Path(path): Path<String>) -> Result<Response, E> {
    Ok(filed(crate::files::remove(&app.lake, &path).await?))
}

/// `GET /sessions/{id}/python`: the variables a session's Python holds, as the console's panel
/// lists them (none, if it has none yet); `DELETE`: stop it (its variables go; the session's
/// temporary tables stay), as a notebook's kernel is restarted. An admin's, as Python is.
async fn session_python(method: axum::http::Method, Path(id): Path<String>, role: axum::Extension<crate::auth::Role>, who: Option<axum::Extension<crate::auth::Principal>>) -> Result<Json<Value>, E> {
    let id = crate::temp::id(&who.map(|w| w.0.name).unwrap_or_default(), &id); // (as the client's requests name it: its user's)
    if *role < crate::auth::Role::Admin {
        return Err(E(crate::codes::coded("42501", "a session's Python is an admin's, as DO is")));
    }
    Ok(Json(match method {
        axum::http::Method::DELETE => j!({"restarted": crate::python::end_session(&id)}),
        axum::http::Method::POST => j!({"done": crate::python::interrupt(&id)}), // (interrupt its running cell)
        _ => crate::python::variables(&id).await?,
    }))
}

/// `GET /python`: the Python in use, and (`?all=1`) each Python this machine has, tried: the
/// console's Choose Python. An admin's, as Python is.
async fn list_pythons(Query(q): Query<HashMap<String, String>>, role: axum::Extension<crate::auth::Role>) -> Result<Json<Value>, E> {
    if *role < crate::auth::Role::Admin {
        return Err(E(anyhow::anyhow!("which Python runs is an admin's, as DO is")));
    }
    let mut v = crate::python::current();
    if q.contains_key("all") {
        v["pythons"] = j!(crate::python::pythons().await);
    }
    Ok(Json(v))
}

/// `PUT /python` `{"path": …}`: run workers with that Python from now on (and next time: kept on
/// this machine). From a page on this machine only, an admin's.
async fn choose_python(axum::extract::ConnectInfo(peer): axum::extract::ConnectInfo<crate::tls::Peer>, headers: axum::http::HeaderMap, role: axum::Extension<crate::auth::Role>, Json(b): Json<Value>) -> Result<Json<Value>, E> {
    if *role < crate::auth::Role::Admin || !peer.from_here(&headers) {
        return Err(E(crate::codes::coded("42501", "which Python runs is chosen by an admin, on this machine")));
    }
    Ok(Json(crate::python::choose(b["path"].as_str().unwrap_or_default()).await?))
}

/// `POST /python/format` `{"code": …}`: the code formatted by the node's Python, as ruff (or black)
/// formats it: the console's Format for Python files and cells. The code is only read, never run.
async fn format_python(Json(b): Json<Value>) -> Result<Json<Value>, E> {
    crate::python::ready("Formatting Python uses the node's Python (its ruff or black)")?;
    let head = j!({"op": "format", "code": b["code"].as_str().unwrap_or_default()});
    let (said, _) = crate::python::ask("", crate::python::Use::Function, head, vec![], Some(std::time::Duration::from_secs(30)), &mut |_| {}).await?;
    Ok(Json(j!({"code": said["code"]})))
}

/// `GET /functions`: the lake's own functions, by name.
async fn list_functions(State(app): State<App>) -> Result<Json<Value>, E> {
    let fns = app.lake.cat.scan::<crate::udf::Udf>("f/", "f0").await?;
    Ok(Json(j!(fns.into_iter().map(|(k, u)| (k[2..].to_string(), serde_json::to_value(u).unwrap_or_default())).collect::<serde_json::Map<_, _>>())))
}

/// `GET /routines`: every routine's name, parameters and result; its code only to whoever is shown code
/// (a user granted some tables needs the first three to call one, never the body: S27).
async fn routines_shown(State(app): State<App>, who: Option<axum::Extension<crate::auth::Principal>>) -> Result<Json<Value>, E> {
    let all = crate::routines::listed(&app.lake).await?;
    if scope_of(who).shows_code() {
        return Ok(Json(j!(*all)));
    }
    let mut hidden = (*all).clone();
    for r in hidden.values_mut() {
        r.body.clear();
    }
    Ok(Json(j!(hidden)))
}

/// A request is a use of the node until its answer has been sent (`idle.rs`: `--idle-exit`), but for the ones a cluster
/// sends all the time and the health checks.
async fn used(State(app): State<App>, req: Request, next: Next) -> Response {
    if crate::node_state::background(req.uri().path()) {
        return next.run(req).await;
    }
    let used = app.lake.node.idle.begin();
    next.run(req).await.map(|body| axum::body::Body::new(crate::node_state::kept(body, used)))
}

/// Who asks (see `auth.rs`, `users.rs`): a token, a user's token, session or password; the
/// program that started the node (`owner`); or, when nothing needs a sign-in, anyone, as an admin.
/// Its role must cover the route; the request then runs as it (`auth::WHO`), and handlers see its
/// role too.
async fn guard(State(app): State<App>, mut req: Request, next: Next) -> Response {
    let path = req.uri().path();
    if app.lake.node.drain.draining() && !crate::node_state::still_taken(path) {
        return (StatusCode::SERVICE_UNAVAILABLE, [("retry-after", "1")], "this node is stopping: ask another").into_response(); // (`drain.rs`)
    }
    if app.members.cut_off() && !crate::node_state::still_taken(path) {
        // (a leader that can't reach the bucket can't commit, and its reads may already be stale:
        // another node leads soon. The cluster's own calls and the health checks still go through.)
        return (StatusCode::SERVICE_UNAVAILABLE, [("retry-after", "1")], crate::members::CUT_OFF_SAYS).into_response();
    }
    let header = req.headers().get("authorization").and_then(|v| v.to_str().ok()).map(String::from);
    let credential = crate::auth::Credential::of_headers(req.headers()); // (read alike at every door: `auth::Credential`)
    let pn = credential.token().is_some_and(|t| t.starts_with("pn_"));
    let from = req.extensions().get::<axum::extract::ConnectInfo<crate::tls::Peer>>().map(|c| c.0.addr);
    let vouched = req.extensions().get::<axum::extract::ConnectInfo<crate::tls::Peer>>().is_some_and(|c| c.0.signed || c.0.own(req.headers())); // (`users::nodes_key`)
    if let Some(axum::extract::ConnectInfo(peer)) = req.extensions().get::<axum::extract::ConnectInfo<crate::tls::Peer>>() {
        if !peer.allowed() {
            return (StatusCode::FORBIDDEN, crate::tls::PLAIN).into_response(); // (`tls.rs`)
        }
        if matches!(credential, crate::auth::Credential::Password { .. }) && !peer.may_send_password() {
            return (StatusCode::FORBIDDEN, crate::tls::PASSWORD_PLAIN).into_response(); // (under PONDRA_TLS=optional: a token may cross, a password never)
        }
        if pn && !peer.may_be_node() {
            return (StatusCode::UNAUTHORIZED, "the nodes' key is taken only with a certificate the nodes' authority signed (PONDRA_TLS_CA)").into_response();
        }
        if pn && !app.auth.admin_set() && !crate::users::nodes_key(false, crate::ext::shared_master(), vouched) {
            return (StatusCode::UNAUTHORIZED, "the lake's keys are in the clear in its bucket, so the nodes' key is taken only from this machine, not through a proxy, or with a certificate the nodes' authority signed (PONDRA_TLS_CA): to run nodes on other machines, give every node --admin-token (or PONDRA_SECRET_KEY)").into_response();
        }
    }
    if credential.token().is_some_and(|t| t.starts_with("pb_")) {
        // (a branch's key: it renews or lets go of its own pin, which `ddl` checks, and opens nothing else: ADR-058)
        if req.uri().path() != "/cluster/ddl" {
            return (StatusCode::UNAUTHORIZED, "a branch's key renews its pin, and opens nothing else").into_response();
        }
        // (no role, and a whole scope: `ddl` lets nothing but its own pin through, and asks it no grant)
        let who = crate::auth::Principal { scope: crate::rights::Scope::everything(), ..crate::auth::Principal::of(crate::auth::Role::None) }.at("http", from);
        return match crate::panics::door(&app.lake.node, crate::auth::WHO.scope(who, next.run(req))).await {
            Ok(r) => told_to(true, r), // (a whole scope is shown code)
            Err(m) => E(anyhow::anyhow!(m)).into_response(),
        };
    }
    if req.uri().path().starts_with("/delta-sharing/") {
        // (a recipient's token or a file's signed link, which the door checks: never a user's sign-in)
        return match crate::panics::door(&app.lake.node, next.run(req)).await {
            Ok(r) => r,
            Err(m) => E(anyhow::anyhow!(m)).into_response(),
        };
    }
    let client = req.extensions().get::<axum::extract::ConnectInfo<crate::tls::Peer>>().map(|c| c.0.client(req.headers()));
    if let Some(why) = credential.malformed() {
        // (a header that can't be read is told so, at every door alike, never taken for no sign-in: an open node's)
        crate::audit::refused_by(&app, "", "http", from, client, &format!("{} {}", req.method(), req.uri().path()), &why).await;
        return challenge(&app, req.uri().path(), req.headers(), (StatusCode::UNAUTHORIZED, why).into_response()).await;
    }
    let needed = crate::auth::Auth::needed(req.uri().path(), req.method().as_str());
    let starting = !*app.lake.caught.borrow();
    let token = credential.token();
    let node = token.is_some_and(|t| t.starts_with("pn_") || app.auth.token_role(t) > crate::auth::Role::None); // (the cluster's own calls: never held)
    if starting && needed > crate::auth::Role::None && !node {
        let _ = app.lake.caught_up().await; // (a node that just started signs users in from what its leader had: `open`)
    }
    // (a sign-in with a user's name and password is counted per address and name: past ten failures a minute
    // that pair is refused at once, the right password too; a bearer token isn't counted: `auth::guessing`)
    let basic_by = credential.user().map(|name| (name.to_string(), client));
    if let Some((name, ip)) = &basic_by {
        if let Some(wait) = crate::auth::guessing(&app.lake, *ip, name) {
            let why = crate::auth::guessing_says(name, wait);
            crate::audit::refused_by(&app, name, "http", from, client, &format!("{} {}", req.method(), req.uri().path()), &why).await;
            tokio::time::sleep(Duration::from_millis(400)).await; // (as a wrong guess does: refused at once, guesses would cost nothing)
            return (StatusCode::TOO_MANY_REQUESTS, [("retry-after", wait.to_string())], why).into_response();
        }
    }
    let signed = crate::users::who_at(&app.lake, &app.auth, &credential, vouched).await;
    let mut open = false; // (a node with no tokens and no users: anyone who reaches it is its admin and one of its nodes)
    let who = match signed {
        Some(p) => p,
        None if crate::statement::owner(&app.auth, req.headers()) => crate::auth::Principal::token(crate::auth::Role::Admin), // (the shell's, local()'s: this machine's own folder)
        None if starting && needed == crate::auth::Role::None => crate::auth::Principal::of(crate::auth::Role::None), // (a probe while it catches up: answered, never held)
        // (still catching up after 10 s: its catalog may lack its leader's users, so nobody it can't sign in is let in as an
        // open node's caller: asked again in a moment, as a table it doesn't hold yet is, `Lake::table_meta_if`)
        None if !*app.lake.caught.borrow() => return (StatusCode::SERVICE_UNAVAILABLE, [("retry-after", "1")], crate::store::CATCHING_UP).into_response(),
        None if app.open().await => match crate::auth::cross_site(req.headers(), req.method(), from, app.members.me()) {
            Some(why) => return (StatusCode::FORBIDDEN, why).into_response(), // (a browser's page from elsewhere: `auth::cross_site`)
            None => {
                open = true;
                crate::auth::Principal::of(crate::auth::Role::Admin)
            }
        },
        None if header.is_some() => {
            if let Some((name, ip)) = &basic_by {
                crate::auth::guessed(&app.lake, *ip, name);
            }
            // (an identity provider's token says why it was refused: expired, another audience, no such user here)
            let why = match credential.secret() {
                Some(t) => crate::sso::why(&app.lake, t).await,
                None => None,
            };
            let why = why.unwrap_or_else(|| "wrong token, or user name and password".into());
            crate::audit::refused_by(&app, credential.user().unwrap_or_default(), "http", from, client, &format!("{} {}", req.method(), req.uri().path()), &why).await;
            tokio::time::sleep(Duration::from_millis(400)).await; // (a guess costs time)
            return challenge(&app, req.uri().path(), req.headers(), (StatusCode::UNAUTHORIZED, why).into_response()).await;
        }
        None => crate::auth::Principal::of(crate::auth::Role::None),
    };
    let who = who.at("http", from);
    // (a branch's pin: a user granted CLONE may ask for it, and `ddl` lets nothing else of theirs through: ADR-058)
    let pin = req.uri().path() == "/cluster/ddl";
    let cloning = pin && who.scope.clones().is_some();
    if who.role < needed && !cloning {
        return match who.role {
            crate::auth::Role::None if header.is_none() && app.auth.owned() => (StatusCode::UNAUTHORIZED, "this node answers only the program that started it (the shell, connect()): open the console from the link it printed, or start the node with --admin-token to connect other tools").into_response(),
            crate::auth::Role::None if header.is_none() && !app.auth.on() && crate::users::signed(&app.lake).await => (StatusCode::UNAUTHORIZED, "this lake has been served with tokens: start the node with them (--admin-token, --write-token, --read-token), or sign in as a user").into_response(),
            crate::auth::Role::None => challenge(&app, req.uri().path(), req.headers(), (StatusCode::UNAUTHORIZED, "sign in: a token, or a user's name and password").into_response()).await,
            _ => {
                let why = match pin {
                    true => "a pin needs CLONE on this database or a schema of it (GRANT CLONE ON DATABASE … TO …)",
                    false => "this needs more rights than this token's or user's",
                };
                crate::audit::refused_by(&app, &who.name, "http", from, client, &format!("{} {}", req.method(), req.uri().path()), why).await;
                (StatusCode::FORBIDDEN, why).into_response()
            }
        };
    }
    // (the cluster's routes are the nodes' own: their tokens, their key, the program that started this one, anyone at an
    // open node. A user's sign-in, a superuser's too, reaches only what `Auth::users_reach` names: the rest carry the
    // lake's keys (the commit stream, the replica log) or write what only the nodes write (a raw flush, the audit's own
    // tables, acks and beats). Checked before `AS_USER`, which makes a node's call its user's)
    if req.uri().path().starts_with("/cluster/") && !(who.operator || open) && !crate::auth::Auth::users_reach(req.uri().path()) {
        let why = "this route is the nodes' own: a user's sign-in, a superuser's too, reaches the cluster's routes only for a branch's pin";
        crate::audit::refused_by(&app, &who.name, "http", from, client, &format!("{} {}", req.method(), req.uri().path()), why).await; // (audited, as every refusal is)
        return (StatusCode::FORBIDDEN, why).into_response();
    }
    // (a change a node sends its leader for a user runs as that user: `auth::AS_USER`. Only from the nodes (their key, the
    // admin token, the program that started this one) and only to `/cluster/*`; any other caller is itself, so a token
    // never acts or is recorded as a user. It only narrows: the user's rights within the caller's role. The audit names
    // the node it came through: door `node`, from the node's address)
    let by_node = who.operator && who.role == crate::auth::Role::Admin && req.uri().path().starts_with("/cluster/");
    let who = match req.headers().get(crate::auth::AS_USER).and_then(|v| v.to_str().ok()) {
        Some(user) if by_node => match crate::users::principal(&app.lake, user).await {
            Ok(p) if p.role > who.role => crate::auth::Principal { role: who.role, ..p.at("node", from) },
            Ok(p) => p.at("node", from),
            Err(e) => return (StatusCode::FORBIDDEN, crate::ext::said(&e)).into_response(),
        },
        _ => who,
    };
    req.extensions_mut().insert(who.role);
    req.extensions_mut().insert(who.clone()); // (the principal itself, for a door that plans for it: `routines::Who`)
    let heavy = queries(req.uri().path());
    let stoppable = stoppable(req.uri().path(), req.uri().query()); // (a query the node may stop for memory: `oom.rs`)
    // (a stopped statement fails its session's transaction: who asked, and the headers `temp::of` reads the session from)
    let asked = stoppable.then(|| (who.clone(), req.headers().clone()));
    // (a Python procedure of an apply calls back with its lent token: those calls are the apply's, as its statements are)
    let applying = crate::auth::lent_applying(token);
    let shows_code = who.scope.shows_code(); // (an error's full words, or the words without its quoted lines: `told_to`)
    let run = crate::auth::WHO.scope(who, next.run(req));
    let run = async move {
        match applying {
            true => crate::protect::APPLYING.scope((), run).await,
            false => run.await,
        }
    };
    match if heavy { crate::panics::work(&app.lake.node, stoppable, run).await } else { crate::panics::door(&app.lake.node, run).await } {
        Ok(r) => told_to(shows_code, r),
        Err(m) => {
            if let (true, Some((who, headers))) = (m == crate::oom::OOM, asked) {
                crate::statement::failed_for_memory(who, &headers); // (the node stopped the statement: its transaction fails, as an error's does)
            }
            E(anyhow::anyhow!(m)).into_response() // (and the node stays up)
        }
    }
}

/// The queries the node may stop for memory (`oom.rs`): a client's query, which its client asks again
/// or gives up on, and a joined page's staged INSERT (`insert_staged`), which its page sends again. Not
/// the cluster's own calls, the other bulk writes or a live query, which lives as long as its client.
fn stoppable(path: &str, query: Option<&str>) -> bool {
    let staged = || path.starts_with("/insert/") && query.is_some_and(|q| q.split('&').any(|p| p == "staged=1"));
    matches!(path, "/sql" | "/mcp") || path.starts_with("/sql/pages/") || staged()
}

/// The routes whose work is queries' (`panics::work`): planned and run on a runtime of their own,
/// so appends, commits, heartbeats and the commit stream keep theirs.
fn queries(path: &str) -> bool {
    matches!(path, "/sql" | "/mcp" | "/live" | "/tier" | "/cluster/stage" | "/cluster/copy" | "/cluster/insert" | "/cluster/job") || path.starts_with("/sql/pages/") || path.starts_with("/insert/")
}

/// A 401 for MCP says where its identity providers are (RFC 9728, as MCP's clients look for them):
/// `WWW-Authenticate: Bearer resource_metadata="…/.well-known/oauth-protected-resource"`.
async fn challenge(app: &App, path: &str, headers: &axum::http::HeaderMap, mut r: Response) -> Response {
    if path.starts_with("/mcp") && crate::sso::all(&app.lake).await.is_ok_and(|p| !p.is_empty()) {
        let host = headers.get("host").and_then(|h| h.to_str().ok()).unwrap_or(app.members.me());
        if let Ok(v) = format!("Bearer resource_metadata=\"{}\"", crate::tls::url(&format!("{host}/.well-known/oauth-protected-resource"))).parse() {
            r.headers_mut().insert("www-authenticate", v);
        }
    }
    r
}

/// `GET /.well-known/oauth-protected-resource`: what MCP's clients read to sign in (RFC 9728): this
/// node's MCP and the identity providers that sign its users in. 404 without one.
async fn protected_resource(State(app): State<App>, headers: axum::http::HeaderMap) -> Response {
    let host = headers.get("host").and_then(|h| h.to_str().ok()).unwrap_or(app.members.me()).to_string();
    match crate::sso::resource(&app.lake, &crate::tls::url(&format!("{host}/mcp"))).await {
        Some(v) => Json(v).into_response(),
        None => (StatusCode::NOT_FOUND, "no identity provider signs users in here (CREATE IDENTITY PROVIDER)").into_response(),
    }
}

/// `POST /login` `{"user", "password"}`: a session (`{"token": "ps_…", "until": ms}`) to send as
/// `Authorization: Bearer …` until it ends (`PONDRA_SESSION_HOURS`); the console's sign-in.
async fn login(State(app): State<App>, axum::extract::ConnectInfo(peer): axum::extract::ConnectInfo<crate::tls::Peer>, who: Option<axum::Extension<crate::auth::Principal>>, headers: axum::http::HeaderMap, body: Bytes) -> Response {
    let b: Value = serde_json::from_slice(&body).unwrap_or_default(); // (JSON, whatever its content type says)
    let (user, password) = (b["user"].as_str().unwrap_or_default(), b["password"].as_str().unwrap_or_default());
    if !peer.may_send_password() {
        return (StatusCode::FORBIDDEN, crate::tls::PASSWORD_PLAIN).into_response(); // (`tls.rs`)
    }
    let ip = Some(peer.client(&headers));
    if let Some(wait) = crate::auth::guessing(&app.lake, ip, user) {
        let why = crate::auth::guessing_says(user, wait);
        crate::audit::refused_by(&app, user, "http", Some(peer.addr), ip, "sign in", &why).await;
        tokio::time::sleep(Duration::from_millis(400)).await; // (as a wrong guess does)
        return (StatusCode::TOO_MANY_REQUESTS, [("retry-after", wait.to_string())], why).into_response();
    }
    if !crate::users::password_ok(&app.lake, user, password).await {
        crate::auth::guessed(&app.lake, ip, user);
        crate::audit::refused_by(&app, user, "http", who.as_ref().and_then(|w| w.0.from), ip, "sign in", "wrong user name or password").await;
        tokio::time::sleep(Duration::from_millis(400)).await; // (a guess costs time)
        return (StatusCode::UNAUTHORIZED, "wrong user name or password").into_response();
    }
    match crate::users::session(&app.lake, user).await {
        Ok((token, until)) => Json(j!({"token": token, "until": until, "user": user})).into_response(),
        Err(e) => E(e).into_response(),
    }
}

/// `GET /whoami`: who this request signs in as, and what it may do.
async fn whoami(State(app): State<App>, who: Option<axum::Extension<crate::auth::Principal>>) -> Json<Value> {
    let p = who.map_or_else(|| crate::auth::Principal::of(crate::auth::Role::None), |w| w.0);
    let role = match p.role {
        crate::auth::Role::Admin => "admin",
        crate::auth::Role::Write => "write",
        crate::auth::Role::Read => "read",
        crate::auth::Role::None => "none",
    };
    Json(j!({"user": p.name, "role": role, "superuser": p.superuser(), "slug": crate::auth::slug(&p), "open": app.open().await}))
}

/// Followers forward metadata writes to the leader, unchanged. The leader checks the caller's own sign-in (security pass 3,
/// K1): only the cluster's own calls go with the node's credential; anyone else's goes on with theirs.
async fn to_leader(State(app): State<App>, req: Request, next: Next) -> Response {
    if app.seq.is_some() {
        return next.run(req).await;
    }
    if app.members.reader() {
        return E(anyhow::anyhow!("read-only node")).into_response();
    }
    let operator = crate::auth::operator(); // (read now, while the request's scope holds: `auth::WHO`)
    // (a caller's own sign-in goes on as it came, at an open node too, whose open leader takes it as it takes none: a node
    // never puts its own credential behind a caller's. One that took itself for open, its catalog not yet holding its
    // leader's users, did, and a branch's key made a user's token at the leader: CI run 41359432087, attempt 2)
    let as_caller = match req.headers().get(axum::http::header::AUTHORIZATION) {
        _ if operator => None, // (the cluster itself: the node's own credential)
        Some(v) => Some(v.clone()),
        None if app.open().await => None, // (an open node: as `nodes_own` says)
        None => return (StatusCode::UNAUTHORIZED, "the leader checks the caller's own sign-in: send an Authorization header").into_response(),
    };
    // (the node's own credential is its token, its lake's key when this process has none, which the process's client lacks)
    let auth = as_caller.or_else(|| app.lake.node.token().and_then(|t| format!("Bearer {t}").parse().ok()));
    proxy(&app.lake.node.net.client(crate::net::Policy::Cluster), &app.members.leader().addr, req, auth).await.unwrap_or_else(|e| E(e).into_response())
}

/// Sends `req` to the leader at `addr` with `http`, the process's client for its lake's nodes (`net::Policy::Cluster`).
/// `as_caller`: the `Authorization` it goes with (the caller's own, or the node's), which replaces the client's default one
/// (reqwest fills a default header only when the request lacks it). None: the client's own.
async fn proxy(http: &reqwest::Client, addr: &str, req: Request, as_caller: Option<HeaderValue>) -> anyhow::Result<Response> {
    let (parts, body) = req.into_parts();
    let url = crate::tls::url(&format!("{addr}{}", parts.uri.path_and_query().map_or("", |p| p.as_str())));
    let mut out = http.request(parts.method, url).body(axum::body::to_bytes(body, usize::MAX).await?);
    if let Some(ct) = parts.headers.get("content-type") {
        out = out.header("content-type", ct);
    }
    if let Some(v) = as_caller {
        out = out.header(axum::http::header::AUTHORIZATION, v);
    }
    let res = out.send().await?;
    // (the leader's answer as it is: its status, content type and the x-pondra- headers, the SQLSTATE and notices among
    // them, which a client of this node reads)
    let status = res.status();
    let kept: Vec<_> = res.headers().iter().filter(|(k, _)| k.as_str() == "content-type" || k.as_str().starts_with("x-pondra-")).map(|(k, v)| (k.clone(), v.clone())).collect();
    let mut resp = Response::new(Body::from(res.bytes().await?));
    *resp.status_mut() = status;
    for (k, v) in kept {
        resp.headers_mut().append(k, v);
    }
    Ok(resp)
}

// ---------------------------------------------------------------- cluster internals

#[derive(Deserialize)]
struct BeatParams {
    from: String,
}

async fn beat(State(app): State<App>, Query(p): Query<BeatParams>, headers: axum::http::HeaderMap) -> Result<Json<(u64, Vec<String>)>, Response> {
    // Test hook: followers listed in the file $PONDRA_DROP_BEATS get no answer (a broken link to the leader).
    let dropped = crate::env::get("PONDRA_DROP_BEATS").map(|f| std::fs::read_to_string(f).unwrap_or_default()).unwrap_or_default();
    if dropped.lines().any(|a| a == p.from) {
        return Err(StatusCode::SERVICE_UNAVAILABLE.into_response());
    }
    let cut_off = || (StatusCode::SERVICE_UNAVAILABLE, [(crate::cluster::CUT_OFF_HEADER, "1")]).into_response(); // (its followers take over at once)
    let Some(node) = crate::cluster::of(&app) else { return Err(StatusCode::NOT_FOUND.into_response()) };
    node.beat(p.from, crate::format::said(&headers)).map(Json).ok_or_else(cut_off)
}

/// A follower's flush, to be sequenced (leader only). A node that doesn't lead refuses it as misdirected (421) before any
/// sequencer sees it, so its follower knows it was never taken and asks again once it follows the leader there is
/// (`log::led`): a leader killed and started again may come back a follower of the term another took, while followers of
/// its old term still send it their flushes until their next beat.
async fn commit(State(app): State<App>, body: Bytes) -> Result<Json<crate::log::Outcome>, Response> {
    let Some(seq) = app.seq.as_ref() else {
        return Err(error_response(Some(StatusCode::MISDIRECTED_REQUEST), &anyhow::anyhow!("not the leader")));
    };
    async {
        let flush = decode_flush(body)?;
        crate::auth::flush_from_writer(&flush)?; // (a write token's: rows, and no more)
        anyhow::Ok(Json(seq.submit(flush).await?))
    }
    .await
    .map_err(|e| E(e).into_response())
}

/// This node's share of a distributed query, sent a piece at a time (a big share is on disk).
async fn stage(State(app): State<App>, Json(slice): Json<crate::spmd::Slice>) -> Result<Response, E> {
    let (shape, parts) = crate::spmd::stage(&app.lake, &slice).await?;
    let done = slice.shuffle.is_none().then(|| crate::spill::Gone(slice.id.clone(), app.lake.node.clone()));
    Ok(Body::from_stream(crate::spmd::reply(&shape, parts, done)).into_response())
}

/// This node's share of an `INSERT … SELECT`, written into the table's folder (see `spmd::insert`):
/// its columns and files.
async fn insert_share(State(app): State<App>, Json((slice, table, stamp)): Json<(crate::spmd::Slice, String, Option<crate::log::Reserved>)>) -> Result<Json<(crate::spmd::Columns, Vec<crate::store::DataFile>)>, E> {
    Ok(Json(crate::spmd::insert_share(&app.lake, &slice, &table, stamp).await?))
}

/// This node's share of a `COPY … TO` a folder, written (see `spmd::copy`): how many rows.
async fn copy(State(app): State<App>, Json((slice, target)): Json<(crate::spmd::Slice, crate::copy::Target)>) -> Result<Json<u64>, E> {
    Ok(Json(crate::spmd::copy_share(&app.lake, &slice, &target).await?))
}

#[derive(Deserialize)]
struct BucketParams {
    id: String,
    exchange: usize,
    to: usize,
    #[serde(default)]
    drop: bool, // the coordinator gave up on this shuffle: forget it and delete what it spilled
    part: Option<usize>, // just this partition of it (a hot one shared out: `skew.rs`)
}

/// A shuffle bucket this node keeps for another node (see `spmd.rs`).
async fn bucket(State(app): State<App>, Query(p): Query<BucketParams>) -> Result<Response, E> {
    if p.drop {
        crate::spmd::forget(&app.lake, &p.id);
        return Ok(Body::empty().into_response());
    }
    let buckets = crate::spmd::bucket(&app.lake, &p.id, p.exchange, p.to, p.part)?;
    Ok(Body::from_stream(crate::spmd::reply("", buckets, None)).into_response()) // (a piece at a time: a big bucket is on disk)
}

/// A share of the leader's data work (tiering, merging, compaction): the files written.
async fn job(State(app): State<App>, Json(job): Json<crate::tier::Job>) -> Result<Json<Vec<DataFile>>, E> {
    Ok(Json(crate::tier::run_job(&app.lake, job).await?))
}

/// The commit stream (see `Frame`): who leads, then the recent frames, then every new one. A follower's are the leader's
/// frames as it relayed them (`members::follow`, `Catalog::relay`), so a page that joins a follower reads the leader's
/// stream from its own node, and the leader serves one stream per follower, not per page. A caller that isn't one of
/// the nodes (`nodes_own`) gets each change's readable keys only (`for_caller`). Its sign-in revoked or expired, the
/// stream ends (`until_revoked`).
async fn feed(State(app): State<App>, who: Option<axum::Extension<crate::auth::Principal>>, headers: axum::http::HeaderMap) -> Response {
    let operator = nodes_own(&app).await; // (read now: the stream is polled after this request's scope has ended)
    let header = headers.get("authorization").and_then(|v| v.to_str().ok()).map(String::from);
    let need = crate::auth::Auth::needed("/cluster/log", "GET"); // (a superuser's own sign-in, as `guard` lets it follow the stream)
    let revoked = until_revoked(app.clone(), who.map(|w| w.0), operator, header, move |p| p.role >= need && p.scope.whole());
    let cat = &app.lake.cat;
    let streamed = operator.then(|| crate::format::Streamed::to(&app.lake, crate::format::said(&headers).1)); // (held while the stream is open: `format::raise`; a page's tab doesn't count)
    // (a joined page's tab, which says no format as the nodes' own client does (`cluster::http`), is a use of the node while it
    // follows, as its other requests are; the nodes' streams never end while a cluster runs, so they aren't: `idle.rs`, 11-03 audit M7)
    let page = (!headers.contains_key("x-pondra-format")).then(|| app.lake.node.idle.begin());
    let start = Frame::Start { term: app.members.leader().n, replicated: cat.replicas > 1 };
    let (recent, rx) = cat.subscribe();
    let live = futures::stream::unfold(rx, |mut rx| async move { rx.recv().await.ok().map(|f| (f, rx)) }); // a lagging follower reconnects
    let frames = futures::stream::iter([start].into_iter().chain(recent)).chain(live).map(move |f| {
        let _ = (&streamed, &page);
        Ok::<_, std::io::Error>(for_caller(f, operator).encode())
    });
    Body::from_stream(app.lake.node.drain.endless(frames.take_until(revoked))).into_response() // (the node's door closing ends it too: `Drain::endless`)
}

/// `POST /cluster/records`: a joined page's own statements, which this node writes as its own (`page_records::record`), under the
/// sign-in's name, as the door `page-reported` (`page_records::REPORTED`). Only a superuser's own sign-in sends them (`page_records::refusal`: never the nodes' key, a
/// user with grants, or a procedure's lent token); a batch past `BATCH_LINES` lines is 413.
async fn records(State(app): State<App>, headers: axum::http::HeaderMap, who: Option<axum::Extension<crate::auth::Principal>>, Json(batch): Json<crate::page_records::Batch>) -> Response {
    let credential = crate::auth::Credential::of_headers(&headers);
    let token = credential.token();
    let Some(axum::Extension(who)) = who else { return (StatusCode::FORBIDDEN, "a joined page's lines need a sign-in").into_response() };
    if let Some(why) = crate::page_records::refusal(&who, token) {
        return (StatusCode::FORBIDDEN, why).into_response();
    }
    if batch.lines.len() > crate::page_records::BATCH_LINES {
        return StatusCode::PAYLOAD_TOO_LARGE.into_response();
    }
    crate::page_records::record(&app, &who, batch).await;
    StatusCode::OK.into_response()
}

/// Resolves once the caller of an answer that goes on (the commit stream, `/watch`, a live query, Flight's follow stream) may no longer have it: its
/// sign-in revoked, expired, or changed so that `still` refuses it (a grant taken back, a superuser no more); a caller that
/// came in with nothing signed, at an open node, once the node is open no more. Each second it asks `Principal::stale`, a
/// version compare, and signs in again only once that moved (a REVOKE, a DROP USER, a token or session ended), as a Postgres
/// connection does at its next statement and a Kafka client at its next request: so such an answer ends within a second of the
/// change, and a stream whose rights don't move costs no catalog read or password check (11-08 audit M7; 11-01 H4, 11-02 H3
/// and 11-03 H1 before). `who` is the caller its door signed in, given now, since the stream is polled after the request. A
/// node, a token and the program that started this node never end.
pub(crate) fn until_revoked(app: App, who: Option<crate::auth::Principal>, operator: bool, header: Option<String>, still: impl Fn(&crate::auth::Principal) -> bool + Send + 'static) -> impl std::future::Future<Output = ()> + Send + 'static {
    async move {
        if operator || who.as_ref().is_some_and(|w| w.operator) {
            return futures::future::pending().await;
        }
        let mut who = who.unwrap_or_else(|| crate::auth::Principal::of(crate::auth::Role::None)); // (nothing signed: asked once the rights move)
        loop {
            tokio::time::sleep(Duration::from_secs(1)).await;
            if !who.stale(&app.lake) {
                continue;
            }
            let epoch = app.lake.cat.rights_version(); // (before it is asked again, as at sign-in: a change meanwhile is seen next time)
            let signed = match &header {
                Some(h) => crate::users::who(&app.lake, &app.auth, &crate::auth::Credential::of_header(Some(h))).await.filter(|p| still(p)),
                None => None,
            };
            who = match signed {
                Some(p) => crate::auth::Principal { epoch: p.epoch.max(epoch), ..p },
                None if app.open().await => crate::auth::Principal { epoch, ..who }, // (an open node's anyone, until it signs its users in)
                None => return,
            };
        }
    }
}

/// The caller of a request, as `until_revoked` checks it again: whether it is one of the nodes' own (`auth::operator`, read
/// while the request's scope holds), and its `Authorization`.
fn caller(headers: &HeaderMap) -> (bool, Option<String>) {
    (crate::auth::operator(), headers.get("authorization").and_then(|v| v.to_str().ok()).map(String::from))
}

/// Is the request being served one of the nodes' own: their tokens, their key, the program that started this node
/// (`auth::operator`), or anyone at an open node, as `guard` lets the cluster's routes through. A node that follows its
/// leader on an open lake sends no token, and must get every key.
async fn nodes_own(app: &App) -> bool { crate::auth::operator() || app.open().await }

/// What a caller sees of one frame: a change with only the keys it may hold (`objects::readable`). The frame stays, so
/// the ids run on without a gap.
fn for_caller(f: Frame, operator: bool) -> Frame {
    match f {
        Frame::Change(d) if !operator => Frame::Change(Arc::new(d.only(crate::objects::readable))),
        f => f,
    }
}

#[derive(Deserialize, Default)]
#[serde(default)]
struct KvParams {
    key: Option<String>,
    from: Option<String>,
    to: Option<String>,
    limit: Option<usize>,
    since: Option<u64>,
    start: Option<String>,
}

/// A page of a range on `GET /cluster/kv`: at most this many keys, and past about this many bytes of values it ends (after
/// at least one key). A range past these bounds without a `limit` is refused (413): an old page asks whole, and is told to ask in pages.
const KV_KEYS: usize = 5_000;
const KV_BYTES: usize = 8 << 20;

/// `GET /cluster/kv`: what a page reads of this node's catalog, as base64url of the bytes (no padding). `?key=K` answers
/// `{"value": …}` (null when there is none), `?from=F[&to=T][&limit=N]` answers `{"entries": [[key, value], …], "more": …,
/// "at": …}` for the range [F, T): no `to` is to the end. `limit` (1 to 5,000) is the page's keys at most; `more` says keys
/// follow it, which the next request asks for (from the last key + `"\0"`, the same `to`), and `at` is the commit the
/// page was read as of. A range's later pages ask `since=` the first page's `at` and `start=` the range's own start, and
/// `moved` says a key the caller may see from that start to `to` changed after that commit (then the range starts over). A caller that isn't one of the nodes
/// (`nodes_own`) is answered only for the keys `objects::readable` allows.
async fn kv(State(app): State<App>, Query(p): Query<KvParams>) -> Result<Json<Value>, Response> {
    use base64::{engine::general_purpose::URL_SAFE_NO_PAD as B64, Engine};
    let operator = nodes_own(&app).await;
    let may = |k: &str| operator || crate::objects::readable(k);
    let cat = &app.lake.cat;
    let failed = |e: anyhow::Error| E(e).into_response();
    if let Some(key) = p.key {
        let value = match may(key.as_str()) {
            true => cat.get_raw(&key).await.map_err(failed)?,
            false => None,
        };
        return Ok(Json(j!({"value": value.map(|v| B64.encode(v))})));
    }
    // (keys are UTF-8 strings: no key sorts after U+10FFFF, which is therefore "to the end")
    let text = |b64: &str| B64.decode(b64).ok().and_then(|b| String::from_utf8(b).ok()).ok_or_else(|| (StatusCode::BAD_REQUEST, "a bound is base64url of a key").into_response());
    let from = text(&p.from.ok_or_else(|| (StatusCode::BAD_REQUEST, "?key= or ?from= says what to read").into_response())?)?;
    let to = match p.to {
        Some(to) => text(&to)?,
        None => "\u{10ffff}".to_string(),
    };
    // (one more key than a page holds, so that `more` is known; no `limit` is the whole range, if it fits a page's bounds)
    let asked = p.limit.map_or(KV_KEYS, |n| n.clamp(1, KV_KEYS));
    let (got, at) = cat.scan_page(&from, &to, asked + 1, &may).await.map_err(failed)?;
    let (kept, more) = match p.limit {
        Some(_) => {
            let (mut kept, mut bytes, mut more) = (Vec::new(), 0, got.len() > asked);
            for (k, v) in got.into_iter().take(asked) {
                if !kept.is_empty() && bytes + v.len() > KV_BYTES {
                    more = true; // (the rest of the range is the next page's)
                    break;
                }
                bytes += v.len();
                kept.push((k, v));
            }
            (kept, more)
        }
        None => {
            if got.len() > asked || got.iter().map(|(_, v)| v.len()).sum::<usize>() > KV_BYTES {
                return Err((StatusCode::PAYLOAD_TOO_LARGE, "this range is more than 5,000 keys or 8 MB: read it in pages, with ?limit=N, and from the last key + \"\\0\" for as long as `more` is true").into_response());
            }
            (got, false)
        }
    };
    // (`since`: a page after the first of a range asks whether the keys it may see moved since that first page's `at`)
    let start = match p.start {
        Some(start) => text(&start)?, // (the range's own start: its earlier pages' keys count too)
        None => from.clone(),
    };
    let moved = p.since.is_some_and(|since| cat.moved_since(&start, &to, since, at, &may));
    let entries: Vec<(String, String)> = kept.into_iter().map(|(k, v)| (k, B64.encode(v))).collect();
    Ok(Json(j!({"entries": entries, "more": more, "at": at, "moved": moved})))
}

#[derive(Deserialize, Default)]
#[serde(default)]
struct AckParams {
    from: String,
    term: u64,
    first: u64,
    upto: u64,
    after: u64,
}

/// Leader: a follower holds a run of changes (replicated commits). Acks meant for another
/// leader's term don't count.
async fn ack(State(app): State<App>, Query(p): Query<AckParams>) -> StatusCode {
    if !app.members.is_leader() || p.term != app.members.leader().n {
        return StatusCode::CONFLICT;
    }
    app.lake.cat.ack(&p.from, p.first, p.upto);
    StatusCode::OK
}

/// A new leader taking over collects what this node holds beyond the bucket (`replica::recover`).
async fn replica(State(app): State<App>, Query(p): Query<AckParams>) -> Result<Vec<u8>, StatusCode> {
    Ok(app.replica.as_ref().ok_or(StatusCode::NOT_FOUND)?.serve(p.term, p.after))
}

// ---------------------------------------------------------------- the API

/// Body: a table's definition (see `write::TableSpec`): `[["user","Utf8"],…]`, or
/// `{"columns": [...], "key": ["user"], "merge": {...}, "publish": [...], "cluster_by": [...]}`.
async fn create_table(State(app): State<App>, Path(name): Path<String>, headers: HeaderMap, body: String) -> Result<Json<Value>, E> {
    if crate::sys::hidden(&name) {
        return Err(E(anyhow::anyhow!("{name} is written by Pondra alone"))); // (a replace's staged table is the leader's own: `write::create_as`)
    }
    if !cluster_call(&app, &headers) {
        crate::protect::check_table(&app.lake, &name).await?; // (it may add columns to a project's table)
    }
    let _guard = app.lock.lock().await;
    Ok(Json(crate::write::create_table(&app.lake, &name, &body).await?))
}

#[derive(Deserialize)]
struct AppendParams {
    #[serde(default)]
    producer: String, // (none: at least once, as a Kafka producer that isn't idempotent)
    #[serde(default)]
    seq: u64,
    prev: Option<u64>, // compare-and-swap on the producer's last seq
}

/// Body: NDJSON rows, or an Arrow IPC stream (content-type application/vnd.apache.arrow.stream).
async fn append(State(app): State<App>, Path(name): Path<String>, Query(p): Query<AppendParams>, who: Option<axum::Extension<crate::auth::Principal>>, headers: HeaderMap, body: Bytes) -> Result<Json<Ack>, E> {
    let who = who.map_or_else(|| crate::auth::Principal::of(crate::auth::Role::None), |w| w.0);
    let (start, scope) = (std::time::Instant::now(), who.scope.clone());
    let (out, unseen) = crate::rights::noting(append_rows(&app, &name, p, headers, body)).await;
    crate::audit::rows(&app, &who, "/append", &name, out.as_ref().err().map(|e| e.told(&scope)), unseen, start.elapsed()); // (the audit log's line for the rows: S23)
    out
}

async fn append_rows(app: &App, name: &str, p: AppendParams, headers: HeaderMap, body: Bytes) -> Result<Json<Ack>, E> {
    app.log()?; // (a read-only node refuses before anything is read)
    if !p.producer.is_empty() && p.seq == 0 {
        return Err(E(anyhow::anyhow!(crate::log::SEQ_FROM_1))); // (0 is "nothing yet": its batch would be taken for a retry)
    }
    let writer = crate::writer::Writer::open(app, None, name, crate::rules::How::Load, false).await?; // (the caller's rights first: `writer::may`)
    let (meta, rules) = (writer.meta(), writer.rules());
    let schema = rules.schema.clone(); // (rows come under SQL's names; the log keeps stored ones: ADR-022)
    let arrow = headers.get("content-type").is_some_and(|v| v.as_bytes().starts_with(b"application/vnd.apache.arrow"));
    let read = || -> anyhow::Result<_> {
        Ok(if arrow {
            // Columns by name, cast to the table's types (pandas, Polars and Arrow differ in string
            // types); a column left out takes its DEFAULT, or is null (as in JSON), e.g. `_deleted`,
            // or one added since.
            let ipc = StreamReader::try_new(&body[..], None)?;
            let given: Vec<String> = ipc.schema().fields().iter().map(|f| f.name().clone()).collect();
            let ipc = ipc.collect::<Result<Vec<_>, _>>()?;
            (ipc.iter().map(|b| crate::query::conform(b, &schema)).collect::<anyhow::Result<Vec<_>>>()?, Some(given))
        } else {
            (vec![crate::json_rows::from_json(&body, &schema)?], None) // (integers round as SQL writes them: invariant 293)
        })
    };
    // (rows that can't be read as the table's are the sender's: 22P02, never the node's 500)
    let (batches, given) = read().map_err(|e| crate::codes::coded("22P02", format!("the rows can't be read as {name}'s: {}", crate::ext::said(&e))))?;
    let batch = concat_batches(&schema, &batches)?;
    let left_out = match given {
        Some(given) => rules.left_out(&given),
        None if rules.fills() => crate::rules::LeftOut::PerRow(crate::rules::absent_keys(meta, &body)?), // (a JSON row without the key)
        None => crate::rules::LeftOut::None,
    };
    // (shaped, then the rights its rows ask: a keyed table's replace their keys', UPDATE too; marked _deleted, DELETE: S11)
    let queued = writer.put(crate::rules::Rows::Named(batch, left_out), Src { producer: p.producer, seq: p.seq, prev: p.prev }).await?;
    Ok(Json(queued.ack.await?))
}

#[derive(Deserialize)]
struct InsertParams {
    job: String,
    staged: Option<u8>, // (1: the body is the columns of rows a joined page staged, `insert_staged`; else it is SQL)
}

/// Bulk `INSERT INTO name <body SQL>`: this node runs the query and writes the Parquet (no log);
/// the leader records the files, together with `job` so a retried job is applied once. Creates
/// the table if missing. With `staged=1` the body is the columns of a joined page's staged rows (`insert_staged`).
async fn insert(State(app): State<App>, Path(name): Path<String>, Query(p): Query<InsertParams>, who: Option<axum::Extension<crate::auth::Principal>>, body: String) -> Result<Json<Value>, E> {
    let who = who.map_or_else(|| crate::auth::Principal::of(crate::auth::Role::None), |w| w.0);
    let (start, scope) = (std::time::Instant::now(), who.scope.clone());
    let (out, unseen) = crate::rights::noting(async {
        match p.staged {
            Some(1) => insert_staged(&app, &name, &p.job, &body, &who).await,
            _ => insert_rows(&app, &name, p, body, &scope).await,
        }
    })
    .await;
    crate::audit::rows(&app, &who, "/insert", &name, out.as_ref().err().map(|e| e.told(&scope)), unseen, start.elapsed()); // (the audit log's line for the rows: S23)
    out
}

async fn insert_rows(app: &App, name: &str, p: InsertParams, query: String, scope: &crate::rights::Scope) -> Result<Json<Value>, E> {
    ensure!(!app.members.reader(), "read-only node");
    if app.lake.table_meta_if(name).await?.is_none() {
        let define = crate::rights::may_here(crate::rights::Action::Define, crate::rights::Object::Table(name)); // (making a table, as CREATE TABLE does)
        if define.is_err() && !scope.whole() {
            // (a user with grants who may not make it is told there is no such table, as of one it holds nothing on, so the code
            // never tells a missing table from a hidden one: 11-08 audit M5)
            return Err(crate::codes::coded("42P01", format!("no table {name}")).into());
        }
        define?;
    }
    crate::rights::may_here(crate::rights::Action::Insert, crate::rights::Object::Table(&name))?; // (its INSERT grant, as SQL's)
    let query = crate::routines::expand(&app.lake, &query, scope).await?;
    // (a table that moved a column: the query's columns fill SQL's first ones, as an INSERT's do: `write::in_sql_order`)
    let query = match app.lake.table_meta_if(name).await? {
        Some(m) if m.moved() => crate::write::whole_rows_of(&app.lake, name, &m, &crate::write::filled(&app.lake, &m, name, &query, scope).await?, &query).await?,
        _ => query,
    };
    let ctx = crate::query::session_for(&app.lake, &query, "", None, scope).await?;
    match crate::write::write_files(&app.lake, &ctx, &name, &query, &p.job, crate::write::stamp(&app.lake, &name, Some(&app.to())).await?).await? {
        Some(f) => Ok(Json(app.record_files(f).await?)),
        None => Ok(Json(j!({"duplicate": true}))),
    }
}

/// A joined page's rows, staged (`staged=1`): this node runs `INSERT INTO name (columns) SELECT c0 AS "a", … FROM` the files the
/// page put in `stage/<slug>/<job>/` (`auth::page_creates`), as the signed-in user, through its own write (`write::on_node_as`):
/// the table's types, NOT NULL, CHECK, keys, identity and defaults, the INSERT grant, the job's duplicate answer and the record
/// are all this node's. The page's files are only a transport, so the node reads what they hold by position and checks it. The
/// staged files are deleted once the INSERT has answered, whatever the answer, or was stopped, so a resend finds none and is
/// answered by the job. `files` is how many the page staged: the node takes the INSERT only when exactly those are there, so a
/// file lost on the way (or swept: `tier::collect_orphans`) is a refusal, never a quiet loss of its rows.
async fn insert_staged(app: &App, name: &str, job: &str, body: &str, who: &crate::auth::Principal) -> Result<Json<Value>, E> {
    #[derive(Deserialize)]
    struct Staged { columns: Vec<String>, files: u64, #[serde(default)] placed: Option<u64> }
    ensure!(!app.members.reader(), "read-only node");
    let Some(slug) = crate::auth::slug(who) else { return Err(crate::codes::coded("42501", "a staged INSERT is a user's: its sign-in has no name to stage under").into()) };
    if !crate::auth::staged_job(job) {
        return Err(crate::codes::coded("22023", "a staged INSERT's job is letters, digits and hyphens, 64 at most").into()); // (invalid_parameter_value)
    }
    crate::rights::may_here(crate::rights::Action::Insert, crate::rights::Object::Table(name))?; // (its INSERT grant, as SQL's)
    let Staged { columns, files, placed } = serde_json::from_str(body)?;
    if files > crate::write::STAGED_FILES_MOST {
        return Err(crate::codes::coded("54000", format!("a staged INSERT takes {} files at most, and this one says {files}", crate::write::STAGED_FILES_MOST)).into());
    }
    let dir = format!("stage/{slug}/{job}/");
    let unstage = Unstage(Some((app.lake.clone(), dir.clone()))); // (whether it answers, fails or is stopped: the page's files are its transport only)
    let there = staged_files(&app.lake, &dir).await?;
    // (none there: a resend after its INSERT took them, which the job answers as a duplicate, or nothing to take)
    // (compared in place: the count is the sender's, and a range collected from it was a vector as long as it says, which
    // for 2^40 files asked for 8 TiB and aborted the node: 11-02 audit H2)
    if !there.is_empty() && (there.len() as u64 != files || there.iter().enumerate().any(|(i, &n)| n != i as u64)) {
        return Err(crate::codes::coded("55000", format!("the page staged {files} files for this INSERT and {} are here: it is sent again whole", there.len())).into());
    }
    // (a column it doesn't have, or names twice, is refused by `whole_rows` as any INSERT's is, before the files are read)
    let picks: Vec<String> = columns.iter().enumerate().map(|(i, c)| format!("c{i} AS \"{}\"", c.replace('"', "\"\""))).collect();
    let url = format!("{}/stage/{slug}/{job}/*.parquet", app.lake.url.trim_end_matches('/')).replace('\'', "''");
    let stmt = crate::write::Stmt::InsertInto(name.to_string(), columns, format!("SELECT {} FROM '{url}'", picks.join(", ")));
    crate::rights::may_run(who.role, &stmt)?;
    // (the page's INSERT without a column list: the order it put the values by, which the leader checks, `log::placed_for`)
    let placed = match (placed, crate::ddl::resolve(&app.lake, name).await) {
        (Some(p), Ok((None, table))) => Some((table, p)),
        _ => None,
    };
    let out = crate::log::placed_as(placed, crate::write::on_node_as(app, stmt, Some(format!("staged:{slug}:{job}")), true, &who.scope)).await;
    unstage.now().await; // (before the answer, so a resend of the same job stages afresh)
    Ok(Json(out?))
}

/// The numbers of the staged files in `dir` (`<n>.parquet`, `auth::page_creates`), in order.
async fn staged_files(lake: &crate::store::Lake, dir: &str) -> anyhow::Result<Vec<u64>> {
    use futures::TryStreamExt;
    let listed = lake.store.list(Some(&object_store::path::Path::parse(dir)?)).try_collect::<Vec<_>>().await?;
    let mut n: Vec<u64> = listed.iter().filter_map(|o| o.location.filename()?.strip_suffix(".parquet")?.parse().ok()).collect();
    n.sort_unstable();
    Ok(n)
}

/// A staged INSERT's folder, deleted once its INSERT has answered (`now`), or in the background when it was dropped unanswered (a
/// stopped INSERT: `stoppable`). A failure is not the answer's, and the leader's orphan sweep takes what is left
/// (`tier::collect_orphans`).
struct Unstage(Option<(Arc<crate::store::Lake>, String)>);

impl Unstage {
    async fn now(mut self) {
        if let Some((lake, dir)) = self.0.take() {
            unstage(&lake, &dir).await;
        }
    }
}

impl Drop for Unstage {
    fn drop(&mut self) {
        if let Some((lake, dir)) = self.0.take() {
            crate::spawns::spawn(async move { unstage(&lake, &dir).await });
        }
    }
}

/// Deletes what is stored under `dir`.
async fn unstage(lake: &crate::store::Lake, dir: &str) {
    use futures::TryStreamExt;
    let Ok(at) = object_store::path::Path::parse(dir) else { return };
    let Ok(listed) = lake.store.list(Some(&at)).try_collect::<Vec<_>>().await else { return };
    for o in listed {
        let _ = object_store::ObjectStoreExt::delete(&lake.store, &o.location).await;
    }
}

/// Whom a request plans for: the principal its guard signed in (`guard`), or nobody when none did (it sees nothing).
fn scope_of(who: Option<axum::Extension<crate::auth::Principal>>) -> crate::rights::Scope { who.map_or_else(crate::rights::Scope::nothing, |w| w.0.scope) }

/// Record an INSERT's files (from another node, or a `pondra sql` somewhere else).
async fn files(State(app): State<App>, Json(f): Json<crate::write::Files>) -> Result<Json<Value>, E> {
    let meta = app.lake.cat.meta(&table_key(f.table())).await?;
    crate::auth::files_from_writer(f.table(), meta.as_ref().map(|m| m.folder(f.table())), &f.paths())?; // (a write token's: into its table's folder)
    Ok(Json(app.record_files(f).await?))
}

/// Body: `{"source": "events", "target": "per_user", "sql": "SELECT … FROM events …"}`.
async fn create_task(State(app): State<App>, Path(name): Path<String>, who: Option<axum::Extension<crate::auth::Principal>>, body: String) -> Result<Json<Value>, E> {
    let mut task: Task = serde_json::from_str(&body)?;
    let scope = scope_of(who);
    task.sql = crate::routines::expand(&app.lake, &task.sql, &scope).await?;
    crate::tasks::create(&app.lake, &name, &task, &scope).await?;
    Ok(Json(j!({"task": name})))
}

/// `POST /views/{name}` with the view's SQL, e.g. `SELECT user, sum(amount) AS total, count(*) AS n
/// FROM events GROUP BY user`; `?window=w&size_secs=60&lateness_secs=10` also emits each
/// window of column `w` once, final, to `{name}_final`; `?session=ts&gap_secs=30&lateness_secs=5`
/// makes it a session view (`views.rs`): `CREATE MATERIALIZED VIEW … WITH (…)` in SQL.
async fn create_view(State(app): State<App>, Path(name): Path<String>, Query(options): Query<std::collections::BTreeMap<String, String>>, who: Option<axum::Extension<crate::auth::Principal>>, sql: String) -> Result<Json<Value>, E> {
    let scope = scope_of(who);
    let (text, sql) = (Some(sql.clone()), crate::routines::expand(&app.lake, &sql, &scope).await?); // (as they are now: ADR-023; as written, if it reads so: `authored.rs`)
    let d = crate::ddl::Ddl::CreateMaterialized { name, sql, options, text };
    let out = {
        let _guard = app.lock.lock().await;
        crate::ddl::apply(&app.lake, d.clone(), &scope).await?
    };
    app.settle(&d).await?; // (once it's filled from the rows already there)
    Ok(Json(out))
}

/// An UPDATE, DELETE or MERGE a follower's SQL asked for (`change.rs`): `[sql, job]`.
async fn change(State(app): State<App>, who: Option<axum::Extension<crate::auth::Principal>>, Json((sql, job, sent)): Json<(String, String, crate::change::Sent)>) -> Result<Json<Value>, E> {
    let seq = app.seq.as_ref().ok_or_else(|| anyhow::anyhow!("not the leader"))?;
    let _guard = app.lock.lock().await;
    let sent = Arc::new(crate::change::unpack(&sent)?); // (what it reads that this node can't: `change::for_leader`)
    Ok(Json(crate::query::SENT.scope(sent, crate::change::run(&app.lake, seq, &sql, &job, &scope_of(who))).await?))
}

/// Whether a request is the cluster's own (the nodes' key, an operator's token, or none on an open
/// lake), which the door it first came in by checked. A user's sign-in, Basic included, is not, so
/// the routes the cluster forwards to check a protected database's objects for it (`protect.rs`).
fn cluster_call(app: &App, headers: &HeaderMap) -> bool {
    match crate::auth::Credential::of_headers(headers) {
        crate::auth::Credential::None => !headers.contains_key("authorization"),
        // (a `pn_` key counts only without an admin token, as in `users::who`)
        c => c.token().is_some_and(|b| (b.starts_with("pn_") && !app.auth.admin_set()) || app.auth.token_role(b) > crate::auth::Role::None),
    }
}

/// A `CREATE`/`DROP` of a schema, view or table that a follower's SQL asked for (`ddl.rs`), and a
/// branch's pin (ADR-058). A follower sends it to the leader, asked with the caller's own token.
async fn ddl(State(app): State<App>, headers: HeaderMap, role: Option<axum::Extension<crate::auth::Role>>, who: Option<axum::Extension<crate::auth::Principal>>, Json(mut d): Json<crate::ddl::Ddl>) -> Result<Json<Value>, E> {
    crate::backup::sent(&d)?;
    let scope = &scope_of(who);
    let credential = crate::auth::Credential::of_headers(&headers);
    let key = credential.token().filter(|k| k.starts_with("pb_"));
    if let Some(key) = key {
        crate::branch::keyed(&app.lake, &d, key).await?; // (a branch's key: its own pin, nothing else)
    } else if role.is_none_or(|r| r.0 < crate::auth::Role::Admin) || !cluster_call(&app, &headers) {
        crate::branch::cloning(&d)?; // (a user with CLONE, or a user's own sign-in, a superuser's too: a pin, and nothing else)
    }
    if !cluster_call(&app, &headers) && key.is_none() {
        crate::protect::check_ddl(&app.lake, &d).await?;
    }
    crate::protect::stamp(&mut d); // (who lifts a protection, when the statement doesn't say)
    let out = match &app.seq {
        Some(seq) => crate::write::ddl_here(&app.lake, seq, &app.lock, d.clone(), scope).await?,
        None if !app.members.is_leader() => Box::pin(crate::write::send_as(&app.lake.url, crate::write::Request::Ddl(d.clone()), credential.token(), &app.lake.node)).await?, // (the leader makes it)
        None => {
            let _guard = app.lock.lock().await;
            crate::ddl::apply(&app.lake, d.clone(), scope).await?
        }
    };
    app.settle(&d).await?; // (ATTACH, CREATE DATABASE: the leader too at once, not in a second)
    let mut out = out;
    if key.is_some() {
        // (a renewal by a branch's key gets no key back: one given before the lake's keys were sealed would get the
        // new one, which lets the pin go (11-01 audit L12), and the branch keeps the key it has: `branch::renew`)
        out.as_object_mut().map(|o| o.remove("key"));
    }
    Ok(Json(out))
}

/// `GET /lookup/{table}/{key}`: the current row of one key, for serving reads — same answer as
/// `SELECT … WHERE key = …`. Upsert tables skip SQL entirely (`serve.rs`); merge tables combine
/// the key's partial rows with a filter + GROUP BY on one thread. Composite keys are
/// comma-separated, in the key's column order.
async fn lookup(State(app): State<App>, Path((name, key)): Path<(String, String)>) -> Result<Response, E> {
    crate::rights::may_here(crate::rights::Action::SelectAll, crate::rights::Object::Table(&name))?; // (a user's: the whole row)
    let lake = &app.lake;
    let meta = lake.table_meta(&name).await?;
    ensure!(!meta.key.is_empty(), "{name} has no key: use /sql");
    // (SQL's order, for a table that moved a column: as `SELECT *` lists them)
    let in_order = |b: RecordBatch| meta.in_sql_order(&b);
    if meta.merge.is_empty() && meta.ttl.is_none() && meta.order.is_none() {
        let shown = crate::answer::schema_of(meta.columns().filter(|(c, _)| *c != "_deleted"))?; // (as `SELECT *` shows it)
        let row = crate::serve::lookup(lake, &name, &meta, &key).await?.map(|r| crate::query::conform(&r, &shown)).transpose()?; // (by name: a row from before an ADD COLUMN lacks it)
        let row = row.map(|r| meta.to_logical(&r).and_then(in_order)).transpose()?;
        return Ok(([("content-type", "application/json")], crate::answer::json(&row)?).into_response());
    }
    let mut where_ = vec![];
    for (col, val) in meta.key.iter().zip(key.split(',')) {
        let text = meta.columns().any(|(c, t)| c == col && (t == "Utf8" || t == "LargeUtf8"));
        ensure!(!val.contains('\''), "quote in key");
        where_.push(match text {
            true => format!("\"{col}\" = '{val}'"),
            false => format!("\"{col}\" = {val}"),
        });
    }
    let (ctx, where_) = (lake.session_with(1), where_.join(" AND "));
    ctx.register_table("__raw", raw(lake, &ctx, &name, &meta, None).await?.into_view())?;
    let cols = meta.columns().map(|(c, _)| format!("\"{c}\"")).collect::<Vec<_>>().join(", ");
    let sql = match meta.merge.is_empty() {
        // Upsert table: the newest version of the key wins (no window over the whole table).
        true => format!("SELECT * FROM (SELECT {cols} FROM __raw WHERE {where_} ORDER BY {}\"_ord\" DESC LIMIT 1){}", meta.order.as_ref().map(|o| format!("\"{o}\" DESC NULLS LAST, ")).unwrap_or_default(), crate::query::live(&meta)),
        // Merge table: combine that key's partial rows.
        false => format!("{} ", latest_sql(&meta, "__raw", false, false)).replace(" GROUP BY ", &format!(" WHERE {where_} GROUP BY ")),
    };
    let shown = |b: &RecordBatch| b.project(&(0..b.num_columns()).filter(|&i| b.schema().field(i).name() != "_deleted").collect::<Vec<_>>());
    let batches = ctx.sql(&sql).await?.collect().await?.iter().map(|b| meta.to_logical(&shown(b)?).and_then(in_order)).collect::<anyhow::Result<Vec<_>>>()?;
    Ok(([("content-type", "application/json")], crate::answer::json(&batches)?).into_response())
}

/// `POST /sql`: statements (SQL, or JSON with `$name` parameters, or that and tables of the
/// caller's own: `routines::Request`), run in order; the last one's answer. A single query as it
/// was sent may be answered from `Results`.
async fn sql(State(app): State<App>, Query(p): Query<crate::statement::SqlParams>, role: axum::Extension<crate::auth::Role>, who: axum::Extension<crate::auth::Principal>, headers: axum::http::HeaderMap, body: Bytes) -> Response {
    let format = p.format.clone(); // (an answer handed to the door in pieces is sent in this format)
    let node = app.lake.node.clone(); // (an answer in pieces is in flight until its last piece is read: `pieces`)
    let said = crate::statement::sql(app, p, role.0, who.0, headers, body).await;
    let mut r = match said.reply {
        Ok(crate::statement::Reply::Rendered(r)) => respond(r, format.as_deref(), &node),
        Ok(crate::statement::Reply::Done(v)) => Json(v).into_response(),
        Err(e) => E::from(e).into_response(),
    };
    r = held_session(r, said.session.as_deref());
    if let Some(h) = crate::statement::notices(&said.heard).and_then(|t| axum::http::HeaderValue::from_str(&t).ok()) {
        r.headers_mut().insert("x-pondra-notices", h);
    }
    r
}

/// The header that keeps a client to this node while its session holds temporary tables (`temp::holds`).
fn held_session(mut r: Response, session: Option<&str>) -> Response {
    if session.is_some_and(crate::temp::holds) {
        let (name, value) = crate::statement::HELD_SESSION;
        r.headers_mut().insert(name, axum::http::HeaderValue::from_static(value)); // (a client keeps to this node meanwhile)
    }
    r
}

/// `GET /secrets/{name}`: a secret's values (`CREATE SECRET`), for a Python procedure's own code
/// (`pondra.secret`) and nothing else: the token must be one lent to a procedure while it runs.
/// The values are kept out of what that procedure says (its notices, its error, the run log).
async fn secret(State(app): State<App>, Path(name): Path<String>, headers: axum::http::HeaderMap) -> Result<Json<Value>, E> {
    let credential = crate::auth::Credential::of_headers(&headers);
    let token = credential.token();
    if crate::auth::lent(token).is_none() {
        return Err(E(crate::codes::coded("42501", "only a procedure's code reads a secret: pondra.secret(name), in a Python procedure")));
    }
    let values = crate::ext::reveal(&app.lake, &name.to_lowercase()).await?;
    crate::auth::revealed(token, values.iter().filter(|(k, _)| *k != "type" && *k != "scope").map(|(_, v)| v.clone()));
    Ok(Json(j!(values)))
}

/// `POST /plan`, `/apply`, `/test`, `/status`, `/capture`: a project's files planned here, applied, its tests run,
/// what differs from the database, or what writing it into the files takes (ADR-047 §4, ADR-060 §7), each with the
/// same body.
async fn apply(State(app): State<App>, uri: axum::http::Uri, role: axum::Extension<crate::auth::Role>, who: axum::Extension<crate::auth::Principal>, headers: axum::http::HeaderMap, Json(ask): Json<crate::apply::Ask>) -> Result<Json<Value>, E> {
    use crate::apply::Verb;
    let verb = match uri.path().rsplit('/').next() {
        Some("plan") => Verb::Plan,
        Some("test") => Verb::Test,
        Some("status") => Verb::Status,
        Some("capture") => Verb::Capture,
        _ => Verb::Apply,
    };
    let who = crate::routines::Who::signed(who.0, role.0, crate::statement::owner(&app.auth, &headers));
    Ok(Json(crate::apply::ask(&app, verb, ask, who).await?))
}

/// An answer as the HTTP door sends it, in `format`'s content type: its bytes, or its pieces (`pieces`), or no body for
/// `Sent` (its answer went to the door in pieces as its plan made them, `query::collect_or_send`, and the door has it).
fn respond(r: Rendered, format: Option<&str>, node: &crate::node_state::NodeState) -> Response {
    let body = match r {
        Rendered::Whole(bytes) => Body::from(bytes),
        Rendered::Pieces(h) => pieces(h, node),
        Rendered::Sent => return StatusCode::NO_CONTENT.into_response(),
    };
    ([("content-type", crate::answer::content_type(format))], body).into_response()
}

/// The body of an answer in pieces (`query::Handed`): its first piece, then the rest as they are written. A channel
/// that closes before the last piece was written fails the body, so a client never takes a cut answer for a whole one.
fn pieces(h: crate::query::Handed, node: &crate::node_state::NodeState) -> Body {
    let crate::query::Handed { first, rest, ended, stop } = h;
    // (counted until the body's last piece is read, not until its writer ends: the pieces still in the channel are what
    // a node stopping after the writer would cut short, so a node told to stop waits for them: `Drain::carried`)
    let busy = node.drain.carried();
    let head = futures::stream::once(async move { Ok::<Bytes, std::io::Error>(Bytes::from(first)) });
    let rest = futures::stream::unfold(Some((rest, ended, busy)), |state| async move {
        let (mut rest, ended, busy) = state?;
        match rest.recv().await {
            Some(piece) => Some((piece, Some((rest, ended, busy)))),
            None if ended.load(std::sync::atomic::Ordering::Acquire) => None, // (the count goes with the state here)
            None => Some((Err(std::io::Error::other("the answer stopped before its end")), None)),
        }
    });
    // (a client that goes mid-answer stops the plan's search still running: the statement's flag, if it has one)
    match stop {
        Some(flag) => Body::from_stream(crate::audit::guarded(flag, head.chain(rest))),
        None => Body::from_stream(head.chain(rest)),
    }
}

/// `GET /sql/pages/{id}?from=10000&rows=10000`: more rows of an answer the console was sent
/// (`pages.rs`), as `typed` has them; with `&format=csv|tsv|ndjson|parquet|xlsx`, every row of it as
/// a file to download (`render`), not run again. 410 once it is no longer kept (run it again).
async fn page(State(app): State<App>, Path(id): Path<String>, Query(q): Query<HashMap<String, String>>) -> Response {
    let num = |k: &str, or: usize| q.get(k).and_then(|v| v.parse().ok()).unwrap_or(or);
    let (from, rows, format) = (num("from", 0), num("rows", crate::answer::SHOWN).clamp(1, crate::answer::MOST), q.get("format").map(String::as_str).filter(|f| *f != "typed"));
    let got = match format {
        Some(f) => crate::pages::page(&app.lake, &id, 0, usize::MAX).map(|b| crate::answer::render(&b, Some(f), &|rows| crate::pages::keep(&app.lake, rows))),
        None => crate::pages::page(&app.lake, &id, from, rows).map(|b| crate::answer::typed(&b, rows, &|rows| crate::pages::keep(&app.lake, rows)).map(bytes::Bytes::from)),
    };
    match got {
        Some(Ok(body)) => ([("content-type", crate::answer::content_type(format))], body).into_response(),
        Some(Err(e)) => E(e).into_response(),
        None => (StatusCode::GONE, "this answer's rows are no longer kept here: run it again").into_response(),
    }
}

#[derive(Deserialize)]
struct WatchParams {
    after: Option<u64>, // default: from now on (earlier: a replay, as far back as the log is kept)
    #[serde(default)]
    marks: bool, // after each batch of rows, a `{"_after": N}` line: resume with `?after=N`
    #[serde(default)]
    changes: bool, // the change feed (`change::feed`): old versions too, and what each row is
}

/// New rows of a table as NDJSON, pushed the moment they commit (a view's rows included); with
/// `?changes=true`, its changes: UPDATE's and DELETE's too, with the rows' system columns.
async fn watch(State(app): State<App>, Path(name): Path<String>, Query(p): Query<WatchParams>, who: Option<axum::Extension<crate::auth::Principal>>, headers: HeaderMap) -> Response {
    if let Err(e) = crate::rights::may_here(crate::rights::Action::SelectAll, crate::rights::Object::Table(&name)) {
        return (StatusCode::FORBIDDEN, crate::ext::said(&e)).into_response(); // (a user's: the whole rows)
    }
    // (the rows come after this request's scope has ended: its sign-in is checked again, and the table's whole rows with it, so
    // a REVOKE, a DROP USER or a session's end stops them within a second: 11-02 audit H3, 11-08 M7)
    let (operator, header) = caller(&headers);
    let table = name.clone();
    let revoked = until_revoked(app.clone(), who.map(|w| w.0), operator, header, move |p| {
        crate::rights::may(p, crate::rights::Action::SelectAll, crate::rights::Object::Table(&table)).is_ok()
    });
    let hwm = app.lake.hwm.subscribe();
    let after = p.after.unwrap_or_else(|| app.lake.visible());
    let (marks, changes) = (p.marks, p.changes);
    let node = app.lake.node.clone(); // (its door closing ends the watch: `Drain::endless`)
    let rows = futures::stream::unfold((app, hwm, after, name), move |(app, mut hwm, after, name)| async move {
        loop {
            hwm.borrow_and_update();
            let now = app.lake.visible();
            if now > after {
                let mark = |mut b: Vec<u8>| {
                    if marks && !b.is_empty() {
                        b.extend(format!("{{\"_after\":{now}}}\n").into_bytes());
                    }
                    b
                };
                let rows = match changes {
                    true => crate::change::feed(&app.lake, &name, after, Some(now)).await,
                    false => tail(&app.lake, &name, after, Some(now), false).await,
                };
                let rows = match rows {
                    Ok(b) => sql_ordered(&app.lake, &name, b).await, // (a table that moved a column: SQL's order)
                    e => e,
                };
                let chunk = rows.and_then(|b| ndjson(&b)).map(mark);
                return Some((chunk.map_err(|e| std::io::Error::other(e.to_string())), (app, hwm, now, name)));
            }
            hwm.changed().await.ok()?;
        }
    });
    Body::from_stream(node.drain.endless(rows.filter(|c| std::future::ready(!matches!(c, Ok(b) if b.is_empty()))).take_until(revoked))).into_response() // (never ends by itself: `Drain::endless`)
}

/// A table's rows (`tail`'s, the change feed's) with its columns in SQL's order (`TableMeta::sql_order`), the system
/// columns where they are; as they are for a table that never moved a column.
async fn sql_ordered(lake: &crate::store::Lake, table: &str, batches: Vec<RecordBatch>) -> anyhow::Result<Vec<RecordBatch>> {
    let Some(meta) = lake.table_meta_if(table).await?.filter(|m| m.moved()) else { return Ok(batches) };
    batches.iter().map(|b| meta.in_sql_order(b)).collect()
}

fn ndjson(batches: &[RecordBatch]) -> anyhow::Result<Vec<u8>> { crate::answer::json_lines(batches) }

async fn tier_now(State(app): State<App>) -> Result<Json<Value>, E> {
    Ok(Json(j!({"rows_tiered": app.tier_all(0).await?})))
}

/// `GET /ready`: 200 once this node answers as it should (it holds what its leader had: invariant
/// 197), 503 while it catches up or drains (`drain.rs`): a load balancer's readiness probe.
async fn ready(State(app): State<App>) -> Response {
    match (app.lake.node.drain.draining(), *app.lake.caught.borrow()) {
        (true, _) => (StatusCode::SERVICE_UNAVAILABLE, "stopping").into_response(),
        (_, false) => (StatusCode::SERVICE_UNAVAILABLE, "catching up with the leader").into_response(),
        _ if app.members.cut_off() => (StatusCode::SERVICE_UNAVAILABLE, "can't reach the bucket: another node leads").into_response(),
        _ => "ready".into_response(),
    }
}

async fn stats(State(app): State<App>, who: Option<axum::Extension<crate::auth::Principal>>, headers: axum::http::HeaderMap) -> Json<Value> {
    let members = &app.members;
    let role = if members.reader() { "reader" } else if app.seq.is_some() { "leader" } else { "follower" };
    let mut s = j!({"lake": crate::ddl::lake_name(&app.lake), "role": role, "leader": members.leader().addr, "term": members.leader().n, "nodes": members.nodes(),
                    "hwm": *app.lake.hwm.borrow(), "shard_runs": app.lake.state::<crate::tasks::Kept>().map_or(0, |k| k.shard_runs.load(std::sync::atomic::Ordering::Relaxed)), "python_workers": crate::python::workers(),
                    "live_queries": crate::live::OPEN.load(std::sync::atomic::Ordering::Relaxed)});
    s["version"] = j!(crate::format::VERSION);
    s["owner"] = j!(crate::statement::owner(&app.auth, &headers)); // (asked with its key: is this the node the asker started? `local()` on a port another node holds)
    s["format"] = j!(crate::format::of(&app.lake.cat).await.map(|f| f.format).unwrap_or_default()); // (the lake's: ADR-039)
    s["keys"] = j!(crate::users::kept(&app.lake).await); // (the lake's own keys: sealed by its master key, or in the clear)
    // Background jobs that failed, by kind: a rising count is a stall (tiering, publishing, …) that
    // otherwise shows only on this node's stderr.
    s["background_failures"] = j!(app.lake.node.metrics.failures().into_iter().filter(|(_, n)| *n > 0).collect::<std::collections::BTreeMap<_, _>>());
    s["signed"] = j!(crate::users::signed(&app.lake).await); // (served with tokens once: a node without them answers no one unsigned)
    s["move_failed"] = j!(app.lake.node.move_failed.get()); // (a move that failed: the lake stays in memory, and why)
    if let (true, Some(node)) = (members.is_leader(), crate::cluster::of(&app)) {
        s["releases"] = j!(node.releases()); // (each live node's: a rolling upgrade's progress)
    }
    s["row_locks"] = j!(crate::locks::count(&app.lake.url)); // (ADR-067 §3: the row locks this lake's leader holds now)
    if let Some(seq) = &app.seq {
        s["untiered_rows"] = j!(app.lake.backlog.load(std::sync::atomic::Ordering::Relaxed));
        let mut ms = seq.commit_ms.lock().unwrap().clone();
        ms.sort_by(f64::total_cmp);
        let pct = |p: f64| ms.get(((ms.len() as f64 * p) as usize).min(ms.len().saturating_sub(1))).copied();
        s["commits"] = j!(ms.len());
        s["commit_ms_p50"] = j!(pct(0.5));
        s["commit_ms_p95"] = j!(pct(0.95));
    }
    if who.is_none_or(|w| w.0.role == crate::auth::Role::None) {
        // (the cluster's names and addresses are a signed-in caller's; the rest is a probe's)
        if let Some(o) = s.as_object_mut() {
            for k in ["lake", "leader", "nodes", "releases"] {
                o.remove(k);
            }
        }
    }
    Json(s)
}

/// `anyhow::ensure!` for handlers.
macro_rules! ensure {
    ($cond:expr, $($msg:tt)*) => {
        if !$cond {
            return Err(E(anyhow::anyhow!($($msg)*)));
        }
    };
}
use ensure;

/// Any error becomes a response whose HTTP status follows its SQLSTATE: a client's bad request is a
/// 4xx, a transient condition 503, a real fault 500 (`codes::http`). The message is the body and
/// `x-pondra-sqlstate` the exact code (ADR-036 §4).
pub struct E(anyhow::Error);
impl<T: Into<anyhow::Error>> From<T> for E {
    fn from(e: T) -> Self { E(e.into()) }
}
impl E {
    /// The words its caller is told (`codes::told`), so the audit log says what the client was told.
    fn told(&self, scope: &crate::rights::Scope) -> String { crate::codes::told(&self.0, scope.shows_code()) }
}
impl std::fmt::Display for E {
    // (as told a client not shown code: `ext::said`)
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { write!(f, "{}", crate::ext::said(&self.0)) }
}
impl IntoResponse for E {
    fn into_response(self) -> Response {
        error_response(None, &self.0)
    }
}

/// An error as a response: its words are the body, its SQLSTATE the `x-pondra-sqlstate` header, and its status the one
/// its code stands for, unless the caller gives its own (a database server answers its own errors this way too).
/// The words are told as to a caller not shown code (`codes::unquoted`), and the full ones ride along as `Told` for
/// `guard`, which knows who asked: an answer made away from a request is never told more than that.
pub fn error_response(status: Option<StatusCode>, e: &anyhow::Error) -> Response {
    let (mapped, code, full) = crate::codes::answer(e, true); // (Postgres's SQLSTATE, for any client: ADR-036 §4)
    let told = crate::codes::unquoted(&full);
    let status = status.unwrap_or_else(|| StatusCode::from_u16(mapped).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR));
    let mut r = (status, [("x-pondra-sqlstate", code)], told.clone()).into_response();
    if full != told {
        r.extensions_mut().insert(Told(full));
    }
    r
}

/// An error's full words, kept on its response until `guard` knows whether its caller is shown them (`told_to`).
#[derive(Clone)]
struct Told(String);

/// `r` as its caller is told it: an error's full words to a caller shown code, the words without its quoted lines to
/// any other (`error_response`).
fn told_to(shows_code: bool, mut r: Response) -> Response {
    match (shows_code, r.extensions_mut().remove::<Told>()) {
        (true, Some(Told(full))) => {
            let (mut parts, _) = r.into_parts();
            parts.headers.remove(axum::http::header::CONTENT_LENGTH);
            Response::from_parts(parts, axum::body::Body::from(full))
        }
        _ => r,
    }
}

#[cfg(test)]
mod tests {
    async fn said(r: axum::response::Response) -> String { String::from_utf8(axum::body::to_bytes(r.into_body(), usize::MAX).await.unwrap().to_vec()).unwrap() }

    /// An error's quoted lines (a procedure's traceback) are told only to a caller shown code, and cut for any other: the
    /// guard decides by the principal it signed in (`told_to`), and an answer made away from it is told as to no one.
    #[tokio::test]
    async fn an_errors_full_words_are_told_only_to_a_caller_shown_code() {
        let e = anyhow::anyhow!("ValueError: bad row\n  line 3: rate = 42");
        assert_eq!(said(super::error_response(None, &e)).await, "ValueError: bad row\n  line 3");
        assert_eq!(said(super::told_to(false, super::error_response(None, &e))).await, "ValueError: bad row\n  line 3");
        let full = super::told_to(true, super::error_response(None, &e));
        assert_eq!(full.headers()["x-pondra-sqlstate"], crate::codes::of(&e));
        assert_eq!(said(full).await, "ValueError: bad row\n  line 3: rate = 42");
        // (no quoted line: the same words either way, and nothing kept beside them)
        let plain = super::error_response(None, &anyhow::anyhow!("no table t"));
        assert!(plain.extensions().get::<super::Told>().is_none());
        assert_eq!(said(super::told_to(true, plain)).await, "no table t");
    }
}
