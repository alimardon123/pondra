//! HTTP API. Every node ingests (`/append`), answers SQL and streams changes (`/watch`); metadata
//! writes (tables, views, tasks, bulk inserts, tiering) go to the leader (followers forward them).
use crate::app::App;
use crate::log::{decode_flush, Ack, Src};
use crate::query::{latest_sql, raw, session, tail};
use crate::store::*;
use crate::tasks::Task;
use axum::body::{Body, Bytes};
use axum::extract::{Path, Query, Request, State};
use axum::http::{HeaderMap, StatusCode};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post, put};
use axum::{Json, Router};
use datafusion::arrow::ipc::reader::StreamReader;
use datafusion::arrow::{compute::concat_batches, json as arrow_json, record_batch::RecordBatch};
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
        .route("/sessions/{id}", axum::routing::delete(|Path(id): Path<String>| async move { Json(j!({"ended": crate::temp::end(&crate::temp::id(&id))})) }))
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
        .route("/objects", get(|State(app): State<App>| async move { Ok::<_, E>(Json(crate::objects::listed(&app.lake).await?)) }))
        .route("/metrics", get(|State(app): State<App>| async move { crate::metrics::render(&app).await.map_err(E) }))
        .route("/cluster/commit", post(commit))
        .route("/cluster/log", get(feed))
        .route("/cluster/kv", get(kv))
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
        .route("/cluster/kafka", get(|| async { Json(crate::kafka::me()) }))
        .merge(crate::iceberg_rest::rest())
        .merge(crate::sharing::routes()) // (recipients sign in there with their own tokens: `sharing.rs`)
        .layer(axum::extract::DefaultBodyLimit::max(1 << 30)) // batches up to 1 GiB
        .layer(middleware::from_fn_with_state(app.clone(), guard))
        .layer(middleware::from_fn_with_state(Arc::new(crate::origins::ORIGINS.clone()), crate::origins::door)) // (outermost: a preflight is answered before guard, and guard's refusals carry the headers)
        .with_state(app)
}

/// `POST /functions/<name>`: a function this lake has, run by an Arrow Flight server of your own
/// (see `udf.rs`): `{"flight": "http://host:port", "args": ["Binary"], "returns": "Utf8"}`.
/// `DELETE /functions/<name>` takes it away; `GET /functions` lists them.
async fn create_function(State(app): State<App>, Path(name): Path<String>, body: Bytes) -> Result<Json<Value>, E> {
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
async fn put_file(State(app): State<App>, Path(path): Path<String>, headers: HeaderMap, body: Bytes) -> Result<Response, E> {
    let if_match = headers.get("if-match").and_then(|v| v.to_str().ok());
    Ok(filed(crate::files::put(&app.lake, &path, if_match, body).await?))
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
async fn session_python(method: axum::http::Method, Path(id): Path<String>, role: axum::Extension<crate::auth::Role>) -> Result<Json<Value>, E> {
    let id = crate::temp::id(&id); // (as the client's requests name it: its user's)
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
async fn routines_shown(State(app): State<App>) -> Result<Json<Value>, E> {
    let all = crate::routines::listed(&app.lake).await?;
    if crate::auth::shows_code() {
        return Ok(Json(j!(*all)));
    }
    let mut hidden = (*all).clone();
    for r in hidden.values_mut() {
        r.body.clear();
    }
    Ok(Json(j!(hidden)))
}

/// Who asks (see `auth.rs`, `users.rs`): a token, a user's token, session or password; the
/// program that started the node (`owner`); or, when nothing needs a sign-in, anyone, as an admin.
/// Its role must cover the route; the request then runs as it (`auth::WHO`), and handlers see its
/// role too.
async fn guard(State(app): State<App>, mut req: Request, next: Next) -> Response {
    let path = req.uri().path();
    if crate::drain::draining() && !crate::drain::still_taken(path) {
        return (StatusCode::SERVICE_UNAVAILABLE, [("retry-after", "1")], "this node is stopping: ask another").into_response(); // (`drain.rs`)
    }
    if app.members.cut_off() && !crate::drain::still_taken(path) {
        // (a leader that can't reach the bucket can't commit, and its reads may already be stale:
        // another node leads soon. The cluster's own calls and the health checks still go through.)
        return (StatusCode::SERVICE_UNAVAILABLE, [("retry-after", "1")], crate::members::CUT_OFF_SAYS).into_response();
    }
    let header = req.headers().get("authorization").and_then(|v| v.to_str().ok()).map(String::from);
    let from = req.extensions().get::<axum::extract::ConnectInfo<crate::tls::Peer>>().map(|c| c.0.addr);
    let vouched = req.extensions().get::<axum::extract::ConnectInfo<crate::tls::Peer>>().is_some_and(|c| c.0.signed || c.0.own(req.headers())); // (`users::nodes_key`)
    if let Some(axum::extract::ConnectInfo(peer)) = req.extensions().get::<axum::extract::ConnectInfo<crate::tls::Peer>>() {
        if !peer.allowed() {
            return (StatusCode::FORBIDDEN, crate::tls::PLAIN).into_response(); // (`tls.rs`)
        }
        if header.as_deref().is_some_and(|h| h.starts_with("Basic ")) && !peer.may_send_password() {
            return (StatusCode::FORBIDDEN, crate::tls::PASSWORD_PLAIN).into_response(); // (under PONDRA_TLS=optional: a token may cross, a password never)
        }
        if header.as_deref().is_some_and(|h| h.starts_with("Bearer pn_")) && !peer.may_be_node() {
            return (StatusCode::UNAUTHORIZED, "the nodes' key is taken only with a certificate the nodes' authority signed (PONDRA_TLS_CA)").into_response();
        }
        if header.as_deref().is_some_and(|h| h.starts_with("Bearer pn_")) && !app.auth.admin_set() && !crate::users::nodes_key(false, crate::ext::shared_master(), vouched) {
            return (StatusCode::UNAUTHORIZED, "the lake's keys are in the clear in its bucket, so the nodes' key is taken only from this machine, not through a proxy, or with a certificate the nodes' authority signed (PONDRA_TLS_CA): to run nodes on other machines, give every node --admin-token (or PONDRA_SECRET_KEY)").into_response();
        }
    }
    if header.as_deref().is_some_and(|h| h.starts_with("Bearer pb_")) {
        // (a branch's key: it renews or lets go of its own pin, which `ddl` checks, and opens nothing else: ADR-058)
        if req.uri().path() != "/cluster/ddl" {
            return (StatusCode::UNAUTHORIZED, "a branch's key renews its pin, and opens nothing else").into_response();
        }
        let who = crate::auth::Principal::of(crate::auth::Role::None).at("http", from);
        return match crate::panics::door(crate::auth::WHO.scope(who, next.run(req))).await {
            Ok(r) => r,
            Err(m) => E(anyhow::anyhow!(m)).into_response(),
        };
    }
    if req.uri().path().starts_with("/delta-sharing/") {
        // (a recipient's token or a file's signed link, which the door checks: never a user's sign-in)
        return match crate::panics::door(next.run(req)).await {
            Ok(r) => r,
            Err(m) => E(anyhow::anyhow!(m)).into_response(),
        };
    }
    let needed = crate::auth::Auth::needed(req.uri().path(), req.method().as_str());
    let starting = !*app.lake.caught.borrow();
    let token = header.as_deref().and_then(|h| h.strip_prefix("Bearer "));
    let node = token.is_some_and(|t| t.starts_with("pn_") || app.auth.token_role(t) > crate::auth::Role::None); // (the cluster's own calls: never held)
    if starting && needed > crate::auth::Role::None && !node {
        let _ = app.lake.caught_up().await; // (a node that just started signs users in from what its leader had: `open`)
    }
    // (a sign-in with a user's name and password is counted per address and name: past ten failures a minute
    // that pair is refused at once, the right password too; a bearer token isn't counted: `auth::guessing`)
    let client = req.extensions().get::<axum::extract::ConnectInfo<crate::tls::Peer>>().map(|c| c.0.client(req.headers()));
    let basic_by = header.as_deref().filter(|h| h.starts_with("Basic ")).map(|h| (basic_user(Some(h)), client));
    if let Some((name, ip)) = &basic_by {
        if let Some(wait) = crate::auth::guessing(*ip, name) {
            let why = crate::auth::guessing_says(name, wait);
            crate::audit::refused_by(&app, name, "http", from, client, &format!("{} {}", req.method(), req.uri().path()), &why).await;
            tokio::time::sleep(Duration::from_millis(400)).await; // (as a wrong guess does: refused at once, guesses would cost nothing)
            return (StatusCode::TOO_MANY_REQUESTS, [("retry-after", wait.to_string())], why).into_response();
        }
    }
    let signed = match &header {
        Some(h) => crate::users::who_at(&app.lake, &app.auth, Some(h), vouched).await,
        None => None,
    };
    let mut open = false; // (a node with no tokens and no users: anyone who reaches it is its admin and one of its nodes)
    let who = match signed {
        Some(p) => p,
        None if crate::statement::owner(&app.auth, req.headers()) => crate::auth::Principal::token(crate::auth::Role::Admin), // (the shell's, local()'s: this machine's own folder)
        None if starting && needed == crate::auth::Role::None => crate::auth::Principal::of(crate::auth::Role::None), // (a probe while it catches up: answered, never held)
        None if app.open().await => match crate::auth::cross_site(req.headers(), req.method(), from, app.members.me()) {
            Some(why) => return (StatusCode::FORBIDDEN, why).into_response(), // (a browser's page from elsewhere: `auth::cross_site`)
            None => {
                open = true;
                crate::auth::Principal::of(crate::auth::Role::Admin)
            }
        },
        None if header.is_some() => {
            if let Some((name, ip)) = &basic_by {
                crate::auth::guessed(*ip, name);
            }
            // (an identity provider's token says why it was refused: expired, another audience, no such user here)
            let why = match token.map(str::to_string).or_else(|| basic_password(header.as_deref())) {
                Some(t) => crate::sso::why(&app.lake, &t).await,
                None => None,
            };
            let why = why.unwrap_or_else(|| "wrong token, or user name and password".into());
            crate::audit::refused_by(&app, &basic_user(header.as_deref()), "http", from, client, &format!("{} {}", req.method(), req.uri().path()), &why).await;
            tokio::time::sleep(Duration::from_millis(400)).await; // (a guess costs time)
            return challenge(&app, req.uri().path(), req.headers(), (StatusCode::UNAUTHORIZED, why).into_response()).await;
        }
        None => crate::auth::Principal::of(crate::auth::Role::None),
    };
    let who = who.at("http", from);
    // (a branch's pin: a user granted CLONE may ask for it, and `ddl` lets nothing else of theirs through: ADR-058)
    let pin = req.uri().path() == "/cluster/ddl";
    let cloning = pin && who.access.as_ref().is_some_and(|a| a.clones().is_some());
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
        crate::audit::refused_by(&app, &who.name, "http", from, client, &format!("{} {}", req.method(), req.uri().path()), why);
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
    let heavy = queries(req.uri().path());
    let stoppable = stoppable(req.uri().path()); // (a query the node may stop for memory: `oom.rs`)
    // (a stopped statement fails its session's transaction: who asked, and the headers `temp::of` reads the session from)
    let asked = stoppable.then(|| (who.clone(), req.headers().clone()));
    // (a Python procedure of an apply calls back with its lent token: those calls are the apply's, as its statements are)
    let applying = crate::auth::lent_applying(token);
    let run = crate::auth::WHO.scope(who, next.run(req));
    let run = async move {
        match applying {
            true => crate::protect::APPLYING.scope((), run).await,
            false => run.await,
        }
    };
    match if heavy { crate::panics::work(stoppable, run).await } else { crate::panics::door(run).await } {
        Ok(r) => r,
        Err(m) => {
            if let (true, Some((who, headers))) = (m == crate::oom::OOM, asked) {
                crate::statement::failed_for_memory(who, &headers); // (the node stopped the statement: its transaction fails, as an error's does)
            }
            E(anyhow::anyhow!(m)).into_response() // (and the node stays up)
        }
    }
}

/// The queries the node may stop for memory (`oom.rs`): a client's query, which its client asks again
/// or gives up on. Not the cluster's own calls, the bulk writes or a live query, which lives as long as
/// its client.
fn stoppable(path: &str) -> bool {
    matches!(path, "/sql" | "/mcp") || path.starts_with("/sql/pages/")
}

/// The routes whose work is queries' (`panics::work`): planned and run on a runtime of their own,
/// so appends, commits, heartbeats and the commit stream keep theirs.
fn queries(path: &str) -> bool {
    matches!(path, "/sql" | "/mcp" | "/live" | "/tier" | "/cluster/stage" | "/cluster/copy" | "/cluster/insert" | "/cluster/job") || path.starts_with("/sql/pages/") || path.starts_with("/insert/")
}

/// The user name in a Basic header (for the audit log: who tried).
fn basic_user(header: Option<&str>) -> String {
    use base64::Engine;
    let b = header.and_then(|h| h.strip_prefix("Basic ")).and_then(|b| base64::engine::general_purpose::STANDARD.decode(b.trim()).ok());
    b.and_then(|b| String::from_utf8(b).ok()).and_then(|up| up.split_once(':').map(|(u, _)| u.to_string())).unwrap_or_default()
}

/// The password in a Basic header (an identity provider's token sent as one says why it was refused).
fn basic_password(header: Option<&str>) -> Option<String> {
    use base64::Engine;
    let b = header.and_then(|h| h.strip_prefix("Basic ")).and_then(|b| base64::engine::general_purpose::STANDARD.decode(b.trim()).ok())?;
    String::from_utf8(b).ok()?.split_once(':').map(|(_, p)| p.to_string())
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
async fn login(State(app): State<App>, axum::extract::ConnectInfo(peer): axum::extract::ConnectInfo<crate::tls::Peer>, headers: axum::http::HeaderMap, body: Bytes) -> Response {
    let b: Value = serde_json::from_slice(&body).unwrap_or_default(); // (JSON, whatever its content type says)
    let (user, password) = (b["user"].as_str().unwrap_or_default(), b["password"].as_str().unwrap_or_default());
    if !peer.may_send_password() {
        return (StatusCode::FORBIDDEN, crate::tls::PASSWORD_PLAIN).into_response(); // (`tls.rs`)
    }
    let ip = Some(peer.client(&headers));
    if let Some(wait) = crate::auth::guessing(ip, user) {
        let why = crate::auth::guessing_says(user, wait);
        crate::audit::refused_by(&app, user, "http", Some(peer.addr), ip, "sign in", &why).await;
        tokio::time::sleep(Duration::from_millis(400)).await; // (as a wrong guess does)
        return (StatusCode::TOO_MANY_REQUESTS, [("retry-after", wait.to_string())], why).into_response();
    }
    if !crate::users::password_ok(&app.lake, user, password).await {
        crate::auth::guessed(ip, user);
        crate::audit::refused_by(&app, user, "http", crate::auth::current().and_then(|p| p.from), ip, "sign in", "wrong user name or password").await;
        tokio::time::sleep(Duration::from_millis(400)).await; // (a guess costs time)
        return (StatusCode::UNAUTHORIZED, "wrong user name or password").into_response();
    }
    match crate::users::session(&app.lake, user).await {
        Ok((token, until)) => Json(j!({"token": token, "until": until, "user": user})).into_response(),
        Err(e) => E(e).into_response(),
    }
}

/// `GET /whoami`: who this request signs in as, and what it may do.
async fn whoami(State(app): State<App>) -> Json<Value> {
    let p = crate::auth::current().unwrap_or_else(|| crate::auth::Principal::of(crate::auth::Role::None));
    let role = match p.role {
        crate::auth::Role::Admin => "admin",
        crate::auth::Role::Write => "write",
        crate::auth::Role::Read => "read",
        crate::auth::Role::None => "none",
    };
    Json(j!({"user": p.name, "role": role, "superuser": p.role == crate::auth::Role::Admin && p.access.is_none(), "open": app.open().await}))
}

/// Followers forward metadata writes to the leader, unchanged.
async fn to_leader(State(app): State<App>, req: Request, next: Next) -> Response {
    if app.seq.is_some() {
        return next.run(req).await;
    }
    if app.members.reader() {
        return E(anyhow::anyhow!("read-only node")).into_response();
    }
    proxy(&app.members.leader().addr, req).await.unwrap_or_else(|e| E(e).into_response())
}

async fn proxy(addr: &str, req: Request) -> anyhow::Result<Response> {
    let (parts, body) = req.into_parts();
    let url = crate::tls::url(&format!("{addr}{}", parts.uri.path_and_query().map_or("", |p| p.as_str())));
    let mut out = crate::cluster::http().request(parts.method, url).body(axum::body::to_bytes(body, usize::MAX).await?);
    if let Some(ct) = parts.headers.get("content-type") {
        out = out.header("content-type", ct);
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

/// A follower's flush, to be sequenced (leader only).
async fn commit(State(app): State<App>, body: Bytes) -> Result<Json<crate::log::Outcome>, E> {
    let seq = app.seq.as_ref().ok_or_else(|| anyhow::anyhow!("not the leader"))?;
    let flush = decode_flush(body)?;
    crate::auth::flush_from_writer(&flush)?; // (a write token's: rows, and no more)
    Ok(Json(seq.submit(flush).await?))
}

/// This node's share of a distributed query, sent a piece at a time (a big share is on disk).
async fn stage(State(app): State<App>, Json(slice): Json<crate::spmd::Slice>) -> Result<Response, E> {
    let (shape, parts) = crate::spmd::stage(&app.lake, &slice).await?;
    let done = slice.shuffle.is_none().then(|| crate::spill::Gone(slice.id.clone()));
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
async fn bucket(Query(p): Query<BucketParams>) -> Result<Response, E> {
    if p.drop {
        crate::spmd::forget(&p.id);
        return Ok(Body::empty().into_response());
    }
    let buckets = crate::spmd::bucket(&p.id, p.exchange, p.to, p.part)?;
    Ok(Body::from_stream(crate::spmd::reply("", buckets, None)).into_response()) // (a piece at a time: a big bucket is on disk)
}

/// A share of the leader's data work (tiering, merging, compaction): the files written.
async fn job(State(app): State<App>, Json(job): Json<crate::tier::Job>) -> Result<Json<Vec<DataFile>>, E> {
    Ok(Json(crate::tier::run_job(&app.lake, job).await?))
}

/// The commit stream (see `Frame`): who leads, then the recent frames, then every new one. A caller that isn't one of
/// the nodes (`nodes_own`) gets each change's readable keys only (`for_caller`), and a follower relays the leader's
/// (`relay`).
async fn feed(State(app): State<App>, headers: axum::http::HeaderMap) -> Response {
    let operator = nodes_own(&app).await; // (read now: the stream is polled after this request's scope has ended)
    if !operator && !app.members.is_leader() {
        return relay(&app).await;
    }
    let cat = &app.lake.cat;
    let streamed = operator.then(|| crate::format::Streamed::to(crate::format::said(&headers).1)); // (held while the stream is open: `format::raise`; a page's tab doesn't count)
    let start = Frame::Start { term: app.members.leader().n, replicated: cat.replicas > 1 };
    let (recent, rx) = cat.subscribe();
    let live = futures::stream::unfold(rx, |mut rx| async move { rx.recv().await.ok().map(|f| (f, rx)) }); // a lagging follower reconnects
    let frames = futures::stream::iter([start].into_iter().chain(recent)).chain(live).map(move |f| {
        let _ = &streamed;
        Ok::<_, std::io::Error>(for_caller(f, operator).encode())
    });
    Body::from_stream(crate::drain::endless(frames)).into_response() // (never ends by itself: it ends when the node's door closes)
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

/// A follower's `/cluster/log` for a page: the leader's stream, frame by frame, as `for_caller` sees it. Its request is
/// this node's own, as `cluster::mirror` makes it (the node's key), never `to_leader`'s proxy, which buffers the body and
/// sends the caller's.
async fn relay(app: &App) -> Response {
    let url = crate::tls::url(&format!("{}/cluster/log", app.members.leader().addr));
    let Ok(res) = crate::cluster::http().get(url).send().await.and_then(|r| r.error_for_status()) else {
        return (StatusCode::SERVICE_UNAVAILABLE, "the leader's commit stream is not reachable from here").into_response();
    };
    let frames = res.bytes_stream().scan(bytes::BytesMut::new(), |buf, chunk| {
        let mut out: Vec<std::io::Result<bytes::Bytes>> = vec![];
        match chunk {
            Ok(c) => {
                buf.extend_from_slice(&c);
                loop {
                    match Frame::take(buf) {
                        Ok(Some(f)) => out.push(Ok(for_caller(f, false).encode())),
                        Ok(None) => break,
                        Err(e) => {
                            out.push(Err(std::io::Error::other(e))); // (the body ends there: the page reconnects, and sees no gap)
                            break;
                        }
                    }
                }
            }
            Err(e) => out.push(Err(std::io::Error::other(e))),
        }
        futures::future::ready(Some(futures::stream::iter(out)))
    }).flatten();
    Body::from_stream(crate::drain::endless(frames)).into_response()
}

#[derive(Deserialize, Default)]
#[serde(default)]
struct KvParams {
    key: Option<String>,
    from: Option<String>,
    to: Option<String>,
}

/// `GET /cluster/kv`: what a page reads of this node's catalog, as base64url of the bytes (no padding). `?key=K` answers
/// `{"value": …}` (null when there is none), `?from=F[&to=T]` answers `{"entries": [[key, value], …]}` for the range
/// [F, T): no `to` is to the end. A caller that isn't one of the nodes (`nodes_own`) is answered only for the keys
/// `objects::readable` allows.
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
    let entries: Vec<(String, String)> = cat.scan_raw(&from, &to).await.map_err(failed)?.into_iter().filter(|(k, _)| may(k.as_str())).map(|(k, v)| (k, B64.encode(v))).collect();
    Ok(Json(j!({"entries": entries})))
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
async fn append(State(app): State<App>, Path(name): Path<String>, Query(p): Query<AppendParams>, headers: HeaderMap, body: Bytes) -> Result<Json<Ack>, E> {
    let start = std::time::Instant::now();
    let out = append_rows(&app, &name, p, headers, body).await;
    crate::audit::rows(&app, "/append", &name, out.as_ref().err().map(|e| e.to_string()), start.elapsed()); // (the audit log's line for the rows: S23)
    out
}

async fn append_rows(app: &App, name: &str, p: AppendParams, headers: HeaderMap, body: Bytes) -> Result<Json<Ack>, E> {
    let log = app.log()?;
    if !p.producer.is_empty() && p.seq == 0 {
        return Err(E(anyhow::anyhow!(crate::log::SEQ_FROM_1))); // (0 is "nothing yet": its batch would be taken for a retry)
    }
    crate::auth::check_rows(&name)?; // (its INSERT grant, as SQL's)
    let meta = app.lake.table_meta(name).await?;
    let rules = meta.rules()?;
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
        None if rules.fills() => crate::rules::LeftOut::PerRow(crate::rules::absent_keys(&meta, &body)?), // (a JSON row without the key)
        None => crate::rules::LeftOut::None,
    };
    let rows = crate::rules::shape(&rules, name, crate::rules::Rows::Named(batch, left_out), crate::rules::How::Load, &crate::defaults::Node(&app.lake)).await?;
    crate::auth::check_load(&meta, name, rows.rows())?; // (a keyed table's rows replace their keys': UPDATE too; marked _deleted, DELETE: S11)
    Ok(Json(log.append_shaped(name.to_string(), Src { producer: p.producer, seq: p.seq, prev: p.prev }, rows).await?))
}

#[derive(Deserialize)]
struct InsertParams {
    job: String,
}

/// Bulk `INSERT INTO name <body SQL>`: this node runs the query and writes the Parquet (no log);
/// the leader records the files, together with `job` so a retried job is applied once. Creates
/// the table if missing.
async fn insert(State(app): State<App>, Path(name): Path<String>, Query(p): Query<InsertParams>, query: String) -> Result<Json<Value>, E> {
    let start = std::time::Instant::now();
    let out = insert_rows(&app, &name, p, query).await;
    crate::audit::rows(&app, "/insert", &name, out.as_ref().err().map(|e| e.to_string()), start.elapsed()); // (the audit log's line for the rows: S23)
    out
}

async fn insert_rows(app: &App, name: &str, p: InsertParams, query: String) -> Result<Json<Value>, E> {
    ensure!(!app.members.reader(), "read-only node");
    if app.lake.table_meta_if(name).await?.is_none() {
        crate::auth::at_least(crate::auth::Role::Admin, "making a table")?; // (as CREATE TABLE is)
    }
    crate::auth::check_rows(&name)?; // (its INSERT grant, as SQL's)
    let query = crate::routines::expand(&app.lake, &query).await?;
    let ctx = session(&app.lake, &query, "").await?;
    match crate::write::write_files(&app.lake, &ctx, &name, &query, &p.job, crate::write::stamp(&app.lake, &name, Some(&app.to())).await?).await? {
        Some(f) => Ok(Json(app.record_files(f).await?)),
        None => Ok(Json(j!({"duplicate": true}))),
    }
}

/// Record an INSERT's files (from another node, or a `pondra sql` somewhere else).
async fn files(State(app): State<App>, Json(f): Json<crate::write::Files>) -> Result<Json<Value>, E> {
    let meta = app.lake.cat.meta(&table_key(f.table())).await?;
    crate::auth::files_from_writer(meta.as_ref().map(|m| m.folder(f.table())), &f.paths())?; // (a write token's: into its table's folder)
    Ok(Json(app.record_files(f).await?))
}

/// Body: `{"source": "events", "target": "per_user", "sql": "SELECT … FROM events …"}`.
async fn create_task(State(app): State<App>, Path(name): Path<String>, body: String) -> Result<Json<Value>, E> {
    let mut task: Task = serde_json::from_str(&body)?;
    task.sql = crate::routines::expand(&app.lake, &task.sql).await?;
    crate::tasks::create(&app.lake, &name, &task).await?;
    Ok(Json(j!({"task": name})))
}

/// `POST /views/{name}` with the view's SQL, e.g. `SELECT user, sum(amount) AS total, count(*) AS n
/// FROM events GROUP BY user`; `?window=w&size_secs=60&lateness_secs=10` also emits each
/// window of column `w` once, final, to `{name}_final`; `?session=ts&gap_secs=30&lateness_secs=5`
/// makes it a session view (`views.rs`): `CREATE MATERIALIZED VIEW … WITH (…)` in SQL.
async fn create_view(State(app): State<App>, Path(name): Path<String>, Query(options): Query<std::collections::BTreeMap<String, String>>, sql: String) -> Result<Json<Value>, E> {
    let sql = crate::routines::expand(&app.lake, &sql).await?; // (as they are now: ADR-023)
    let d = crate::ddl::Ddl::CreateMaterialized { name, sql, options };
    let out = {
        let _guard = app.lock.lock().await;
        crate::ddl::apply(&app.lake, d.clone()).await?
    };
    crate::ddl::settle(&app.lake, &d).await?; // (once it's filled from the rows already there)
    Ok(Json(out))
}

/// An UPDATE, DELETE or MERGE a follower's SQL asked for (`change.rs`): `[sql, job]`.
async fn change(State(app): State<App>, Json((sql, job, sent)): Json<(String, String, crate::change::Sent)>) -> Result<Json<Value>, E> {
    let seq = app.seq.as_ref().ok_or_else(|| anyhow::anyhow!("not the leader"))?;
    let _guard = app.lock.lock().await;
    let sent = Arc::new(crate::change::unpack(&sent)?); // (what it reads that this node can't: `change::for_leader`)
    Ok(Json(crate::query::SENT.scope(sent, crate::change::run(&app.lake, seq, &sql, &job)).await?))
}

/// Whether a request is the cluster's own (the nodes' key, an operator's token, or none on an open
/// lake), which the door it first came in by checked. A user's sign-in, Basic included, is not, so
/// the routes the cluster forwards to check a protected database's objects for it (`protect.rs`).
fn cluster_call(app: &App, headers: &HeaderMap) -> bool {
    match headers.get("authorization").and_then(|v| v.to_str().ok()) {
        None => true,
        // (a `pn_` key counts only without an admin token, as in `users::who`)
        Some(h) => h.strip_prefix("Bearer ").is_some_and(|b| (b.starts_with("pn_") && !app.auth.admin_set()) || app.auth.token_role(b) > crate::auth::Role::None),
    }
}

/// A `CREATE`/`DROP` of a schema, view or table that a follower's SQL asked for (`ddl.rs`), and a
/// branch's pin (ADR-058). A follower sends it to the leader, asked with the caller's own token.
async fn ddl(State(app): State<App>, headers: HeaderMap, role: Option<axum::Extension<crate::auth::Role>>, Json(mut d): Json<crate::ddl::Ddl>) -> Result<Json<Value>, E> {
    crate::backup::sent(&d)?;
    let bearer = headers.get("authorization").and_then(|v| v.to_str().ok()).and_then(|h| h.strip_prefix("Bearer ")).map(String::from);
    let key = bearer.as_deref().filter(|k| k.starts_with("pb_"));
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
        Some(seq) => crate::write::ddl_here(&app.lake, seq, &app.lock, d.clone()).await?,
        None if !app.members.is_leader() => Box::pin(crate::write::send_as(&app.lake.url, crate::write::Request::Ddl(d.clone()), bearer.as_deref())).await?, // (the leader makes it)
        None => {
            let _guard = app.lock.lock().await;
            crate::ddl::apply(&app.lake, d.clone()).await?
        }
    };
    crate::ddl::settle(&app.lake, &d).await?; // (ATTACH, CREATE DATABASE: the leader too at once, not in a second)
    Ok(Json(out))
}

/// `GET /lookup/{table}/{key}`: the current row of one key, for serving reads — same answer as
/// `SELECT … WHERE key = …`. Upsert tables skip SQL entirely (`serve.rs`); merge tables combine
/// the key's partial rows with a filter + GROUP BY on one thread. Composite keys are
/// comma-separated, in the key's column order.
async fn lookup(State(app): State<App>, Path((name, key)): Path<(String, String)>) -> Result<Response, E> {
    crate::auth::check_all(&name)?; // (a user's: the whole row)
    let lake = &app.lake;
    let meta = lake.table_meta(&name).await?;
    ensure!(!meta.key.is_empty(), "{name} has no key: use /sql");
    if meta.merge.is_empty() && meta.ttl.is_none() && meta.order.is_none() {
        let shown = crate::answer::schema(&meta.columns.iter().filter(|(c, _)| c != "_deleted").cloned().collect::<Vec<_>>())?; // (as `SELECT *` shows it)
        let row = crate::serve::lookup(lake, &name, &meta, &key).await?.map(|r| crate::query::conform(&r, &shown)).transpose()?; // (by name: a row from before an ADD COLUMN lacks it)
        let row = row.map(|r| meta.to_logical(&r)).transpose()?;
        let mut w = arrow_json::ArrayWriter::new(Vec::new());
        w.write_batches(&row.iter().collect::<Vec<_>>())?;
        w.finish()?;
        let body = if row.is_none() { b"[]".to_vec() } else { w.into_inner() };
        return Ok(([("content-type", "application/json")], body).into_response());
    }
    let mut where_ = vec![];
    for (col, val) in meta.key.iter().zip(key.split(',')) {
        let text = meta.columns.iter().any(|(c, t)| c == col && (t == "Utf8" || t == "LargeUtf8"));
        ensure!(!val.contains('\''), "quote in key");
        where_.push(match text {
            true => format!("\"{col}\" = '{val}'"),
            false => format!("\"{col}\" = {val}"),
        });
    }
    let (ctx, where_) = (lake.session_with(1), where_.join(" AND "));
    ctx.register_table("__raw", raw(lake, &ctx, &name, &meta, None).await?.into_view())?;
    let cols = meta.columns.iter().map(|(c, _)| format!("\"{c}\"")).collect::<Vec<_>>().join(", ");
    let sql = match meta.merge.is_empty() {
        // Upsert table: the newest version of the key wins (no window over the whole table).
        true => format!("SELECT * FROM (SELECT {cols} FROM __raw WHERE {where_} ORDER BY {}\"_ord\" DESC LIMIT 1){}", meta.order.as_ref().map(|o| format!("\"{o}\" DESC NULLS LAST, ")).unwrap_or_default(), crate::query::live(&meta)),
        // Merge table: combine that key's partial rows.
        false => format!("{} ", latest_sql(&meta, "__raw", false, false)).replace(" GROUP BY ", &format!(" WHERE {where_} GROUP BY ")),
    };
    let shown = |b: &RecordBatch| b.project(&(0..b.num_columns()).filter(|&i| b.schema().field(i).name() != "_deleted").collect::<Vec<_>>());
    let batches = ctx.sql(&sql).await?.collect().await?.iter().map(|b| meta.to_logical(&shown(b)?)).collect::<anyhow::Result<Vec<_>>>()?;
    let mut w = arrow_json::ArrayWriter::new(Vec::new());
    w.write_batches(&batches.iter().collect::<Vec<_>>())?;
    w.finish()?;
    Ok(([("content-type", "application/json")], w.into_inner()).into_response())
}

/// `POST /sql`: statements (SQL, or JSON with `$name` parameters, or that and tables of the
/// caller's own: `routines::Request`), run in order; the last one's answer. A single query as it
/// was sent may be answered from `Results`.
async fn sql(State(app): State<App>, Query(p): Query<crate::statement::SqlParams>, role: axum::Extension<crate::auth::Role>, headers: axum::http::HeaderMap, body: Bytes) -> Response {
    let format = p.format.clone(); // (an answer handed to the door in pieces is sent in this format)
    let said = crate::statement::sql(app, p, role.0, headers, body).await;
    let mut r = match said.reply {
        Ok(crate::statement::Reply::Rendered(r)) => respond(r, format.as_deref()),
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
    let token = headers.get("authorization").and_then(|v| v.to_str().ok()).and_then(|v| v.strip_prefix("Bearer "));
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
async fn apply(State(app): State<App>, uri: axum::http::Uri, role: axum::Extension<crate::auth::Role>, headers: axum::http::HeaderMap, Json(ask): Json<crate::apply::Ask>) -> Result<Json<Value>, E> {
    use crate::apply::Verb;
    let verb = match uri.path().rsplit('/').next() {
        Some("plan") => Verb::Plan,
        Some("test") => Verb::Test,
        Some("status") => Verb::Status,
        Some("capture") => Verb::Capture,
        _ => Verb::Apply,
    };
    let who = crate::routines::Who { role: role.0, files: crate::statement::owner(&app.auth, &headers), depth: 0 };
    Ok(Json(crate::apply::ask(&app, verb, ask, who).await?))
}

/// An answer as the HTTP door sends it, in `format`'s content type: its bytes, or its pieces (`pieces`), or no body for
/// `Sent` (its answer went to the door in pieces as its plan made them, `query::collect_or_send`, and the door has it).
fn respond(r: Rendered, format: Option<&str>) -> Response {
    let body = match r {
        Rendered::Whole(bytes) => Body::from(bytes),
        Rendered::Pieces(h) => pieces(h),
        Rendered::Sent => return StatusCode::NO_CONTENT.into_response(),
    };
    ([("content-type", crate::answer::content_type(format))], body).into_response()
}

/// The body of an answer in pieces (`query::Handed`): its first piece, then the rest as they are written. A channel
/// that closes before the last piece was written fails the body, so a client never takes a cut answer for a whole one.
fn pieces(h: crate::query::Handed) -> Body {
    let crate::query::Handed { first, rest, ended, stop } = h;
    // (counted until the body's last piece is read, not until its writer ends: the pieces still in the channel are what
    // a node stopping after the writer would cut short, so a node told to stop waits for them: `drain::Busy::carried`)
    let busy = crate::drain::Busy::carried();
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
async fn page(Path(id): Path<String>, Query(q): Query<HashMap<String, String>>) -> Response {
    let num = |k: &str, or: usize| q.get(k).and_then(|v| v.parse().ok()).unwrap_or(or);
    let (from, rows, format) = (num("from", 0), num("rows", crate::answer::SHOWN).clamp(1, crate::answer::MOST), q.get("format").map(String::as_str).filter(|f| *f != "typed"));
    let got = match format {
        Some(f) => crate::pages::page(&id, 0, usize::MAX).map(|b| crate::answer::render(&b, Some(f), crate::pages::keep)),
        None => crate::pages::page(&id, from, rows).map(|b| crate::answer::typed(&b, rows, crate::pages::keep).map(bytes::Bytes::from)),
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
async fn watch(State(app): State<App>, Path(name): Path<String>, Query(p): Query<WatchParams>) -> Response {
    if let Err(e) = crate::auth::check_all(&name) {
        return (StatusCode::FORBIDDEN, crate::ext::said(&e)).into_response(); // (a user's: the whole rows)
    }
    let hwm = app.lake.hwm.subscribe();
    let after = p.after.unwrap_or_else(|| app.lake.visible());
    let (marks, changes) = (p.marks, p.changes);
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
                let chunk = rows.and_then(|b| ndjson(&b)).map(mark);
                return Some((chunk.map_err(|e| std::io::Error::other(e.to_string())), (app, hwm, now, name)));
            }
            hwm.changed().await.ok()?;
        }
    });
    Body::from_stream(crate::drain::endless(rows.filter(|c| std::future::ready(!matches!(c, Ok(b) if b.is_empty()))))).into_response() // (never ends by itself: `drain::endless`)
}

fn ndjson(batches: &[RecordBatch]) -> anyhow::Result<Vec<u8>> {
    let mut w = arrow_json::LineDelimitedWriter::new(Vec::new());
    w.write_batches(&batches.iter().collect::<Vec<_>>())?;
    w.finish()?;
    Ok(w.into_inner())
}

async fn tier_now(State(app): State<App>) -> Result<Json<Value>, E> {
    Ok(Json(j!({"rows_tiered": app.tier_all(0).await?})))
}

/// `GET /ready`: 200 once this node answers as it should (it holds what its leader had: invariant
/// 197), 503 while it catches up or drains (`drain.rs`): a load balancer's readiness probe.
async fn ready(State(app): State<App>) -> Response {
    match (crate::drain::draining(), *app.lake.caught.borrow()) {
        (true, _) => (StatusCode::SERVICE_UNAVAILABLE, "stopping").into_response(),
        (_, false) => (StatusCode::SERVICE_UNAVAILABLE, "catching up with the leader").into_response(),
        _ if app.members.cut_off() => (StatusCode::SERVICE_UNAVAILABLE, "can't reach the bucket: another node leads").into_response(),
        _ => "ready".into_response(),
    }
}

async fn stats(State(app): State<App>, headers: axum::http::HeaderMap) -> Json<Value> {
    let members = &app.members;
    let role = if members.reader() { "reader" } else if app.seq.is_some() { "leader" } else { "follower" };
    let mut s = j!({"lake": crate::ddl::lake_name(&app.lake), "role": role, "leader": members.leader().addr, "term": members.leader().n, "nodes": members.nodes(),
                    "hwm": *app.lake.hwm.borrow(), "shard_runs": crate::tasks::SHARD_RUNS.load(std::sync::atomic::Ordering::Relaxed), "python_workers": crate::python::workers(),
                    "live_queries": crate::live::OPEN.load(std::sync::atomic::Ordering::Relaxed)});
    s["version"] = j!(crate::format::VERSION);
    s["owner"] = j!(crate::statement::owner(&app.auth, &headers)); // (asked with its key: is this the node the asker started? `local()` on a port another node holds)
    s["format"] = j!(crate::format::of(&app.lake.cat).await.map(|f| f.format).unwrap_or_default()); // (the lake's: ADR-039)
    s["keys"] = j!(crate::users::kept(&app.lake).await); // (the lake's own keys: sealed by its master key, or in the clear)
    // Background jobs that failed, by kind: a rising count is a stall (tiering, publishing, …) that
    // otherwise shows only on this node's stderr.
    s["background_failures"] = j!(crate::metrics::failures().into_iter().filter(|(_, n)| *n > 0).collect::<std::collections::BTreeMap<_, _>>());
    s["signed"] = j!(crate::users::signed(&app.lake).await); // (served with tokens once: a node without them answers no one unsigned)
    s["move_failed"] = j!(crate::memory::MOVE_FAILED.lock().unwrap_or_else(std::sync::PoisonError::into_inner).clone()); // (a move that failed: the lake stays in memory, and why)
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
    if crate::auth::current().is_none_or(|p| p.role == crate::auth::Role::None) {
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
impl std::fmt::Display for E {
    // (the words a client is sent, so the audit log says what the client was told: `ext::said`)
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { write!(f, "{}", crate::ext::said(&self.0)) }
}
impl IntoResponse for E {
    fn into_response(self) -> Response {
        error_response(None, &self.0)
    }
}

/// An error as a response: its words are the body, its SQLSTATE the `x-pondra-sqlstate` header, and its status the one
/// its code stands for, unless the caller gives its own (a database server answers its own errors this way too).
pub fn error_response(status: Option<StatusCode>, e: &anyhow::Error) -> Response {
    let (mapped, code, told) = crate::codes::answer(e, crate::auth::shows_code()); // (Postgres's SQLSTATE, for any client: ADR-036 §4; the words its caller may see)
    let status = status.unwrap_or_else(|| StatusCode::from_u16(mapped).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR));
    (status, [("x-pondra-sqlstate", code)], told).into_response()
}
