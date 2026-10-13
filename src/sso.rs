//! Single sign-on (ADR-074): a lake's identity providers (`CREATE IDENTITY PROVIDER`, catalog `ip/`)
//! and the tokens they sign. Every door already asks `users::who` (a bearer token) or `users::sign_in`
//! (a name and a password); a token shaped like a JWT goes from there to `check`. It is checked where
//! it arrives, by the node it reaches, against its provider's published keys alone: nothing is
//! written and the leader isn't asked (invariant 409). It signs in as its provider's user claim: a
//! user made here, or anyone when the provider says so, with the roles its roles claim names
//! (invariant 410; `users::claimed`).
use crate::store::{json, Lake};
use crate::users::{Words, W};
use anyhow::{bail, ensure, Context, Result};
use aws_lc_rs::signature;
use base64::{engine::general_purpose::URL_SAFE_NO_PAD as B64U, Engine};
use serde::{Deserialize, Serialize};
use serde_json::{json as j, Value};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// The lake's format that knows identity providers: a node from before takes their tokens for nobody's
/// and, on a lake with no other user, the lake for one nobody signs in to (invariant 412).
pub const FORMAT: u32 = 5;

pub fn key(name: &str) -> String { format!("ip/{name}") }

/// An identity provider, as `CREATE IDENTITY PROVIDER` made it.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct Provider {
    pub issuer: String,
    pub client_id: String, // (the console's app at the provider: its ID tokens' audience)
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub audience: Vec<String>, // (other audiences its tokens may carry: drivers', services')
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub secret: Option<String>, // (a secret's name: the client's secret the console's sign-in sends, ADR-074 §5)
    pub user_claim: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub roles_claim: Option<String>,
    #[serde(default)]
    pub any: bool, // (USERS 'any': anyone it vouches for; else only users made here)
    pub scopes: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
}

impl Provider {
    /// The statement that makes it: what `pondra.objects` and `SHOW CREATE` show.
    pub fn sql(&self, name: &str) -> String {
        let q = |s: &str| format!("'{}'", s.replace('\'', "''"));
        let mut o = vec![format!("ISSUER {}", q(&self.issuer)), format!("CLIENT_ID {}", q(&self.client_id))];
        if !self.audience.is_empty() {
            o.push(format!("AUDIENCE ({})", self.audience.iter().map(|a| q(a)).collect::<Vec<_>>().join(", ")));
        }
        o.extend(self.secret.as_deref().map(|s| format!("SECRET {}", q(s))));
        o.push(format!("USER_CLAIM {}", q(&self.user_claim)));
        o.extend(self.roles_claim.as_deref().map(|r| format!("ROLES_CLAIM {}", q(r))));
        o.push(format!("USERS {}", q(if self.any { "any" } else { "listed" })));
        o.push(format!("SCOPES {}", q(&self.scopes)));
        o.extend(self.label.as_deref().map(|l| format!("LABEL {}", q(l))));
        format!("CREATE IDENTITY PROVIDER {name} ({})", o.join(", "))
    }
}

// ---------------------------------------------------------------- statements

/// `CREATE [OR REPLACE] IDENTITY PROVIDER [IF NOT EXISTS] name (…)`, `DROP IDENTITY PROVIDER [IF EXISTS]
/// name`; None for anything else.
pub fn statement(sql: &str) -> Option<crate::write::Stmt> {
    use crate::write::Stmt;
    let w = Words::of(sql)?;
    let first: Vec<String> = w.0.iter().rev().take(5).map(W::word).collect(); // (the words are kept last first)
    let at = match first.first()?.as_str() {
        "create" if first.get(1).is_some_and(|s| s == "or") => 3,
        "create" | "drop" | "alter" => 1,
        _ => return None,
    };
    if first.get(at).map(String::as_str) != Some("identity") || first.get(at + 1).map(String::as_str) != Some("provider") {
        return None;
    }
    Some(match parse(w) {
        Ok(c) => Stmt::Ddl(vec![crate::ddl::Ddl::Users(c)]),
        Err(e) => Stmt::Invalid(format!("{e:#}")),
    })
}

const OPTIONS: &str = "ISSUER, CLIENT_ID, AUDIENCE, SECRET, USER_CLAIM, ROLES_CLAIM, USERS, SCOPES, LABEL";

fn parse(mut w: Words) -> Result<crate::users::Change> {
    use crate::users::Change;
    let verb = w.next().map(|x| x.word()).unwrap_or_default();
    let replace = verb == "create" && w.is("or") && { w.expect("replace")?; true };
    w.expect("identity")?;
    w.expect("provider")?;
    match verb.as_str() {
        "alter" => bail!("ALTER IDENTITY PROVIDER isn't supported: CREATE OR REPLACE IDENTITY PROVIDER name (…) changes one"), // (0A000, by its words)
        "drop" => {
            let if_exists = w.is("if") && { w.expect("exists")?; true };
            let name = named(&w.name()?)?;
            w.done()?;
            Ok(Change::DropProvider { name, if_exists })
        }
        _ => {
            let if_not_exists = w.is("if") && { w.expect("not")?; w.expect("exists")?; true };
            ensure!(!(replace && if_not_exists), "CREATE OR REPLACE … IF NOT EXISTS: one or the other");
            let name = named(&w.name()?)?;
            ensure!(w.sym('('), "CREATE IDENTITY PROVIDER {name} (ISSUER 'https://…', CLIENT_ID '…')");
            let mut given: HashMap<String, Vec<String>> = HashMap::new();
            loop {
                let option = match w.next() {
                    Some(W::Word(o, false)) => o,
                    _ => bail!("expected an option ({OPTIONS}) {}", w.near()),
                };
                w.sym('=');
                let values = match w.sym('(') {
                    true => {
                        let mut all = vec![w.string()?];
                        while w.sym(',') {
                            all.push(w.string()?);
                        }
                        ensure!(w.sym(')'), "expected ) {}", w.near());
                        all
                    }
                    false => match w.next() {
                        Some(W::Str(s)) => vec![s],
                        Some(W::Word(s, _)) => vec![s],
                        _ => bail!("{} takes a quoted value {}", option.to_uppercase(), w.near()),
                    },
                };
                ensure!(given.insert(option.clone(), values).is_none(), "{} given twice", option.to_uppercase());
                if w.sym(')') {
                    break;
                }
                ensure!(w.sym(','), "expected , or ) {}", w.near());
            }
            w.done()?;
            Ok(Change::CreateProvider { name, provider: made(given)?, replace, if_not_exists })
        }
    }
}

/// A provider's name: letters, digits and _, as a secret's.
fn named(name: &str) -> Result<String> {
    ensure!(!name.is_empty() && name.len() <= 63 && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') && !name.starts_with(|c: char| c.is_ascii_digit()),
        "{name}: an identity provider's name is letters, digits and _, at most 63");
    Ok(name.to_string())
}

fn made(mut given: HashMap<String, Vec<String>>) -> Result<Provider> {
    let mut one = |k: &str| -> Result<Option<String>> {
        match given.remove(k) {
            None => Ok(None),
            Some(mut v) if v.len() == 1 => Ok(v.pop()),
            Some(_) => bail!("{} takes one value", k.to_uppercase()),
        }
    };
    let issuer = one("issuer")?.context("an identity provider needs its ISSUER, as its tokens name it: ISSUER 'https://acme.okta.com'")?;
    ensure!(reachable(&issuer) && !issuer.contains(['?', '#']), "ISSUER {issuer}: an https:// address (http:// only on this machine), as the provider's tokens name it");
    let client_id = one("client_id")?.filter(|c| !c.is_empty()).context("an identity provider needs the CLIENT_ID of Pondra's app there")?;
    let secret = one("secret")?.map(|s| named(&s.to_lowercase())).transpose()?;
    let user_claim = one("user_claim")?.unwrap_or_else(|| "email".into());
    let roles_claim = one("roles_claim")?;
    let any = match one("users")?.as_deref().map(str::to_lowercase).as_deref() {
        None | Some("listed") => false,
        Some("any") => true,
        Some(other) => bail!("USERS '{other}': 'listed' (only users made here sign in) or 'any' (anyone the provider vouches for)"),
    };
    let scopes = one("scopes")?.unwrap_or_else(|| "openid email profile".into());
    ensure!(scopes.split_whitespace().any(|s| s == "openid"), "SCOPES '{scopes}': OpenID Connect's sign-in needs openid among them");
    let label = one("label")?;
    for c in [Some(&user_claim), roles_claim.as_ref()].into_iter().flatten() {
        ensure!(!c.is_empty() && c.len() <= 64, "a claim's name is at most 64 characters");
    }
    let audience = given.remove("audience").unwrap_or_default();
    let mut rest: Vec<String> = given.into_keys().map(|k| k.to_uppercase()).collect();
    rest.sort();
    ensure!(rest.is_empty(), "{}: not an identity provider's option ({OPTIONS})", rest.join(", "));
    Ok(Provider { issuer, client_id, audience, secret, user_claim, roles_claim, any, scopes, label })
}

/// May a provider be reached at `url`: https, or http on this machine (tests, a provider beside the node)?
fn reachable(url: &str) -> bool {
    match reqwest::Url::parse(url) {
        Ok(u) if u.scheme() == "https" => u.host().is_some(),
        Ok(u) if u.scheme() == "http" => matches!(u.host_str(), Some("127.0.0.1" | "localhost" | "[::1]")),
        _ => false,
    }
}

// ---------------------------------------------------------------- the leader carries them out

/// Leader: make (or replace) a provider. Its issuer is asked for its keys first, so a mistyped one is
/// refused here, not at everyone's sign-in; with it the lake is marked as signed (invariant 412).
pub async fn create(lake: &Lake, name: String, p: Provider, replace: bool, if_not_exists: bool) -> Result<Value> {
    if lake.cat.get::<Value>(&key(&name)).await?.is_some() {
        ensure!(replace || if_not_exists, "identity provider {name} already exists (CREATE OR REPLACE IDENTITY PROVIDER changes it)");
        if !replace {
            return Ok(j!({"identity_provider": name, "unchanged": true}));
        }
    }
    crate::format::require(lake, FORMAT, "an identity provider").await?;
    if let Some(s) = &p.secret {
        ensure!(lake.cat.get::<Value>(&format!("e/{s}")).await?.is_some(), "no secret {s} (CREATE SECRET {s} (TYPE generic, CLIENT_SECRET '…'))");
    }
    let keys = fetch(&p.issuer).await.with_context(|| format!("identity provider {name}: its issuer {} was asked for its keys", p.issuer))?;
    let open = !crate::users::signed(lake).await && !crate::users::any(lake).await; // (a lake served with tokens is marked: invariant 379)
    let mut puts = vec![(key(&name), json(&p))];
    puts.extend(crate::users::signed_mark(lake).await?);
    lake.cat.commit(puts, &[]).await?;
    forget();
    remember(&p.issuer, Ok(keys.clone()));
    let mut out = j!({"identity_provider": name, "issuer": p.issuer, "keys": keys.len()});
    if open {
        // (told through the door: `write.rs` passes a DDL answer's notice on)
        out["notice"] = format!("the lake took requests from anyone before: now everyone signs in, through {name} or as a user here (`pondra token --dir …` makes an admin's token)").into();
    }
    Ok(out)
}

pub async fn drop(lake: &Lake, name: &str, if_exists: bool) -> Result<Value> {
    if lake.cat.get::<Value>(&key(name)).await?.is_none() {
        ensure!(if_exists, "no identity provider {name}");
        return Ok(j!({"identity_provider": name, "dropped": false}));
    }
    lake.cat.commit(vec![], &[key(name)]).await?;
    forget();
    Ok(j!({"identity_provider": name, "dropped": true}))
}

/// What this node worked out from the providers goes at once (elsewhere, within `users::fresh`'s second).
fn forget() {
    ALL.lock().unwrap().clear();
    crate::users::forget();
}

crate::per_node!(
    /// The lake's providers as of a catalog version (`users::fresh`): every sign-in by token reads them.
    static ALL: Mutex<HashMap<String, (Option<u64>, Instant, Arc<Vec<(String, Provider)>>)>> = Default::default());

/// The lake's providers, by name.
pub async fn all(lake: &Lake) -> Result<Arc<Vec<(String, Provider)>>> {
    let version = lake.cat.version();
    if let Some(c) = ALL.lock().unwrap().get(&lake.url).filter(|c| crate::users::fresh(lake, version, c.0, c.1)) {
        return Ok(c.2.clone());
    }
    let found: Vec<(String, Provider)> = lake.cat.scan::<Provider>("ip/", "ip0").await?.into_iter().map(|(k, p)| (k[3..].to_string(), p)).collect();
    let found = Arc::new(found);
    ALL.lock().unwrap().insert(lake.url.clone(), (version, Instant::now(), found.clone()));
    Ok(found)
}

/// `GET /sso`: each provider's name, label and issuer, for a sign-in page's buttons (nothing secret).
pub async fn listed(lake: &Lake) -> Value {
    let all = all(lake).await.unwrap_or_default();
    j!({"providers": all.iter().map(|(n, p)| j!({"name": n, "label": p.label.clone().unwrap_or_else(|| n.clone()), "issuer": p.issuer})).collect::<Vec<_>>()})
}

/// RFC 9728's metadata for `resource` (this node's MCP): the providers' issuers as its authorization servers.
pub async fn resource(lake: &Lake, resource: &str) -> Option<Value> {
    let all = all(lake).await.ok().filter(|a| !a.is_empty())?;
    let mut issuers: Vec<&str> = all.iter().map(|(_, p)| p.issuer.as_str()).collect();
    issuers.dedup();
    let mut scopes: Vec<&str> = all.iter().flat_map(|(_, p)| p.scopes.split_whitespace()).collect();
    scopes.sort();
    scopes.dedup();
    Some(j!({"resource": resource, "authorization_servers": issuers, "bearer_methods_supported": ["header"], "scopes_supported": scopes, "resource_name": "Pondra"}))
}

/// Does a provider sign in people with no user here (`USERS 'any'`)? The Postgres port then asks anyone
/// for a password, which may be such a token.
pub async fn anyone(lake: &Lake) -> bool { all(lake).await.is_ok_and(|a| a.iter().any(|(_, p)| p.any)) }

// ---------------------------------------------------------------- a token checked

/// The longest token taken: an ID token with many groups is a few KB.
const MAX: usize = 16 << 10;
/// The clocks' leeway for `exp` and `nbf`.
const LEEWAY: f64 = 60.0;
/// The only algorithms taken: never `none`, nor an HMAC (a public key taken for its secret is the
/// classic forgery).
const ALGORITHMS: [&str; 6] = ["RS256", "RS384", "RS512", "PS256", "ES256", "ES384"];

/// Is `s` shaped like a JWT (three base64url parts, the first a JSON object's)? Only those go to the
/// providers: a password or a Pondra token never does.
pub fn shaped(s: &str) -> bool {
    s.len() <= MAX && s.starts_with("eyJ") && s.bytes().filter(|&b| b == b'.').count() == 2 && s.bytes().all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.'))
}

/// Whom a provider's token names.
#[derive(Debug, Clone, PartialEq)]
pub struct Claimed {
    pub user: String,       // (its user claim, lowercased: a user's name here)
    pub roles: Vec<String>, // (its roles claim's values)
    pub provider: String,
    pub any: bool, // (its provider signs in anyone, not only users made here)
    pub expires_ms: u64,
}

/// None: not a token for the providers (not shaped like one, or the lake has none). Some(Err): one
/// refused, saying why.
pub async fn check(lake: &Lake, token: &str) -> Option<Result<Claimed>> {
    if !shaped(token) {
        return None;
    }
    let providers = match all(lake).await {
        Ok(p) if p.is_empty() => return None,
        Ok(p) => p,
        Err(e) => return Some(Err(e)),
    };
    Some(checked(&providers, token, crate::log::now_ms() as f64 / 1000.0).await)
}

/// Why a provider's token signs no one in, for the door to say (None: it isn't one, or it signs someone in).
pub async fn why(lake: &Lake, token: &str) -> Option<String> {
    let e = match check(lake, token).await? {
        Err(e) => e,
        Ok(c) => crate::users::claimed(lake, &c).await.err()?,
    };
    Some(format!("the identity provider's token: {e:#}"))
}

async fn checked(providers: &[(String, Provider)], token: &str, now: f64) -> Result<Claimed> {
    let mut parts = token.splitn(3, '.');
    let (h, c, s) = (parts.next().unwrap_or_default(), parts.next().unwrap_or_default(), parts.next().unwrap_or_default());
    let part = |p: &str, what: &str| -> Result<Value> {
        let v: Value = serde_json::from_slice(&B64U.decode(p).ok().with_context(|| format!("its {what} isn't base64url"))?).ok().with_context(|| format!("its {what} isn't JSON"))?;
        ensure!(v.is_object(), "its {what} isn't a JSON object");
        Ok(v)
    };
    let (header, claims) = (part(h, "header")?, part(c, "claims")?);
    let alg = header["alg"].as_str().unwrap_or_default();
    ensure!(ALGORITHMS.contains(&alg), "it is signed with {alg:?}, which isn't taken (only {})", ALGORITHMS.join(", "));
    ensure!(header.get("crit").is_none(), "its header names extensions it needs understood (crit), which aren't"); // (RFC 7515 §4.1.11)
    let iss = claims["iss"].as_str().context("it names no issuer (iss)")?;
    let signature = B64U.decode(s).ok().context("its signature isn't base64url")?;
    let mut why = None;
    for (name, p) in providers.iter().filter(|(_, p)| p.issuer == iss) {
        if !audience_ok(p, &claims["aud"]) {
            why.get_or_insert_with(|| format!("its audience ({}) is none of identity provider {name}'s (CLIENT_ID, AUDIENCE)", claims["aud"]));
            continue;
        }
        let key = key_for(p, header["kid"].as_str(), alg).await?;
        verify(&key, alg, token[..h.len() + 1 + c.len()].as_bytes(), &signature)?;
        let exp = claims["exp"].as_f64().context("it has no expiry (exp)")?;
        ensure!(exp + LEEWAY > now, "it expired {} s ago", (now - exp).round());
        ensure!(claims["nbf"].as_f64().is_none_or(|nbf| nbf - LEEWAY <= now), "it isn't valid yet (nbf)");
        match claimed(name, p, &claims, exp) {
            Ok(c) => return Ok(c),
            Err(e) => why = Some(format!("{e:#}")), // (another provider of this issuer may name the user by another claim)
        }
    }
    bail!(why.unwrap_or_else(|| format!("no identity provider here has issuer {iss}")))
}

fn audience_ok(p: &Provider, aud: &Value) -> bool {
    let ok = |a: &str| a == p.client_id || p.audience.iter().any(|x| x == a);
    match aud {
        Value::String(a) => ok(a),
        Value::Array(all) => all.iter().filter_map(Value::as_str).any(ok),
        _ => false,
    }
}

/// Who the claims name, by the provider's claims.
fn claimed(name: &str, p: &Provider, c: &Value, exp: f64) -> Result<Claimed> {
    let user = c[&p.user_claim].as_str().map(str::trim).filter(|u| !u.is_empty()).with_context(|| format!("it has no {} claim, which names its user for identity provider {name}", p.user_claim))?;
    if p.user_claim == "email" {
        // (an address the provider hasn't verified names nobody: some providers say so as text)
        let verified = match &c["email_verified"] {
            Value::Bool(b) => *b,
            Value::String(s) => !s.eq_ignore_ascii_case("false"),
            _ => true,
        };
        ensure!(verified, "its email ({user}) isn't verified by identity provider {name}");
    }
    let roles: Vec<String> = match p.roles_claim.as_deref().map(|r| &c[r]) {
        Some(Value::Array(all)) => all.iter().filter_map(Value::as_str).map(String::from).collect(),
        Some(Value::String(s)) => s.split_whitespace().map(String::from).collect(),
        _ => vec![],
    };
    Ok(Claimed { user: user.to_lowercase(), roles: roles.into_iter().take(64).collect(), provider: name.into(), any: p.any, expires_ms: (exp * 1000.0) as u64 })
}

// ---------------------------------------------------------------- the providers' keys

#[derive(Clone, Debug)]
enum Jwk {
    Rsa { kid: Option<String>, n: Vec<u8>, e: Vec<u8> },
    Ec { kid: Option<String>, crv: String, point: Vec<u8> }, // (the point uncompressed: 0x04, x, y)
}

impl Jwk {
    fn kid(&self) -> Option<&str> {
        match self {
            Jwk::Rsa { kid, .. } | Jwk::Ec { kid, .. } => kid.as_deref(),
        }
    }
    fn fits(&self, alg: &str) -> bool {
        match self {
            Jwk::Rsa { .. } => alg.starts_with("RS") || alg.starts_with("PS"),
            Jwk::Ec { crv, .. } => (alg == "ES256" && crv == "P-256") || (alg == "ES384" && crv == "P-384"),
        }
    }
}

/// An issuer's keys: when last fetched, and when last asked (a provider is asked at most once a minute).
struct Keys {
    keys: Vec<Jwk>,
    fetched: Option<Instant>,
    asked: Instant,
}

crate::per_node!(
    /// Each issuer's keys, as last fetched.
    static KEYS: Mutex<HashMap<String, Keys>> = Default::default());
crate::per_node!(
    /// One fetch at a time an issuer: the sign-ins waiting on it read what it found.
    static FETCHING: Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>> = Default::default());

const HOUR: Duration = Duration::from_secs(3_600);
const MINUTE: Duration = Duration::from_secs(60);

/// The key that signed a token: its `kid` (or the issuer's only key of its kind), from the keys
/// fetched last; fetched again when they lack it (at most once a minute: a stream of made-up `kid`s
/// can't make the node hammer the provider), and beside a sign-in once they are an hour old. A provider
/// that can't be reached is signed in with the keys it had.
async fn key_for(p: &Provider, kid: Option<&str>, alg: &str) -> Result<Jwk> {
    let pick = |keys: &[Jwk]| {
        let fitting: Vec<&Jwk> = keys.iter().filter(|k| k.fits(alg)).collect();
        match kid {
            Some(kid) => fitting.into_iter().find(|k| k.kid() == Some(kid)).cloned(),
            None if fitting.len() == 1 => Some(fitting[0].clone()),
            None => None,
        }
    };
    // (the key if any, whether the keys are an hour old or less, and whether the provider may be asked again)
    let look = || KEYS.lock().unwrap().get(&p.issuer).map(|k| (pick(&k.keys), k.fetched.is_some_and(|f| f.elapsed() < HOUR), k.asked.elapsed() >= MINUTE));
    if let Some((Some(key), fresh, may)) = look() {
        if !fresh && may {
            // (an hour old: asked again beside this sign-in, which goes on with the key it has, so a key the
            // provider took away stops signing in within the hour, and no sign-in waits on the provider)
            if let Some(k) = KEYS.lock().unwrap().get_mut(&p.issuer) {
                k.asked = Instant::now();
            }
            let issuer = p.issuer.clone();
            crate::panics::task(async move { remember(&issuer, fetch(&issuer).await) });
        }
        return Ok(key);
    }
    let gate = FETCHING.lock().unwrap().entry(p.issuer.clone()).or_default().clone();
    let _one = gate.lock().await;
    match look() {
        Some((Some(key), _, _)) => return Ok(key), // (fetched while this one waited)
        Some((None, _, false)) => bail!("{} has no key {} (its keys were fetched again less than a minute ago)", p.issuer, kid.unwrap_or("of its kind")),
        _ => {}
    }
    remember(&p.issuer, fetch(&p.issuer).await);
    match look() {
        Some((Some(key), _, _)) => Ok(key),
        _ => bail!("{} has no key {} for {alg}", p.issuer, kid.unwrap_or("of its kind")),
    }
}

/// Keep what a fetch found (or, when it failed, the keys there were, asked again in a minute).
fn remember(issuer: &str, found: Result<Vec<Jwk>>) {
    let mut all = KEYS.lock().unwrap();
    let now = Instant::now();
    match found {
        Ok(keys) => {
            all.insert(issuer.to_string(), Keys { keys, fetched: Some(now), asked: now });
        }
        Err(e) => {
            all.entry(issuer.to_string()).or_insert(Keys { keys: vec![], fetched: None, asked: now }).asked = now;
            eprintln!("identity provider {issuer}: its keys weren't fetched: {e:#}");
        }
    }
}

/// An issuer's keys, from its discovery document's `jwks_uri` (OpenID Connect Discovery: the document
/// must name the issuer exactly as its tokens do).
async fn fetch(issuer: &str) -> Result<Vec<Jwk>> {
    let doc = get_json(&format!("{}/.well-known/openid-configuration", issuer.trim_end_matches('/'))).await?;
    ensure!(doc["issuer"].as_str() == Some(issuer), "its discovery document names issuer {}, not {issuer} (its tokens must name the issuer the document does)", doc["issuer"]);
    let uri = doc["jwks_uri"].as_str().context("its discovery document has no jwks_uri")?;
    let keys = jwks(&get_json(uri).await?);
    ensure!(!keys.is_empty(), "{uri} has no RSA or EC key for signatures");
    Ok(keys)
}

async fn get_json(url: &str) -> Result<Value> {
    ensure!(reachable(url), "{url}: a provider is asked over https (http only on this machine)");
    let mut r = crate::ext::web().get(url).timeout(Duration::from_secs(10)).send().await.with_context(|| url.to_string())?;
    ensure!(r.status().is_success(), "{url} answered {}", r.status());
    let mut body = vec![];
    while let Some(piece) = r.chunk().await.with_context(|| url.to_string())? {
        body.extend_from_slice(&piece);
        ensure!(body.len() <= 1 << 20, "{url} answered more than 1 MB");
    }
    serde_json::from_slice(&body).with_context(|| format!("{url} didn't answer JSON"))
}

/// A JWKS's keys for signatures that this node can check.
fn jwks(v: &Value) -> Vec<Jwk> {
    let keys = v["keys"].as_array().map(Vec::as_slice).unwrap_or_default();
    keys.iter().filter(|k| k["use"].as_str().is_none_or(|u| u == "sig")).filter_map(|k| {
        let kid = k["kid"].as_str().map(String::from);
        let bytes = |f: &str| k[f].as_str().and_then(|s| B64U.decode(s.trim_end_matches('=')).ok());
        match k["kty"].as_str()? {
            "RSA" => Some(Jwk::Rsa { kid, n: bytes("n")?, e: bytes("e")? }),
            "EC" => {
                let crv = k["crv"].as_str()?;
                let size = match crv {
                    "P-256" => 32,
                    "P-384" => 48,
                    _ => return None,
                };
                let (x, y) = (bytes("x")?, bytes("y")?);
                (x.len() == size && y.len() == size).then(|| Jwk::Ec { kid, crv: crv.into(), point: [&[4u8][..], &x, &y].concat() })
            }
            _ => None,
        }
    }).collect()
}

fn verify(key: &Jwk, alg: &str, signed: &[u8], sig: &[u8]) -> Result<()> {
    use signature::{UnparsedPublicKey, VerificationAlgorithm};
    let ec = |a: &'static dyn VerificationAlgorithm, point: &[u8]| UnparsedPublicKey::new(a, point).verify(signed, sig).is_ok();
    let ok = match (key, alg) {
        (Jwk::Rsa { n, e, .. }, _) => {
            let params = match alg {
                "RS256" => &signature::RSA_PKCS1_2048_8192_SHA256,
                "RS384" => &signature::RSA_PKCS1_2048_8192_SHA384,
                "RS512" => &signature::RSA_PKCS1_2048_8192_SHA512,
                "PS256" => &signature::RSA_PSS_2048_8192_SHA256,
                _ => bail!("{alg} isn't an RSA key's"),
            };
            signature::RsaPublicKeyComponents { n: n.as_slice(), e: e.as_slice() }.verify(params, signed, sig).is_ok()
        }
        (Jwk::Ec { point, .. }, "ES256") => ec(&signature::ECDSA_P256_SHA256_FIXED, point),
        (Jwk::Ec { point, .. }, "ES384") => ec(&signature::ECDSA_P384_SHA384_FIXED, point),
        _ => false,
    };
    ensure!(ok, "its signature isn't its issuer's");
    Ok(())
}

// ---------------------------------------------------------------- tests

#[cfg(test)]
mod tests {
    use super::*;
    use aws_lc_rs::{rand::SystemRandom, signature::{EcdsaKeyPair, KeyPair, RsaKeyPair, ECDSA_P256_SHA256_FIXED_SIGNING, RSA_PKCS1_SHA256}};

    fn provider(any: bool) -> Provider {
        made([("issuer", "https://idp.example"), ("client_id", "pondra"), ("roles_claim", "groups")].into_iter().map(|(k, v)| (k.to_string(), vec![v.to_string()])).chain([("users".to_string(), vec![if any { "any" } else { "listed" }.to_string()])]).collect()).unwrap()
    }

    fn b64(v: &Value) -> String { B64U.encode(serde_json::to_vec(v).unwrap()) }

    /// A token signed with `sign`, and the JWKS entry of the key that signed it.
    fn token(header: Value, claims: Value, sign: impl Fn(&[u8]) -> Vec<u8>) -> String {
        let signed = format!("{}.{}", b64(&header), b64(&claims));
        let sig = sign(signed.as_bytes());
        format!("{signed}.{}", B64U.encode(sig))
    }

    fn keys_of(issuer: &str, keys: Vec<Jwk>) { remember(issuer, Ok(keys)); }

    #[tokio::test]
    async fn checked_by_rsa_and_ec_keys() {
        let rng = SystemRandom::new();
        let rsa = RsaKeyPair::generate(aws_lc_rs::rsa::KeySize::Rsa2048).unwrap();
        let ec_doc = EcdsaKeyPair::generate_pkcs8(&ECDSA_P256_SHA256_FIXED_SIGNING, &rng).unwrap();
        let ec = EcdsaKeyPair::from_pkcs8(&ECDSA_P256_SHA256_FIXED_SIGNING, ec_doc.as_ref()).unwrap();
        let public = aws_lc_rs::signature::RsaPublicKeyComponents::<Vec<u8>>::from(rsa.public_key()); // (its n and e, as a JWKS gives them)
        keys_of("https://idp.example", vec![
            Jwk::Rsa { kid: Some("r1".into()), n: public.n.clone(), e: public.e.clone() },
            Jwk::Ec { kid: Some("e1".into()), crv: "P-256".into(), point: ec.public_key().as_ref().to_vec() },
        ]);
        let p = vec![("okta".to_string(), provider(false))];
        let now = 1_000_000.0;
        let claims = j!({"iss": "https://idp.example", "aud": "pondra", "exp": now + 300.0, "email": "Ana@Acme.com", "groups": ["analysts", "x"]});
        let rs = |m: &[u8]| { let mut s = vec![0; rsa.public_modulus_len()]; rsa.sign(&RSA_PKCS1_SHA256, &rng, m, &mut s).unwrap(); s };
        let t = token(j!({"alg": "RS256", "kid": "r1"}), claims.clone(), rs);
        let c = checked(&p, &t, now).await.unwrap();
        assert_eq!((c.user.as_str(), c.roles.clone(), c.provider.as_str(), c.any), ("ana@acme.com", vec!["analysts".to_string(), "x".into()], "okta", false));
        let es = |m: &[u8]| ec.sign(&rng, m).unwrap().as_ref().to_vec();
        assert!(checked(&p, &token(j!({"alg": "ES256", "kid": "e1"}), claims.clone(), es), now).await.is_ok());
        // refused: expired, another audience, another issuer, not verified, a changed claim, none, HS256
        let p = &p;
        let err = |t: String| async move { format!("{:#}", checked(p, &t, now).await.unwrap_err()) };
        let with = |k: &str, v: Value| { let mut c = claims.clone(); c[k] = v; c };
        assert!(err(token(j!({"alg": "RS256", "kid": "r1"}), with("exp", j!(now - 61.0)), rs)).await.contains("expired"));
        assert!(checked(&p, &token(j!({"alg": "RS256", "kid": "r1"}), with("exp", j!(now - 30.0)), rs), now).await.is_ok()); // (within the leeway)
        assert!(err(token(j!({"alg": "RS256", "kid": "r1"}), with("aud", j!(["other"])), rs)).await.contains("audience"));
        assert!(err(token(j!({"alg": "RS256", "kid": "r1"}), with("iss", j!("https://evil.example")), rs)).await.contains("no identity provider"));
        assert!(err(token(j!({"alg": "RS256", "kid": "r1"}), with("email_verified", j!(false)), rs)).await.contains("verified"));
        assert!(err(token(j!({"alg": "RS256", "kid": "r1"}), with("nbf", j!(now + 120.0)), rs)).await.contains("valid yet"));
        let good = token(j!({"alg": "RS256", "kid": "r1"}), claims.clone(), rs);
        let (head, rest) = good.split_once('.').unwrap();
        let sig = rest.split_once('.').unwrap().1;
        let forged = format!("{head}.{}.{sig}", b64(&with("email", j!("boss@acme.com"))));
        assert!(err(forged).await.contains("signature"));
        assert!(err(format!("{}.{}.", b64(&j!({"alg": "none"})), b64(&claims))).await.contains("isn't taken"));
        assert!(err(token(j!({"alg": "HS256", "kid": "r1"}), claims.clone(), |_| vec![1; 32])).await.contains("isn't taken"));
        assert!(err(token(j!({"alg": "ES256", "kid": "r1"}), claims.clone(), es)).await.contains("no key")); // (an RSA key's kid for an EC signature)
        assert!(err(token(j!({"alg": "RS256", "kid": "r1", "crit": ["exp"]}), claims.clone(), rs)).await.contains("crit"));
        // two providers of one issuer: the first names its users by a claim this token lacks, the second signs it in
        let upn = Provider { user_claim: "upn".into(), ..provider(false) };
        let both = vec![("azure".to_string(), upn), ("okta".to_string(), provider(false))];
        assert_eq!(checked(&both, &good, now).await.unwrap().provider, "okta");
    }

    #[test]
    fn statements() {
        let p = |s: &str| match statement(s) {
            Some(crate::write::Stmt::Ddl(d)) => match &d[0] {
                crate::ddl::Ddl::Users(c) => serde_json::to_value(c).unwrap(),
                _ => panic!(),
            },
            Some(crate::write::Stmt::Invalid(e)) => j!({"error": e}),
            _ => j!(null),
        };
        let c = p("CREATE IDENTITY PROVIDER okta (ISSUER 'https://acme.okta.com', CLIENT_ID '0oa1', AUDIENCE ('api://pondra', 'b'), ROLES_CLAIM 'groups', USERS 'any')");
        assert_eq!((c["do"].as_str(), c["provider"]["any"].as_bool(), c["provider"]["audience"][1].as_str(), c["provider"]["user_claim"].as_str()), (Some("create_provider"), Some(true), Some("b"), Some("email")));
        let back = Provider::deserialize(&c["provider"]).unwrap();
        assert_eq!(p(&back.sql("okta"))["provider"], c["provider"]); // (its statement makes it again)
        assert_eq!(p("create or replace identity provider g (issuer = 'http://127.0.0.1:9100', client_id = 'x')")["replace"], true);
        assert_eq!(p("DROP IDENTITY PROVIDER IF EXISTS okta")["do"], "drop_provider");
        for (sql, says) in [
            ("CREATE IDENTITY PROVIDER o (CLIENT_ID 'x')", "ISSUER"),
            ("CREATE IDENTITY PROVIDER o (ISSUER 'http://idp.example', CLIENT_ID 'x')", "https"),
            ("CREATE IDENTITY PROVIDER o (ISSUER 'https://i', CLIENT_ID 'x', COLOUR 'red')", "COLOUR"),
            ("CREATE IDENTITY PROVIDER o (ISSUER 'https://i', CLIENT_ID 'x', USERS 'all')", "listed"),
            ("CREATE IDENTITY PROVIDER o (ISSUER 'https://i', CLIENT_ID 'x', SCOPES 'email')", "openid"),
            ("ALTER IDENTITY PROVIDER o SET (LABEL 'x')", "CREATE OR REPLACE"),
        ] {
            assert!(p(sql)["error"].as_str().unwrap_or_default().contains(says), "{sql}: {}", p(sql));
        }
        assert_eq!(p("CREATE USER ann"), j!(null)); // (users' own)
        assert!(shaped("eyJhbGciOiJSUzI1NiJ9.eyJpc3MiOiJ4In0.c2ln") && !shaped("pt_abc") && !shaped("eyJ.a") && !shaped("a password with spaces"));
    }
}
