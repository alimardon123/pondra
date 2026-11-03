//! DuckDB's extension statements (`INSTALL`, `FORCE INSTALL`, `LOAD`) for the extensions Pondra has built in, and Pondra's own
//! extensions (ADR-075 §6): `INSTALL name FROM 'file or URL'`, `UNINSTALL name`, `LOAD name`. A script that loads a built-in one
//! runs unchanged: the statement is answered with a notice saying nothing had to be done. Any other name is refused by name,
//! never answered as if it were done (principle 10). An installed extension's record is kept at `xt/<name>` and its bytes at
//! `ext/<name>/<version>/<sha256>.wasm`; E1 keeps the file and never runs it (its functions come in E2). The node's doors answer
//! the words through `settings.rs`, before anything expands the statement. `INSTALL … FROM` and `UNINSTALL` are writes
//! (`write.rs`): the node that got the statement reads the file, and the leader keeps the record, so every node sees it. A host
//! that only runs SQL through the front (`pondra sql`) asks `row` instead, and answers one row naming what happened.

#[cfg(feature = "lake")]
use crate::ddl::Ddl;
#[cfg(feature = "lake")]
use crate::store::{json, Lake};
#[cfg(feature = "lake")]
use crate::write::{Request, Stmt};
use crate::codes::coded;
#[cfg(feature = "lake")]
use anyhow::Context;
use anyhow::Result;
use datafusion::sql::sqlparser::{dialect::GenericDialect, tokenizer::{Token, Tokenizer}};
#[cfg(feature = "lake")]
use std::sync::Arc;

/// What a statement comes to: its command tag, and the notice the client is told.
pub struct Said {
    pub tag: &'static str,
    pub notice: String,
}

/// DuckDB's extensions Pondra has built in, each with a few words on what in Pondra does its job. A name goes in when a
/// DuckDB script that loads it and uses its main function or a feature of it runs here unchanged (checked by hand, on the
/// binary). Each description says what does not run: a JSON array file, `timezone()`, an http(s) read with no secret.
const BUILT_IN: &[(&str, &str)] = &[
    ("httpfs", "reads s3:// files, and http(s):// files a secret covers"),
    ("parquet", "reads Parquet files"),
    ("json", "reads newline-delimited JSON files"),
    ("icu", "time zones: SET TimeZone and AT TIME ZONE"),
    ("iceberg", "reads Iceberg tables"),
    ("delta", "reads Delta tables"),
    ("aws", "reads s3:// files with the AWS credential chain"),
];

/// DuckDB's repositories: `INSTALL x FROM core` names one of these, and is answered as DuckDB answers it.
const REPOS: &[&str] = &["core", "core_nightly", "community"];

/// An installed extension's record, kept at `xt/<name>` in the catalog (ADR-075 §6): its version, its file's hash and size,
/// where it came from, who installed it and when. Its bytes are at `ext/<name>/<version>/<sha256>.wasm`.
#[derive(serde::Serialize, serde::Deserialize, Clone)]
pub struct Installed {
    pub version: String,
    pub sha256: String,
    pub bytes: u64,
    pub from: String,
    pub by: String,
    pub at_ms: u64,
    /// Its functions, as its `describe()` said when it was installed: a session registers those its query names.
    #[serde(default)]
    pub functions: Vec<Described>,
}

/// One of an extension's functions as its record keeps it: its name, its arguments' and answer's types as Arrow writes them
/// (`Utf8`, `Int64`), and whether it may answer the same arguments differently.
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
pub struct Described {
    pub name: String,
    pub arguments: Vec<String>,
    pub returns: String,
    #[serde(default)]
    pub volatile: bool,
    /// A table function's columns, by name and type (a scalar has none, and its `returns`).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub columns: Vec<(String, String)>,
}

/// What a statement does with a name.
#[derive(Clone, Copy, Debug, PartialEq)]
enum Verb {
    Install,
    Load,
    Uninstall,
}

/// Where an `INSTALL` is from: one of DuckDB's repositories, a bare word that is none of them (refused: a Pondra extension is
/// named by its file), or a string, which is a file on this machine or a URL.
#[derive(Debug, PartialEq)]
enum Origin {
    Repo,
    Word(String),
    Source(String),
}

/// A statement's words: the verb, the name (lower case), what it is from and its version, and `IF EXISTS` for an UNINSTALL.
#[derive(Debug)]
struct Ask {
    verb: Verb,
    name: String,
    from: Option<Origin>,
    version: Option<String>,
    if_exists: bool,
}

/// The answer to `sql` as the words say it, for the statements this file takes: `INSTALL` and `LOAD` of a name, and
/// `UNINSTALL`. A statement none of these is None, and goes on as it did.
fn read_sql(sql: &str) -> Option<Ask> {
    if !matches!(crate::text::head(sql).as_str(), "install" | "force" | "load" | "uninstall") {
        return None;
    }
    let tokens = Tokenizer::new(&GenericDialect {}, sql).tokenize().ok()?;
    read(&tokens)
}

/// The statement the tokens are, or None when they are not one of these: `[FORCE] INSTALL name [FROM repo | 'file or URL']
/// [VERSION 'v']`, `LOAD name`, `UNINSTALL [IF EXISTS] name`. The name is a word, bare or quoted, or a string.
fn read(tokens: &[Token]) -> Option<Ask> {
    let mut words: Vec<&Token> = tokens.iter().filter(|t| !matches!(t, Token::Whitespace(_) | Token::EOF)).collect();
    if matches!(words.last(), Some(Token::SemiColon)) {
        words.pop();
    }
    let at = |i: usize| words.get(i).copied();
    let mut i = 0;
    let forced = keyword(at(0), "force");
    if forced {
        i += 1;
    }
    let verb = match () {
        _ if keyword(at(i), "install") => Verb::Install,
        _ if keyword(at(i), "load") => Verb::Load,
        _ if keyword(at(i), "uninstall") => Verb::Uninstall,
        _ => return None,
    };
    if forced && verb != Verb::Install {
        return None; // (FORCE LOAD and FORCE UNINSTALL: none of DuckDB's statements)
    }
    i += 1;
    let mut if_exists = false;
    if verb == Verb::Uninstall && keyword(at(i), "if") && keyword(at(i + 1), "exists") {
        if_exists = true;
        i += 2;
    }
    let name = name_of(at(i))?.to_lowercase();
    i += 1;
    let (mut from, mut version) = (None, None);
    if verb == Verb::Install {
        if keyword(at(i), "from") {
            from = Some(source(at(i + 1))?);
            i += 2;
        }
        if keyword(at(i), "version") {
            version = Some(name_of(at(i + 1))?);
            i += 2;
        }
    }
    (i == words.len()).then_some(Ask { verb, name, from, version, if_exists })
}

/// Is this token the keyword `word` (and not a quoted identifier of that name)?
fn keyword(t: Option<&Token>, word: &str) -> bool {
    matches!(t, Some(Token::Word(w)) if w.quote_style.is_none() && w.value.eq_ignore_ascii_case(word))
}

/// A name as a token: a word (bare or quoted) or a string.
fn name_of(t: Option<&Token>) -> Option<String> {
    match t? {
        Token::Word(w) => Some(w.value.clone()),
        Token::SingleQuotedString(s) | Token::DoubleQuotedString(s) => Some(s.clone()),
        _ => None,
    }
}

/// What a FROM names: a repository (as a word or a string), a bare word that is none (refused), or a string.
fn source(t: Option<&Token>) -> Option<Origin> {
    let (text, bare) = match t? {
        Token::Word(w) => (w.value.clone(), w.quote_style.is_none()),
        Token::SingleQuotedString(s) | Token::DoubleQuotedString(s) => (s.clone(), false),
        _ => return None,
    };
    Some(match (REPOS.contains(&text.to_lowercase().as_str()), bare) {
        (true, _) => Origin::Repo,
        (false, true) => Origin::Word(text),
        (false, false) => Origin::Source(text),
    })
}

fn built_in(name: &str) -> bool { BUILT_IN.iter().any(|(built, _)| *built == name) }

/// A name an extension may take: lower-case letters, digits, `_` and `-`, so it is a single part of a catalog key and a path.
fn valid(name: &str) -> bool { !name.is_empty() && name.chars().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_' || c == '-') }

/// Is the statement one the node writes (a file read, a record kept or dropped) rather than one it answers from the words?
fn writes(ask: &Ask) -> bool {
    valid(&ask.name)
        && match (ask.verb, &ask.from) {
            (Verb::Uninstall, _) => true,
            (Verb::Install, Some(Origin::Source(_))) => !built_in(&ask.name),
            _ => false,
        }
}

/// The answer to a statement the node gives from the words. `installed` says whether a LOAD's name has a record (the lake's).
fn answer(ask: &Ask, installed: bool) -> Result<Said> {
    let tag = match ask.verb {
        Verb::Install => "INSTALL",
        Verb::Load => "LOAD",
        Verb::Uninstall => "UNINSTALL",
    };
    let name = ask.name.as_str();
    if name.contains('/') || name.ends_with(".duckdb_extension") {
        return Err(coded("0A000", format!("{name}: a DuckDB extension file is DuckDB's own native code, which Pondra doesn't run")));
    }
    if let Some(Origin::Word(word)) = &ask.from {
        return Err(coded("0A000", format!("FROM {word}: the DuckDB repositories are core, core_nightly and community; a Pondra extension is named by its file: INSTALL {name} FROM 'path or URL' (ADR-075)")));
    }
    if !valid(name) {
        return Err(coded("0A000", format!("{name}: an extension's name is letters, digits, _ and -")));
    }
    if built_in(name) {
        if matches!(ask.from, Some(Origin::Source(_))) {
            return Err(coded("0A000", format!("{name} is built into Pondra: an extension can't take a built-in's name")));
        }
        let step = if ask.verb == Verb::Install { "install" } else { "load" };
        return Ok(Said { tag, notice: format!("{name} is built into Pondra: nothing to {step}") });
    }
    if ask.verb == Verb::Load && installed {
        return Ok(Said { tag, notice: format!("{name} is installed: its functions are on in every session") });
    }
    let built: Vec<&str> = BUILT_IN.iter().map(|(n, _)| *n).collect();
    Err(coded("0A000", format!("{name} isn't built into Pondra: install a Pondra extension from its file, INSTALL {name} FROM 'path or URL' (ADR-075); the DuckDB extensions built in are {}", built.join(", "))))
}

/// Is `sql` a statement the node answers from the words (and its lake's records, for a LOAD)? `settings.rs` routes these to
/// `statement`; the writes (`write`) and every other statement go on as they did.
pub fn is(sql: &str) -> bool { read_sql(sql).is_some_and(|ask| !writes(&ask)) }

/// The answer to `sql` if `is` says the node answers it: a LOAD of an installed name is a notice, and anything else `answer`'s.
#[cfg(feature = "lake")]
pub async fn statement(lake: &Lake, sql: &str) -> Option<Result<Said>> {
    let ask = read_sql(sql).filter(|ask| !writes(ask))?;
    let installed = match ask.verb {
        Verb::Load => match lake.cat.get::<Installed>(&key(&ask.name)).await {
            Ok(found) => found.is_some(),
            Err(e) => return Some(Err(e)),
        },
        _ => false,
    };
    Some(answer(&ask, installed))
}

/// The statement as a row naming what happened, for a host that runs SQL through the front and has no door of its own
/// (`pondra sql`): `SELECT 'httpfs is built into Pondra: …' AS notice`. None for a statement the node answers from its lake or
/// writes, which the node is sent (a LOAD of a name that isn't built in).
pub fn row(sql: &str) -> Option<Result<String>> {
    let ask = read_sql(sql).filter(|ask| !writes(ask) && (ask.verb != Verb::Load || built_in(&ask.name)))?;
    Some(answer(&ask, false).map(|said| format!("SELECT '{}' AS notice", said.notice.replace('\'', "''"))))
}

/// The write a statement is, for `write::parse`: `INSTALL name FROM 'file or URL'`, which the node that got it reads (`fetch`),
/// or `UNINSTALL name`, which the leader carries out. None for the rest.
#[cfg(feature = "lake")]
pub fn write(sql: &str) -> Option<Stmt> {
    let ask = read_sql(sql).filter(writes)?;
    let d = match (ask.verb, ask.from) {
        (Verb::Uninstall, _) => Ddl::UninstallExtension { name: ask.name, if_exists: ask.if_exists },
        (_, Some(Origin::Source(from))) => Ddl::FetchExtension { name: ask.name, from, version: ask.version },
        _ => return None,
    };
    Some(Stmt::Ddl(vec![d]))
}

/// A record's catalog key.
#[cfg(feature = "lake")]
fn key(name: &str) -> String { format!("xt/{name}") }

/// `INSTALL name FROM 'file or URL'` on the node the statement reached. The file is read here (a path on this machine, or a URL
/// through the store of the secret that covers it), checked, and kept under its hash, so any node can read it; then the record
/// goes to the leader: under this node's lock when it leads, else sent to it, as `backup::statement` sends its attach.
#[cfg(feature = "lake")]
pub async fn fetch(lake: &Lake, lock: Option<&tokio::sync::Mutex<()>>, d: Ddl) -> Result<serde_json::Value> {
    let Ddl::FetchExtension { name, from, version } = d else { anyhow::bail!("only INSTALL … FROM is read on the node") };
    if cfg!(target_arch = "wasm32") {
        return Err(coded("0A000", format!("{name}: extensions in the page come after 1.0 (X3 in the plan)")));
    }
    unsigned_allowed(&name)?;
    let max = crate::env::or("PONDRA_EXTENSION_FILE_MB", 64u64) * (1 << 20);
    let bytes = bytes_of(lake, &from, max).await?;
    let record = keep(lake, &name, &from, version, bytes).await?;
    let install = Ddl::InstallExtension { name, record };
    match lock {
        Some(lock) => {
            let _guard = lock.lock().await;
            crate::ddl::apply(lake, install).await
        }
        None => Box::pin(crate::write::send(&lake.url, Request::Ddl(install))).await,
    }
}

/// The file's bytes: a path on this machine (`std::fs`), or a URL read through the store its secret makes, as `read_parquet`
/// reads one. The check every read makes comes first (`ext::check_url`: a path on the server's disk is the node's owner's, and a
/// URL needs a secret that covers it). A file over `max` is refused before it is read whole.
/// Unsigned is every extension in 0.80: a node installs or runs one only when its admin starts it with
/// `PONDRA_ALLOW_UNSIGNED_EXTENSIONS=true`. Asked of each node as it loads a part (`part`), not only at INSTALL: a record
/// another node installed, a backup restored or a branch copied is run only where its admin allowed it (audit 11-01 H1).
#[cfg(feature = "lake")]
fn unsigned_allowed(name: &str) -> Result<()> {
    if !crate::env::flag("PONDRA_ALLOW_UNSIGNED_EXTENSIONS") {
        return Err(coded("0A000", format!("{name} is unsigned: this node loads unsigned extensions only when it runs with PONDRA_ALLOW_UNSIGNED_EXTENSIONS=true (an admin's choice, as DuckDB's allow_unsigned_extensions is)")));
    }
    Ok(())
}

/// Whether a record's `sha256` may name a file: 64 lower-case hex digits, as `keep` writes it. Checked before it names a key
/// in the lake or a file in the compiled-code cache (`wasm::compiled`): a record is read from a bucket, and a path there would
/// name a file elsewhere on the node's disk.
pub(crate) fn sha_ok(sha256: &str) -> bool {
    sha256.len() == 64 && sha256.bytes().all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

/// The record names the bytes in the lake: its `sha256` is a hash, and the file under it hashes to it. Asked by the leader
/// as it keeps a record (`install`: a raw `/cluster/ddl` request may carry one its caller built) and by each node as it
/// loads one (`part`: a bucket writer may have swapped the file). Not so: XX001, INSTALL it again.
#[cfg(feature = "lake")]
async fn the_file(lake: &Lake, name: &str, r: &Installed) -> Result<bytes::Bytes> {
    let again = |why: String| coded("XX001", format!("the extension {name} {why}: INSTALL it again"));
    if !sha_ok(&r.sha256) {
        return Err(again(format!("names no file (its record's sha256 {:?} isn't 64 hex digits)", r.sha256)));
    }
    let bytes = lake.object(&file_key(name, &r.version, &r.sha256)).await.with_context(|| format!("reading the file of the extension {name}"))?;
    if crate::text::hex(&crate::crypto::sha256(&bytes)) != r.sha256 {
        return Err(again("has a file that isn't the one installed (its SHA-256 isn't the record's)".to_string()));
    }
    Ok(bytes)
}

/// An extension's functions as its record keeps them, from its loaded part.
#[cfg(all(feature = "lake", not(target_arch = "wasm32")))]
fn described(part: &crate::wasm::Loaded) -> Vec<Described> {
    part.functions.iter().map(|f| Described {
        name: f.name.clone(),
        arguments: f.arguments.iter().map(ToString::to_string).collect(),
        returns: f.returns.to_string(),
        volatile: f.volatile,
        columns: f.columns.iter().map(|(n, t)| (n.clone(), t.to_string())).collect(),
    }).collect()
}

#[cfg(feature = "lake")]
async fn bytes_of(lake: &Lake, from: &str, max: u64) -> Result<Vec<u8>> {
    use object_store_df::ObjectStoreExt as _;
    crate::ext::check_url(lake, from).await?;
    let too_big = |size: u64| coded("54000", format!("{} is {} MB: a Pondra extension's file is at most {} MB (PONDRA_EXTENSION_FILE_MB)", file_of(from), size >> 20, max >> 20));
    if crate::ext::on_disk(from) {
        let path = from.strip_prefix("file://").unwrap_or(from);
        let size = std::fs::metadata(path).with_context(|| format!("reading {from}"))?.len();
        if size > max {
            return Err(too_big(size));
        }
        return Ok(std::fs::read(path).with_context(|| format!("reading {from}"))?);
    }
    let (store, at) = crate::ext::store(lake, from).await?;
    let got = store.get(&at).await?;
    if got.meta.size > max {
        return Err(too_big(got.meta.size));
    }
    Ok(got.bytes().await?.to_vec())
}

/// Checks the file's bytes, loads them (`wasm::load`: compiled, and asked what it has), and keeps them at
/// `ext/<name>/<version>/<sha256>.wasm` (content-addressed: a file already there is kept already); then makes the record of
/// them, its version and functions as the file says. A file that isn't WebAssembly is refused by its name, one that isn't a
/// component Pondra can load saying why, a `VERSION` the file doesn't say by both versions, and a function whose name another
/// function has by that name (`clash`).
#[cfg(all(feature = "lake", not(target_arch = "wasm32")))]
pub async fn keep(lake: &Lake, name: &str, from: &str, version: Option<String>, bytes: Vec<u8>) -> Result<Installed> {
    let file = file_of(from);
    if !bytes.starts_with(b"\0asm") {
        return Err(coded("22023", format!("{file} is not a WebAssembly file")));
    }
    let sha256 = crate::text::hex(&crate::crypto::sha256(&bytes));
    let part = part_of(bytes.clone(), &sha256, &format!("{name} ({file})")).await?;
    if let Some(asked) = version.filter(|v| *v != part.version) {
        return Err(coded("22023", format!("{file} is {} {}, not {asked}", part.name, part.version)));
    }
    let version = part.version.clone();
    let functions = described(&part);
    clash(lake, name, &functions).await?;
    let size = bytes.len() as u64;
    let at = file_key(name, &version, &sha256);
    match lake.put(&at, bytes).await {
        Ok(()) => {}
        // (these bytes are kept already, unless another file was put under their hash: refused, as `part` would refuse it)
        Err(e) if matches!(e.downcast_ref::<object_store::Error>(), Some(object_store::Error::AlreadyExists { .. })) => {
            if crate::text::hex(&crate::crypto::sha256(&lake.object(&at).await?)) != sha256 {
                return Err(coded("XX001", format!("{at} holds another file than its name says: remove it, then INSTALL {name} again")));
            }
        }
        Err(e) => return Err(e),
    }
    Ok(Installed {
        version,
        sha256,
        bytes: size,
        from: kept_from(from),
        by: crate::auth::current().map(|p| p.name).filter(|n| !n.is_empty()).unwrap_or_else(|| "admin".to_string()),
        at_ms: crate::log::now_ms(),
        functions,
    })
}

/// A page has no host for an extension's part (X3 in the plan: after 1.0); `fetch` refuses before this.
#[cfg(all(feature = "lake", target_arch = "wasm32"))]
pub async fn keep(_: &Lake, name: &str, _: &str, _: Option<String>, _: Vec<u8>) -> Result<Installed> {
    Err(coded("0A000", format!("{name}: extensions in the page come after 1.0 (X3 in the plan)")))
}

/// Refuses functions whose names are taken: by the engine's own functions (`upper`), the lake's routines, or another
/// installed extension's (an extension installed again over itself takes its own names back). A function's name is a
/// plain lower-case word, so a query names it as it names any function.
#[cfg(all(feature = "lake", not(target_arch = "wasm32")))]
async fn clash(lake: &Lake, name: &str, functions: &[Described]) -> Result<()> {
    let state = lake.session().state();
    let routines = crate::routines::listed(lake).await?;
    let others = listed(lake).await?;
    for f in functions {
        let n = f.name.as_str();
        if n.is_empty() || !n.chars().next().is_some_and(|c| c.is_ascii_lowercase() || c == '_') || !n.chars().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_') {
            return Err(coded("42602", format!("{name}'s function {n}: a function's name is lower-case letters, digits and _")));
        }
        if state.scalar_functions().contains_key(n) || state.aggregate_functions().contains_key(n) || state.window_functions().contains_key(n) || state.table_functions().contains_key(n) {
            return Err(coded("42723", format!("{name}'s function {n}: Pondra has a function {n} already")));
        }
        if routines.iter().any(|(r, _)| r == n) {
            return Err(coded("42723", format!("{name}'s function {n}: the routine {n} has that name; drop it or install another file")));
        }
        if let Some((other, _)) = others.iter().find(|(o, r)| o != name && r.functions.iter().any(|g| g.name == n)) {
            return Err(coded("42723", format!("{name}'s function {n}: the extension {other} has a function {n} already")));
        }
    }
    Ok(())
}

/// The parts loaded in this process, by their file's SHA-256: a part holds no lake's state and its file never changes under
/// its hash, so one serves every lake and node in the process. (Kept until the process ends: an extension installed again is
/// a new file, a new entry.)
#[cfg(all(feature = "lake", not(target_arch = "wasm32")))]
static PARTS: std::sync::LazyLock<std::sync::Mutex<std::collections::HashMap<String, Arc<crate::wasm::Loaded>>>> = std::sync::LazyLock::new(Default::default);

/// A part from its bytes, compiled off the async threads (a megabyte takes Cranelift a second the first time; the disk cache
/// makes it milliseconds after) and kept in `PARTS`. One that needs a capability this Pondra doesn't offer is refused naming
/// `who` and the import (`words (words-0.2.wasm) needs pondra:http/http@1.0.0, …`, 0A000).
#[cfg(all(feature = "lake", not(target_arch = "wasm32")))]
async fn part_of(bytes: Vec<u8>, sha256: &str, who: &str) -> Result<Arc<crate::wasm::Loaded>> {
    if let Some(p) = PARTS.lock().unwrap_or_else(std::sync::PoisonError::into_inner).get(sha256) {
        return Ok(p.clone());
    }
    let sha = sha256.to_string();
    let loaded = tokio::task::spawn_blocking(move || crate::wasm::load(&bytes, &sha)).await?;
    let part = Arc::new(loaded.map_err(|e| match crate::codes::of(&e) {
        "0A000" => coded("0A000", format!("{who} {e}")),
        _ => e,
    })?);
    PARTS.lock().unwrap_or_else(std::sync::PoisonError::into_inner).insert(sha256.to_string(), part.clone());
    Ok(part)
}

/// An installed extension's part, on a node its admin allows unsigned extensions on (`unsigned_allowed`): from `PARTS`, else
/// its file read from the lake its record is in, hashing to the record's `sha256` (`the_file`). Its functions are those its
/// record names, or it is refused (XX001): a record is what a session registers by, and a node runs only what was installed.
#[cfg(all(feature = "lake", not(target_arch = "wasm32")))]
async fn part(lake: &Lake, name: &str, r: &Installed) -> Result<Arc<crate::wasm::Loaded>> {
    unsigned_allowed(name)?;
    let cached = PARTS.lock().unwrap_or_else(std::sync::PoisonError::into_inner).get(&r.sha256).cloned();
    let part = match cached {
        Some(p) => p,
        None => part_of(the_file(lake, name, r).await?.to_vec(), &r.sha256, name).await?,
    };
    if described(&part) != r.functions {
        return Err(coded("XX001", format!("the extension {name}'s file doesn't have the functions its record names: INSTALL it again")));
    }
    Ok(part)
}

/// The functions of the lake's installed extensions that `text` names (the query and the views it reads), in its session
/// (`query::session_at`): each a scalar function by its own name, run by its extension's part. An extension no query names
/// is never loaded, so one whose file can't be read fails only the queries that call it. On every node a spread query
/// reaches, its session registers the same, from the same records.
#[cfg(all(feature = "lake", not(target_arch = "wasm32")))]
pub async fn functions(lake: &Lake, ctx: &datafusion::prelude::SessionContext, text: &str) -> Result<()> {
    let installed = listed(lake).await?;
    if installed.is_empty() {
        return Ok(());
    }
    for (name, r) in &named(installed, text) {
        let part = part(lake, name, r).await?;
        for f in &part.functions {
            if f.is_table() {
                ctx.register_udtf(&f.name, Arc::new(TableCall { part: part.clone(), function: f.clone() }));
            } else {
                ctx.register_udf(datafusion::logical_expr::ScalarUDF::new_from_impl(Call::new(part.clone(), f)));
            }
        }
    }
    Ok(())
}

/// A page has no host for an extension's part (X3 in the plan: after 1.0): a query naming a function of one of the lake's
/// installed extensions is refused by its name, as INSTALL is, not as an unknown function (audit 11-01 L2).
#[cfg(all(feature = "lake", target_arch = "wasm32"))]
pub async fn functions(lake: &Lake, _: &datafusion::prelude::SessionContext, text: &str) -> Result<()> {
    match named(listed(lake).await?, text).first() {
        Some((name, _)) => Err(coded("0A000", format!("{name} is an extension, and extensions in the page come after 1.0 (X3 in the plan): run its functions on a node"))),
        None => Ok(()),
    }
}

/// Does `sql`, or a view it reads, call one of the installed extensions' functions that `which` picks? The views are read
/// only when an extension has such a function and `sql` alone doesn't name it (most lakes have no extension).
#[cfg(all(feature = "lake", not(target_arch = "wasm32")))]
pub async fn calls(lake: &Lake, sql: &str, which: impl Fn(&Described) -> bool) -> bool {
    let Ok(installed) = listed(lake).await else { return true };
    let names: Vec<&str> = installed.iter().flat_map(|(_, r)| &r.functions).filter(|f| which(f)).map(|f| f.name.as_str()).collect();
    if names.is_empty() || names.iter().any(|n| crate::text::mentions(sql, n)) {
        return !names.is_empty();
    }
    match crate::routines::with_views(lake, sql).await {
        Ok(text) => names.iter().any(|n| crate::text::mentions(&text, n)),
        Err(_) => true,
    }
}

/// One of an extension's table functions in a session: `f(…)` in FROM, its arguments values worked out once, as a
/// Python table function's are (`pyfn::TableFn`).
#[cfg(all(feature = "lake", not(target_arch = "wasm32")))]
#[derive(Debug)]
struct TableCall {
    part: Arc<crate::wasm::Loaded>,
    function: crate::wasm::Function,
}

#[cfg(all(feature = "lake", not(target_arch = "wasm32")))]
impl datafusion::catalog::TableFunctionImpl for TableCall {
    fn call(&self, args: &[datafusion::prelude::Expr]) -> datafusion::error::Result<Arc<dyn datafusion::catalog::TableProvider>> {
        use datafusion::optimizer::simplify_expressions::ExprSimplifier;
        let simplifier = ExprSimplifier::new(datafusion::logical_expr::simplify::SimplifyContext::builder().with_current_time().build());
        let mut values = vec![];
        for a in args {
            match simplifier.simplify(crate::optimize::whole_literals(a.clone())?)? {
                datafusion::prelude::Expr::Literal(v, _) => values.push(v.to_array()?),
                e => return datafusion::common::plan_err!("{}: its arguments are values (worked out once), not {e}", self.function.name),
            }
        }
        Ok(Arc::new(Called { part: self.part.clone(), function: self.function.clone(), values }))
    }
}

/// A table function called with its arguments: its rows, streamed from the part as the query reads them.
#[cfg(all(feature = "lake", not(target_arch = "wasm32")))]
#[derive(Debug)]
struct Called {
    part: Arc<crate::wasm::Loaded>,
    function: crate::wasm::Function,
    values: Vec<datafusion::arrow::array::ArrayRef>,
}

#[cfg(all(feature = "lake", not(target_arch = "wasm32")))]
#[async_trait::async_trait]
impl datafusion::catalog::TableProvider for Called {
    fn schema(&self) -> datafusion::arrow::datatypes::SchemaRef { self.function.schema() }
    fn table_type(&self) -> datafusion::datasource::TableType { datafusion::datasource::TableType::Temporary }

    async fn scan(&self, _: &dyn datafusion::catalog::Session, projection: Option<&Vec<usize>>, _: &[datafusion::prelude::Expr], limit: Option<usize>) -> datafusion::error::Result<Arc<dyn datafusion::physical_plan::ExecutionPlan>> {
        let rows: Arc<dyn datafusion::physical_plan::streaming::PartitionStream> = Arc::new(Streamed { part: self.part.clone(), function: self.function.name.clone(), values: self.values.clone(), schema: self.function.schema() });
        Ok(Arc::new(datafusion::physical_plan::streaming::StreamingTableExec::try_new(self.function.schema(), vec![rows], projection, [], false, limit)?))
    }
}

/// The call's rows as one stream, a batch each time the query asks for one: opened, and each batch made, on a blocking
/// thread for as long as that takes within the call's limits (`calling`), and no thread held between them. A query that
/// stops reading drops the stream, and with it the call. (Made two batches ahead on a thread of its own, a call held that
/// thread while a slow client wasn't reading: audit 11-01 M1.)
#[cfg(all(feature = "lake", not(target_arch = "wasm32")))]
#[derive(Debug)]
struct Streamed {
    part: Arc<crate::wasm::Loaded>,
    function: String,
    values: Vec<datafusion::arrow::array::ArrayRef>,
    schema: datafusion::arrow::datatypes::SchemaRef,
}

#[cfg(all(feature = "lake", not(target_arch = "wasm32")))]
impl datafusion::physical_plan::streaming::PartitionStream for Streamed {
    fn schema(&self) -> &datafusion::arrow::datatypes::SchemaRef { &self.schema }

    fn execute(&self, _: Arc<datafusion::execution::TaskContext>) -> datafusion::execution::SendableRecordBatchStream {
        use datafusion::error::DataFusionError;
        enum At {
            Start(Arc<crate::wasm::Loaded>, String, Vec<datafusion::arrow::array::ArrayRef>),
            Open(Box<crate::wasm::Rows>),
        }
        let gone = |e: tokio::task::JoinError| DataFusionError::External(Box::new(e));
        let start = At::Start(self.part.clone(), self.function.clone(), self.values.clone());
        let batches = futures::stream::try_unfold(start, move |at| async move {
            let (rows, batch) = tokio::task::spawn_blocking(move || {
                let mut rows = match at {
                    At::Start(part, function, values) => Box::new(calling(|| part.rows(&function, &values, crate::wasm::Limits::now()))?),
                    At::Open(rows) => rows,
                };
                let batch = calling(|| rows.next())?;
                anyhow::Ok((rows, batch))
            })
            .await
            .map_err(gone)?
            .map_err(crate::codes::for_datafusion)?;
            Ok::<_, DataFusionError>(batch.map(|b| (b, At::Open(rows))))
        });
        Box::pin(datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(self.schema.clone(), batches))
    }
}

/// Runs one extension call (a scalar function's batch, a table function's opening or next batch), waiting while
/// [`CALLS`] others run: each holds a thread for as long as its batch takes, up to `PONDRA_EXTENSION_SECS`, and past
/// that many the queries that call more wait their turn rather than take every thread of the node (audit 11-01 M1).
#[cfg(all(feature = "lake", not(target_arch = "wasm32")))]
fn calling<T>(f: impl FnOnce() -> T) -> T {
    use std::sync::{Condvar, LazyLock, Mutex};
    static TURNS: LazyLock<(Mutex<usize>, Condvar)> = LazyLock::new(|| (Mutex::new(0), Condvar::new()));
    struct Turn;
    impl Drop for Turn {
        fn drop(&mut self) {
            *TURNS.0.lock().unwrap_or_else(|e| e.into_inner()) -= 1;
            TURNS.1.notify_one();
        }
    }
    let mut busy = TURNS.0.lock().unwrap_or_else(|e| e.into_inner());
    while *busy >= *CALLS {
        busy = TURNS.1.wait(busy).unwrap_or_else(|e| e.into_inner());
    }
    *busy += 1;
    drop(busy);
    let _turn = Turn;
    f()
}

/// How many extension calls a node runs at once: one a core.
#[cfg(all(feature = "lake", not(target_arch = "wasm32")))]
static CALLS: std::sync::LazyLock<usize> = std::sync::LazyLock::new(|| crate::env::cores().max(1));

/// One of an extension's functions in a session: its batches go to the part, a fresh instance for each (`wasm::Loaded::call`).
#[cfg(all(feature = "lake", not(target_arch = "wasm32")))]
#[derive(Debug)]
struct Call {
    function: String,
    returns: datafusion::arrow::datatypes::DataType,
    signature: datafusion::logical_expr::Signature,
    part: Arc<crate::wasm::Loaded>,
}

#[cfg(all(feature = "lake", not(target_arch = "wasm32")))]
impl Call {
    fn new(part: Arc<crate::wasm::Loaded>, f: &crate::wasm::Function) -> Self {
        use datafusion::logical_expr::{Signature, Volatility};
        let volatility = if f.volatile { Volatility::Volatile } else { Volatility::Immutable };
        let signature = if f.arguments.is_empty() { Signature::nullary(volatility) } else { Signature::exact(f.arguments.clone(), volatility) };
        Call { function: f.name.clone(), returns: f.returns.clone(), signature, part }
    }
}

// (one function of one file: the same name and hash is the same function)
#[cfg(all(feature = "lake", not(target_arch = "wasm32")))]
impl PartialEq for Call {
    fn eq(&self, other: &Self) -> bool { self.function == other.function && Arc::ptr_eq(&self.part, &other.part) }
}
#[cfg(all(feature = "lake", not(target_arch = "wasm32")))]
impl Eq for Call {}
#[cfg(all(feature = "lake", not(target_arch = "wasm32")))]
impl std::hash::Hash for Call {
    fn hash<H: std::hash::Hasher>(&self, h: &mut H) { (&self.function, Arc::as_ptr(&self.part)).hash(h) }
}

#[cfg(all(feature = "lake", not(target_arch = "wasm32")))]
impl datafusion::logical_expr::ScalarUDFImpl for Call {
    fn name(&self) -> &str { &self.function }
    fn signature(&self) -> &datafusion::logical_expr::Signature { &self.signature }
    fn return_type(&self, _: &[datafusion::arrow::datatypes::DataType]) -> datafusion::error::Result<datafusion::arrow::datatypes::DataType> { Ok(self.returns.clone()) }

    fn invoke_with_args(&self, args: datafusion::logical_expr::ScalarFunctionArgs) -> datafusion::error::Result<datafusion::logical_expr::ColumnarValue> {
        use datafusion::logical_expr::ColumnarValue;
        let rows = args.number_rows;
        let columns = args.args.iter().map(|a| match a {
            ColumnarValue::Array(a) => Ok(a.clone()),
            ColumnarValue::Scalar(s) => s.to_array_of_size(rows),
        }).collect::<datafusion::error::Result<Vec<_>>>()?;
        // (on the node's runtime its worker first hands its other tasks to another, so a call that takes seconds holds no
        // door or heartbeat behind it: audit 11-01 M1; elsewhere, a test's one-thread runtime or a blocking thread, as it is)
        let call = || calling(|| self.part.call(&self.function, &columns, rows));
        let answer = match tokio::runtime::Handle::try_current().map(|h| h.runtime_flavor()) {
            Ok(tokio::runtime::RuntimeFlavor::MultiThread) => tokio::task::block_in_place(call),
            _ => call(),
        };
        answer.map(ColumnarValue::Array).map_err(crate::codes::for_datafusion)
    }
}

/// The leader keeps a record (`Ddl::InstallExtension`): the same file already installed is answered "unchanged"; any other
/// replaces the record there, if any.
/// A record is checked again here, whoever sent it (`fetch` from any node, or a raw `/cluster/ddl` request): its file is in
/// the lake under its hash (`the_file`), and its functions' names are free (`clash`).
#[cfg(feature = "lake")]
pub async fn install(lake: &Lake, name: &str, record: Installed) -> Result<serde_json::Value> {
    the_file(lake, name, &record).await?;
    #[cfg(not(target_arch = "wasm32"))]
    clash(lake, name, &record.functions).await?;
    if let Some(have) = lake.cat.get::<Installed>(&key(name)).await? {
        if have.sha256 == record.sha256 {
            return Ok(serde_json::json!({"extension": name, "unchanged": true, "notice": format!("{name} is unchanged: the same file is installed ({})", have.version)}));
        }
    }
    let out = serde_json::json!({"extension": name, "version": record.version, "sha256": record.sha256});
    lake.cat.commit(vec![(key(name), json(&record))], &[]).await?;
    Ok(out)
}

/// The leader drops a record (`Ddl::UninstallExtension`). Its bytes stay: they are named by their hash, and nothing else is
/// removed. Not installed: 42704, or a notice with `IF EXISTS`.
#[cfg(feature = "lake")]
pub async fn uninstall(lake: &Lake, name: &str, if_exists: bool) -> Result<serde_json::Value> {
    if lake.cat.get::<Installed>(&key(name)).await?.is_none() {
        if !if_exists {
            return Err(coded("42704", format!("extension {name} is not installed")));
        }
        return Ok(serde_json::json!({"extension": name, "removed": false, "notice": format!("extension {name} is not installed, skipping")}));
    }
    lake.cat.commit(vec![], &[key(name)]).await?;
    Ok(serde_json::json!({"extension": name, "removed": true}))
}

/// Where an extension's file is kept: `ext/<name>/<version>/<sha256>.wasm`, in the lake whose record names it.
#[cfg(feature = "lake")]
fn file_key(name: &str, version: &str, sha256: &str) -> String { format!("ext/{name}/{version}/{sha256}.wasm") }

/// The files a catalog's records name, for a branch to copy with them (`branch.rs`): a branch is a lake of its own, and keeps
/// the file of each extension it holds, as it keeps its workspace's files. (A backup copies `ext/` whole: `backup.rs`.)
#[cfg(feature = "lake")]
pub fn files(catalog: &std::collections::BTreeMap<String, bytes::Bytes>) -> Vec<String> {
    let records = catalog.range("xt/".to_string().."xt0".to_string());
    records.filter_map(|(k, v)| Some((k.strip_prefix("xt/")?, serde_json::from_slice::<Installed>(v).ok()?))).map(|(name, r)| file_key(name, &r.version, &r.sha256)).collect()
}

/// The installed extensions with their records, by name: read again only after a commit, as `routines::listed` is (every
/// query asks, some twice).
#[cfg(feature = "lake")]
pub async fn listed(lake: &Lake) -> Result<Vec<(String, Installed)>> {
    type Seen = std::sync::Mutex<std::collections::HashMap<String, (u64, Arc<Vec<(String, Installed)>>)>>;
    crate::per_node!(static SEEN: Seen = Default::default());
    let version = lake.cat.version();
    if let (Some(v), Some((at, all))) = (version, SEEN.lock().unwrap_or_else(std::sync::PoisonError::into_inner).get(&lake.url)) {
        if *at == v {
            return Ok(all.to_vec());
        }
    }
    let all: Arc<Vec<(String, Installed)>> = Arc::new(lake.cat.scan::<Installed>("xt/", "xt0").await?.into_iter().map(|(k, v)| (k["xt/".len()..].to_string(), v)).collect());
    if let Some(v) = version {
        SEEN.lock().unwrap_or_else(std::sync::PoisonError::into_inner).insert(lake.url.clone(), (v, all.clone()));
    }
    Ok(all.to_vec())
}

/// The installed extensions with a function `text` names (a query and the views it reads), as words.
#[cfg(feature = "lake")]
fn named(installed: Vec<(String, Installed)>, text: &str) -> Vec<(String, Installed)> {
    let text = text.to_lowercase();
    let words: std::collections::HashSet<&str> = text.split(|c: char| !(c.is_alphanumeric() || c == '_')).collect();
    installed.into_iter().filter(|(_, r)| r.functions.iter().any(|f| words.contains(f.name.as_str()))).collect()
}

/// Where a record says its file came from: the file or URL as INSTALL named it, without a URL's query or fragment (a presigned
/// URL's signature is a credential, and the record is shown to every reader by `extensions()`).
#[cfg(all(feature = "lake", not(target_arch = "wasm32")))]
fn kept_from(from: &str) -> String { from.split(['?', '#']).next().unwrap_or(from).to_string() }

/// The file's own name (`words-0.1.wasm`), for what is said of it: the part after its last `/`, before any `?` or `#`.
fn file_of(from: &str) -> String {
    from.split(['?', '#']).next().unwrap_or(from).rsplit('/').next().unwrap_or(from).to_string()
}

/// `extensions()`: each extension's name, version, file's hash and size, where it came from, who installed it and when, and
/// whether it is built in (the DuckDB names, with the version `built in`). `SHOW EXTENSIONS` reads it.
#[cfg(feature = "lake")]
pub fn register(ctx: &datafusion::prelude::SessionContext, lake: Arc<Lake>) {
    ctx.register_udtf("extensions", Arc::new(Listing(lake)));
}

#[cfg(feature = "lake")]
#[derive(Debug)]
struct Listing(Arc<Lake>);

#[cfg(feature = "lake")]
impl datafusion::catalog::TableFunctionImpl for Listing {
    fn call(&self, _: &[datafusion::prelude::Expr]) -> datafusion::error::Result<Arc<dyn datafusion::catalog::TableProvider>> {
        Ok(Arc::new(Extensions(self.0.clone())))
    }
}

#[cfg(feature = "lake")]
struct Extensions(Arc<Lake>);

#[cfg(feature = "lake")]
impl std::fmt::Debug for Extensions {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result { write!(f, "extensions()") }
}

#[cfg(feature = "lake")]
#[async_trait::async_trait]
impl datafusion::catalog::TableProvider for Extensions {
    fn schema(&self) -> datafusion::arrow::datatypes::SchemaRef {
        use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
        Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("version", DataType::Utf8, false),
            Field::new("sha256", DataType::Utf8, true),
            Field::new("bytes", DataType::UInt64, true),
            Field::new("installed_from", DataType::Utf8, true),
            Field::new("installed_by", DataType::Utf8, true),
            Field::new("installed_at", DataType::Timestamp(TimeUnit::Millisecond, None), true),
            Field::new("built_in", DataType::Boolean, false),
        ]))
    }
    fn table_type(&self) -> datafusion::datasource::TableType { datafusion::datasource::TableType::View }

    async fn scan(&self, state: &dyn datafusion::catalog::Session, projection: Option<&Vec<usize>>, _: &[datafusion::prelude::Expr], _: Option<usize>) -> datafusion::error::Result<Arc<dyn datafusion::physical_plan::ExecutionPlan>> {
        use datafusion::arrow::array::{Array, BooleanArray, StringArray, TimestampMillisecondArray, UInt64Array};
        let e = |e: anyhow::Error| datafusion::error::DataFusionError::External(e.into());
        let installed = listed(&self.0).await.map_err(e)?;
        // (the built-in seven first, with no record; then the installed, by name)
        let rows: Vec<(String, Option<Installed>)> = BUILT_IN.iter().map(|(n, _)| (n.to_string(), None)).chain(installed.into_iter().map(|(n, r)| (n, Some(r)))).collect();
        let columns: Vec<Arc<dyn Array>> = vec![
            Arc::new(rows.iter().map(|(n, _)| Some(n.as_str())).collect::<StringArray>()),
            Arc::new(rows.iter().map(|(_, r)| Some(r.as_ref().map_or("built in", |r| r.version.as_str()))).collect::<StringArray>()),
            Arc::new(rows.iter().map(|(_, r)| r.as_ref().map(|r| r.sha256.as_str())).collect::<StringArray>()),
            Arc::new(rows.iter().map(|(_, r)| r.as_ref().map(|r| r.bytes)).collect::<UInt64Array>()),
            Arc::new(rows.iter().map(|(_, r)| r.as_ref().map(|r| r.from.as_str())).collect::<StringArray>()),
            Arc::new(rows.iter().map(|(_, r)| r.as_ref().map(|r| r.by.as_str())).collect::<StringArray>()),
            Arc::new(rows.iter().map(|(_, r)| r.as_ref().map(|r| r.at_ms as i64)).collect::<TimestampMillisecondArray>()),
            Arc::new(BooleanArray::from(rows.iter().map(|(_, r)| r.is_none()).collect::<Vec<bool>>())),
        ];
        let batch = datafusion::arrow::record_batch::RecordBatch::try_new(self.schema(), columns)?;
        datafusion::catalog::MemTable::try_new(self.schema(), vec![vec![batch]])?.scan(state, projection, &[], None).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn answered(sql: &str) -> Result<Said> {
        let ask = read_sql(sql).unwrap_or_else(|| panic!("{sql}: not an extension statement"));
        answer(&ask, false)
    }

    fn asked(sql: &str) -> Ask {
        read_sql(sql).unwrap_or_else(|| panic!("{sql}: not an extension statement"))
    }

    #[test]
    fn every_form_is_read() {
        let forms = [
            ("INSTALL httpfs", "INSTALL"),
            ("install httpfs;", "INSTALL"),
            ("INSTALL 'httpfs'", "INSTALL"),
            ("INSTALL \"httpfs\"", "INSTALL"),
            ("FORCE INSTALL httpfs", "INSTALL"),
            ("force install HTTPFS", "INSTALL"),
            ("INSTALL httpfs FROM community", "INSTALL"),
            ("INSTALL httpfs FROM 'community' VERSION 'v1.0'", "INSTALL"),
            ("-- a note\nINSTALL httpfs", "INSTALL"),
            ("LOAD httpfs", "LOAD"),
            ("load 'httpfs'", "LOAD"),
            ("LOAD \"httpfs\";", "LOAD"),
        ];
        for (sql, tag) in forms {
            let said = answered(sql).unwrap_or_else(|e| panic!("{sql}: {e:#}"));
            assert_eq!(said.tag, tag, "{sql}");
            assert!(said.notice.starts_with("httpfs is built into Pondra: nothing to "), "{sql}: {}", said.notice);
        }
    }

    #[test]
    fn a_built_in_name_is_told_nothing_had_to_be_done() {
        assert_eq!(answered("INSTALL httpfs").unwrap().notice, "httpfs is built into Pondra: nothing to install");
        assert_eq!(answered("LOAD httpfs").unwrap().notice, "httpfs is built into Pondra: nothing to load");
    }

    #[test]
    fn any_other_name_is_refused_by_name() {
        for sql in ["INSTALL spatial", "LOAD spatial", "FORCE INSTALL 'spatial'", "INSTALL spatial FROM community"] {
            let e = answered(sql).err().unwrap_or_else(|| panic!("{sql}: answered"));
            assert_eq!(crate::codes::of(&e), "0A000", "{sql}");
            let words = format!("{e:#}");
            assert!(words.starts_with("spatial isn't built into Pondra") && words.contains("(ADR-075)") && words.contains("httpfs"), "{words}");
        }
    }

    #[test]
    fn a_duckdb_extension_file_is_refused() {
        for sql in ["LOAD '/x/y.duckdb_extension'", "LOAD 'y.duckdb_extension'", "INSTALL 'x/httpfs'"] {
            let e = answered(sql).err().unwrap_or_else(|| panic!("{sql}: answered"));
            assert_eq!(crate::codes::of(&e), "0A000", "{sql}");
        }
    }

    #[test]
    fn the_rest_is_not_an_extension_statement() {
        for sql in [
            "LOAD DATA LOCAL INPATH 'x.csv' INTO TABLE t",
            "SELECT 1",
            "INSTALL",
            "INSTALL httpfs; SELECT 1",
            "FORCE LOAD httpfs",
            "FORCE UNINSTALL words",
            "INSTALL httpfs EXTRA",
            "LOAD httpfs, parquet",
            "LOAD_DATA x",
            "SET x = 1",
            "UNINSTALL",
            "UNINSTALL words EXTRA",
        ] {
            assert!(read_sql(sql).is_none(), "{sql}");
            assert!(!is(sql), "{sql}");
        }
    }

    #[test]
    fn a_row_names_it_for_a_host_with_no_door_of_its_own() {
        assert_eq!(row("INSTALL httpfs").unwrap().unwrap(), "SELECT 'httpfs is built into Pondra: nothing to install' AS notice");
        assert_eq!(row("LOAD aws").unwrap().unwrap(), "SELECT 'aws is built into Pondra: nothing to load' AS notice");
        assert_eq!(crate::codes::of(&row("INSTALL spatial").unwrap().unwrap_err()), "0A000");
        assert!(row("SELECT 1").is_none());
        assert!(row("INSTALL words FROM '/data/words-0.1.wasm'").is_none(), "a write: the node that gets it reads the file");
        assert!(row("UNINSTALL words").is_none());
    }

    #[test]
    fn the_pondra_forms_are_read_in_their_words() {
        let a = asked("INSTALL words FROM '/data/words-0.1.wasm'");
        assert_eq!((a.verb, a.name.as_str(), a.from, a.version), (Verb::Install, "words", Some(Origin::Source("/data/words-0.1.wasm".into())), None));
        let a = asked("install WORDS from 'https://x.org/words.wasm' version '0.1';");
        assert_eq!((a.name.as_str(), a.from, a.version), ("words", Some(Origin::Source("https://x.org/words.wasm".into())), Some("0.1".into())));
        assert_eq!(asked("INSTALL words FROM community").from, Some(Origin::Repo));
        assert_eq!(asked("INSTALL words FROM spatial").from, Some(Origin::Word("spatial".into())));
        assert_eq!(asked("INSTALL words FROM 'core_nightly'").from, Some(Origin::Repo));
        let a = asked("UNINSTALL words");
        assert_eq!((a.verb, a.name.as_str(), a.if_exists), (Verb::Uninstall, "words", false));
        assert!(asked("UNINSTALL IF EXISTS Words").if_exists);
    }

    #[test]
    fn what_the_node_writes_and_what_it_answers() {
        assert!(!is("INSTALL words FROM '/data/words-0.1.wasm'"), "a write, not an answer");
        assert!(!is("UNINSTALL words"));
        assert!(is("INSTALL httpfs") && is("INSTALL words") && is("LOAD words") && is("INSTALL words FROM community"));
        assert!(is("INSTALL httpfs FROM 'https://x.org/httpfs.wasm'"), "answered from the words, as a built-in's name is refused");
        let e = answered("INSTALL httpfs FROM 'https://x.org/httpfs.wasm'").err().unwrap();
        assert!(format!("{e:#}").contains("can't take a built-in's name"));
        let e = answered("INSTALL words FROM spatial").err().unwrap();
        assert!(format!("{e:#}").starts_with("FROM spatial: the DuckDB repositories"), "{e:#}");
        assert!(answered("INSTALL 'a/b' FROM 'x'").is_err_and(|e| crate::codes::of(&e) == "0A000"));
    }

    #[test]
    fn a_file_is_named_by_its_last_part() {
        assert_eq!(file_of("https://x.org/a/words-0.2.wasm?sig=1"), "words-0.2.wasm");
        assert_eq!(file_of("/data/words-0.1.wasm"), "words-0.1.wasm");
        assert!(valid("words") && valid("my-ext_2") && !valid("a/b") && !valid("") && !valid("Words"));
    }

    /// `tests/extensions/words` (tools/extension_fixture.sh): word_count, shout and boom.
    #[cfg(feature = "lake")]
    const WORDS: &[u8] = include_bytes!("../tests/fixtures/extensions/words-0.1.wasm");

    /// The record and the bytes: the file loaded and asked what it has, kept under its hash, a second copy kept already;
    /// a file that isn't WebAssembly, or isn't a component, refused by its name; a VERSION the file doesn't say, and functions
    /// another extension has, refused; the record's replacing or unchanged, and dropping it, as the leader does them.
    #[cfg(feature = "lake")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_installed_extension_is_kept_until_uninstalled() -> Result<()> {
        let dir = std::env::temp_dir().join(format!("pondra-extension-{}", uuid::Uuid::new_v4()));
        let lake = Lake::open(dir.to_str().unwrap(), true, false).await?;
        let e = keep(&lake, "words", "/data/words-0.1.txt", None, b"text".to_vec()).await.err().unwrap();
        assert_eq!((crate::codes::of(&e), format!("{e:#}")), ("22023", "words-0.1.txt is not a WebAssembly file".to_string()));
        let e = keep(&lake, "words", "/data/module.wasm", None, b"\0asm\x01\0\0\0".to_vec()).await.err().unwrap();
        assert!(crate::codes::of(&e) == "22023" && format!("{e:#}").contains("not a WebAssembly component"), "{e:#}");

        let first = keep(&lake, "words", "/data/words-0.1.wasm", None, WORDS.to_vec()).await?;
        assert_eq!((first.version.as_str(), first.bytes as usize), ("0.1", WORDS.len()), "the version is the file's own");
        assert_eq!(first.functions[0], Described { name: "word_count".into(), arguments: vec!["Utf8".into()], returns: "Int64".into(), volatile: false, columns: vec![] });
        let e = keep(&lake, "words", "/data/words.wasm", Some("0.2".into()), WORDS.to_vec()).await.err().unwrap();
        assert_eq!((crate::codes::of(&e), format!("{e:#}")), ("22023", "words.wasm is words 0.1, not 0.2".to_string()));
        let again = keep(&lake, "words", "https://x.org/words.wasm", Some("0.1".into()), WORDS.to_vec()).await?;
        assert_eq!(again.sha256, first.sha256, "the same bytes are kept once");
        let out = install(&lake, "words", first.clone()).await?;
        assert_eq!(out["sha256"], serde_json::json!(first.sha256));
        assert_eq!(install(&lake, "words", again).await?["unchanged"], true, "the same file is unchanged");
        let names: Vec<String> = listed(&lake).await?.into_iter().map(|(n, _)| n).collect();
        assert_eq!(names, ["words"]);
        assert!(keep(&lake, "words", "/data/words-0.1.wasm", None, WORDS.to_vec()).await.is_ok(), "installed again over itself: its own names");
        let e = keep(&lake, "words2", "/data/words-0.1.wasm", None, WORDS.to_vec()).await.err().unwrap();
        assert_eq!((crate::codes::of(&e), format!("{e:#}")), ("42723", "words2's function word_count: the extension words has a function word_count already".to_string()));
        let upper = Described { name: "upper".into(), arguments: vec!["Utf8".into()], returns: "Utf8".into(), volatile: false, columns: vec![] };
        let e = clash(&lake, "mine", &[upper]).await.err().unwrap();
        assert_eq!((crate::codes::of(&e), format!("{e:#}")), ("42723", "mine's function upper: Pondra has a function upper already".to_string()));
        // (a branch copies the file each record names: there it is)
        let named = files(&lake.cat.scan_raw("", "\x7f").await?);
        assert_eq!(named, [format!("ext/words/0.1/{}.wasm", first.sha256)]);
        assert_eq!(lake.object(&named[0]).await?.to_vec(), WORDS, "the file is kept where its record says");

        assert_eq!(uninstall(&lake, "words", false).await?["removed"], true);
        assert!(listed(&lake).await?.is_empty());
        let e = uninstall(&lake, "words", false).await.err().unwrap();
        assert_eq!((crate::codes::of(&e), format!("{e:#}")), ("42704", "extension words is not installed".to_string()));
        assert_eq!(uninstall(&lake, "words", true).await?["removed"], false, "IF EXISTS takes nothing and says so");
        lake.cat.close().await?;
        let _ = std::fs::remove_dir_all(&dir);
        Ok(())
    }

    /// No more extension calls run at once than `CALLS`: the rest wait their turn.
    #[cfg(feature = "lake")]
    #[test]
    fn calls_past_one_a_core_wait_their_turn() {
        use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};
        static NOW: AtomicUsize = AtomicUsize::new(0);
        static MOST: AtomicUsize = AtomicUsize::new(0);
        let threads: Vec<_> = (0..*CALLS + 3).map(|_| std::thread::spawn(|| calling(|| {
            MOST.fetch_max(NOW.fetch_add(1, SeqCst) + 1, SeqCst);
            std::thread::sleep(std::time::Duration::from_millis(30));
            NOW.fetch_sub(1, SeqCst);
        }))).collect();
        for t in threads {
            t.join().unwrap();
        }
        assert!(MOST.load(SeqCst) <= *CALLS, "{} at once, past {}", MOST.load(SeqCst), *CALLS);
    }

    /// In a query: an installed extension's functions answer by their own names, over a column of strings as a table reads
    /// it (a view of strings, cast to the type describe() gave), for each row and its NULLs; its own error is 38000 in its
    /// words; and once uninstalled, its names are no function. (On a runtime of many threads, as the node's: a scalar call
    /// hands its worker's other tasks on first, `block_in_place`.)
    #[cfg(feature = "lake")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_installed_extensions_functions_answer_in_a_query() -> Result<()> {
        use datafusion::arrow::array::{AsArray, Int64Array};
        let dir = std::env::temp_dir().join(format!("pondra-extension-{}", uuid::Uuid::new_v4()));
        let lake = Lake::open(dir.to_str().unwrap(), true, false).await?;
        // (each node asks the switch as it loads a part, the node this test is included: `unsigned_allowed`)
        std::env::set_var("PONDRA_ALLOW_UNSIGNED_EXTENSIONS", "true");
        let record = keep(&lake, "words", "/data/words-0.1.wasm", None, WORDS.to_vec()).await?;
        install(&lake, "words", record).await?;
        let run = |sql: &'static str| {
            let lake = &lake;
            async move {
                let ctx = crate::query::session(lake, sql, "").await?;
                anyhow::Ok(ctx.sql(sql).await?.collect().await?)
            }
        };
        let got = run("SELECT word_count(t) AS n, shout(t) AS s FROM (VALUES (arrow_cast('a quick test', 'Utf8View')), (NULL), ('')) AS v(t)").await?;
        assert_eq!(got[0].column(0).as_primitive::<datafusion::arrow::datatypes::Int64Type>(), &Int64Array::from(vec![Some(3), None, Some(0)]));
        assert_eq!(got[0].column(1).as_string::<i32>().iter().collect::<Vec<_>>(), [Some("A QUICK TEST"), None, Some("")]);
        let got = run("SELECT count(*) AS n, sum(i) AS s FROM series(2500)").await?;
        assert_eq!(got[0].column(1).as_primitive::<datafusion::arrow::datatypes::Int64Type>().value(0), 2500 * 2501 / 2, "a table function's rows, every batch");
        let got = run("SELECT word FROM words_of('to be or not') WHERE place > 2 ORDER BY place").await?;
        assert_eq!(got[0].column(0).as_string::<i32>().iter().collect::<Vec<_>>(), [Some("or"), Some("not")]);
        assert!(run("SELECT * FROM series(2500) LIMIT 3").await?.iter().map(|b| b.num_rows()).sum::<usize>() == 3, "a query that stops reading lets the call go");
        let e = run("SELECT boom(1)").await.err().unwrap();
        assert_eq!((crate::codes::of(&e), crate::codes::words(&e)), ("38000", "words's boom: boom always fails".to_string()));
        uninstall(&lake, "words", false).await?;
        assert!(run("SELECT word_count('a b')").await.is_err(), "uninstalled: no such function");
        lake.cat.close().await?;
        let _ = std::fs::remove_dir_all(&dir);
        Ok(())
    }
}
