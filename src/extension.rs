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
        return Ok(Said { tag, notice: format!("{name} is installed: this release keeps its file and runs none of its functions yet") });
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
    // (unsigned is every extension in 1.0: a node loads one only when its admin starts it so)
    if !crate::env::flag("PONDRA_ALLOW_UNSIGNED_EXTENSIONS") {
        return Err(coded("0A000", format!("{name} is unsigned: this node loads unsigned extensions only when it runs with PONDRA_ALLOW_UNSIGNED_EXTENSIONS=true (an admin's choice, as DuckDB's allow_unsigned_extensions is)")));
    }
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

/// Checks the file's bytes and keeps them at `ext/<name>/<version>/<sha256>.wasm` (content-addressed: a file already there is
/// kept already), and makes the record of them. A file that isn't WebAssembly is refused by its name.
#[cfg(feature = "lake")]
pub async fn keep(lake: &Lake, name: &str, from: &str, version: Option<String>, bytes: Vec<u8>) -> Result<Installed> {
    let file = file_of(from);
    if !bytes.starts_with(b"\0asm") {
        return Err(coded("22023", format!("{file} is not a WebAssembly file")));
    }
    let version = version.or_else(|| version_in(name, &file)).unwrap_or_else(|| "0".to_string());
    let sha256 = crate::text::hex(&crate::crypto::sha256(&bytes));
    let size = bytes.len() as u64;
    match lake.put(&file_key(name, &version, &sha256), bytes).await {
        Ok(()) => {}
        Err(e) if matches!(e.downcast_ref::<object_store::Error>(), Some(object_store::Error::AlreadyExists { .. })) => {} // (these bytes are kept already)
        Err(e) => return Err(e),
    }
    Ok(Installed {
        version,
        sha256,
        bytes: size,
        from: from.to_string(),
        by: crate::auth::current().map(|p| p.name).filter(|n| !n.is_empty()).unwrap_or_else(|| "admin".to_string()),
        at_ms: crate::log::now_ms(),
    })
}

/// The leader keeps a record (`Ddl::InstallExtension`): the same file already installed is answered "unchanged"; any other
/// replaces the record there, if any.
#[cfg(feature = "lake")]
pub async fn install(lake: &Lake, name: &str, record: Installed) -> Result<serde_json::Value> {
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

/// The installed extensions with their records, by name.
#[cfg(feature = "lake")]
pub async fn listed(lake: &Lake) -> Result<Vec<(String, Installed)>> {
    Ok(lake.cat.scan::<Installed>("xt/", "xt0").await?.into_iter().map(|(k, v)| (k["xt/".len()..].to_string(), v)).collect())
}

/// The file's own name, which carries its version (`words-0.1.wasm`): the part after its last `/`, before any `?` or `#`.
fn file_of(from: &str) -> String {
    from.split(['?', '#']).next().unwrap_or(from).rsplit('/').next().unwrap_or(from).to_string()
}

/// The version a file name carries for `name`: `words-0.1.wasm` is `0.1` for `words`. A name without one is None.
fn version_in(name: &str, file: &str) -> Option<String> {
    file.strip_suffix(".wasm")?.strip_prefix(name)?.strip_prefix('-').filter(|v| !v.is_empty()).map(str::to_string)
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
    fn the_version_is_the_files_or_none() {
        assert_eq!(file_of("https://x.org/a/words-0.2.wasm?sig=1"), "words-0.2.wasm");
        assert_eq!(file_of("/data/words-0.1.wasm"), "words-0.1.wasm");
        assert_eq!(version_in("words", "words-0.1.wasm").as_deref(), Some("0.1"));
        assert_eq!(version_in("words", "words.wasm"), None);
        assert_eq!(version_in("words", "other-0.1.wasm"), None);
        assert_eq!(version_in("words", "words-.wasm"), None);
        assert!(valid("words") && valid("my-ext_2") && !valid("a/b") && !valid("") && !valid("Words"));
    }

    /// The record and the bytes: kept under their hash, a second copy kept already, a file that isn't WebAssembly refused by
    /// its name, and the record's replacing or unchanged, and dropping it, as the leader does them.
    #[cfg(feature = "lake")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_installed_extension_is_kept_until_uninstalled() -> Result<()> {
        let dir = std::env::temp_dir().join(format!("pondra-extension-{}", uuid::Uuid::new_v4()));
        let lake = Lake::open(dir.to_str().unwrap(), true, false).await?;
        let wasm = b"\0asm\x01\0\0\0".to_vec();
        let e = keep(&lake, "words", "/data/words-0.1.txt", None, b"text".to_vec()).await.err().unwrap();
        assert_eq!((crate::codes::of(&e), format!("{e:#}")), ("22023", "words-0.1.txt is not a WebAssembly file".to_string()));

        let first = keep(&lake, "words", "/data/words-0.1.wasm", None, wasm.clone()).await?;
        assert_eq!((first.version.as_str(), first.bytes), ("0.1", 8));
        let again = keep(&lake, "words", "https://x.org/words.wasm", Some("0.1".into()), wasm.clone()).await?;
        assert_eq!(again.sha256, first.sha256, "the same bytes are kept once");
        let out = install(&lake, "words", first.clone()).await?;
        assert_eq!(out["sha256"], serde_json::json!(first.sha256));
        assert_eq!(install(&lake, "words", again).await?["unchanged"], true, "the same file is unchanged");
        let names: Vec<String> = listed(&lake).await?.into_iter().map(|(n, _)| n).collect();
        assert_eq!(names, ["words"]);
        // (a branch copies the file each record names: there it is)
        let named = files(&lake.cat.scan_raw("", "\x7f").await?);
        assert_eq!(named, [format!("ext/words/0.1/{}.wasm", first.sha256)]);
        assert_eq!(lake.object(&named[0]).await?.to_vec(), wasm, "the file is kept where its record says");

        assert_eq!(uninstall(&lake, "words", false).await?["removed"], true);
        assert!(listed(&lake).await?.is_empty());
        let e = uninstall(&lake, "words", false).await.err().unwrap();
        assert_eq!((crate::codes::of(&e), format!("{e:#}")), ("42704", "extension words is not installed".to_string()));
        assert_eq!(uninstall(&lake, "words", true).await?["removed"], false, "IF EXISTS takes nothing and says so");
        lake.cat.close().await?;
        let _ = std::fs::remove_dir_all(&dir);
        Ok(())
    }
}
