//! The `connections` extension (designs/etl-and-bi-tools.md §3.7; redesign step 4, l1-wip/step4-plan.md): a connection
//! is a named place outside the lake — where it is, the secret it signs in with, and how it is reached. This file is
//! the whole extension: its kind, its statements and its types, each an entry in the registry, so the core lists,
//! shows, describes and grants connections with no edit of its own. Its credentials stay in a secret, which it names;
//! SHOW CREATE and `pondra.objects` show that name, never a value.
use crate::connections::{key, ConnectionType, ConnectionTypes, Definition, Holds, SECRET, URL};
use crate::objects::{Kind, Kinds, OnClone};
use crate::registry::BuiltIn;
use crate::statements::{Parsed, Right, Run, Statement, Statements};
use crate::store::{json, Lake};
use crate::codes::coded;
use anyhow::Result;
use datafusion::sql::sqlparser::{ast, dialect::GenericDialect, keywords::Keyword, parser::Parser, tokenizer::Token};
use futures::future::BoxFuture;
use serde::Deserialize;
use serde_json::{json as j, Value};
use std::collections::BTreeMap;

/// The extension: the kind, its statements and the types this build has.
pub static CONNECTIONS: BuiltIn = BuiltIn {
    name: "connections",
    requires: &["catalog"],
    register: |r| {
        r.section_mut::<Kinds>().add(&KIND);
        let s = r.section_mut::<Statements>();
        for st in [&CREATE, &CREATE_OR_REPLACE, &ALTER, &DROP] {
            s.add(st);
        }
        let t = r.section_mut::<ConnectionTypes>();
        for ty in TYPES {
            t.add(ty);
        }
    },
};

const KIND: Kind = Kind {
    name: "connection",
    family: "connection",
    verbs: &["CREATE", "CREATE OR REPLACE", "ALTER", "DROP", "COMMENT ON", "SHOW CREATE"],
    prefix: crate::connections::PREFIX,
    privileges: &["READ", "WRITE"],
    on_clone: OnClone::Copy, // (a branch keeps the definition; its secret is the branch's own, made there)
    project: Some("connections come to a project's files in a later step: make them in the database (CREATE CONNECTION …)"),
    order: 4, // (after the secrets they name)
    secret: false,
    about: "a named place outside the lake: where it is, the secret it signs in with, and how it is reached",
    also: &["linked service", "external location", "foreign server"],
    shown: Some(shown),
};

const MADE: &str = "makes a connection: CREATE [OR REPLACE] CONNECTION [IF NOT EXISTS] name TYPE t [WITH (option = value, …)]";
const CREATE: Statement = Statement { words: &["CREATE", "CONNECTION"], parse: made, run: Run::Leader(make), right: Right::Admin, needs: &[], about: MADE, extension: "connections" };
const CREATE_OR_REPLACE: Statement = Statement { words: &["CREATE", "OR", "REPLACE", "CONNECTION"], parse: made, run: Run::Leader(make), right: Right::Admin, needs: &[], about: MADE, extension: "connections" };
const ALTER: Statement = Statement { words: &["ALTER", "CONNECTION"], parse: altered, run: Run::Leader(change), right: Right::Admin, needs: &[], about: "changes a connection: ALTER CONNECTION [IF EXISTS] name SET (option = value, …) | RESET (option, …) | RENAME TO new_name", extension: "connections" };
const DROP: Statement = Statement { words: &["DROP", "CONNECTION"], parse: dropped, run: Run::Leader(remove), right: Right::Admin, needs: &[], about: "drops a connection: DROP CONNECTION [IF EXISTS] name", extension: "connections" };

/// The types for files Pondra reaches today: buckets, a folder, and another node's served store. A type for tables or
/// messages is its own extension's entry (`delta`, `iceberg`, `kafka`, `share`), so ATTACH and CREATE CONNECTION list the
/// same types.
static TYPES: &[&ConnectionType] = &[
    &ConnectionType { name: "s3", holds: Holds::Files, options: &[URL, SECRET], schemes: &["s3"], secret_types: &["s3", "r2"], about: "an S3 bucket, or one that speaks S3 (R2, MinIO): url = 's3://bucket/folder'", open: Some(bucket) },
    &ConnectionType { name: "gcs", holds: Holds::Files, options: &[URL, SECRET], schemes: &["gs"], secret_types: &["gcs"], about: "a Google Cloud Storage bucket: url = 'gs://bucket/folder'", open: Some(bucket) },
    &ConnectionType { name: "azure", holds: Holds::Files, options: &[URL, SECRET], schemes: &["az", "abfs", "abfss"], secret_types: &["azure"], about: "an Azure Blob Storage or Data Lake container: url = 'az://container/folder'", open: Some(bucket) },
    &ConnectionType { name: "folder", holds: Holds::Files, options: &[URL], schemes: &[""], secret_types: &[], about: "a folder on the machine, for a lake in a folder: url = '/data/drop'", open: FOLDER },
    &ConnectionType { name: "pondra", holds: Holds::Files, options: &[URL, SECRET], schemes: &["pondra"], secret_types: &["pondra"], about: "the store another Pondra node serves: url = 'pondra://host:7070/folder'", open: None }, // (its files through a path come with the served store's reach: piece 5)
];

/// A bucket's files: its store, signed with the secret's settings (none: the node's own credentials, which the admin who made
/// the connection lends with it), under the folder its url names.
fn bucket(url: &str, secret: Option<&BTreeMap<String, String>>) -> Result<std::sync::Arc<dyn object_store::ObjectStore>> {
    let u = url::Url::parse(url)?;
    let root = url::Url::parse(&format!("{}://{}", u.scheme(), u.host_str().unwrap_or_default()))?;
    let store = crate::ext::build(&root, secret, crate::ext::For::Files)?;
    let folder = u.path().trim_matches('/');
    Ok(match folder.is_empty() {
        true => store,
        false => std::sync::Arc::new(object_store::prefix::PrefixStore::new(store, folder)),
    })
}

/// A folder's files, on this machine (a page has none).
#[cfg(not(target_arch = "wasm32"))]
const FOLDER: Option<crate::connections::Opener> = Some(|url, _| {
    let dir = url.strip_prefix("file://").unwrap_or(url);
    std::fs::create_dir_all(dir)?; // (COPY … TO '@drop/out/' writes into it)
    Ok(std::sync::Arc::new(object_store::local::LocalFileSystem::new_with_prefix(dir)?))
});
#[cfg(target_arch = "wasm32")]
const FOLDER: Option<crate::connections::Opener> = None;

/// SHOW CREATE CONNECTION's statement: its options as they were given, the secret by its name.
fn shown(name: &str, def: &Value) -> String {
    let Ok(d) = serde_json::from_value::<Definition>(def.clone()) else { return format!("-- connection {name}: its definition can't be read by this build") };
    let mut s = format!("CREATE CONNECTION {} TYPE {}", crate::objects::ident(name), d.ty);
    if !d.options.is_empty() {
        let options: Vec<String> = d.options.iter().map(|(k, v)| format!("{k} = {}", value_sql(k, v))).collect();
        s.push_str(&format!(" WITH ({})", options.join(", ")));
    }
    s
}

fn value_sql(key: &str, v: &Value) -> String {
    match v {
        Value::String(s) if key == "secret" => crate::objects::ident(s),
        Value::String(s) => format!("'{}'", s.replace('\'', "''")),
        other => other.to_string(),
    }
}

// ---------------------------------------------------------------- the statements, read

fn parser(sql: &str) -> Result<Parser<'static>> {
    Ok(Parser::new(&GenericDialect {}).try_with_sql(sql)?)
}

/// A statement not in its form: a syntax error that shows the form.
fn syntax(form: &str) -> anyhow::Error {
    coded("42601", form.to_string())
}

/// A value an option can't take, or an option a type doesn't have.
fn invalid(why: String) -> anyhow::Error {
    coded("22023", why)
}

/// The next word, which must be `w` (a word sqlparser has no keyword for, as CONNECTION).
fn word(p: &mut Parser, w: &str, form: &str) -> Result<()> {
    match p.next_token().token {
        Token::Word(x) if x.value.eq_ignore_ascii_case(w) => Ok(()),
        _ => Err(syntax(form)),
    }
}

fn name(p: &mut Parser, form: &str) -> Result<String> {
    let n = crate::text::ident(&p.parse_identifier().map_err(|_| syntax(form))?);
    crate::ddl::check(&n)?;
    Ok(n)
}

fn end(p: &mut Parser, form: &str) -> Result<()> {
    while p.consume_token(&Token::SemiColon) {}
    match p.peek_token().token {
        Token::EOF => Ok(()),
        _ => Err(syntax(form)),
    }
}

/// `(option = value, …)`: a value is a string, a number, true or false, or a word (a secret's name).
fn options(p: &mut Parser, form: &str) -> Result<BTreeMap<String, Value>> {
    p.expect_token(&Token::LParen).map_err(|_| syntax(form))?;
    let mut out = BTreeMap::new();
    loop {
        let k = p.parse_identifier().map_err(|_| syntax(form))?.value.to_lowercase();
        if !(p.consume_token(&Token::Eq) || p.consume_token(&Token::RArrow)) {
            return Err(syntax(form));
        }
        let what = |v: &dyn std::fmt::Display| invalid(format!("{k} = {v}: a string, a number, true or false, or a secret's name"));
        let v = match p.parse_expr().map_err(|_| syntax(form))? {
            ast::Expr::Value(v) => match v.value {
                ast::Value::SingleQuotedString(s) | ast::Value::DoubleQuotedString(s) => Value::String(s),
                ast::Value::Number(n, _) => serde_json::from_str(&n).map_err(|_| what(&n))?,
                ast::Value::Boolean(b) => Value::Bool(b),
                other => return Err(what(&other)),
            },
            ast::Expr::Identifier(i) => Value::String(crate::text::ident(&i)),
            other => return Err(what(&other)),
        };
        if out.insert(k.clone(), v).is_some() {
            return Err(invalid(format!("{k} is given twice")));
        }
        if !p.consume_token(&Token::Comma) {
            break;
        }
    }
    p.expect_token(&Token::RParen).map_err(|_| syntax(form))?;
    Ok(out)
}

/// `(option, …)`, as RESET names them.
fn names(p: &mut Parser, form: &str) -> Result<Vec<String>> {
    p.expect_token(&Token::LParen).map_err(|_| syntax(form))?;
    let mut all = vec![];
    loop {
        all.push(p.parse_identifier().map_err(|_| syntax(form))?.value.to_lowercase());
        if !p.consume_token(&Token::Comma) {
            break;
        }
    }
    p.expect_token(&Token::RParen).map_err(|_| syntax(form))?;
    Ok(all)
}

/// `CREATE [OR REPLACE] CONNECTION [IF NOT EXISTS] name TYPE t [WITH (option = value, …)]`.
fn made(sql: &str) -> Result<Parsed> {
    let form = "CREATE [OR REPLACE] CONNECTION [IF NOT EXISTS] name TYPE t [WITH (option = value, …)]";
    let mut p = parser(sql)?;
    p.expect_keyword(Keyword::CREATE).map_err(|_| syntax(form))?;
    let replace = p.parse_keywords(&[Keyword::OR, Keyword::REPLACE]);
    word(&mut p, "connection", form)?;
    let if_not_exists = p.parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);
    if replace && if_not_exists {
        return Err(syntax("CREATE OR REPLACE CONNECTION or CREATE CONNECTION IF NOT EXISTS: one of them"));
    }
    let name = name(&mut p, form)?;
    if !p.parse_keyword(Keyword::TYPE) {
        return Err(syntax(&format!("CREATE CONNECTION {name} TYPE t: {}", type_names())));
    }
    let ty = p.parse_identifier().map_err(|_| syntax(form))?.value.to_lowercase();
    let options = if p.parse_keyword(Keyword::WITH) { options(&mut p, form)? } else { BTreeMap::new() };
    end(&mut p, form)?;
    Ok(Parsed { object: name.clone(), tree: j!({"name": name, "type": ty, "options": options, "replace": replace, "if_not_exists": if_not_exists}) })
}

/// `ALTER CONNECTION [IF EXISTS] name SET (option = value, …) | RESET (option, …) | RENAME TO new_name`.
fn altered(sql: &str) -> Result<Parsed> {
    let form = "ALTER CONNECTION [IF EXISTS] name SET (option = value, …) | RESET (option, …) | RENAME TO new_name";
    let mut p = parser(sql)?;
    p.expect_keyword(Keyword::ALTER).map_err(|_| syntax(form))?;
    word(&mut p, "connection", form)?;
    let if_exists = p.parse_keywords(&[Keyword::IF, Keyword::EXISTS]);
    let name = name(&mut p, form)?;
    let (mut set, mut reset, mut rename) = (BTreeMap::new(), vec![], None);
    if p.parse_keywords(&[Keyword::RENAME, Keyword::TO]) {
        rename = Some(self::name(&mut p, form)?);
    } else if p.parse_keyword(Keyword::SET) {
        set = options(&mut p, form)?;
    } else if p.parse_keyword(Keyword::RESET) {
        reset = names(&mut p, form)?;
    } else {
        return Err(syntax(form));
    }
    end(&mut p, form)?;
    Ok(Parsed { object: name.clone(), tree: j!({"name": name, "set": set, "reset": reset, "rename": rename, "if_exists": if_exists}) })
}

/// `DROP CONNECTION [IF EXISTS] name`.
fn dropped(sql: &str) -> Result<Parsed> {
    let form = "DROP CONNECTION [IF EXISTS] name";
    let mut p = parser(sql)?;
    p.expect_keyword(Keyword::DROP).map_err(|_| syntax(form))?;
    word(&mut p, "connection", form)?;
    let if_exists = p.parse_keywords(&[Keyword::IF, Keyword::EXISTS]);
    let name = name(&mut p, form)?;
    end(&mut p, form)?;
    Ok(Parsed { object: name.clone(), tree: j!({"name": name, "if_exists": if_exists}) })
}

// ---------------------------------------------------------------- the statements, carried out by the leader

fn type_names() -> String {
    format!("this build's types are {}", crate::connections::named(|_| true))
}

/// A definition as it may be kept: its type is one this build has, its options are the type's, and the secret it names is
/// there and of a type it takes.
async fn checked(lake: &Lake, name: &str, d: &Definition) -> Result<()> {
    let t = crate::connections::types().iter().find(|t| t.name == d.ty).copied();
    let t: &ConnectionType = t.ok_or_else(|| invalid(format!("CREATE CONNECTION {name} TYPE {}: {}", d.ty, type_names())))?;
    let known = t.option_names();
    if let Some(k) = d.options.keys().find(|k| !known.contains(&k.as_str())) {
        return Err(invalid(format!("a TYPE {} connection has no option {k}: its options are {}", t.name, known.join(", "))));
    }
    if let Some(o) = t.options.iter().find(|o| o.required && !d.options.contains_key(o.name)) {
        return Err(invalid(format!("a TYPE {} connection needs {} ({}): {}", t.name, o.name, o.about, t.about)));
    }
    if let Some(url) = d.options.get("url") {
        let url = url.as_str().ok_or_else(|| invalid(format!("url is a string: {}", t.about)))?;
        if !t.reaches(url) {
            return Err(invalid(format!("{url}: {}", t.about)));
        }
        // (every node must reach a folder: a lake in a bucket is served by nodes on other machines)
        if t.schemes == [""] && !matches!(crate::store::place(&lake.url), Ok(crate::store::Place::Folder)) {
            return Err(invalid("a TYPE folder connection is for a lake in a folder: this lake's nodes may run on other machines, so name a bucket, or a node's served store (TYPE pondra)".into()));
        }
    }
    if let Some(s) = d.options.get("secret") {
        let s = s.as_str().ok_or_else(|| invalid("secret names a secret: secret = name".into()))?;
        let kept = lake.cat.get::<Value>(&crate::ext::secret_key(s)).await?;
        // (on a branch, a secret isn't copied: it is made there, as this says)
        let kept = kept.ok_or_else(|| coded("42704", format!("no secret {s}: make it first (CREATE SECRET {s} (TYPE {}, …))", t.secret_types.first().copied().unwrap_or("…"))))?;
        let kind = kept["kind"].as_str().unwrap_or_default();
        if !t.secret_types.contains(&kind) {
            return Err(invalid(format!("secret {s} is of TYPE {kind}: a TYPE {} connection takes a secret of TYPE {}", t.name, t.secret_types.join(" or "))));
        }
    }
    Ok(())
}

#[derive(Deserialize)]
struct Made {
    name: String,
    #[serde(rename = "type")]
    ty: String,
    options: BTreeMap<String, Value>,
    replace: bool,
    if_not_exists: bool,
}

fn make<'a>(lake: &'a Lake, tree: Value) -> BoxFuture<'a, Result<Value>> {
    Box::pin(async move {
        let m: Made = serde_json::from_value(tree)?;
        let d = Definition { ty: m.ty, options: m.options };
        checked(lake, &m.name, &d).await?;
        if lake.cat.get::<Value>(&key(&m.name)).await?.is_some() {
            if !(m.replace || m.if_not_exists) {
                return Err(coded("42710", format!("connection {} already exists (CREATE OR REPLACE CONNECTION)", m.name)));
            }
            if !m.replace {
                return Ok(j!({"connection": m.name, "unchanged": true}));
            }
        }
        lake.cat.commit(vec![(key(&m.name), json(&d))], &[]).await?;
        Ok(j!({"connection": m.name}))
    })
}

#[derive(Deserialize)]
struct Altered {
    name: String,
    set: BTreeMap<String, Value>,
    reset: Vec<String>,
    rename: Option<String>,
    if_exists: bool,
}

fn change<'a>(lake: &'a Lake, tree: Value) -> BoxFuture<'a, Result<Value>> {
    Box::pin(async move {
        let a: Altered = serde_json::from_value(tree)?;
        let Some(mut d) = lake.cat.get::<Definition>(&key(&a.name)).await? else {
            if !a.if_exists {
                return Err(coded("42704", format!("no connection {}", a.name)));
            }
            return Ok(j!({"connection": a.name, "exists": false}));
        };
        if let Some(to) = a.rename {
            if lake.cat.get::<Value>(&key(&to)).await?.is_some() {
                return Err(coded("42710", format!("connection {to} already exists")));
            }
            lake.cat.commit(vec![(key(&to), json(&d))], &[key(&a.name)]).await?;
            return Ok(j!({"connection": a.name, "renamed": to})); // (its comment follows: `objects::follow`)
        }
        for k in &a.reset {
            if d.options.remove(k).is_none() {
                return Err(invalid(format!("connection {} has no option {k} to reset", a.name)));
            }
        }
        d.options.extend(a.set);
        checked(lake, &a.name, &d).await?;
        lake.cat.commit(vec![(key(&a.name), json(&d))], &[]).await?;
        Ok(j!({"connection": a.name, "altered": true}))
    })
}

#[derive(Deserialize)]
struct Dropped {
    name: String,
    if_exists: bool,
}

fn remove<'a>(lake: &'a Lake, tree: Value) -> BoxFuture<'a, Result<Value>> {
    Box::pin(async move {
        let d: Dropped = serde_json::from_value(tree)?;
        if lake.cat.get::<Value>(&key(&d.name)).await?.is_none() {
            if !d.if_exists {
                return Err(coded("42704", format!("no connection {}", d.name)));
            }
            return Ok(j!({"connection": d.name, "dropped": false}));
        }
        lake.cat.commit(vec![], &[key(&d.name)]).await?;
        Ok(j!({"connection": d.name, "dropped": true}))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::write::Stmt;

    async fn lake() -> Result<std::sync::Arc<Lake>> {
        Lake::open(&format!("memory://connections-{}", uuid::Uuid::new_v4().simple()), true, false).await
    }

    /// One statement through the core's one path: `write::parse`, then the leader's `ddl::apply`. Err: its refusal, with its code.
    async fn run(lake: &Lake, sql: &str) -> std::result::Result<Vec<Value>, (String, String)> {
        let mut out = vec![];
        match crate::write::parse(sql) {
            Some(Stmt::Ddl(ds)) => {
                for d in ds {
                    let e = crate::ddl::apply(lake, d).await.map_err(|e| (crate::codes::of(&e).to_string(), format!("{e:#}")))?;
                    out.push(e);
                }
                Ok(out)
            }
            Some(Stmt::Refused(code, e)) => Err((code.to_string(), e)),
            Some(Stmt::Invalid(e)) => Err(("XX000".into(), e)),
            _ => panic!("{sql}: not a statement of the catalog's"),
        }
    }

    fn code(r: std::result::Result<Vec<Value>, (String, String)>) -> (String, String) {
        r.expect_err("refused")
    }

    #[tokio::test]
    async fn a_connection_is_made_shown_changed_renamed_and_dropped() -> Result<()> {
        let lake = lake().await?;
        run(&lake, "CREATE CONNECTION drop_zone TYPE s3 WITH (url = 's3://bucket/incoming')").await.unwrap();
        let kept: Definition = lake.cat.get(&key("drop_zone")).await?.expect("kept");
        assert_eq!(kept, Definition { ty: "s3".into(), options: [("url".to_string(), j!("s3://bucket/incoming"))].into() });
        // (the core lists and shows it with no edit of its own: `Kind::shown`)
        let all = crate::objects::list(&lake).await?;
        let o = all.iter().find(|o| o.kind == "connection").expect("listed");
        assert_eq!((o.name.as_str(), o.definition.as_deref()), ("drop_zone", Some("CREATE CONNECTION drop_zone TYPE s3 WITH (url = 's3://bucket/incoming')")));
        assert_eq!(code(run(&lake, "CREATE CONNECTION drop_zone TYPE s3 WITH (url = 's3://other')").await).0, "42710");
        assert_eq!(run(&lake, "CREATE CONNECTION IF NOT EXISTS drop_zone TYPE s3 WITH (url = 's3://other')").await.unwrap()[0]["unchanged"], true);
        run(&lake, "CREATE OR REPLACE CONNECTION drop_zone TYPE s3 WITH (url = 's3://other/in')").await.unwrap();
        // (its grants are its kind's privileges, through the core's GRANT: `users::On::Object`)
        run(&lake, "CREATE ROLE analyst").await.unwrap();
        run(&lake, "GRANT READ ON CONNECTION drop_zone TO analyst").await.unwrap();
        assert_eq!(code(run(&lake, "GRANT USAGE ON CONNECTION drop_zone TO analyst").await), ("0LP01".into(), "USAGE: a connection's privileges are READ and WRITE".into()));
        assert_eq!(code(run(&lake, "GRANT READ ON CONNECTION nope TO analyst").await), ("42704".into(), "no connection nope".into()));
        run(&lake, "ALTER CONNECTION drop_zone SET (url => 's3://bucket/later')").await.unwrap();
        assert_eq!(shown("drop_zone", &lake.cat.get::<Value>(&key("drop_zone")).await?.expect("kept")), "CREATE CONNECTION drop_zone TYPE s3 WITH (url = 's3://bucket/later')");
        assert_eq!(code(run(&lake, "ALTER CONNECTION drop_zone RESET (url)").await).0, "22023", "url is required");
        // (a comment follows a rename, and goes with a drop: `objects::follow`)
        crate::objects::apply(&lake, crate::objects::Op::Comment { kind: "connection".into(), name: "drop_zone".into(), text: Some("the partner's drop".into()), if_exists: false }).await?;
        run(&lake, "ALTER CONNECTION drop_zone RENAME TO landing").await.unwrap();
        assert!(lake.cat.get::<Value>(&key("drop_zone")).await?.is_none());
        let o = crate::objects::list(&lake).await?.into_iter().find(|o| o.kind == "connection").expect("listed");
        assert_eq!((o.name.as_str(), o.comment.as_deref()), ("landing", Some("the partner's drop")));
        assert_eq!(code(run(&lake, "ALTER CONNECTION drop_zone SET (url = 's3://x')").await).0, "42704");
        assert_eq!(run(&lake, "ALTER CONNECTION IF EXISTS drop_zone SET (url = 's3://x')").await.unwrap()[0]["exists"], false);
        run(&lake, "DROP CONNECTION landing").await.unwrap();
        assert!(crate::objects::list(&lake).await?.iter().all(|o| o.kind != "connection"));
        assert!(lake.cat.scan::<String>("cm/", "cm0").await?.is_empty(), "its comment went with it");
        assert_eq!(code(run(&lake, "DROP CONNECTION landing").await).0, "42704");
        assert_eq!(run(&lake, "DROP CONNECTION IF EXISTS landing").await.unwrap()[0]["dropped"], false);
        Ok(())
    }

    #[tokio::test]
    async fn a_definition_is_checked_against_its_type() -> Result<()> {
        let lake = lake().await?;
        let (c, e) = code(run(&lake, "CREATE CONNECTION shop TYPE postgres WITH (host = 'db')").await);
        assert_eq!(c, "22023");
        assert!(e.ends_with("TYPE postgres: this build's types are azure, delta, folder, gcs, iceberg, kafka, pondra, s3 or share"), "{e}");
        let (c, e) = code(run(&lake, "CREATE CONNECTION b TYPE s3 WITH (url = 's3://b', region = 'eu')").await);
        assert_eq!((c.as_str(), e.as_str()), ("22023", "a TYPE s3 connection has no option region: its options are url, secret"));
        assert!(code(run(&lake, "CREATE CONNECTION b TYPE s3").await).1.starts_with("a TYPE s3 connection needs url (where it is)"));
        assert_eq!(code(run(&lake, "CREATE CONNECTION b TYPE gcs WITH (url = 's3://b')").await).1, "s3://b: a Google Cloud Storage bucket: url = 'gs://bucket/folder'");
        assert!(code(run(&lake, "CREATE CONNECTION f TYPE folder WITH (url = '/data/drop')").await).1.starts_with("a TYPE folder connection is for a lake in a folder"), "a lake in memory");
        assert!(code(run(&lake, "CREATE CONNECTION f TYPE folder WITH (url = '/data/drop', secret = k)").await).1.contains("has no option secret"), "a folder takes no secret");
        // (the secret it names is there, and of a type it takes; its name is shown, never a value)
        assert_eq!(code(run(&lake, "CREATE CONNECTION b TYPE s3 WITH (url = 's3://b', secret = k)").await), ("42704".into(), "no secret k: make it first (CREATE SECRET k (TYPE s3, …))".into()));
        lake.cat.commit(vec![(crate::ext::secret_key("k"), json(&j!({"kind": "gcs", "scope": null, "sealed": ""})))], &[]).await?;
        assert_eq!(code(run(&lake, "CREATE CONNECTION b TYPE s3 WITH (url = 's3://b', secret = k)").await).1, "secret k is of TYPE gcs: a TYPE s3 connection takes a secret of TYPE s3 or r2");
        run(&lake, "CREATE CONNECTION b TYPE gcs WITH (url = 'gs://b/in', secret = k)").await.unwrap();
        assert_eq!(shown("b", &lake.cat.get::<Value>(&key("b")).await?.expect("kept")), "CREATE CONNECTION b TYPE gcs WITH (secret = k, url = 'gs://b/in')");
        Ok(())
    }

    /// A connection's files are reached by its name (`'@drop_zone/…'`, read at `cn://drop_zone/…`): through a store made
    /// from its definition, made again when it changes, after READ or WRITE on it; the caller needs no USAGE on its secret.
    #[tokio::test]
    async fn a_connections_files_are_reached_by_its_name() -> Result<()> {
        use object_store_df::ObjectStoreExt as _;
        let dir = std::env::temp_dir().join(format!("pondra-cn-{}", uuid::Uuid::new_v4().simple()));
        let lake = Lake::open(dir.join("lake").to_str().unwrap(), true, false).await?;
        let (drop, later) = (dir.join("drop"), dir.join("later"));
        run(&lake, &format!("CREATE CONNECTION drop_zone TYPE folder WITH (url = '{}')", drop.display())).await.unwrap();
        // (the paths that take one: a file reference, a reader's list, COPY's source and target, a LOCATION)
        let q = crate::routines::expand(&lake, "SELECT * FROM '@drop_zone/in/*.csv' JOIN read_parquet(['@Drop_Zone/a.parquet', 's3://b/c.parquet']) ON true").await?;
        let mut urls: Vec<String> = crate::ext_names::names(&q).iter().filter_map(|n| crate::ext_names::spec(n)).flat_map(|s| s.urls).collect();
        urls.sort();
        assert_eq!(urls, ["cn://drop_zone/a.parquet", "cn://drop_zone/in/*.csv", "s3://b/c.parquet"]);
        assert!(matches!(crate::ext::statement("COPY (SELECT 1 AS a) TO '@drop_zone/out/'"), Some(Stmt::CopyTo(_, to, _)) if to == "cn://drop_zone/out/"));
        assert!(matches!(crate::ext::statement("COPY INTO t FROM '@drop_zone/in/*.csv'"), Some(Stmt::CopyInto(_, crate::copy::Source::Path(p), _)) if p == "cn://drop_zone/in/*.csv"));
        assert_eq!(crate::connections::place("@drop_zone"), Some("cn://drop_zone/".into()));
        assert_eq!(crate::connections::place("@/x"), None);
        // (its store: the folder, then the one its url is changed to)
        let (store, path) = crate::ext::store(&lake, "cn://drop_zone/in/a.csv").await?;
        store.put(&path, bytes::Bytes::from_static(b"a\n1\n").into()).await?;
        assert_eq!(std::fs::read_to_string(drop.join("in/a.csv"))?, "a\n1\n");
        run(&lake, &format!("ALTER CONNECTION drop_zone SET (url = '{}')", later.display())).await.unwrap();
        let (store, path) = crate::ext::store(&lake, "cn://drop_zone/b.csv").await?;
        store.put(&path, bytes::Bytes::from_static(b"b").into()).await?;
        assert!(later.join("b.csv").exists() && !drop.join("b.csv").exists());
        let refused = |url: &'static str| {
            let lake = lake.clone();
            async move { crate::ext::store(&lake, url).await.map(|_| ()).unwrap_err() }
        };
        let e = refused("cn://nope/x.csv").await;
        assert_eq!((crate::codes::of(&e), e.to_string().starts_with("no connection nope")), ("42704", true));
        run(&lake, "CREATE CONNECTION shop TYPE delta WITH (url = 's3://b/tables')").await.unwrap();
        assert_eq!(refused("cn://shop/x").await.to_string(), "connection shop holds tables, not files: '@shop/…' reads files");
        // (READ to read, WRITE to write: a user's grants, not its secrets')
        run(&lake, "CREATE USER ana").await.unwrap();
        let may = |privilege: &'static str| {
            let lake = lake.clone();
            async move {
                let ana = crate::users::principal(&lake, "ana").await.unwrap();
                crate::auth::WHO.scope(ana, async { crate::ext::through("cn://drop_zone/a.csv", privilege) }).await
            }
        };
        let e = may("READ").await.unwrap_err();
        assert_eq!((crate::codes::of(&e), e.to_string()), ("42501", "'@drop_zone/…': READ on connection drop_zone isn't granted to this user (an admin grants it: GRANT READ ON CONNECTION drop_zone TO …)".into()));
        run(&lake, "GRANT READ ON CONNECTION drop_zone TO ana").await.unwrap();
        may("READ").await?;
        assert!(may("WRITE").await.is_err(), "READ isn't WRITE");
        _ = std::fs::remove_dir_all(&dir);
        Ok(())
    }

    /// A type for tables is its own extension's entry (`read_delta`, `catalogs`, `kafka_client`, `sharing_client`), and
    /// ATTACH reads the same entries: its TYPE list and each type's options are CREATE CONNECTION's.
    #[tokio::test]
    async fn attach_and_create_connection_read_one_list_of_types() -> Result<()> {
        let lake = lake().await?;
        run(&lake, "CREATE CONNECTION tables TYPE delta WITH (url = 's3://bucket/tables', read_only = true)").await.unwrap();
        assert_eq!(shown("tables", &lake.cat.get::<Value>(&key("tables")).await?.expect("kept")), "CREATE CONNECTION tables TYPE delta WITH (read_only = true, url = 's3://bucket/tables')");
        assert_eq!(code(run(&lake, "CREATE CONNECTION k TYPE kafka WITH (url = 's3://b')").await).1, "s3://b: a Kafka cluster, its topics read as tables: url = 'kafka://broker:9092'");
        let said = |sql: &'static str| {
            let lake = lake.clone();
            async move { code(run(&lake, sql).await) }
        };
        let types = "delta, iceberg, kafka or share (or a Pondra lake, with no TYPE)";
        assert_eq!(said("ATTACH 's3://b' AS x (TYPE parquet)").await, ("22023".into(), format!("ATTACH … (TYPE parquet): {types}")));
        assert_eq!(said("ATTACH 's3://b' AS x (TYPE s3)").await.1, format!("ATTACH … (TYPE s3): {types}"), "a type for files isn't attached");
        assert_eq!(said("ATTACH 'pg://h' AS x (TYPE postgres)").await.0, "0A000");
        assert_eq!(said("ATTACH 'kafka://b:9092' AS x (TYPE delta)").await.1, "ATTACH 'kafka://b:9092' … (TYPE delta): a kafka:// URL is TYPE kafka's");
        assert_eq!(said("ATTACH 's3://b' AS x (TYPE kafka)").await.1, "ATTACH … (TYPE kafka) takes a kafka:// URL: a Kafka cluster, its topics read as tables: url = 'kafka://broker:9092'");
        assert_eq!(said("ATTACH 's3://b' AS x (TYPE delta, ENDPOINT 'https://c')").await, ("22023".into(), "ATTACH … (TYPE delta) has no option endpoint: secret, read_only (endpoint is TYPE iceberg's)".into()));
        assert_eq!(said("ATTACH 's3://b' AS x (TYPE iceberg, FORMAT x)").await.1, "ATTACH … (TYPE iceberg) has no option format: endpoint, secret, read_only, endpoint_type, access_delegation_mode");
        assert_eq!(said("ATTACH 's3://b' AS x (TYPE delta, URL 's3://c')").await.1, "ATTACH … (TYPE delta) has no option url: secret, read_only", "its url is ATTACH's own");
        assert_eq!(run(&lake, "ATTACH 's3://b/t' AS x (TYPE delta, READ_ONLY)").await.unwrap()[0]["type"], "delta");
        Ok(())
    }

    #[test]
    fn the_statements_are_read_in_their_forms() {
        let tree = |sql: &str| match crate::write::parse(sql) {
            Some(Stmt::Ddl(ds)) => match &ds[..] {
                [crate::ddl::Ddl::Entry { words, object, tree }] => (words.clone(), object.clone(), tree.clone()),
                _ => panic!("one entry"),
            },
            _ => panic!("{sql}: an entry"),
        };
        let (words, object, t) = tree("/* made */ create or replace connection \"Drop\" type S3 with (url = 's3://b', secret = K, n = 2, tls = true);");
        assert_eq!((words.as_str(), object.as_str()), ("CREATE OR REPLACE CONNECTION", "Drop"));
        assert_eq!(t, j!({"name": "Drop", "type": "s3", "options": {"url": "s3://b", "secret": "k", "n": 2, "tls": true}, "replace": true, "if_not_exists": false}));
        assert_eq!(tree("ALTER CONNECTION IF EXISTS d RENAME TO e").2, j!({"name": "d", "set": {}, "reset": [], "rename": "e", "if_exists": true}));
        assert_eq!(tree("ALTER CONNECTION d RESET (url, secret)").2["reset"], j!(["url", "secret"]));
        assert_eq!(tree("DROP CONNECTION IF EXISTS d").0, "DROP CONNECTION");
        for (sql, said) in [
            ("CREATE CONNECTION d", "CREATE CONNECTION d TYPE t: this build's types are azure, delta, folder, gcs, iceberg, kafka, pondra, s3 or share"),
            ("CREATE CONNECTION d TYPE s3 WITH (url)", "CREATE [OR REPLACE] CONNECTION [IF NOT EXISTS] name TYPE t [WITH (option = value, …)]"),
            ("ALTER CONNECTION d", "ALTER CONNECTION [IF EXISTS] name SET (option = value, …) | RESET (option, …) | RENAME TO new_name"),
            ("DROP CONNECTION d e", "DROP CONNECTION [IF EXISTS] name"),
        ] {
            match crate::write::parse(sql) {
                Some(Stmt::Refused("42601", e)) => assert_eq!(e, said),
                _ => panic!("{sql}: a syntax error"),
            }
        }
        match crate::write::parse("CREATE CONNECTION d TYPE s3 WITH (url = 'a', url = 'b')") {
            Some(Stmt::Refused("22023", e)) => assert_eq!(e, "url is given twice"),
            _ => panic!("refused"),
        }
    }
}
