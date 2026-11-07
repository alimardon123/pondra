//! The registry's connection types section (designs/redesign/redesign.md §6, designs/etl-and-bi-tools.md §3.7): what a
//! connection of each type holds, the options it takes, the places it reaches and the secrets it may name. The
//! `connections` extension (`connect.rs`) adds the kind and its statements and reads this section to check a
//! connection's definition; ATTACH and the paths that name a connection read it too (l1-wip/step4-plan.md, pieces 3
//! and 4), so a new type is one entry and no other edit.
use crate::registry::Section;
use anyhow::Result;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;
use std::sync::Arc;

/// What a connection of a type holds, and so how it is used (§3.7's rows).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Holds {
    /// Files: a path that starts with its name, `'@drop/incoming/*.csv'`.
    Files,
    /// Tables: by name once attached, `shop.public.orders`.
    Tables,
    /// Calls: a URL that starts with its name, `http_get('@rates/latest')`.
    Calls,
    /// Messages: a stream or a channel, read and written by name.
    Messages,
}

impl Holds {
    pub fn word(self) -> &'static str {
        match self {
            Holds::Files => "files",
            Holds::Tables => "tables",
            Holds::Calls => "calls",
            Holds::Messages => "messages",
        }
    }
}

/// One option a type takes.
pub struct Opt {
    pub name: &'static str,
    pub required: bool,
    pub about: &'static str,
}

/// Where it is: a Files type's folder, a Tables type's catalog or table folder, brokers…
pub const URL: Opt = Opt { name: "url", required: true, about: "where it is" };
/// The secret it signs in with, by its name: its value is the secret's, never the connection's.
pub const SECRET: Opt = Opt { name: "secret", required: false, about: "the secret it signs in with, by name" };
/// Read only: nothing is written through it.
pub const READ_ONLY: Opt = Opt { name: "read_only", required: false, about: "nothing is written through it" };

/// A Files type's store for a connection's `url`, signed with its secret's settings (None: it names none). Its paths are the
/// connection's own: `'@drop/incoming/a.csv'` is `incoming/a.csv` in it.
pub type Opener = fn(url: &str, secret: Option<&BTreeMap<String, String>>) -> Result<Arc<dyn object_store::ObjectStore>>;

/// A type of connection.
pub struct ConnectionType {
    pub name: &'static str,
    pub holds: Holds,
    /// Every option it takes, `url` and `secret` among them, in the order a refusal lists them.
    pub options: &'static [Opt],
    /// The schemes its `url` may have (`s3` for `s3://bucket/…`; `""` for a path with no scheme, a folder's); empty:
    /// any, the type's own reader checks it (a Delta table's folder is in a bucket or on the machine).
    pub schemes: &'static [&'static str],
    /// The types of secret its `secret` may name (`CREATE SECRET … (TYPE s3, …)`); empty: it takes none.
    pub secret_types: &'static [&'static str],
    pub about: &'static str,
    /// Its files' store, for a type that holds files and is read through a path (`'@name/…'`); None: not yet, or not files.
    pub open: Option<Opener>,
}

impl ConnectionType {
    /// Its options' names, in their order.
    pub fn option_names(&self) -> Vec<&'static str> {
        self.options.iter().map(|o| o.name).collect()
    }

    /// Is its url's scheme one it takes? (`memory://x`'s is `memory`; a path's is `""`.)
    pub fn reaches(&self, url: &str) -> bool {
        let scheme = url.split_once("://").map_or(String::new(), |(s, _)| s.to_lowercase());
        self.schemes.is_empty() || self.schemes.contains(&scheme.as_str()) || (scheme == "file" && self.schemes.contains(&""))
    }
}

/// A connection as the catalog keeps it (`cn/<name>`): its type and its options, the secret's name among them.
#[derive(Serialize, Deserialize, Clone, PartialEq, Debug)]
pub struct Definition {
    #[serde(rename = "type")]
    pub ty: String,
    pub options: BTreeMap<String, Value>,
}

/// Where the catalog keeps the connection `name`.
pub const PREFIX: &str = "cn/";

pub fn key(name: &str) -> String {
    format!("{PREFIX}{name}")
}

/// The scheme a connection's files are read at: `cn://drop/incoming/a.csv` is `'@drop/incoming/a.csv'`. Its URL never
/// enters the statement, its plan or what it says: the store `cn://drop` is made from the connection (`ext::register`).
pub const SCHEME: &str = "cn";

/// A path that starts with a connection's name, `'@drop/incoming/*.csv'` (or `'@drop'`, its files), as the URL it is read
/// at; None: not such a path.
pub fn place(path: &str) -> Option<String> {
    let rest = path.strip_prefix('@')?;
    let (name, after) = rest.split_once('/').unwrap_or((rest, ""));
    let named = !name.is_empty() && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_');
    named.then(|| format!("{SCHEME}://{}/{after}", name.to_ascii_lowercase()))
}

/// The connection a URL is read through (`cn://drop/…`), by name.
pub fn named_in(url: &str) -> Option<&str> {
    let rest = url.strip_prefix(SCHEME)?.strip_prefix("://")?;
    Some(rest.split_once('/').map_or(rest, |(n, _)| n))
}

/// The registry's connection types: each one's name once.
#[derive(Default)]
pub struct ConnectionTypes(Vec<&'static ConnectionType>);

impl Section for ConnectionTypes {
    const NAME: &'static str = "connection types";
}

impl ConnectionTypes {
    pub fn add(&mut self, t: &'static ConnectionType) {
        assert!(self.0.iter().all(|o| o.name != t.name), "two connection types are named {}", t.name);
        self.0.push(t);
    }

    pub fn all(&self) -> &[&'static ConnectionType] {
        &self.0
    }

    pub fn find(&self, name: &str) -> Option<&'static ConnectionType> {
        self.0.iter().find(|t| t.name == name).copied()
    }
}

/// This build's connection types.
pub fn types() -> &'static [&'static ConnectionType] {
    crate::registry::get().find::<ConnectionTypes>().map_or(&[], |t| t.all())
}

/// This build's type of that name.
pub fn find(name: &str) -> Option<&'static ConnectionType> {
    types().iter().find(|t| t.name == name).copied()
}

/// The names of this build's types that pass `which`, in words and in order: "delta, iceberg, kafka or share".
pub fn named(which: impl Fn(&ConnectionType) -> bool) -> String {
    let mut all: Vec<&str> = types().iter().filter(|t| which(t)).map(|t| t.name).collect();
    all.sort_unstable();
    match all.split_last() {
        None => String::new(),
        Some((last, [])) => last.to_string(),
        Some((last, rest)) => format!("{} or {last}", rest.join(", ")),
    }
}

/// Does ATTACH attach a type's tables as a database? Those that hold tables or messages (a topic is read as a table).
pub fn attached(t: &ConnectionType) -> bool {
    matches!(t.holds, Holds::Tables | Holds::Messages)
}
