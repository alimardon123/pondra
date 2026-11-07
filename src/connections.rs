//! The registry's connection types section (designs/redesign/redesign.md §6, designs/etl-and-bi-tools.md §3.7): what a
//! connection of each type holds, the options it takes, the places it reaches and the secrets it may name. The
//! `connections` extension (`connect.rs`) adds the kind and its statements and reads this section to check a
//! connection's definition; ATTACH and the paths that name a connection read it too (l1-wip/step4-plan.md, pieces 3
//! and 4), so a new type is one entry and no other edit.
use crate::registry::Section;

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

/// A type of connection.
pub struct ConnectionType {
    pub name: &'static str,
    pub holds: Holds,
    /// Its options, beside `secret`, which every type that names `secret_types` takes.
    pub options: &'static [Opt],
    /// The schemes its `url` may have (`s3` for `s3://bucket/…`); empty: a folder's path, with no scheme or `file`.
    pub schemes: &'static [&'static str],
    /// The types of secret it may name (`CREATE SECRET … (TYPE s3, …)`); empty: it takes none.
    pub secret_types: &'static [&'static str],
    pub about: &'static str,
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
