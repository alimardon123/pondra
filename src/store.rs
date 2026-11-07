//! The lake: object storage (a local dir or s3://bucket/prefix on S3, R2, MinIO…) with the
//! catalog (SlateDB) inside it. Nothing else holds state.
//!
//! The leader is the catalog's only writer. It also streams every commit to the followers the
//! moment it is durable, and they lay those changes over their own (slightly behind) view of the
//! catalog: every node sees a commit within milliseconds, while the bucket stays the source of truth.
use anyhow::{bail, Context, Result};
use crate::{finish::Finish, seq::Identity, types::Enum}; // (catalog uses engine: counted once each, tools/layers.py)
use bytes::Bytes;
use datafusion::arrow::datatypes::Schema;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::DataFusionError;
use datafusion::execution::disk_manager::{DiskManager, DiskManagerBuilder, DiskManagerMode};
use datafusion::execution::runtime_env::{RuntimeEnv, RuntimeEnvBuilder};
use datafusion::execution::spill_file::{SpillFile, TempFileFactory};
use datafusion::prelude::{SessionConfig, SessionContext};
use object_store::aws::{AmazonS3Builder, AmazonS3ConfigKey};
use object_store::azure::{AzureConfigKey, MicrosoftAzureBuilder};
use object_store::gcp::{GoogleCloudStorageBuilder, GoogleConfigKey};
use object_store::ClientConfigKey;
#[cfg(not(target_arch = "wasm32"))]
use object_store::local::LocalFileSystem;
use object_store::{path::Path, prefix::PrefixStore, ObjectStore, ObjectStoreExt, PutMode, PutOptions};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use crate::kv::{self, Kv, Written};
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, Mutex, Weak};
use datafusion::common::instant::Instant;
use std::time::Duration;
use tokio::sync::{broadcast, mpsc, oneshot, watch};

pub type Store = Arc<dyn ObjectStore>;

/// A user's file, next to the tables (`PUT /files`): the one kind of lake object that may be
/// replaced or removed, so no cache keeps it (ADR-034). Everything else is written once.
pub fn replaceable(path: &str) -> bool { path.starts_with("files/") }

/// A lake's files and its log: the objects that may go to disk, and that a page may read through its node. The
/// catalog's objects are not among them, since they hold what a page must not.
pub(crate) fn lake_file(key: &str) -> bool { key.starts_with("data/") || key.starts_with("log/") }

fn version_of(m: &object_store::ObjectMeta) -> String {
    match &m.e_tag {
        Some(e) => format!("{}:{}", e.trim_matches('"'), m.size),
        None => format!("@{}:{}", m.last_modified.timestamp_micros(), m.size),
    }
}

/// A file replaced since the version an editor read (`If-Match`): `PUT /files` answers 412.
#[derive(Debug)]
pub struct Changed;
impl std::fmt::Display for Changed {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result { f.write_str(crate::lake_files::CHANGED) }
}
impl std::error::Error for Changed {}

/// A table's column fields by stored name, format 5's shape: what a writer outside store.rs hands over to make an entry
/// (`TableMeta::made`) and what an entry written below lake format 6 is read from (`Record`). The entry keeps the list
/// (`Stored`), and readers read it.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Made {
    pub columns: Vec<(String, String)>,
    pub names: BTreeMap<String, String>,
    pub dropped: Vec<String>,
    pub shown: Vec<String>,
    pub not_null: Vec<String>,
    pub defaults: BTreeMap<String, String>,
    pub identity: BTreeMap<String, Identity>,
    pub enums: BTreeMap<String, Enum>,
    pub sql_types: BTreeMap<String, String>,
    /// Each column's Arrow type as SQL reads it, by stored name, where its files hold another (`Col::sql_type`): filled
    /// only for another engine's table (`ext::table_meta`).
    pub logical_types: BTreeMap<String, String>,
}

#[cfg(test)]
impl Made {
    fn name_of<'a>(&'a self, stored: &'a str) -> &'a str { self.names.get(stored).map_or(stored, String::as_str) }
}

/// The column edits as piece 6a made them, on the fields by stored name: what the list's own edits (`Stored`'s) are held
/// to, over random sequences of them (`the_list_edits_leave_the_entry_as_the_field_edits_did`).
#[cfg(test)]
impl Made {
    fn add_column(&mut self, name: &str, dtype: &str, held: Option<String>, labels: Option<Enum>) -> String {
        let taken = |s: &str| self.columns.iter().any(|(n, _)| n == s);
        let stored = (1..).map(|i| if i == 1 { name.to_string() } else { format!("{name}~{i}") }).find(|s| !taken(s)).expect("a free name");
        if stored != name {
            self.names.insert(stored.clone(), name.to_string());
        }
        if let Some(e) = labels {
            self.enums.insert(stored.clone(), e); // (ALTER TABLE … ADD COLUMN c mood)
        }
        if let Some(t) = held {
            self.sql_types.insert(stored.clone(), t); // (ALTER TABLE … ADD COLUMN c UUID)
        }
        self.columns.push((stored.clone(), dtype.to_string()));
        stored
    }

    fn rename_column(&mut self, stored: &str, to: &str) {
        match to == stored {
            true => self.names.remove(stored),
            false => self.names.insert(stored.to_string(), to.to_string()),
        };
    }

    fn drop_column(&mut self, stored: &str) {
        self.names.remove(stored);
        self.dropped.push(stored.to_string());
    }

    fn retype_column(&mut self, stored: &str, dtype: String) {
        if let Some(c) = self.columns.iter_mut().find(|(c, _)| c == stored) {
            c.1 = dtype;
        }
    }

    fn hold_column(&mut self, stored: &str, held: Option<String>) {
        match held {
            Some(t) => _ = self.sql_types.insert(stored.to_string(), t),
            None => {
                self.sql_types.remove(stored);
                self.enums.remove(stored);
            }
        }
    }

    fn set_order(&mut self, stored: Vec<String>) {
        let as_stored: Vec<&str> = self.columns.iter().map(|(s, _)| s.as_str()).filter(|s| !self.dropped.iter().any(|d| d == s) && *s != "_deleted").collect();
        self.shown = match as_stored.starts_with(&stored.iter().map(String::as_str).collect::<Vec<_>>()) {
            true => vec![],
            false => stored,
        };
    }

    fn set_default(&mut self, stored: &str, expr: Option<String>) {
        match expr {
            Some(e) => _ = self.defaults.insert(stored.to_string(), e),
            None => _ = self.defaults.remove(stored),
        }
    }

    fn set_not_null(&mut self, stored: &str, not_null: bool) {
        self.not_null.retain(|c| c != stored);
        if not_null {
            self.not_null.push(stored.to_string());
        }
    }
}

/// One column of a table as its entry stores it from lake format 6 (redesign 1b, piece 6b): its id (its Parquet field id,
/// its stored place + 1) and every fact about it, where before format 6 each fact was a map of its own by stored name.
/// Written as `[name, type]`, then `"not null"` and `"dropped"` when they hold, then an object of the facts that aren't
/// at their default (`Facts`), so the list is no bigger than format 5's fields (`entry_size`).
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Col {
    pub id: u32,
    /// The name SQL knows it by.
    pub name: String,
    /// The name its files and the log hold it under, when it was renamed (otherwise `name`).
    pub stored: Option<String>,
    /// Its Arrow type, as text: its files' (`sql_type` says SQL's, when SQL reads another).
    pub dtype: String,
    /// Its Arrow type as SQL reads it, when its files hold another: a mapped Delta table's struct, whose fields its files
    /// keep under their physical names (`ext::table_meta`), read under SQL's (`query::named`). Read through `sql_type()`.
    pub sql_type: Option<String>,
    pub dropped: bool,
    pub not_null: bool,
    /// `DEFAULT expr`, the SQL expression.
    pub default: Option<String>,
    pub identity: Option<Identity>,
    /// The type a text column is held as (uuid, json, varchar(n): `types.rs`).
    pub held: Option<String>,
    /// An enum column's labels and type.
    pub labels: Option<Enum>,
    /// What it means, beside what it is (`About`).
    pub about: Option<Box<About>>,
}

/// What a column means, beside what it is, by key: the semantic layer plan's *about* (a label, synonyms, a format, a
/// unit, instructions, tags; designs/semantic-layer-plan.md §5), each key's value as JSON. It is kept on the column
/// record, so a rename, a drop, a clone, a `logical()` copy and a description keep it, and no side map by name can lose
/// it. Nothing sets it yet: its keys come with the plan's phase S1. Format 6's list writes it with the column's facts.
/// Format 5's fields have no place for it, so a lake below format 6 never holds one (S1 refuses to set it there), and an
/// entry written in that shape (a new one, a `logical()` copy) leaves it out.
pub type About = BTreeMap<String, serde_json::Value>;

/// A table's columns as its entry stores them from lake format 6: every column, dropped ones too, in field-id order, and
/// SQL's order of them by id when it isn't the stored one (before format 6, `shown`). Written as `[columns]`, or
/// `[columns, order]` when a column was moved; a column's id only where it isn't its place + 1.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Stored {
    pub columns: Vec<Col>,
    pub order: Vec<u32>,
}

const NOT_NULL: &str = "not null";
const DROPPED: &str = "dropped";
/// `TableMeta::placed` of no columns: FNV-1a's offset basis.
const PLACED_NONE: u64 = 0xcbf29ce484222325;

/// A column's facts past its name, type and flags, as its entry writes them: each left out at its default (the id too,
/// where it is the column's place + 1). (A missing one reads as None: no `default`, which would ask `Default` of each.)
#[derive(Serialize, Deserialize)]
struct Facts<S, I, E, A> {
    #[serde(skip_serializing_if = "Option::is_none")]
    id: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    stored: Option<S>,
    #[serde(skip_serializing_if = "Option::is_none")]
    default: Option<S>,
    #[serde(skip_serializing_if = "Option::is_none")]
    identity: Option<I>,
    #[serde(skip_serializing_if = "Option::is_none")]
    held: Option<S>,
    #[serde(skip_serializing_if = "Option::is_none")]
    labels: Option<E>,
    #[serde(skip_serializing_if = "Option::is_none")]
    sql_type: Option<S>,
    #[serde(skip_serializing_if = "Option::is_none")]
    about: Option<A>,
}

/// A column at its place in the list, for writing: its id is written only when it isn't `place`.
struct At<'a>(&'a Col, u32);

impl Serialize for At<'_> {
    fn serialize<S: serde::Serializer>(&self, s: S) -> std::result::Result<S::Ok, S::Error> {
        use serde::ser::SerializeSeq;
        let (c, place) = (self.0, self.1);
        let facts = Facts { id: (c.id != place).then_some(c.id), stored: c.stored.as_ref(), default: c.default.as_ref(), identity: c.identity.as_ref(), held: c.held.as_ref(), labels: c.labels.as_ref(), sql_type: c.sql_type.as_ref(), about: c.about.as_deref() };
        let any = facts.id.is_some() || c.stored.is_some() || c.default.is_some() || c.identity.is_some() || c.held.is_some() || c.labels.is_some() || c.sql_type.is_some() || c.about.is_some();
        let mut seq = s.serialize_seq(Some(2 + c.not_null as usize + c.dropped as usize + any as usize))?;
        seq.serialize_element(&c.name)?;
        seq.serialize_element(&c.dtype)?;
        if c.not_null {
            seq.serialize_element(NOT_NULL)?;
        }
        if c.dropped {
            seq.serialize_element(DROPPED)?;
        }
        if any {
            seq.serialize_element(&facts)?;
        }
        seq.end()
    }
}

impl Serialize for Stored {
    fn serialize<S: serde::Serializer>(&self, s: S) -> std::result::Result<S::Ok, S::Error> {
        use serde::ser::SerializeSeq;
        struct Cols<'a>(&'a [Col]);
        impl Serialize for Cols<'_> {
            fn serialize<S: serde::Serializer>(&self, s: S) -> std::result::Result<S::Ok, S::Error> { s.collect_seq(self.0.iter().zip(1..).map(|(c, place)| At(c, place))) }
        }
        let mut seq = s.serialize_seq(Some(1 + !self.order.is_empty() as usize))?;
        seq.serialize_element(&Cols(&self.columns))?;
        if !self.order.is_empty() {
            seq.serialize_element(&self.order)?;
        }
        seq.end()
    }
}

/// What follows a column's name and type: a flag, or its facts.
enum Rest {
    NotNull,
    Dropped,
    Facts(Facts<String, Identity, Enum, About>),
}

impl<'de> Deserialize<'de> for Rest {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> std::result::Result<Rest, D::Error> {
        struct V;
        impl<'de> serde::de::Visitor<'de> for V {
            type Value = Rest;
            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result { write!(f, "\"{NOT_NULL}\", \"{DROPPED}\" or a column's facts") }
            fn visit_str<E: serde::de::Error>(self, v: &str) -> std::result::Result<Rest, E> {
                match v {
                    NOT_NULL => Ok(Rest::NotNull),
                    DROPPED => Ok(Rest::Dropped),
                    _ => Err(E::unknown_variant(v, &[NOT_NULL, DROPPED])),
                }
            }
            fn visit_map<M: serde::de::MapAccess<'de>>(self, m: M) -> std::result::Result<Rest, M::Error> { Deserialize::deserialize(serde::de::value::MapAccessDeserializer::new(m)).map(Rest::Facts) }
        }
        d.deserialize_any(V)
    }
}

impl<'de> Deserialize<'de> for Col {
    /// A column as `At` writes it, its id 0 when the list gives it none (`Stored` gives it its place's).
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> std::result::Result<Col, D::Error> {
        struct V;
        impl<'de> serde::de::Visitor<'de> for V {
            type Value = Col;
            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result { f.write_str("a column: [name, type, flags…, facts]") }
            fn visit_seq<A: serde::de::SeqAccess<'de>>(self, mut seq: A) -> std::result::Result<Col, A::Error> {
                use serde::de::Error;
                let name = seq.next_element::<String>()?.ok_or_else(|| A::Error::invalid_length(0, &self))?;
                let dtype = seq.next_element::<String>()?.ok_or_else(|| A::Error::invalid_length(1, &self))?;
                let mut c = Col { name, dtype, ..Default::default() };
                while let Some(r) = seq.next_element::<Rest>()? {
                    match r {
                        Rest::NotNull => c.not_null = true,
                        Rest::Dropped => c.dropped = true,
                        Rest::Facts(f) => {
                            c.id = f.id.unwrap_or_default();
                            (c.stored, c.default, c.identity, c.held, c.labels, c.sql_type, c.about) = (f.stored, f.default, f.identity, f.held, f.labels, f.sql_type, f.about.map(Box::new));
                        }
                    }
                }
                Ok(c)
            }
        }
        d.deserialize_seq(V)
    }
}

impl<'de> Deserialize<'de> for Stored {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> std::result::Result<Stored, D::Error> {
        struct V;
        impl<'de> serde::de::Visitor<'de> for V {
            type Value = Stored;
            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result { f.write_str("a table's columns: [columns] or [columns, order]") }
            fn visit_seq<A: serde::de::SeqAccess<'de>>(self, mut seq: A) -> std::result::Result<Stored, A::Error> {
                let mut columns: Vec<Col> = seq.next_element()?.ok_or_else(|| serde::de::Error::invalid_length(0, &self))?;
                let order = seq.next_element()?.unwrap_or_default();
                for (c, place) in columns.iter_mut().zip(1..) {
                    if c.id == 0 {
                        c.id = place;
                    }
                }
                Ok(Stored { columns, order })
            }
        }
        d.deserialize_seq(V)
    }
}

impl Stored {
    /// The list from the fields by stored name. Facts for a name no column has are passed over, as every reader did.
    /// The tests' check of `from_fields`, which moves the fields in rather than copying them.
    #[cfg(test)]
    pub fn of(m: &Made) -> Stored {
        // (sets and places, not scans: a wide table's 500 columns against its drops and order were 0.1 ms an entry)
        fn set(v: &[String]) -> std::collections::HashSet<&str> { v.iter().map(String::as_str).collect() }
        let (dropped, not_null) = (set(&m.dropped), set(&m.not_null));
        let columns = m
            .columns
            .iter()
            .enumerate()
            .map(|(i, (s, t))| Col {
                id: i as u32 + 1,
                name: m.name_of(s).to_string(),
                stored: m.names.contains_key(s).then(|| s.clone()),
                dtype: t.clone(),
                dropped: dropped.contains(s.as_str()),
                not_null: not_null.contains(s.as_str()),
                default: m.defaults.get(s).cloned(),
                identity: m.identity.get(s).cloned(),
                held: m.sql_types.get(s).cloned(),
                labels: m.enums.get(s).cloned(),
                sql_type: m.logical_types.get(s).cloned(),
                about: None,
            })
            .collect();
        let mut place = std::collections::HashMap::with_capacity(if m.shown.is_empty() { 0 } else { m.columns.len() });
        if !m.shown.is_empty() {
            for (i, (c, _)) in m.columns.iter().enumerate() {
                place.entry(c.as_str()).or_insert(i as u32 + 1); // (the first, as a scan finds it)
            }
        }
        let order = m.shown.iter().filter_map(|s| place.get(s.as_str()).copied()).collect();
        Stored { columns, order }
    }

    /// `of`, taking the fields: each name, type and fact moved into its column rather than copied (an old-shaped
    /// entry as it is read: format 5's wide entry decoded and listed in about 0.3 ms with copies, where 6b-2 took 0.23).
    pub fn from_fields(mut m: Made) -> Stored {
        fn set(v: &[String]) -> std::collections::HashSet<&str> { v.iter().map(String::as_str).collect() }
        let order = match m.shown.is_empty() {
            true => vec![],
            false => {
                let mut place = std::collections::HashMap::with_capacity(m.columns.len());
                for (i, (c, _)) in m.columns.iter().enumerate() {
                    place.entry(c.as_str()).or_insert(i as u32 + 1); // (the first, as `of`'s scan finds it)
                }
                m.shown.iter().filter_map(|s| place.get(s.as_str()).copied()).collect()
            }
        };
        let (dropped, not_null) = (std::mem::take(&mut m.dropped), std::mem::take(&mut m.not_null));
        let (dropped, not_null) = (set(&dropped), set(&not_null));
        let columns = std::mem::take(&mut m.columns)
            .into_iter()
            .enumerate()
            .map(|(i, (s, dtype))| {
                let (is_dropped, is_not_null) = (dropped.contains(s.as_str()), not_null.contains(s.as_str()));
                let (default, identity, held, labels, sql_type) = (m.defaults.remove(&s), m.identity.remove(&s), m.sql_types.remove(&s), m.enums.remove(&s), m.logical_types.remove(&s));
                let (name, stored) = match m.names.remove(&s) {
                    Some(n) => (n, Some(s)),
                    None => (s, None),
                };
                Col { id: i as u32 + 1, name, stored, dtype, sql_type, dropped: is_dropped, not_null: is_not_null, default, identity, held, labels, about: None }
            })
            .collect();
        Stored { columns, order }
    }

    /// The fields by stored name from the list: `of`'s inverse (the dropped and NOT NULL columns in id order). The tests'
    /// reference for what `Record` writes from the list in format 5's shape.
    #[cfg(test)]
    pub fn fields(&self) -> Made {
        let mut m = Made { columns: Vec::with_capacity(self.columns.len()), ..Default::default() };
        for c in &self.columns {
            let s = c.stored.as_ref().unwrap_or(&c.name);
            m.columns.push((s.clone(), c.dtype.clone()));
            if c.stored.is_some() {
                m.names.insert(s.clone(), c.name.clone());
            }
            if c.dropped {
                m.dropped.push(s.clone());
            }
            if c.not_null {
                m.not_null.push(s.clone());
            }
            if let Some(d) = &c.default {
                m.defaults.insert(s.clone(), d.clone());
            }
            if let Some(i) = &c.identity {
                m.identity.insert(s.clone(), i.clone());
            }
            if let Some(t) = &c.held {
                m.sql_types.insert(s.clone(), t.clone());
            }
            if let Some(e) = &c.labels {
                m.enums.insert(s.clone(), e.clone());
            }
            if let Some(t) = &c.sql_type {
                m.logical_types.insert(s.clone(), t.clone());
            }
        }
        m.shown = self.order.iter().filter_map(|id| self.columns.get(self.place(*id)?)).map(|c| c.stored_name().to_string()).collect();
        m
    }

    /// The list for readers (`column.rs`): each column by id, SQL's order as places.
    fn column_list(&self) -> crate::column::Columns {
        let by_id = self.columns.iter().map(|c| crate::column::Column { id: c.id, name: c.name.clone(), stored: c.stored_name().to_string(), dtype: c.dtype.clone(), dropped: c.dropped }).collect();
        crate::column::Columns::from_parts(by_id, self.sql_places())
    }

    /// The place of the column whose id is `id`: `id - 1` in every list written so far, else found by its id. The format
    /// writes an id only where it isn't the column's place + 1, so nothing takes one for the other: the one reading of an
    /// id (`order`'s, `column::Columns::by_id`'s).
    fn place(&self, id: u32) -> Option<usize> {
        let at = (id as usize).checked_sub(1)?;
        match self.columns.get(at) {
            Some(c) if c.id == id => Some(at),
            _ => self.columns.iter().position(|c| c.id == id),
        }
    }

    /// The id a new column takes: one past the highest (its place + 1, while every id is).
    fn next_id(&self) -> u32 { self.columns.iter().map(|c| c.id).max().unwrap_or(0) + 1 }

    /// The list of a table's replaced rows (`sys::deleted`): `of`'s columns as stored, each with its id, names, type and
    /// drop mark but none of its facts (no write is checked there), in stored order, then `_old_version`.
    pub(crate) fn replaced(of: &Stored) -> Stored {
        let mut columns: Vec<Col> = of.columns.iter().map(|c| Col { id: c.id, name: c.name.clone(), stored: c.stored.clone(), dtype: c.dtype.clone(), dropped: c.dropped, ..Default::default() }).collect();
        columns.push(Col { id: of.next_id(), name: "_old_version".into(), dtype: "Int64".into(), ..Default::default() });
        Stored { columns, order: vec![] }
    }

    /// The live columns' places in SQL's order: those `order` names first, then the others as stored.
    fn sql_places(&self) -> Vec<usize> {
        let first: Vec<usize> = self.order.iter().filter_map(|id| self.place(*id)).collect();
        crate::column::places(self.columns.len(), |i| self.columns[i].dropped, &first)
    }

    /// The column stored as `stored` (a dropped one too).
    fn col(&self, stored: &str) -> Option<&Col> { self.columns.iter().find(|c| c.stored_name() == stored) }

    /// `col` for names that come in stored order, as a file's or a batch's fields mostly do: one pass for all of them.
    fn lookup(&self) -> Lookup<'_> { Lookup { columns: &self.columns, next: 0 } }

    /// SQL's view of the list (`TableMeta::logical`): the live columns alone, under SQL's names, each with its facts, and
    /// SQL's order of them. Its places are no longer ids, so it is never written as a list.
    fn logical(&self) -> Stored {
        let mut ids = vec![0; self.columns.len()];
        let columns = self
            .columns
            .iter()
            .zip(&mut ids)
            .filter(|(c, _)| !c.dropped)
            .zip(1..)
            .map(|((c, id), place)| {
                *id = place;
                Col { id: place, stored: None, ..c.clone() }
            })
            .collect();
        let order = self.order.iter().filter_map(|id| ids.get(self.place(*id)?).copied().filter(|&p| p > 0)).collect();
        Stored { columns, order }
    }

    // The edits to a table's columns, on the list itself (redesign 1b, piece 6b-3): each names a column by its stored
    // name, as piece 6a's did on the fields by stored name, and leaves the list those edits' fields would make
    // (`the_list_edits_leave_the_entry_as_the_field_edits_did`).

    /// The column stored as `stored`.
    fn at(&mut self, stored: &str) -> Option<&mut Col> { self.columns.iter_mut().find(|c| c.stored_name() == stored) }

    fn add_column(&mut self, name: &str, dtype: &str, held: Option<String>, labels: Option<Enum>) -> String {
        let taken = |s: &str| self.columns.iter().any(|c| c.stored_name() == s);
        let stored = (1..).map(|i| if i == 1 { name.to_string() } else { format!("{name}~{i}") }).find(|s| !taken(s)).expect("a free name");
        let id = self.next_id();
        self.columns.push(Col { id, name: name.to_string(), stored: (stored != name).then(|| stored.clone()), dtype: dtype.to_string(), held, labels, ..Default::default() });
        stored
    }

    fn rename_column(&mut self, stored: &str, to: &str) {
        if let Some(c) = self.at(stored) {
            (c.name, c.stored) = match to == stored {
                true => (stored.to_string(), None),
                false => (to.to_string(), Some(stored.to_string())),
            };
        }
    }

    fn drop_column(&mut self, stored: &str) {
        if let Some(c) = self.at(stored) {
            (c.name, c.stored, c.dropped) = (stored.to_string(), None, true);
        }
    }

    fn retype_column(&mut self, stored: &str, dtype: String) {
        if let Some(c) = self.at(stored) {
            c.dtype = dtype;
        }
    }

    fn hold_column(&mut self, stored: &str, held: Option<String>) {
        if let Some(c) = self.at(stored) {
            if held.is_none() {
                c.labels = None;
            }
            c.held = held;
        }
    }

    fn set_order(&mut self, stored: Vec<String>) {
        let as_stored: Vec<&str> = self.columns.iter().filter(|c| !c.dropped && c.stored_name() != "_deleted").map(Col::stored_name).collect();
        self.order = match as_stored.starts_with(&stored.iter().map(String::as_str).collect::<Vec<_>>()) {
            true => vec![],
            false => stored.iter().filter_map(|s| self.columns.iter().find(|c| c.stored_name() == s)).map(|c| c.id).collect(),
        };
    }

    fn set_default(&mut self, stored: &str, expr: Option<String>) {
        if let Some(c) = self.at(stored) {
            c.default = expr;
        }
    }

    fn set_not_null(&mut self, stored: &str, not_null: bool) {
        if let Some(c) = self.at(stored) {
            c.not_null = not_null;
        }
    }

    fn append(&mut self, more: impl IntoIterator<Item = (String, String)>) {
        for (name, dtype) in more {
            let id = self.next_id();
            self.columns.push(Col { id, name, dtype, ..Default::default() });
        }
    }
}

/// Columns found by stored name, each first looked for after the last one found (`Stored::lookup`).
struct Lookup<'a> {
    columns: &'a [Col],
    next: usize,
}

impl<'a> Lookup<'a> {
    fn find(&mut self, stored: &str) -> Option<&'a Col> {
        let i = match self.columns.get(self.next) {
            Some(c) if c.stored_name() == stored => self.next,
            _ => self.columns.iter().position(|c| c.stored_name() == stored)?,
        };
        self.next = i + 1;
        Some(&self.columns[i])
    }
}

impl Col {
    /// The name its files and the log hold it under.
    pub fn stored_name(&self) -> &str { self.stored.as_deref().unwrap_or(&self.name) }

    /// Its Arrow type as SQL reads it: `sql_type`, else its files' (`dtype`).
    pub fn sql_type(&self) -> &str { self.sql_type.as_deref().unwrap_or(&self.dtype) }
}

/// A table's columns as its entry keeps them in memory (redesign 1b, piece 6b): the list (`Stored`), the one shape the
/// column edits and the readers work on, and the shape the entry is written in.
///
/// An entry read as its list is written as its list. An entry read in format 5's shape is written in it again, and so is
/// a new entry and a `logical()` copy: format 5's fields by stored name (`Made`), `columns` always (a 0.80 node needs the
/// field). A lake is format 6 only once every node knows format 6 (`format::raise`), so whatever a 0.80 node reads in a
/// rolling upgrade is in the shape it knows: a table's entry (`t/`), a dropped table's (`dt/`), and what one node hands
/// another (a job, a filing, a query's tables). `Catalog::write` gives each `t/` entry its lake's shape as it commits.
///
/// Writing format 5's fields is the rule for upgrades from 0.80, not the end state: the release that no longer upgrades a
/// lake from 0.80 writes every entry as its list, and this encoder goes (ADR-039).
#[derive(Clone, Default)]
struct Record {
    list: Stored,
    /// Written as the list (format 6's shape), not as format 5's fields.
    listed: bool,
    /// The columns' places by their stored names, for `col` on a wide table: made once, kept by a clone, emptied by an
    /// edit.
    by_stored: crate::memo::Shared<datafusion::common::HashMap<Box<str>, u32>>,
    /// The renamed and the dropped columns, made once from the list: few, or none, on most tables.
    marked: crate::memo::Shared<Marked>,
}

/// The renamed and the dropped columns (`Record::marked`), each by stored name and place, in order of stored name: what
/// `name_of`, `is_dropped` and `mapped` search. (The names kept beside the places: a search reads them without going to
/// each column, as the fields' maps were read.)
#[derive(Default)]
struct Marked {
    renamed: Vec<(Box<str>, u32)>,
    dropped: Vec<(Box<str>, u32)>,
}

/// Up to this many columns, `col` looks through them (quicker than its index, and nothing kept).
const SCANNED: usize = 16;

impl Record {
    /// The list of these fields, to be written in their shape again.
    fn of(m: Made) -> Record { Record { list: Stored::from_fields(m), ..Default::default() } }

    /// The list, written as it or not.
    fn new(list: Stored, listed: bool) -> Record { Record { list, listed, ..Default::default() } }

    /// A decoded entry's: the list it was written as, or the list of its format 5's fields.
    fn read(list: Option<Stored>, m: Made) -> Record {
        match list {
            Some(list) => Record::new(list, true),
            None => Record::of(m),
        }
    }

    /// The column stored as `stored`: looked through, or found by its index on a wide table.
    fn col(&self, stored: &str) -> Option<&Col> {
        let c = &self.list.columns;
        if c.len() <= SCANNED {
            return self.list.col(stored);
        }
        let by = self.by_stored.get_ref(|| c.iter().zip(0..).map(|(c, i)| (c.stored_name().into(), i)).collect());
        Some(&c[*by.get(stored)? as usize])
    }

    fn marked(&self) -> &Marked {
        self.marked.get_ref(|| {
            let places = |f: fn(&Col) -> bool| {
                let mut places: Vec<(Box<str>, u32)> = self.list.columns.iter().zip(0..).filter(|(c, _)| f(c)).map(|(c, i)| (c.stored_name().into(), i)).collect();
                places.sort_unstable();
                places
            };
            Marked { renamed: places(|c| c.stored.is_some()), dropped: places(|c| c.dropped) }
        })
    }

    /// Of `marked`'s renamed or dropped columns, the one stored as `stored`.
    fn among(&self, marked: &[(Box<str>, u32)], stored: &str) -> Option<&Col> {
        let i = marked.binary_search_by(|(s, _)| s.as_ref().cmp(stored)).ok()?;
        Some(&self.list.columns[marked[i].1 as usize])
    }

    fn edit<R>(&mut self, f: impl FnOnce(&mut Stored) -> R) -> R {
        (self.by_stored, self.marked) = Default::default();
        f(&mut self.list)
    }
}

impl Serialize for Record {
    /// As a part of its entry (`#[serde(flatten)]`): `"list"`, or format 5's fields, `columns` first and always, each
    /// other one only when it holds anything, as 0.80 wrote them. Format 5's are borrowed from the list as they are
    /// written (`Fields`): no name or fact is copied.
    fn serialize<S: serde::Serializer>(&self, s: S) -> std::result::Result<S::Ok, S::Error> {
        use serde::ser::SerializeMap;
        let (mut m, l) = (s.serialize_map(None)?, &self.list);
        if self.listed {
            m.serialize_entry("list", l)?;
            return m.end();
        }
        m.serialize_entry("columns", &Each(|| l.columns.iter().map(|c| (c.stored_name(), c.dtype.as_str()))))?;
        let f = Fields::of(l);
        macro_rules! unless_empty {
            ($($field:ident),*) => { $(if !f.$field.is_empty() { m.serialize_entry(stringify!($field), &f.$field)?; })* };
        }
        unless_empty!(names, dropped, shown, not_null, defaults, identity, enums, sql_types, logical_types);
        m.end()
    }
}

/// Format 5's fields other than `columns`, borrowed from a list as `Record` writes them: what `Stored::fields` makes,
/// each set of columns in id order and each map in order of stored name.
#[derive(Default)]
struct Fields<'a> {
    names: BTreeMap<&'a str, &'a str>,
    dropped: Vec<&'a str>,
    shown: Vec<&'a str>,
    not_null: Vec<&'a str>,
    defaults: BTreeMap<&'a str, &'a str>,
    identity: BTreeMap<&'a str, &'a Identity>,
    enums: BTreeMap<&'a str, &'a Enum>,
    sql_types: BTreeMap<&'a str, &'a str>,
    logical_types: BTreeMap<&'a str, &'a str>,
}

impl<'a> Fields<'a> {
    fn of(l: &'a Stored) -> Fields<'a> {
        let mut f = Fields::default();
        for c in &l.columns {
            let s = c.stored_name();
            if c.stored.is_some() {
                f.names.insert(s, &c.name);
            }
            if c.dropped {
                f.dropped.push(s);
            }
            if c.not_null {
                f.not_null.push(s);
            }
            if let Some(d) = &c.default {
                f.defaults.insert(s, d);
            }
            if let Some(i) = &c.identity {
                f.identity.insert(s, i);
            }
            if let Some(t) = &c.held {
                f.sql_types.insert(s, t);
            }
            if let Some(e) = &c.labels {
                f.enums.insert(s, e);
            }
            if let Some(t) = &c.sql_type {
                f.logical_types.insert(s, t);
            }
        }
        f.shown = l.order.iter().filter_map(|id| l.columns.get(l.place(*id)?)).map(Col::stored_name).collect();
        f
    }
}

/// A sequence written from what its function gives (a list's columns as format 5's pairs, `Record`).
struct Each<F>(F);

impl<F: Fn() -> I, I: IntoIterator<Item: Serialize>> Serialize for Each<F> {
    fn serialize<S: serde::Serializer>(&self, s: S) -> std::result::Result<S::Ok, S::Error> { s.collect_seq((self.0)()) }
}

/// `TableMeta` as written: the struct, and beside it its decode (redesign 1b, piece 6b-3). `Wire` is the struct's own
/// fields with the column record's two shapes in place of `record` (format 6's `list`, or format 5's nine fields), and
/// `From<Wire>` makes the record once, so an entry decodes in one pass, in either shape, with no `#[serde(flatten)]` on
/// the way in. A new field is added to the struct as to any other: its decode takes it too.
macro_rules! entry {
    ($(#[$m:meta])* pub struct $name:ident { $(#[$rm:meta])* record: Record, $($(#[$fm:meta])* $vis:vis $field:ident: $ty:ty,)* }) => {
        $(#[$m])*
        pub struct $name {
            $(#[$rm])*
            record: Record,
            $($(#[$fm])* $vis $field: $ty,)*
        }

        /// An entry as written, in either shape (`entry!`).
        #[derive(Deserialize)]
        struct Wire {
            #[serde(default)]
            list: Option<Stored>,
            #[serde(default)]
            columns: Vec<(String, String)>,
            #[serde(default)]
            names: BTreeMap<String, String>,
            #[serde(default)]
            dropped: Vec<String>,
            #[serde(default)]
            shown: Vec<String>,
            #[serde(default)]
            not_null: Vec<String>,
            #[serde(default)]
            defaults: BTreeMap<String, String>,
            #[serde(default)]
            identity: BTreeMap<String, Identity>,
            #[serde(default)]
            enums: BTreeMap<String, Enum>,
            #[serde(default)]
            sql_types: BTreeMap<String, String>,
            #[serde(default)]
            logical_types: BTreeMap<String, String>,
            $($(#[$fm])* $field: $ty,)*
        }

        impl From<Wire> for $name {
            fn from(w: Wire) -> $name {
                let (columns, names, dropped, shown, not_null) = (w.columns, w.names, w.dropped, w.shown, w.not_null);
                let made = Made { columns, names, dropped, shown, not_null, defaults: w.defaults, identity: w.identity, enums: w.enums, sql_types: w.sql_types, logical_types: w.logical_types };
                $name { record: Record::read(w.list, made), $($field: w.$field,)* }
            }
        }

        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D: serde::Deserializer<'de>>(d: D) -> std::result::Result<$name, D::Error> { Wire::deserialize(d).map(Into::into) }
        }
    };
}

entry! {
/// A table: columns, the Parquet files already tiered, and the last log segment they cover.
#[derive(Serialize, Clone, Default)]
pub struct TableMeta {
    /// Its columns: a list of them, each with its facts (`Record`, `Stored`), written as the list from lake format 6 and
    /// as format 5's fields by stored name before it. Read through the accessors (`columns()` and the rest, or the list:
    /// `cols`), changed through the column edits (`add_column` and the rest).
    #[serde(flatten)]
    record: Record,
    // (each field below is decoded by `Wire` too: add one here as to any struct)
    #[serde(default)]
    pub key: Vec<String>, // primary key: non-empty = upsert table (latest row per key wins)
    #[serde(default)]
    pub merge: BTreeMap<String, String>, // merge table: rows per key combine (column -> sum|min|max)
    pub files: Vec<DataFile>, // the recent files; older ones of append tables are sealed (`manifest.rs`)
    #[serde(default)]
    pub sealed: Option<crate::manifest::Sealed>,
    pub tiered: u64, // every segment <= `tiered` is already inside `files` (or sealed)
    #[serde(default)]
    pub garbage: Vec<(String, u64)>, // replaced files + when; deleted after the retention period
    /// The commit that last put rows into its files (tiered from the log, or a file commit): with
    /// its purges, what tells a version that changed its rows from one that only rewrote its files
    /// (`iceberg::publish`: a `replace`, which writers' conflict checks pass over).
    #[serde(default)]
    pub rows_at: u64,
    /// An upsert table whose older versions are positions (`tier::shadow`): each round since it
    /// held one generation of files (made, or compacted), so every generation it has is published.
    #[serde(default)]
    pub shadows: bool,
    /// The position-delete files of replaced files, and when: deleted after the retention period
    /// too, unless another of the table's files still names one (a delete file may name several).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub garbage_deletes: Vec<(String, u64)>,
    /// Those files' records, while they are kept: a writer that read the table before they were
    /// replaced names them, and its change is carried over by their rows' ids (`adopt::carry`).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub replaced: Vec<DataFile>,
    #[serde(default)]
    pub publish: Vec<String>, // open formats other engines also read it in: "delta", "iceberg"
    #[serde(default)]
    pub cluster: Vec<String>, // append tables: each file's rows sorted by these (see `tier::clustered`)
    #[serde(default)]
    pub ttl: Option<(String, u64)>, // keyed tables: a row whose (timestamp) column is older than this many seconds is gone
    #[serde(default)]
    pub partition: Option<String>, // append tables: every file holds one value of this ("col", "day(col)", "hour(col)", "month(col)")
    /// Keyed tables: of a key's rows, the one with the greatest value of this column is current
    /// (its event time: a late row doesn't replace a newer one), not the one that came last.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub order: Option<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub sketch: BTreeMap<String, String>, // append tables: each key-like column's distinct values, sketched (`sketch.rs`)
    #[serde(default)]
    pub ids: bool, // every row has its system columns (`sys.rs`): tables made from round 19 on
    #[serde(default)]
    pub changed: bool, // append tables: UPDATE, DELETE or MERGE has replaced rows (`{t}$deleted` holds the old ones)
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub purges: Vec<(u64, u64)>, // changed tables: (commit, when): the old rows of every change up to it are out of the files (`tier::purge`)
    /// Where its files, manifests and Delta and Iceberg copies are: `data/{folder}/`. None: its
    /// name, as for every table before round 26; a renamed table keeps the folder it had
    /// (`ALTER TABLE … RENAME TO`, ADR-030).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub folder: Option<String>,
    /// `CHECK` constraints (ADR-036 §3): (name, condition over the columns by their SQL names),
    /// which every row written must not make false (`defaults.rs`).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub checks: Vec<(String, String)>,
    /// UNIQUE, and PRIMARY KEY and FOREIGN KEY said NOT ENFORCED, by stored column names
    /// (`constraints.rs`, ADR-057): an enforced UNIQUE has every write checked on the leader.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub constraints: Vec<crate::constraints::Constraint>,
    /// Not the lake's: files outside it a query reads as a table (`ext.rs`), never in the catalog.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ext: Option<crate::ext_names::Spec>,
    /// Another engine's table (`scan.rs`): what matches its files' columns to its own.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub outside: Option<crate::scan::Table>,
    /// Other engines' properties of the table (Iceberg's: `write.delete.mode`, say), kept and
    /// published for them; Pondra's own options are fields above.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub properties: BTreeMap<String, String>,
    /// A history view's table (SCD type 2, ADR-036 §8): every version of each key as it came;
    /// reads give each its `__start_at` and `__end_at` (`views::history_view`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub history: Option<crate::views::History>,
    /// A GROUP BY view's table whose answers need a last step (avg, stddev, HAVING, …: ADR-055):
    /// its columns are what adds up, and reads give the view's columns from them (`finish.rs`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub finish: Option<Finish>,
    /// `retention = '7 days'` (ADR-043): how long the table's past is kept (`ddl::KEEP_MS` when not
    /// set): read `AT (…)` (`past.rs`), and undropped.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retention_secs: Option<u64>,
    /// From which commit (and time, ms) on the table can be read as it was: a purge let the old
    /// versions of earlier changes go (`tier::purge`). (0, 0): all of it; None: changed before
    /// this was kept, and whole only from its oldest purge kept (`past::kept_since`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub past_from: Option<(u64, u64)>,
    /// When the table was made: the log's end then and the time (ms). `t AT (…)` before it is
    /// refused (`past::table`); None for a table made before this was kept.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub made: Option<(u64, u64)>,
    /// The job of the `CREATE TABLE … AS SELECT` that made it: a definition sent again by that statement is
    /// its own, and another statement's is refused while the table is there (`write::create_table`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub made_by: Option<String>,
    /// A clone's (`CREATE TABLE c CLONE t`, ADR-043): the folders of other tables whose files it
    /// lists too. Files there go only by the orphan sweep, which counts every table listing them.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub shares: Vec<String>,
    /// The rules made from this parse, once (`rules`): never in the catalog, and a clone starts without them.
    #[serde(skip)]
    pub rules: crate::memo::Memo<crate::rules::Table>,
    /// Its column list (`cols`), made once for an entry read from the catalog: a clone's is empty, as `rules`' is.
    #[serde(skip)]
    pub column_memo: crate::memo::Memo<crate::column::Columns>,
}
}


impl TableMeta {
    /// The changes whose old rows are out of the table's files: reads skip their `{t}$deleted` rows.
    pub fn purged(&self) -> u64 { self.purges.last().map_or(0, |p| p.0) }

    /// Is this column an upsert table's delete markers (`_deleted`)? Others never see it: published,
    /// its rows are positions (`tier::shadow`), and Iceberg keeps the name for its own.
    pub fn marker(&self, column: &str) -> bool { column == "_deleted" && !self.key.is_empty() }

    // The column record is this file's own (redesign 1b, piece 6b): a reader outside it reads it through these, and a
    // writer through the column edits below (`add_column` and the rest).

    /// The columns' fields by stored name (format 5's shape), made from the list: the tests' reference.
    #[cfg(test)]
    fn fields(&self) -> Made { self.list().fields() }

    /// Its columns as its entry keeps them: every one, dropped ones too, each with its facts, in field-id order.
    pub(crate) fn list(&self) -> &Stored { &self.record.list }

    /// Every column the entry keeps, dropped ones too, as (stored name, Arrow type text), in stored order: a file's
    /// schema (`schema`, `read_schema`). SQL's view of them is `sql_columns`, `star` or the list (`cols`).
    pub fn columns(&self) -> impl ExactSizeIterator<Item = (&str, &str)> + Clone + '_ { self.list().columns.iter().map(|c| (c.stored_name(), c.dtype.as_str())) }

    /// `columns`, copied out: for a reader that keeps them or adds to them.
    pub fn to_columns(&self) -> Vec<(String, String)> { self.columns().map(|(c, t)| (c.to_string(), t.to_string())).collect() }

    /// The stored columns' Arrow schema (`columns`), each nullable: a file's, as the table writes it.
    pub fn schema(&self) -> Result<datafusion::arrow::datatypes::SchemaRef> { crate::answer::schema_of(self.columns()) }

    /// `schema` as a scan reads it, text as Utf8View (`query::read_schema`).
    pub fn read_schema(&self) -> Result<datafusion::arrow::datatypes::SchemaRef> { crate::answer::read_schema_of(self.columns()) }
    /// The stored columns, taken out of an entry made for the purpose (`described`, `with_sys`).
    pub fn into_columns(self) -> Vec<(String, String)> { self.record.list.columns.into_iter().map(|c| (c.stored.unwrap_or(c.name), c.dtype)).collect() }

    /// One edit of the list: what was made from it (its column list, its rules, its index) is made again.
    fn edit<R>(&mut self, f: impl FnOnce(&mut Stored) -> R) -> R {
        (self.column_memo, self.rules) = Default::default();
        self.record.edit(f)
    }

    /// These columns from now on, written as the list or not: what was made from the old ones is made again.
    fn set_list(&mut self, list: Stored, listed: bool) {
        (self.column_memo, self.rules) = Default::default();
        self.record = Record::new(list, listed);
    }

    /// An entry with these columns (stored name, Arrow type text) in stored order and nothing else: a table being made,
    /// or a test's. The rest is set on it as on any entry (its fields, and the column edits).
    pub fn with_columns(columns: Vec<(String, String)>) -> TableMeta { TableMeta::made(Made { columns, ..Default::default() }) }

    /// An entry with these columns and their facts, and nothing else: a table being made (`Made`'s fields, by stored
    /// name). The rest is set on it as on any entry.
    pub fn made(c: Made) -> TableMeta { TableMeta { record: Record::of(c), ..Default::default() } }

    /// An entry with this list of columns and nothing else: a table made from another's columns (`Stored::replaced`), or
    /// a reader's copy. The rest is set on it as on any entry.
    pub(crate) fn with_list(list: Stored) -> TableMeta { TableMeta { record: Record::new(list, false), ..Default::default() } }

    /// The entry written as its list (format 6's shape), as an entry read as one is: a test's.
    #[cfg(test)]
    pub(crate) fn as_list(mut self) -> TableMeta {
        self.record.listed = true;
        self
    }

    /// The column stored as `stored` (or named so in `logical()`'s copy), with its facts: a dropped one too.
    pub fn col(&self, stored: &str) -> Option<&Col> { self.record.col(stored) }

    /// Is this column (by the names its entry uses) a uuid, held as text (`types.rs`)?
    pub fn uuid(&self, column: &str) -> bool { self.col(column).is_some_and(|c| c.held.as_deref() == Some("uuid")) }

    /// Does SQL read any column as another type than its files hold (`Col::sql_type`)? Then `query::named` reads it so.
    pub fn relabeled(&self) -> bool { self.list().columns.iter().any(|c| c.sql_type.is_some()) }

    /// Has any column been renamed or dropped (ADR-022)? If not, SQL sees `columns` as stored.
    pub fn mapped(&self) -> bool {
        let m = self.record.marked();
        !m.renamed.is_empty() || !m.dropped.is_empty()
    }

    /// This entry's live columns (under the names it keeps them by: stored, or SQL's in `logical()`'s) in SQL's order
    /// (`shown`, then the others as stored). Without `shown`, `columns` as they are, dropped ones out. Kept to this file:
    /// a reader takes the order from `sql_order`, `sql_columns`, `star`, `sql_names` or the list (`cols`), never works it
    /// out (redesign 1b).
    fn in_order(&self) -> impl Iterator<Item = &Col> {
        let l = self.list();
        l.sql_places().into_iter().map(|i| &l.columns[i])
    }

    /// Its columns as one list (`column.rs`): each with its id, SQL's name and its stored one, in SQL's order (as
    /// `in_order`), the dropped ones marked. Made from the entry's list each time it is asked: `cols` keeps it once for
    /// an entry read from the catalog. Of the entry as stored only: `logical()`'s copy has left out the dropped columns
    /// and taken SQL's names, so its places are no longer ids.
    pub fn column_list(&self) -> crate::column::Columns { self.record.list.column_list() }

    /// `column_list`, made once for this version of the entry (it can't change under an `Arc`: a change is made on a
    /// clone, whose memo is empty).
    pub fn cols(self: &Arc<Self>) -> Arc<crate::column::Columns> { self.column_memo.get_or_init(|| self.column_list()) }

    /// Was a column moved, so SQL's order isn't the stored one (`ALTER COLUMN … FIRST` or `AFTER`, `ADD COLUMN … FIRST`)?
    /// The one test of it: a reader reorders, or a write fills SQL's order, only then (`in_sql_order`).
    pub fn moved(&self) -> bool {
        !self.list().order.is_empty() && self.list().sql_places().windows(2).any(|w| w[0] > w[1])
    }

    /// Rows under SQL's names (`to_logical`'s, a log tail's, the change feed's) with the table's columns in SQL's order,
    /// any other columns (the rows' system columns) where they are: the one reorder the doors use. As they are when no
    /// column was moved.
    pub fn in_sql_order(&self, b: &RecordBatch) -> Result<RecordBatch> {
        match self.moved() {
            true => Ok(crate::answer::batch_reordered(b, &self.sql_order())?),
            false => Ok(b.clone()),
        }
    }

    /// Was the column stored as `stored` dropped?
    pub fn is_dropped(&self, stored: &str) -> bool { self.record.among(&self.record.marked().dropped, stored).is_some() }

    /// The live columns' SQL names in SQL's order (`_deleted` among them, if the table has it): what `SELECT *` lists, and
    /// what an INSERT without a column list fills, first to last (`write::implicit`).
    pub fn sql_order(&self) -> Vec<String> { self.in_order().map(|c| c.name.clone()).collect() }

    /// Where an INSERT without a column list puts each value, as one number: the stored columns in SQL's order (`in_order`).
    /// A move, an added or a dropped column changes it, a rename doesn't. Of the entry as stored. The leader refuses rows a
    /// node placed by an order this entry doesn't place them by (`places`; `log::Ack::moved`, `write::record`): one that
    /// hadn't seen a move yet filled the old order (11-03 audit M9).
    pub fn placed(&self) -> u64 { self.placings().last().unwrap_or(PLACED_NONE) }

    /// Are values put by the order `placed` (`placed`, of the entry a node had) where this entry puts them? They are when it
    /// is this entry's order, or its first columns': a node that hadn't seen an ADD COLUMN at the end yet filled the columns
    /// it knew, and the new one takes its DEFAULT, as here. A move, a dropped column or one added before the others changes
    /// the first columns, so they aren't (11-07 audit L3).
    pub fn places(&self, placed: u64) -> bool { self.placings().any(|p| p == placed) }

    /// `placed` of the order's first column, of its first two, and on to all of them (FNV-1a over the stored names, each
    /// ended by a 0).
    fn placings(&self) -> impl Iterator<Item = u64> + '_ {
        self.in_order().scan(PLACED_NONE, |h, c| {
            *h = c.stored_name().bytes().chain([0]).fold(*h, |h, b| (h ^ b as u64).wrapping_mul(0x100000001b3));
            Some(*h)
        })
    }

    /// SQL's columns (`logical_columns`) in SQL's order (`in_order`): what `SELECT *` and RETURNING * answer.
    pub fn sql_columns(&self) -> Vec<(String, String)> { self.in_order().map(|c| (c.name.clone(), c.sql_type().to_string())).collect() }

    /// What `SELECT *` lists: `sql_columns` without a keyed table's delete markers (`marker`). What SHOW CREATE writes and
    /// what a view's `*` reads (`ddl::keep_readers`).
    pub fn star(&self) -> Vec<(String, String)> { self.star_cols().map(|c| (c.name.clone(), c.sql_type().to_string())).collect() }

    /// `star`'s columns as the list has them, each with its facts (`objects::table_sql`).
    pub fn star_cols(&self) -> impl Iterator<Item = &Col> { self.in_order().filter(|c| !self.marker(&c.name)) }

    /// `sql_order` without `_deleted` (a keyed table's or not): what an INSERT without a column list fills, first to last
    /// (`write::implicit`), and what `ALTER COLUMN … FIRST | AFTER` places a column among.
    pub fn sql_names(&self) -> Vec<String> { self.sql_order().into_iter().filter(|c| c != "_deleted").collect() }

    /// A stored column's name in SQL.
    pub fn name_of<'a>(&'a self, stored: &'a str) -> &'a str { self.record.among(&self.record.marked().renamed, stored).map_or(stored, |c| &c.name) }

    /// The folder under `data/` its files are in (`name`'s, unless it was renamed).
    pub fn folder<'a>(&'a self, name: &'a str) -> &'a str { self.folder.as_deref().unwrap_or(name) }

    /// The columns SQL sees, as (stored name, name in SQL, type): all but the dropped.
    pub fn live(&self) -> impl Iterator<Item = (&str, &str, &str)> {
        self.list().columns.iter().filter(|c| !c.dropped).map(|c| (c.stored_name(), c.name.as_str(), c.dtype.as_str()))
    }

    /// The column SQL calls `name`: its stored name.
    pub fn stored(&self, name: &str) -> Option<&str> { self.live().find(|(_, n, _)| *n == name).map(|(s, _, _)| s) }

    /// The table as SQL sees it: columns (and key, clustering, merges, partition, TTL) under their
    /// SQL names, dropped ones gone. Everything that works on SQL names uses this; storage keeps
    /// `self`.
    pub fn logical(&self) -> TableMeta {
        if !self.mapped() {
            return self.clone();
        }
        let n = |c: &String| self.name_of(c).to_string();
        let mut t = self.clone();
        (t.key, t.merge, t.cluster) = (self.sql_key(), self.merge.iter().map(|(c, f)| (n(c), f.clone())).collect(), self.cluster.iter().map(n).collect());
        (t.ttl, t.order, t.partition) = (self.ttl.as_ref().map(|(c, s)| (n(c), *s)), self.order.as_ref().map(n), self.sql_partition());
        t.constraints = self.constraints.iter().map(|c| c.named(&n)).collect();
        t.set_list(self.list().logical(), false); // (SQL's names, the dropped columns gone: its places are no longer ids, so never written as a list)
        t
    }

    /// `logical()`, borrowed when it is the entry itself (no column renamed or dropped): no copy of its files.
    pub fn logical_ref(&self) -> std::borrow::Cow<'_, TableMeta> {
        match self.mapped() {
            true => std::borrow::Cow::Owned(self.logical()),
            false => std::borrow::Cow::Borrowed(self),
        }
    }

    /// The key under SQL's names, as `logical()` has it, without copying the entry.
    pub fn sql_key(&self) -> Vec<String> { self.key.iter().map(|c| self.name_of(c).to_string()).collect() }

    /// `partition_by` under SQL's names (`day(ts)`, or a column), as `logical()` has it, without copying the entry.
    pub fn sql_partition(&self) -> Option<String> {
        self.partition.as_ref().map(|p| match p.split_once('(') {
            Some((f, c)) => format!("{f}({})", self.name_of(c.trim_end_matches(')'))),
            None => self.name_of(p).to_string(),
        })
    }

    /// SQL's columns, as `logical()` has them, without copying the rest of the entry (its files).
    pub fn logical_columns(&self) -> Vec<(String, String)> {
        if !self.mapped() {
            return self.to_columns();
        }
        self.live().map(|(_, n, t)| (n.to_string(), t.to_string())).collect()
    }

    // The edits to a table's columns: the one way DDL changes them (redesign 1b, piece 6a). Each names a column by its
    // stored name and edits the list (piece 6b-3: `Stored`'s), as the fields by stored name were edited before it.

    /// ADD COLUMN `name`: stored under its name, or `name~2`, `name~3`… when an older column (renamed or dropped) is
    /// stored under that, since files are never rewritten. Last in stored order, and in SQL's unless `set_order` puts it
    /// elsewhere. Returns its stored name.
    pub fn add_column(&mut self, name: &str, dtype: &str, held: Option<String>, labels: Option<Enum>) -> String { self.edit(|f| f.add_column(name, dtype, held, labels)) }

    /// RENAME COLUMN: the column stored as `stored` is `to` in SQL from now on (its files keep the stored name).
    pub fn rename_column(&mut self, stored: &str, to: &str) { self.edit(|f| f.rename_column(stored, to)) }

    /// DROP COLUMN: marked dropped, kept in the list (older files still hold it). The table facts that name it (its
    /// clustering, merge, constraints, checks) are the caller's.
    pub fn drop_column(&mut self, stored: &str) { self.edit(|f| f.drop_column(stored)) }

    /// ALTER COLUMN … TYPE that widens it: its Arrow type from now on.
    pub fn retype_column(&mut self, stored: &str, dtype: String) { self.edit(|f| f.retype_column(stored, dtype)) }

    /// The type a text column is held as (a uuid, JSON, a length: `types.rs`), or none: as text, its enum gone too.
    pub fn hold_column(&mut self, stored: &str, held: Option<String>) { self.edit(|f| f.hold_column(stored, held)) }

    /// SQL's order of the live columns, by stored name: kept only when it isn't the stored order (or its start), so a
    /// table whose columns are back in stored order keeps nothing and pays nothing (`moved`).
    pub fn set_order(&mut self, stored: Vec<String>) { self.edit(|f| f.set_order(stored)) }

    /// SET or DROP DEFAULT.
    pub fn set_default(&mut self, stored: &str, expr: Option<String>) { self.edit(|f| f.set_default(stored, expr)) }

    /// SET or DROP NOT NULL.
    pub fn set_not_null(&mut self, stored: &str, not_null: bool) { self.edit(|f| f.set_not_null(stored, not_null)) }

    /// An enum type changed (ALTER TYPE … ADD VALUE or RENAME, `types.rs`): every column of this table of that type
    /// takes it, in the same commit.
    pub fn edit_enums(&mut self, of: &str, mut g: impl FnMut(&mut Enum)) {
        self.edit(|l| l.columns.iter_mut().filter_map(|c| c.labels.as_mut()).filter(|e| e.of.as_deref() == Some(of)).for_each(&mut g))
    }

    /// Columns kept as they are named (a system table's, `sys::with_sys`'s system columns), after the others: no
    /// renamed or dropped column is stored under these names.
    pub fn append_columns(&mut self, more: impl IntoIterator<Item = (String, String)>) { self.edit(|l| l.append(more)) }

    /// The table of `of`'s replaced rows (`sys::deleted`) takes its columns: the same list, `_old_version` after it.
    pub fn follow_columns(&mut self, of: &TableMeta) { self.set_list(Stored::replaced(of.list()), self.record.listed) }

    /// The table a write's rules see (`rules::Table`), made from the rule fields alone: never `files` or the
    /// statistics. Each column SQL sees (`live`, the one mapping of stored names to SQL's) takes its rules by its
    /// stored name, as the entry keeps them.
    pub fn to_rules(&self) -> Result<crate::rules::Table> {
        let columns = self
            .list()
            .columns
            .iter()
            .filter(|c| !c.dropped)
            .map(|c| crate::rules::Column {
                name: c.name.clone(),
                stored: c.stored_name().to_string(),
                not_null: c.not_null,
                default: c.default.clone(),
                always: c.identity.as_ref().is_some_and(|i| i.always),
                held: c.held.clone(),
                labels: c.labels.as_ref().map(|e| (e.of.clone(), e.labels.clone())),
            })
            .collect();
        Ok(crate::rules::Table {
            schema: crate::answer::schema(&self.logical_columns())?,
            columns,
            key: self.key.iter().map(|c| self.name_of(c).to_string()).collect(),
            key_checked: crate::constraints::key_checked(self),
            unique: self
                .constraints
                .iter()
                .filter(|c| c.enforced && c.kind == crate::constraints::Kind::Unique)
                .map(|c| (c.name.clone(), c.columns.iter().map(|s| self.name_of(s).to_string()).collect()))
                .collect(),
            checks: self.checks.clone(),
            mapped: self.mapped(),
            order: self.sql_order(),
            planned: Default::default(),
        })
    }

    /// The rules of this entry, made once per parse and kept beside it: `Catalog::parsed` hands out one `Arc` per
    /// entry's bytes, so a write takes them with no lookup. An owned or changed entry calls `to_rules` instead.
    pub fn rules(self: &Arc<Self>) -> Result<Arc<crate::rules::Table>> {
        self.rules.get_or_try_init(|| self.to_rules())
    }

    /// The table as its readers see it, to describe it (listings, `pg_catalog`, MCP): `logical`, its columns in SQL's order
    /// (`sql_columns`), but a view that finishes its answers (`finish.rs`) shows the columns it answers, not its partial ones.
    /// Only to describe: its `columns` aren't the stored order once a column moved.
    pub fn described(&self) -> TableMeta {
        match &self.finish {
            Some(f) => {
                let mut t = self.logical();
                let list = t.described_as(&f.columns);
                t.set_list(list, t.record.listed);
                t.merge = BTreeMap::new();
                t
            }
            None if self.list().order.is_empty() && !self.relabeled() => self.logical(),
            None => {
                let mut t = self.logical();
                let list = Stored { order: vec![], ..t.described_as(&self.sql_columns()) };
                t.set_list(list, t.record.listed);
                t
            }
        }
    }

    /// These columns (SQL's name, Arrow type text) in place of this `logical()` copy's (`described`): each with the
    /// facts of the copy's column of its name (the first, if two share one) and its place for an id, and the copy's
    /// order of the names still there.
    fn described_as(&self, columns: &[(String, String)]) -> Stored {
        let mut first = std::collections::HashMap::with_capacity(columns.len());
        for ((c, _), place) in columns.iter().zip(1u32..) {
            first.entry(c.as_str()).or_insert(place);
        }
        let l = self.list();
        let order = l.order.iter().filter_map(|id| l.columns.get(l.place(*id)?)).filter_map(|c| first.get(c.stored_name()).copied()).collect();
        let columns = columns
            .iter()
            .zip(1u32..)
            .map(|((name, dtype), id)| {
                let of = self.col(name);
                let facts = of.filter(|_| first.get(name.as_str()) == Some(&id));
                Col {
                    id,
                    name: name.clone(),
                    stored: None,
                    dtype: dtype.clone(),
                    sql_type: None, // (`dtype` is SQL's already: `sql_columns`)
                    dropped: of.is_some_and(|c| c.dropped),
                    not_null: of.is_some_and(|c| c.not_null),
                    default: facts.and_then(|c| c.default.clone()),
                    identity: facts.and_then(|c| c.identity.clone()),
                    held: facts.and_then(|c| c.held.clone()),
                    labels: facts.and_then(|c| c.labels.clone()),
                    about: facts.and_then(|c| c.about.clone()),
                }
            })
            .collect();
        Stored { columns, order }
    }

    /// `b` (columns by their SQL names) as stored: renamed columns under their stored names. A
    /// column under a name SQL no longer knows (a writer from before a rename or drop) is left
    /// out, never taken for the column now stored under it. Every writer into the log gives SQL's
    /// names (`log::pack` calls this).
    pub fn to_stored(&self, b: &RecordBatch) -> Result<RecordBatch> {
        if !self.mapped() {
            return Ok(b.clone());
        }
        let (mut fields, mut columns) = (vec![], vec![]);
        for (f, c) in b.schema().fields().iter().zip(b.columns()) {
            let name = match self.stored(f.name()) {
                Some(s) => s,
                None if self.col(f.name()).is_some() => continue, // (an old name)
                None => f.name(), // (not the table's: reads leave it out)
            };
            fields.push(Arc::new(f.as_ref().clone().with_name(name)));
            columns.push(c.clone());
        }
        let options = datafusion::arrow::record_batch::RecordBatchOptions::new().with_row_count(Some(b.num_rows()));
        Ok(RecordBatch::try_new_with_options(Arc::new(Schema::new_with_metadata(fields, b.schema().metadata().clone())), columns, &options)?)
    }

    /// `b` (as stored) under the names SQL knows its columns by, dropped columns left out.
    pub fn to_logical(&self, b: &RecordBatch) -> Result<RecordBatch> {
        if !self.mapped() {
            return Ok(b.clone());
        }
        let (mut found, mut fields, mut columns) = (self.list().lookup(), vec![], vec![]);
        for (f, c) in b.schema().fields().iter().zip(b.columns()) {
            match found.find(f.name()) {
                Some(col) if col.dropped => continue,
                Some(col) => fields.push(Arc::new(f.as_ref().clone().with_name(&col.name))),
                None => fields.push(f.clone()),
            }
            columns.push(c.clone());
        }
        let options = datafusion::arrow::record_batch::RecordBatchOptions::new().with_row_count(Some(b.num_rows()));
        Ok(RecordBatch::try_new_with_options(Arc::new(Schema::new_with_metadata(fields, b.schema().metadata().clone())), columns, &options)?)
    }

    /// The TTL as a SQL condition that keeps live rows ("" if none).
    pub fn ttl_sql(&self) -> String {
        self.ttl.as_ref().map(|(c, s)| format!("\"{c}\" >= now() - INTERVAL '{s} seconds'")).unwrap_or_default()
    }

    /// Is `row` past the TTL, as `ttl_sql` keeps it out? A key lookup applies this to the row it found, so a lookup and a
    /// statement agree (`serve::lookup`, `serve::lookup_versioned`). Its timestamp column (a date counts from its midnight, UTC)
    /// is older than the TTL's seconds before `now_us` (microseconds since the epoch), or NULL, which that comparison keeps out
    /// too. A column the row lacks is NULL. A table with no TTL keeps every row. `row` is one row.
    pub fn past_ttl(&self, row: &RecordBatch, now_us: i64) -> Result<bool> {
        use datafusion::arrow::array::{Array, AsArray};
        use datafusion::arrow::compute::cast;
        use datafusion::arrow::datatypes::{DataType, TimeUnit, TimestampMicrosecondType};
        let Some((c, secs)) = &self.ttl else { return Ok(false) };
        let Some(column) = row.column_by_name(c) else { return Ok(true) };
        let column = cast(column, &DataType::Timestamp(TimeUnit::Microsecond, None))?;
        let ts = column.as_primitive::<TimestampMicrosecondType>();
        let cutoff = now_us.saturating_sub(i64::try_from(*secs).unwrap_or(i64::MAX).saturating_mul(1_000_000));
        Ok(ts.is_null(0) || ts.value(0) < cutoff)
    }
}

pub use crate::data_file::{DataFile, Lineage};

impl TableMeta {
    /// Files replaced or taken out: they go after the retention period, and their position-delete
    /// files with them (`tier::expire`).
    pub fn discard(&mut self, files: &[DataFile]) {
        let now = crate::log::now_ms();
        self.garbage.extend(files.iter().map(|f| (f.path.clone(), now)));
        self.garbage_deletes.extend(files.iter().flat_map(|f| f.delete_files()).map(|p| (p.clone(), now)));
        self.replaced.extend(files.iter().map(|f| DataFile { stats: Default::default(), sketch: Default::default(), nulls: None, ..f.clone() }));
    }
}

/// One log segment = one node's flush, holding rows for many tables. Small segments are stored
/// inside the catalog write itself (one round trip to object storage per flush); big ones as an
/// object written by the node that received the rows.
#[derive(Serialize, Deserialize, Clone, Default)]
pub struct Segment {
    pub path: String,                                // empty = inline, under data_key(seg)
    pub parts: BTreeMap<String, Vec<(u64, u64, u64)>>, // table -> (offset, length, rows) of Arrow IPC streams
    pub ts_ms: u64,
    /// A file commit (ADR-029 §7): the files it put into tables and took out of them, whose rows
    /// the log's readers read from the files — the change feed, Kafka topics, `/watch`, tasks.
    /// Queries and tiering don't: the files are the tables' own already.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub files: BTreeMap<String, Filed>,
}

/// One table's files in a file commit, as they were then.
#[derive(Serialize, Deserialize, Clone, Default)]
pub struct Filed {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub added: Vec<DataFile>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub removed: Vec<DataFile>,
    /// Files it deleted rows of by position: each as it was, with the deletes it took.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub deleted: Vec<(DataFile, Vec<crate::scan::Delete>)>,
}

impl Segment {
    /// Rows of `table` this segment gives the log's readers: its parts', then its files'.
    pub fn rows_of(&self, table: &str) -> u64 {
        let parts = self.parts.get(table).map_or(0, |p| p.iter().map(|x| x.2).sum::<u64>());
        parts + self.files.get(table).map_or(0, |f| f.added.iter().map(|d| d.rows).sum::<u64>())
    }
}

pub fn table_key(t: &str) -> String { format!("t/{t}") }

/// The folder table `t`'s files are in, under `data/` (its name, unless it was renamed).
pub async fn folder_of(lake: &Lake, t: &str) -> Result<String> {
    Ok(lake.cat.meta(&table_key(t)).await?.and_then(|m| m.folder.clone()).unwrap_or_else(|| t.to_string()))
}
pub fn seg_key(n: u64) -> String { format!("s/{n:020}") }
pub fn producer_key(p: &str) -> String { format!("p/{p}") }

/// The store's own records in the catalog (`records`): the log's segments and their inline data, each producer's mark,
/// and the commit's numbers.
pub const PREFIXES: &[crate::records::Prefix] = &[
    crate::records::Prefix { prefix: "s/", owner: "store", holds: "the log's segments: each commit's rows and files, by number (seg_key)" },
    crate::records::Prefix { prefix: "d/", owner: "store", holds: "a small segment's data, inside the catalog (data_key)" },
    crate::records::Prefix { prefix: "p/", owner: "store", holds: "each producer's last sequence, for exactly-once (a job's, a feed's, a Kafka group's offsets)" },
    crate::records::Prefix { prefix: "c", owner: "store", holds: "the last commit's number" },
    crate::records::Prefix { prefix: "n", owner: "store", holds: "the next segment's number" },
    crate::records::Prefix { prefix: QUIET, owner: "store", holds: "a commit's mark that a read sees nothing changed (retention's)" },
];

/// How long a producer's mark is kept after its last commit, from its name and the time `now` (ms; 0: kept for
/// good, as every mark was before expiries). Three windows:
/// - the named window, `copy::retention_ms` (64 days): a job a client named (`POST /sql?job=`,
///   `POST /insert/{t}?job=`, the Python client's `job=`, `PONDRA_JOB`) whose name doesn't start `auto-`. The docs
///   promise that a job sent again is applied once, and a daily load may be re-run days later;
/// - the retry window (`job_kept`): an id that starts `auto-` (the clients' own: `auto-until<S>-` ids, which say
///   their last resend time, `client_until`, and the others, `log::auto_job`'s and the older clients'), a
///   transaction's COMMIT and a COPY's chunks, unless a client named the transaction's job;
/// - 0 for the rest: views', tasks', Kafka's and `/append`'s producers, and the records of COPY INTO files
///   (`job:load:`) and of Iceberg snapshots (`job:iceberg:`), which their own code keeps or expires.
///
/// The retry window was checked against the paths that send each kind again, not taken on trust:
/// - an `auto-` id is resent only by its client's own retries (the clients' `retry_secs`, python/pondra/client.py,
///   js/index.js), by a fenced `pondra sql` starting over with its job (write.rs ~2299, `PONDRA_JOB` is passed on),
///   and by the inbox: a leader committed the request and died before it deleted it, and the next writer waits out
///   `alive`'s 30 s (terms.rs, inbox.rs);
/// - a transaction's COMMIT (`txn:{id}:{table}`, a fresh id per transaction: txn.rs `start`) is sent once, by
///   the node that runs it (txn.rs `commit`: `write::post` to the leader, or `commit_here` on the leader), and
///   a failed send goes back to the client as an error with the transaction gone, so nothing sends it again;
///   a named transaction (`txn:job-{j}:`) is sent again by its client, so it takes the named window;
/// - a COPY chunk (`copy:{job}`, pg.rs ~642) is appended once, and its error goes back to the client, which
///   can't resume a COPY: nothing sends a chunk again.
pub fn kept_ms(producer: &str, now: u64) -> u64 {
    let named = |job: &str| job_kept(job, now);
    if let Some(job) = producer.strip_prefix("sql:") {
        return named(job);
    }
    if let Some(job) = producer.strip_prefix("txn:job-") {
        return named(job);
    }
    if producer.starts_with("txn:") || producer.starts_with("copy:") {
        return retry_ms();
    }
    if producer.starts_with("job:load:") || producer.starts_with("job:iceberg:") {
        return 0;
    }
    if let Some(job) = producer.strip_prefix("job:") {
        return named(job);
    }
    0
}

/// The window of a job id, from its name (`kept_ms`):
/// - `auto-until<S>-…` (a client's id, `S` its last resend time in unix seconds: its first send plus its `retry_secs`):
///   the mark is kept until `S` plus the retry window, so a resend that is still allowed finds it. Measured from `now`
///   (a commit): `max(retry_ms, S - now) + retry_ms`, and 24 hours at most, whatever `S` says;
/// - any other `auto-…` id: the clients' own ids (`log::auto_job`, an older client's `auto-{producer}-{n}`, and the
///   Python client's `producer=`, which is the user's own name, so an `auto-` name a user picks lands here too): the
///   retry window, whatever follows the prefix;
/// - any other job: a name a client chose, which is kept for the named window (`copy::retention_ms`).
fn job_kept(job: &str, now: u64) -> u64 {
    if let Some(until_s) = client_until(job) {
        let left = until_s.saturating_mul(1000).saturating_sub(now);
        return left.max(retry_ms()).saturating_add(retry_ms()).min(UNTIL_CAP_MS);
    }
    if job.starts_with("auto-") {
        return retry_ms();
    }
    crate::copy::retention_ms()
}

/// The most a mark is kept for its client's deadline (`job_kept`): a day.
const UNTIL_CAP_MS: u64 = 24 * 60 * 60 * 1000;

/// `S` of a job id `auto-until<S>-…` (digits, then a dash), if it is one: the last moment its client may send it again.
pub fn client_until(job: &str) -> Option<u64> {
    let (digits, _) = job.strip_prefix("auto-until")?.split_once('-')?;
    (!digits.is_empty() && digits.bytes().all(|b| b.is_ascii_digit())).then(|| digits.parse().unwrap_or(u64::MAX))
}

/// The retry window (`job_kept`): twice the lease (`locks::lease`, PONDRA_LOCK_LEASE_SECS, 60 s by
/// default), and at least a minute, so 120 s by default. It has no setting of its own.
pub fn retry_ms() -> u64 {
    static MS: std::sync::OnceLock<u64> = std::sync::OnceLock::new();
    *MS.get_or_init(|| 2 * crate::locks::lease().as_secs().max(60) * 1000)
}
pub fn data_key(seg: u64) -> String { format!("d/{seg:020}") } // small segments live inside the catalog
pub fn json<T: Serialize>(v: &T) -> Vec<u8> { serde_json::to_vec(v).expect("serializable") }

/// The open formats a new table is published in unless it says otherwise (the node's `--publish`).
pub fn default_publish() -> Vec<String> {
    crate::env::get("PONDRA_PUBLISH").unwrap_or_default().split(',').filter(|f| !f.is_empty()).map(String::from).collect()
}

const TAIL_BYTES: usize = 256 << 20; // decoded log segments kept in memory, per node
const AHEAD: u64 = 256; // replicated acks: commits acknowledged before the bucket has them, at most

const RECENT: Duration = Duration::from_secs(30); // replayed to (re)connecting followers…
const RECENT_BYTES: usize = 64 << 20; // …within this much memory (past it, a reconnecting
// follower reads from its own view until it has caught up, instead of the leader buffering more)

/// Query read cache in memory, MB (env PONDRA_CACHE_MB, default 1024).
fn cache_mb() -> usize { crate::env::value("PONDRA_CACHE_MB") }

/// Did the user give the read caches (a bucket's byte ranges, lookups' row groups) a budget of their own, `PONDRA_CACHE_MB`?
/// Then they keep it beside the whole line for queries and caches (`ledger::Charged::inside`).
pub(crate) fn own_cache_budget() -> bool { crate::env::get("PONDRA_CACHE_MB").is_some() }

/// The folder the node's SSD tiers and replica logs go under: `PONDRA_CACHE_DIR` when set (the operator's own
/// choice), else `pondra-cache-{uid}` in the temp dir, made 0700 and used only while it is this user's alone
/// (`memory::own_dir`: the temp dir is shared, so a folder with a predictable name may be another user's).
pub(crate) fn cache_base() -> std::io::Result<std::path::PathBuf> {
    match crate::env::get("PONDRA_CACHE_DIR") {
        Some(dir) => Ok(std::path::PathBuf::from(dir)),
        None => crate::memory::own_dir(&std::env::temp_dir(), "pondra-cache"),
    }
}

/// The local SSD tier for a lake on object storage (`serve --cache-dir/--cache-gb`, or env
/// PONDRA_CACHE_DIR / PONDRA_CACHE_GB): under `cache_base`, 20 GB, or a quarter of the free disk
/// if that is less (a notebook's sandbox may have a few GB). 0 GB turns it off.
fn disk_tier(url: &str, store: &Store) -> Option<Arc<crate::cache::Disk>> {
    let dir = match cache_base() {
        Ok(dir) => dir,
        Err(e) => {
            static SAID: std::sync::Once = std::sync::Once::new(); // (the same for every lake, so said once)
            SAID.call_once(|| eprintln!("no SSD tier ({e}): lakes on object storage read the bucket without one"));
            return None;
        }
    };
    let bytes = match crate::env::gib("PONDRA_CACHE_GB") {
        Some(bytes) => bytes as u64,
        None => space(&dir).map_or(20 << 30, |(free, _)| (free / 4).min(20 << 30)),
    };
    let dir = dir.join(url.trim_start_matches("s3://").replace(['/', ':', '\\'], "_")); // one folder per lake
    (bytes > 0).then(|| crate::cache::Disk::open(dir, bytes, store.clone()).ok()).flatten()
}

/// Free and total bytes of the disk that holds `dir` (or its nearest existing parent), where the
/// platform says.
pub(crate) fn space(dir: &std::path::Path) -> Option<(u64, u64)> {
    #[cfg(unix)]
    {
        let at = std::ffi::CString::new(dir.ancestors().find(|a| a.exists())?.as_os_str().as_encoded_bytes()).ok()?;
        let mut s: libc::statvfs = unsafe { std::mem::zeroed() };
        (unsafe { libc::statvfs(at.as_ptr(), &mut s) } == 0).then(|| (s.f_bavail as u64 * s.f_frsize as u64, s.f_blocks as u64 * s.f_frsize as u64))
    }
    #[cfg(not(unix))]
    {
        let _ = dir;
        None
    }
}

/// Room a lake on local disk keeps for its own upkeep: a quarter of the disk, 256 MB at most (of
/// a disk of `total` bytes). Commits wait while less is free (`log::Sequencer`), so the catalog can
/// still flush and tiering write the files that let the log go: a disk the lake filled to its last
/// byte had room for neither, and kept a log that never drained. Spills keep it too.
fn lake_keeps(total: u64) -> u64 { (total / 4).min(256 << 20) }

pub fn short_of_room(dir: &std::path::Path) -> Option<(u64, u64)> {
    space(dir).map(|(free, total)| (free, lake_keeps(total))).filter(|(free, keep)| free < keep)
}

/// The folder of this process's spills, DataFusion's and a shuffle's (`spill::dir`): a `memory::Folder` of
/// this process's own, made by the first spill that needs one. A static's end runs no destructor, so the
/// folder goes when the program ends through `memory::leave` (which `cli.rs` calls before each exit); where
/// that can't remove it (Windows, while its lock file is open), it stays for the next process's sweep. A
/// folder that can't be made is remembered as the reason why.
static SPILLS: std::sync::OnceLock<Result<crate::memory::Folder, String>> = std::sync::OnceLock::new();

/// The folder of this process's spills (`SPILLS`), made on first use. Its error when there is none: DataFusion's
/// spills then go to the OS temp folder, and a shuffle fails with the reason.
pub(crate) fn spill_folder() -> std::io::Result<&'static std::path::Path> {
    let folder = SPILLS.get_or_init(|| {
        crate::memory::Folder::make("spill").map_err(|e| {
            eprintln!("no spill folder of this process's ({e}): queries spill to the temporary folder, shuffles fail");
            e.to_string()
        })
    });
    match folder {
        Ok(folder) => Ok(&folder.path),
        Err(why) => Err(std::io::Error::other(why.clone())),
    }
}

/// `PONDRA_SPILL_GB`: how many GB of spills the node may keep on disk. None when unset (a negative setting is none).
pub(crate) fn spill_gb() -> Option<f64> { crate::env::parse::<f64>("PONDRA_SPILL_GB").filter(|gb| *gb >= 0.0) }

/// The bytes that may spill under `dir`: `PONDRA_SPILL_GB` when set, else `dir`'s disk's free space less `lake_keeps`; None with no `dir`.
/// The lakes in memory spill under the same setting and room, counted with these (`memory::spilled`, `others_held`).
pub(crate) fn spill_cap(dir: Option<&std::path::Path>) -> Option<u64> {
    match spill_gb() {
        Some(gb) => Some(gb_bytes(gb)),
        None => dir.and_then(space).map(|(free, total)| free.saturating_sub(lake_keeps(total))),
    }
}

/// Where DataFusion's spills go, and how much of the disk they may take (`spill_cap`). Not the
/// OS temp folder, which is held in memory on many Linux systems, where a spill frees nothing. The
/// first spill makes the folder and DataFusion's own directory in it (`Spills`): a lake that never
/// spills makes neither, so nothing is written when it opens. The folder is the user's own, under the
/// disk with the room (`memory::spill_base`), and folders of processes that ended are swept from it.
/// The cap is the free space when the first spill makes the disk, less the room a lake keeps, or the
/// setting in GB; it is looked at again as queries start (`spill_room`). `0` turns spilling off: a
/// query that needs it is refused (53200). A negative setting is no setting.
fn spill_disk() -> DiskManagerBuilder { spill_disk_merging(spill_fan_in().unwrap_or_else(|| merge_files(partitions()))) }

/// The spill factory a page in a Worker gives the core (`give_spills`, wasm32): the lakes that open while it is set spill through
/// it (`spill_disk_merging`). Not `per_node!`: a freed page's member closes in the background, and its `reset()` would wipe the
/// factory of the page made after it (node.rs's CLAIMED says the same of the claim).
static SPILL_FACTORY: Mutex<Option<Arc<dyn TempFileFactory>>> = Mutex::new(None);

/// Gives the core the factory a page's spill file comes from (wasm32: `web/`'s `spill.rs`), for the lakes that open while it is
/// set. True when it is given; false when one is set already, which a page never leaves behind (it takes its own back, `take_spills`).
#[cfg(target_arch = "wasm32")]
pub fn give_spills(factory: Arc<dyn TempFileFactory>) -> bool {
    let mut given = SPILL_FACTORY.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
    if given.is_some() {
        return false;
    }
    *given = Some(factory);
    true
}

/// Takes back the factory `give_spills` gave, when it is this one (`Arc::ptr_eq`): a page ends with its own and leaves another's be.
#[cfg(target_arch = "wasm32")]
pub fn take_spills(factory: &Arc<dyn TempFileFactory>) {
    let mut given = SPILL_FACTORY.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
    if given.as_ref().is_some_and(|g| Arc::ptr_eq(g, factory)) {
        *given = None;
    }
}

/// `spill_disk`, its merges reading at most `fan_in` spilled runs at once.
fn spill_disk_merging(fan_in: usize) -> DiskManagerBuilder {
    if spill_gb() == Some(0.0) {
        return DiskManagerBuilder::default().with_mode(DiskManagerMode::Disabled);
    }
    // (a page on wasm has no folder to spill to: it spills through the factory it gave the core before this lake opened
    // (`give_spills`), and with none it has no disk, so a query past its pool is refused (53200) rather than trapping the module
    // in `spill_folder`. Only a lake that opens while the factory is set spills through it: `Custom` reserves the sorts' spill
    // memory, so a page with none keeps its pool as it was (designs/l2-page-spill.md, S1).)
    if cfg!(target_arch = "wasm32") {
        let given = SPILL_FACTORY.lock().unwrap_or_else(std::sync::PoisonError::into_inner).clone();
        return match given {
            Some(factory) => DiskManagerBuilder::default().with_mode(DiskManagerMode::Custom(factory)).with_max_spill_merge_fan_in(fan_in),
            None => DiskManagerBuilder::default().with_mode(DiskManagerMode::Disabled),
        };
    }
    // (the lake's own manager holds no file: `Spills` makes the one that does, with this fan-in too, since sorts read it here)
    DiskManagerBuilder::default().with_mode(DiskManagerMode::Custom(Arc::new(Spills { fan_in, made: std::sync::OnceLock::new() }))).with_max_spill_merge_fan_in(fan_in)
}

/// `PONDRA_SPILL_FAN_IN`: how many spilled runs any merge reads at once, in place of `merge_files` and `merge_fan_in`
/// (0 is DataFusion's own, no limit).
fn spill_fan_in() -> Option<usize> { crate::env::parse::<usize>("PONDRA_SPILL_FAN_IN") }

/// How many spilled runs a merge reads at once, one pass whenever its memory holds them all (Alimardon, 10-31: stream, and
/// spill as little as can be): 512 open files over `partitions`, 4 to 64. Memory bounds it before that: a GROUP BY's
/// merge reserves from the aggregate's own fair share of the pool, and a sort's takes runs only while the pool has room.
/// Under a 2 GB cap a GROUP BY of 34 M strings and a three-way join made 25 to 64 runs a partition: merged 5 at a time
/// (#150) they ran 40-90% slower, writing their rows again each pass (B50, 10-31). The sorts feeding a window take fewer
/// (`merge_fan_in`).
fn merge_files(partitions: usize) -> usize { (512 / partitions.max(1)).clamp(4, 64) }

/// How many spilled runs the merge of a sort that feeds a window reads at once (`window_sorts`), with `memory` for queries
/// over `partitions`: as many as half a partition's share holds at about 4 MB a run (the merge reserves a run's largest
/// batch twice, two batches deep), 4 to 64. That reservation is a sort's merge, which no one may take back, so with as
/// many runs as the pool held the window over its rows was refused (invariant 423). Under 2 GB this is 21; at the
/// window check's 256 MB, 8 (it passes at 16 and is refused at 32).
fn merge_fan_in(memory: usize, partitions: usize) -> usize { (memory / partitions.max(1) / (8 << 20)).clamp(4, 64) }

/// `ctx` with the spill disk of the sorts that feed a window (their merges read `merge_fan_in` runs at once) in place of
/// the lake's; the pool, the caches and the stores as they are. `held::HeldInExec` runs its input on it. The disk is made
/// once for the process, as its spill folder is; spills off, or a disk that can't be made, leave `ctx` as it is.
#[cfg(not(target_arch = "wasm32"))]
pub(crate) fn window_sorts(ctx: Arc<datafusion::execution::TaskContext>) -> Arc<datafusion::execution::TaskContext> {
    static DISK: std::sync::OnceLock<Option<Arc<DiskManager>>> = std::sync::OnceLock::new();
    let disk = DISK.get_or_init(|| {
        let fan_in = spill_fan_in().unwrap_or_else(|| merge_fan_in(memory_limit(), partitions()));
        (spill_gb() != Some(0.0)).then(|| spill_disk_merging(fan_in).build().ok().map(Arc::new)).flatten()
    });
    let Some(disk) = disk else { return ctx };
    let built = RuntimeEnvBuilder { disk_manager: Some(disk.clone()), ..RuntimeEnvBuilder::from_runtime_env(&ctx.runtime_env()) }.build_arc();
    let Ok(rt) = built else { return ctx };
    Arc::new(datafusion::execution::TaskContext::new(ctx.task_id(), ctx.session_id(), ctx.session_config().clone(), ctx.scalar_functions().clone(), ctx.higher_order_functions().clone(), ctx.aggregate_functions().clone(), ctx.window_functions().clone(), rt))
}

/// DataFusion's spill disk for one lake, made by its first spill (`TempFileFactory`): the same manager `spill_disk`
/// would have built when the lake opened, in the spill folder (or the OS temp folder, where there is none), with
/// `fan_in` and the cap of `spill_cap`. `made` holds it once it is built, or the reason it could not be. It is
/// registered (`register_spiller`) when it is made, so `spill_room` caps it and `spill_fits` counts what it holds.
struct Spills {
    fan_in: usize,
    made: std::sync::OnceLock<Result<Arc<DiskManager>, String>>,
}

impl TempFileFactory for Spills {
    fn create_temp_file(&self, description: &str) -> datafusion::error::Result<Arc<dyn SpillFile>> {
        let made = self.made.get_or_init(|| {
            let folder = spill_folder().ok();
            let builder = match folder {
                Some(path) => DiskManagerBuilder::default().with_mode(DiskManagerMode::Directories(vec![path.to_path_buf()])),
                None => DiskManagerBuilder::default(), // (no folder: the OS temp folder, as before)
            };
            let builder = builder.with_max_spill_merge_fan_in(self.fan_in);
            let builder = match spill_cap(folder) {
                Some(cap) => builder.with_max_temp_directory_size(cap), // (where the platform can't say, DataFusion's own 100 GB stays)
                None => builder,
            };
            let disk = Arc::new(builder.build().map_err(|e| e.to_string())?);
            register_spiller(&disk);
            Ok(disk)
        });
        match made {
            Ok(disk) => disk.create_tmp_file(description),
            Err(why) => Err(DataFusionError::ResourcesExhausted(format!("no spill folder to spill to while {description}: {why}"))),
        }
    }
}

/// `gb` gibibytes, in bytes.
fn gb_bytes(gb: f64) -> u64 { (gb * (1u64 << 30) as f64) as u64 }

/// The disk managers that hold this process's lakes' spill files (one for each lake, made by its first spill: `Spills`),
/// held weakly: a lake that goes takes its manager with it. `register_spiller` adds each one (unless spilling is off),
/// `spill_room` sets their caps, `spill_fits` counts what they hold.
static SPILLERS: Mutex<Vec<Weak<DiskManager>>> = Mutex::new(Vec::new());

/// Adds a spill disk to `SPILLERS`, once `Spills` has made it. A disabled one has no cap to set, so it is left out.
fn register_spiller(disk: &Arc<DiskManager>) {
    if spill_gb() != Some(0.0) {
        SPILLERS.lock().unwrap_or_else(std::sync::PoisonError::into_inner).push(Arc::downgrade(disk));
    }
}

/// The disk managers still alive, the dead ones dropped from `SPILLERS`.
fn live_spillers() -> Vec<Arc<DiskManager>> {
    let mut spillers = SPILLERS.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
    spillers.retain(|w| w.strong_count() > 0);
    spillers.iter().filter_map(Weak::upgrade).collect()
}

/// The cap of one disk manager that holds `used` bytes of spills, given the setting and what shuffles hold
/// (`shuffled`) or the folder's disk (`disk`: free and total bytes, where the platform says). With the setting,
/// the shuffles and the spills together stay within it; without, a spill may take the disk's free room beyond
/// what a lake keeps. None where neither is known: the cap stays as it was.
fn room_cap(gb: Option<f64>, used: u64, shuffled: u64, disk: Option<(u64, u64)>) -> Option<u64> {
    match gb {
        Some(gb) => Some(gb_bytes(gb).saturating_sub(shuffled)),
        None => disk.map(|(free, total)| used + free.saturating_sub(lake_keeps(total))),
    }
}

/// Sets every live lake's cap from the disk as it is now (`room_cap`). Under the setting, the shuffles, the lakes in
/// memory and the other spill disks (a window's sorts have their own: `window_sorts`) take their bytes out of it first
/// (without it, the disk's free room already says what they hold). (Each was given the whole setting, so a window's sorts
/// and a GROUP BY spilling beside them could take twice it: audit 11-01 M13.)
fn set_room(gb: Option<f64>) {
    let shuffled = gb.map_or(0, |_| crate::spill::held() + crate::memory::spilled());
    let disk = space(&crate::memory::spill_base()); // (the disk the folder is, or would be on: not made here, the first spill makes it)
    let managers = live_spillers();
    let all: u64 = managers.iter().map(|m| m.used_disk_space()).sum();
    for manager in managers {
        let used = manager.used_disk_space();
        let others = gb.map_or(0, |_| all.saturating_sub(used));
        if let Some(cap) = room_cap(gb, used, shuffled + others, disk) {
            let _ = manager.set_max_temp_directory_size(cap); // (a disabled manager refuses: none is registered)
        }
    }
}

/// Looked at again as each query's session is made (`session_with`), at most every 500 ms: a node started on a
/// full disk spills once room is freed, and one started on an empty disk stops taking room that others need.
/// Two queries at once may both look, which is harmless. Several lakes in a process each get their own spills
/// plus the free room, so together they can overshoot between two looks, by what half a second writes.
pub(crate) fn spill_room() {
    static START: std::sync::LazyLock<Instant> = std::sync::LazyLock::new(Instant::now);
    static LOOKED: AtomicU64 = AtomicU64::new(u64::MAX); // when the disk was last looked at, ms since `START` (MAX: never)
    let now = START.elapsed().as_millis() as u64;
    let last = LOOKED.load(Relaxed);
    if last != u64::MAX && now.saturating_sub(last) < 500 {
        return;
    }
    LOOKED.store(now, Relaxed);
    if live_spillers().is_empty() {
        return; // (no spill disk made yet, so nothing to size and no disk to look at: a page never makes one)
    }
    set_room(spill_gb());
}

/// Whether `bytes` more fit on the spill disk: within the setting, with `held` already on it (shuffles' pieces
/// and spills), or in its free room beyond what a lake keeps. Where the platform can't say how much room there
/// is, it fits.
pub(crate) fn piece_fits(gb: Option<f64>, held: u64, bytes: u64, disk: Option<(u64, u64)>) -> bool {
    match gb {
        Some(gb) => held.saturating_add(bytes) <= gb_bytes(gb),
        None => disk.map_or(true, |(free, total)| bytes <= free.saturating_sub(lake_keeps(total))),
    }
}

/// What this process spills, other than its lakes in memory: the shuffles' pieces (`spill::held`) and the queries' spill
/// disks (`live_spillers`). The lakes' bytes come with `memory::spilled`, so one check sees all of it.
pub(crate) fn others_held() -> u64 { crate::spill::held() + live_spillers().iter().map(|m| m.used_disk_space()).sum::<u64>() }

/// Whether a shuffle's piece of `bytes` fits on the spill disk now: `Spill::write` asks before it writes one.
/// No spill folder: it fits, and the write fails with its own error.
pub(crate) fn spill_fits(bytes: u64) -> Result<()> {
    let Ok(folder) = spill_folder() else { return Ok(()) };
    let gb = spill_gb();
    let held = gb.map_or(0, |_| others_held() + crate::memory::spilled());
    match piece_fits(gb, held, bytes, space(folder)) {
        true => Ok(()),
        false => Err(no_room(bytes, gb)),
    }
}

/// The refusal of a piece that doesn't fit (53100): how much more it needed, and the way out. The setting is
/// named only when it is set.
fn no_room(bytes: u64, gb: Option<f64>) -> anyhow::Error {
    let way = match gb {
        Some(gb) => format!("Free space on the spill disk, or raise PONDRA_SPILL_GB (now {gb} GB) to let it spill more."),
        None => "Free space on the spill disk to let it spill more.".to_string(),
    };
    let more = bytes as f64 / (1u64 << 20) as f64;
    crate::codes::coded("53100", format!("The shuffle needed {more:.1} MB more on disk than there is room for. {way}"))
}

pub struct Lake {
    pub url: String, // absolute local dir or "s3://bucket/prefix" (gs://, az://, abfss:// too; memory://id in this process)
    pub store: Store,
    pub cat: Catalog,
    pub hwm: watch::Sender<u64>, // the last committed segment this node knows of
    pub backlog: std::sync::atomic::AtomicU64, // leader: rows in the log not yet tiered (all tables)
    pub rt: Arc<RuntimeEnv>,         // shared by all queries: object store registry + Parquet metadata cache
    tail: Arc<Mutex<Tail>>, // decoded log rows
    pub tails: Arc<Mutex<crate::query::Tails>>, // tables' tails as queries read them (`query::Tail`): by table, one a form (`of.1`)
    kept: (Arc<Footers>, Arc<FileStatistics>), // DataFusion's caches of Parquet footers and of files' statistics (`rt`'s)
    pub disk: Option<Arc<crate::cache::Disk>>, // lakes on object storage: the local SSD tier
    pub groups: Arc<crate::serve::Groups>,     // decoded row groups for key lookups
    pub hot: Arc<crate::hot::Hot>,             // decoded columns of files queries read lately
    pub attached: std::sync::RwLock<Vec<(String, Arc<Lake>)>>, // other lakes, read as `name.table` (`--attach`)
    cached: Option<Arc<crate::cache::CachedStore>>, // what DataFusion reads the bucket through
    pub ids: crate::sys::Ids, // the row ids this process stamps rows with (a block reserved from the leader)
    pub caught: watch::Sender<bool>, // false while a node that just started catches up with its leader (`caught_up`)
    pub to: std::sync::OnceLock<crate::log::To>, // where this process's flushes and sequences' values go: its sequencer, or the leader's
    pub owner_key: std::sync::OnceLock<String>, // the key of the program that started this process's node on the lake (`Auth::owner_key`): what its owner's tasks are signed with (`runs::signed`)
    pub sequences: tokio::sync::Mutex<HashMap<String, crate::seq::Block>>, // the sequences' values this node hands out (`seq::next`)
    bases: std::sync::RwLock<BTreeMap<String, (String, Option<Store>)>>, // a branch's bases (ADR-047): id -> place, and its store once read
    sessions: Mutex<std::collections::HashMap<usize, datafusion::execution::SessionState>>, // (`session_with`'s, made once a partition count)
    me: std::sync::Weak<Lake>,
}

impl std::fmt::Debug for Lake {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result { write!(f, "Lake({})", self.url) }
}

/// How much memory queries may use on this node (`PONDRA_MEMORY_GB`, else a third of RAM). Past
/// it, sorts, aggregations and joins spill to temporary files, or the query stops with an error:
/// a query never takes the node down. (A third, not half: what DataFusion counts here is the big
/// hash tables and sort buffers, not the Parquet decoding and the batches in flight around them,
/// so the node's own memory runs ahead of this number.) The queries and the caches fit one whole
/// line together (`whole_line`): `PONDRA_MEMORY_GB` itself when it is set.
#[cfg(not(target_arch = "wasm32"))]
pub fn memory_limit() -> usize {
    crate::env::query_memory(|| ram().map_or(4 << 30, |r| r / 3))
}

crate::per_node!(
    /// The node's line for query memory (`ledger::Ledger`), by its queries' line, its whole line and whether that is the
    /// user's (`PONDRA_MEMORY_GB`): every lake the node opens shares it. (By size, so a lake opened after the setting
    /// changed, or a test's with memory of its own, gets a line of its own.)
    static LEDGERS: Mutex<HashMap<(usize, usize, bool), Weak<crate::ledger::Ledger>>> = Default::default());

/// The node's lines now (`LEDGERS`): the memory watch takes memory back from the caches charged to them (`oom.rs`).
pub(crate) fn ledgers() -> Vec<Arc<crate::ledger::Ledger>> {
    LEDGERS.lock().unwrap_or_else(std::sync::PoisonError::into_inner).values().filter_map(Weak::upgrade).collect()
}

/// A new lake's pool for its queries: its share of the node's line (`memory_limit`), which every lake of the node shares.
fn lake_pool() -> Arc<crate::ledger::LakePool> {
    let key = (memory_limit(), whole_line(), crate::env::memory_whole().is_some());
    let mut lines = LEDGERS.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
    lines.retain(|_, l| l.strong_count() > 0);
    let ledger = lines.get(&key).and_then(Weak::upgrade).unwrap_or_else(|| {
        let made = crate::ledger::Ledger::new(key.0, key.1, key.2);
        lines.insert(key, Arc::downgrade(&made));
        made
    });
    ledger.pool()
}

/// The whole line a node's queries and caches share (`ledger::Ledger`): `PONDRA_MEMORY_GB` when it is set, which is then
/// the queries' line too, and a spiller it has no room for spills; else the memory watch's trim line (`oom::trim_line`),
/// past which a grow takes cache bytes back and goes on within its share; none (`usize::MAX`) where neither is known.
fn whole_line() -> usize {
    crate::env::memory_whole().or_else(crate::oom::trim_line).unwrap_or(usize::MAX)
}

/// A page's lake holds what the page's own pool does (`env::page_query_memory`, of its device's line): its memory is its
/// line, not a machine's (`ram` is None there, and 4 GiB is past a 32-bit `usize`).
#[cfg(target_arch = "wasm32")]
pub fn memory_limit() -> usize { crate::env::page_query_memory(crate::env::device_line()) }

/// The machine's memory, if it says: `/proc` on Linux, the OS's own call elsewhere. Less when a
/// cgroup limits this process (a container, a service with `MemoryMax`): `/proc/meminfo` is the
/// host's, and a node in a 4 GB container on a 64 GB host sized its queries for 64 and was killed.
pub fn ram() -> Option<usize> {
    static RAM: std::sync::OnceLock<Option<usize>> = std::sync::OnceLock::new();
    *RAM.get_or_init(read_ram)
}

// (a page's memory is its line, which `page.rs` takes from its device: none is the machine's)
#[cfg(target_arch = "wasm32")]
fn read_ram() -> Option<usize> {
    None
}

#[cfg(not(target_arch = "wasm32"))]
fn read_ram() -> Option<usize> {
    let proc = std::fs::read_to_string("/proc/meminfo").ok().and_then(|m| m.lines().find_map(|l| l.strip_prefix("MemTotal:")?.trim().strip_suffix("kB")?.trim().parse::<usize>().ok()));
    proc.map(|kb| (kb << 10).min(cgroup_memory().unwrap_or(usize::MAX))).or_else(|| {
        let s = sysinfo::System::new_with_specifics(sysinfo::RefreshKind::nothing().with_memory(sysinfo::MemoryRefreshKind::nothing().with_ram()));
        Some(s.total_memory() as usize).filter(|&b| b > 0)
    })
}

/// The least memory limit of this process's cgroups and their parents (v2 `memory.max`, v1
/// `memory.limit_in_bytes`; "max" or none: no limit). In a container the cgroup is its root.
#[cfg(not(target_arch = "wasm32"))]
fn cgroup_memory() -> Option<usize> {
    let groups = std::fs::read_to_string("/proc/self/cgroup").ok()?;
    let limits = groups.lines().filter_map(|line| {
        let (controllers, path) = line.split_once(':')?.1.split_once(':')?; // "0::/path" (v2), "4:memory:/path" (v1)
        let (base, file) = match controllers {
            "" => ("/sys/fs/cgroup", "memory.max"),
            c if c.split(',').any(|c| c == "memory") => ("/sys/fs/cgroup/memory", "memory.limit_in_bytes"),
            _ => return None,
        };
        let read = |dir: &std::path::Path| std::fs::read_to_string(format!("{base}{}/{file}", dir.display())).ok()?.trim().parse::<usize>().ok();
        std::path::Path::new(path).ancestors().filter_map(read).min()
    });
    limits.min()
}

/// This process's resident memory, if the OS says.
#[cfg(not(target_arch = "wasm32"))]
pub fn resident() -> Option<usize> {
    let proc = std::fs::read_to_string("/proc/self/statm").ok().and_then(|s| s.split_whitespace().nth(1)?.parse::<usize>().ok());
    proc.map(|pages| pages * page_size()).or_else(|| {
        use sysinfo::{ProcessRefreshKind, ProcessesToUpdate, System};
        let (pid, mut s) = (sysinfo::get_current_pid().ok()?, System::new());
        s.refresh_processes_specifics(ProcessesToUpdate::Some(&[pid]), false, ProcessRefreshKind::nothing().with_memory());
        Some(s.process(pid)?.memory() as usize).filter(|&b| b > 0)
    })
}

/// The page's live heap bytes, as the program that runs the page reads them (`page::Options::memory`, which re-exports this).
pub type Memory = Arc<dyn Fn() -> usize + Send + Sync>;

// A page's live bytes, as the program that runs the page reads them (`page::Options::memory`): process-wide, as the page's allocator is.
#[cfg(target_arch = "wasm32")]
static READER: std::sync::OnceLock<Memory> = std::sync::OnceLock::new();

/// Gives the page's live bytes to the core's watch (`oom`): set once, and a second call keeps the first.
#[cfg(target_arch = "wasm32")]
pub(crate) fn read_resident_with(reading: Memory) {
    let _ = READER.set(reading);
}

/// A page's resident memory is the bytes its program reads (`read_resident_with`), and none until one is given.
#[cfg(target_arch = "wasm32")]
pub fn resident() -> Option<usize> {
    READER.get().map(|reading| reading())
}

/// The size of a page in bytes, which `/proc/self/statm` counts in: 4 KB, or 64 KB on some arm64 builds
/// (where a count of pages read as 4 KB pages was 16 times too little). Asked each time: glibc answers
/// from memory, and a static would be a node's state for nothing.
#[cfg(not(target_arch = "wasm32"))]
fn page_size() -> usize {
    #[cfg(unix)]
    {
        let bytes = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
        if bytes > 0 {
            return bytes as usize;
        }
    }
    4096
}

pub type Rows = Arc<Vec<datafusion::arrow::record_batch::RecordBatch>>;

/// Decoded log rows, each table's by segment, `TAIL_BYTES` at most. A table's log is read only
/// after its `tiered` mark (a read that began before reads its own from the store), so its rows up
/// to the mark go when the node sees the commit that moved it (`arrived`), or a read finds it
/// moved, and past the limit the oldest segment goes. Kept by recent use instead, every node held
/// 256 MB of rows long since in files; kept until a read, a table read once and then only
/// written held them too (fluss_bench: 267 MB of a node's 740 once it was idle).
#[derive(Default)]
struct Tail {
    rows: std::collections::HashMap<String, BTreeMap<u64, Rows>>,
    bytes: usize,
}

/// The log's decoded rows, which the log's readers need until their segments are tiered: kept, and counted.
impl crate::ledger::Charged for Mutex<Tail> {
    fn held(&self) -> usize { self.lock().unwrap_or_else(std::sync::PoisonError::into_inner).bytes }
}

impl Tail {
    fn size(rows: &Rows) -> usize { rows.iter().map(|b| b.get_array_memory_size()).sum() }

    fn forget(&mut self, table: &str, upto: u64) {
        let Some(t) = self.rows.get_mut(table) else { return };
        let kept = t.split_off(&upto.saturating_add(1));
        self.bytes -= std::mem::replace(t, kept).values().map(Self::size).sum::<usize>();
        if t.is_empty() {
            self.rows.remove(table);
        }
    }

    fn keep(&mut self, table: &str, n: u64, rows: Rows) {
        self.bytes += Self::size(&rows);
        if let Some(old) = self.rows.entry(table.to_string()).or_default().insert(n, rows) {
            self.bytes -= Self::size(&old);
        }
        while self.bytes > TAIL_BYTES {
            let Some((first, oldest)) = self.rows.iter().filter_map(|(t, r)| Some((*r.keys().next()?, t.clone()))).min() else { break };
            self.forget(&oldest, first);
        }
    }
}

/// How this process reaches a lake on another server (ADR-058): the settings of the bucket key it
/// is read with (a secret's, `ext::build`; None: the environment's), whether nothing here may write
/// it, and where its leader is asked (a URL, with a TYPE pondra secret's token).
#[derive(Clone, Default, Debug, Serialize, Deserialize)]
pub struct Reach {
    pub read_only: bool,
    pub params: Option<BTreeMap<String, String>>,
    pub endpoint: Option<String>,
    pub token: Option<String>,
    pub secret: Option<String>, // (the bucket secret's name: who may use it reads the lake here, `users::across`)
}

crate::per_node!(static REACH: std::sync::RwLock<BTreeMap<String, Reach>> = std::sync::RwLock::new(BTreeMap::new()));

/// Say how `url` (a lake) is reached. Asked again (an attachment and a branch of it in one
/// process), what the new one leaves out is kept, and read-only stays read-only.
pub fn reach_by(url: &str, r: Reach) {
    let mut all = REACH.write().unwrap();
    let old = all.remove(url.trim_end_matches('/')).unwrap_or_default();
    let read_only = old.read_only || r.read_only;
    all.insert(url.trim_end_matches('/').to_string(), Reach { read_only, params: r.params.or(old.params), endpoint: r.endpoint.or(old.endpoint), token: r.token.or(old.token), secret: r.secret.or(old.secret) });
}

/// What this process knows of how to reach other places, for a process it starts to lead one of them
/// for a moment (`node::lead_once`): it holds the same keys, so a backup in another bucket is written
/// with the secret that reaches it (ADR-070).
pub const REACH_VAR: &str = "PONDRA_REACH";

pub fn reaches() -> String { serde_json::to_string(&*REACH.read().unwrap()).unwrap_or_default() }

/// The places the process that started this one could reach (`REACH_VAR`), as it reached them.
pub fn inherit() {
    let Some(text) = crate::env::get(REACH_VAR) else { return };
    match serde_json::from_str::<BTreeMap<String, Reach>>(&text) {
        Ok(all) => all.into_iter().for_each(|(url, r)| reach_by(&url, r)),
        Err(e) => eprintln!("{REACH_VAR}: {e}"),
    }
}

/// How `url`, or a lake it is in, is reached (the longest registered prefix: `url` itself, or a
/// path under it).
pub fn reach_of(url: &str) -> Option<Reach> {
    let url = url.trim_end_matches('/');
    let all = REACH.read().unwrap();
    all.iter().filter(|(k, _)| url.strip_prefix(k.as_str()).is_some_and(|rest| rest.is_empty() || rest.starts_with('/')))
        .max_by_key(|(k, _)| k.len()).map(|(_, r)| r.clone())
}

/// Open a lake's object store. For a bucket (`s3://`, `gs://`, `az://`, `abfss://`), also returns
/// the bucket-level client that query scans use (DataFusion addresses objects by their full path
/// in the bucket). Credentials and endpoints come from the environment, as each cloud's own tools
/// read them: `AWS_*` (`AWS_ENDPOINT` for R2 and MinIO), `GOOGLE_*`, `AZURE_*`. A lake on another
/// server is read with the key its attachment names (`reach_of`), and a read-only one refuses writes.
pub fn open_store(url: &str) -> Result<(String, Store, Option<(String, Store)>)> {
    if in_memory(url) {
        let url = url.trim_end_matches('/').to_string();
        let held = crate::memory::store(&url);
        return Ok((url.clone(), held.clone(), Some((url, held)))); // (DataFusion reads its files at their URLs, as a bucket's)
    }
    #[cfg(feature = "doors")]
    if served(url) {
        // (the store is the node that holds the lake's, read through its door: `served.rs`; nothing is connected yet)
        let at = url.trim_start_matches("pondra://").trim_end_matches('/');
        anyhow::ensure!(!at.is_empty() && !at.contains('/'), "{url}: a node serves one lake, named by pondra://host:port alone");
        let url = format!("pondra://{at}");
        let store: Store = Arc::new(Counted(Arc::new(crate::served::Served::new(at))));
        return Ok((url.clone(), store.clone(), Some((url, store)))); // (the lake and its bucket are one, as in memory)
    }
    #[cfg(not(feature = "doors"))]
    if served(url) {
        anyhow::bail!("{url}: this form reads no lake through a node's door (pondra://): open its bucket or folder instead");
    }
    if place(url)? == Place::Folder {
        return folder(url);
    }
    let (scheme, rest) = url.split_once("://").filter(|(s, _)| BUCKETS.contains(s)).expect("(a bucket: `place` refused any other URL)");
    let (bucket, prefix) = rest.trim_end_matches('/').split_once('/').unwrap_or((rest, ""));
    let root = format!("{scheme}://{bucket}");
    let reach = reach_of(url);
    let params = reach.as_ref().and_then(|r| r.params.as_ref());
    // Idle connections are dropped after 15 s rather than reused: through proxies and NATs that
    // silently forget idle connections, a reused one hung a PUT for the full 30 s timeout on R2.
    let (idle, after) = (ClientConfigKey::PoolIdleTimeout, "15s");
    // Every request to the bucket takes a turn of its budget, retries too (`budget.rs`, C5).
    let turns = || crate::budget::Budget::of(&root);
    let mut whole: Store = match (params, scheme) {
        (Some(_), _) => crate::ext::build(&url::Url::parse(&root)?, params, crate::ext::For::Lake)?,
        (None, "s3") => Arc::new(AmazonS3Builder::from_env().with_bucket_name(bucket).with_allow_http(true).with_config(AmazonS3ConfigKey::Client(idle), after).with_http_connector(turns()).build()?),
        (None, "gs") => Arc::new(GoogleCloudStorageBuilder::from_env().with_bucket_name(bucket).with_config(GoogleConfigKey::Client(idle), after).with_http_connector(turns()).build()?),
        (None, _) => Arc::new(MicrosoftAzureBuilder::from_env().with_url(&root).with_config(AzureConfigKey::Client(idle), after).with_http_connector(turns()).build()?),
    };
    if reach.as_ref().is_some_and(|r| r.read_only) {
        whole = Arc::new(ReadOnly(whole, url.trim_end_matches('/').to_string())); // (before the prefix: the lake's store and the bucket's refuse alike)
    }
    let store: Store = if prefix.is_empty() { whole.clone() } else { Arc::new(PrefixStore::new(whole.clone(), prefix)) };
    // (the bucket's client, which scans and the SSD tier's misses go through, is counted apart:
    // each request goes through one of the two)
    Ok((url.trim_end_matches('/').to_string(), Arc::new(Counted(store)), Some((root, Arc::new(Counted(whole)) as Store))))
}

/// A folder's store, made if it isn't there.
#[cfg(not(target_arch = "wasm32"))]
fn folder(url: &str) -> Result<(String, Store, Option<(String, Store)>)> {
    // (a folder made here is its user's alone: other users would read its catalog and keys; one already there keeps its permissions)
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        std::fs::DirBuilder::new().recursive(true).mode(0o700).create(url)?;
    }
    #[cfg(not(unix))]
    std::fs::create_dir_all(url)?;
    let dir = std::fs::canonicalize(url)?.to_string_lossy().trim_start_matches(r"\\?\").to_string(); // (Windows verbatim prefix)
    // (`Local`: SlateDB's collector advances its manifest and compactions boundary files with conditional
    // overwrites, and a folder that refuses them never deletes those files. The soak of one local lake
    // for 5 hours grew its catalog from 206 to 13,373 objects, and commits' p95 from 0.4 to 5.2 ms. The
    // boundary stays on (`boundary_files_enabled`): it is what lets a reader whose manifest's successor was
    // deleted list again. The lock in `Local` orders one process's updates only: a folder has one writer,
    // and a writer fenced by a newer one restarts (`fatal`).)
    // (each write is flushed to the disk, the file and then its folder, before it returns: a commit acknowledged on a
    // folder survives a power loss as one in a bucket does, and a catalog file renamed into place is never found
    // empty after one. It cost a one-client commit 1.0 ms at the median, 8 clients 10% of their commits a second and a
    // 1M-row INSERT 3%, on a sandbox's ext4: 10-31)
    Ok((dir.clone(), Arc::new(Counted(Arc::new(Local(LocalFileSystem::new_with_prefix(&dir)?.with_fsync(true))))), None))
}

/// A page has no folders: its lake is in memory or in a bucket.
#[cfg(target_arch = "wasm32")]
fn folder(_: &str) -> Result<(String, Store, Option<(String, Store)>)> {
    Err(crate::codes::coded("0A000", "a page opens no folder: its lake is in memory or in a bucket"))
}

// Lakes held in this process's memory (`memory://`, DuckDB's `:memory:`) live in `memory.rs`, which
// spills their files to disk past a budget.
use crate::memory::in_memory;

/// Is `url` a lake served by the node that holds it (`pondra://host:port`, ADR-072 §8)?
pub fn served(url: &str) -> bool { url.starts_with("pondra://") }

/// The schemes of a bucket's lake (`s3://bucket/prefix`): `open_store` and `place` both take only these.
const BUCKETS: [&str; 5] = ["s3", "gs", "az", "abfs", "abfss"];

/// The kinds of place a lake can be (`place`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Place { Memory, Served, Bucket, Folder }

/// What kind of place `url` is: memory, a node's lake, a bucket, or a folder on this machine (no scheme). Any other
/// scheme is refused here, before anything is opened: a URL no door knows was once made as a folder at a relative path.
pub(crate) fn place(url: &str) -> Result<Place> {
    if in_memory(url) {
        return Ok(Place::Memory);
    }
    if served(url) {
        return Ok(Place::Served);
    }
    match url.split_once("://") {
        None => Ok(Place::Folder),
        Some((scheme, _)) if BUCKETS.contains(&scheme) => Ok(Place::Bucket),
        Some(_) => Err(crate::codes::coded("22023", format!("{url} is not a place a lake can be: a folder, a bucket (s3://, gs://, az://, abfs://, abfss://), memory, or a node's lake (pondra://host:port)"))),
    }
}

/// A lake's bucket, as DataFusion names its store (`s3://bucket`), or None for a folder.
pub fn bucket_url(url: &str) -> Option<String> {
    let (scheme, rest) = url.split_once("://")?;
    Some(format!("{scheme}://{}", rest.split('/').next().unwrap_or_default()))
}

/// A lake's store, counting the requests that cost the most and meet a bucket's rate limits
/// (S3: 3,500 writes a second per prefix): writes, lists and deletes (`GET /metrics`).
#[derive(Debug)]
pub(crate) struct Counted(pub(crate) Store);

impl std::fmt::Display for Counted {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { self.0.fmt(f) }
}

#[async_trait::async_trait]
impl ObjectStore for Counted {
    async fn put_opts(&self, at: &Path, body: object_store::PutPayload, opts: PutOptions) -> object_store::Result<object_store::PutResult> {
        let r = self.0.put_opts(at, body, opts).await;
        if !matches!(r, Err(object_store::Error::NotSupported { .. } | object_store::Error::NotImplemented { .. })) {
            crate::metrics::add(&crate::metrics::OBJECT_WRITES, 1); // (a local disk refuses SlateDB's tagged PUT before writing; it tries again untagged)
        }
        r
    }
    async fn put_multipart_opts(&self, at: &Path, opts: object_store::PutMultipartOptions) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
        crate::metrics::add(&crate::metrics::OBJECT_WRITES, 1);
        Ok(Box::new(CountedParts(self.0.put_multipart_opts(at, opts).await?)))
    }
    async fn get_opts(&self, at: &Path, opts: object_store::GetOptions) -> object_store::Result<object_store::GetResult> {
        crate::metrics::add(&crate::metrics::OBJECT_READS, 1); // (a HEAD too: billed as a GET)
        self.0.get_opts(at, opts).await
    }
    fn delete_stream(&self, at: futures::stream::BoxStream<'static, object_store::Result<Path>>) -> futures::stream::BoxStream<'static, object_store::Result<Path>> {
        use futures::StreamExt;
        self.0.delete_stream(at.inspect(|_| crate::metrics::add(&crate::metrics::OBJECT_DELETES, 1)).boxed())
    }
    fn list(&self, prefix: Option<&Path>) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>> {
        crate::metrics::add(&crate::metrics::OBJECT_LISTS, 1);
        self.0.list(prefix)
    }
    // (forwarded: the store's own offset, not the trait's default that lists everything)
    fn list_with_offset(&self, prefix: Option<&Path>, offset: &Path) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>> {
        crate::metrics::add(&crate::metrics::OBJECT_LISTS, 1);
        self.0.list_with_offset(prefix, offset)
    }
    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<object_store::ListResult> {
        crate::metrics::add(&crate::metrics::OBJECT_LISTS, 1);
        self.0.list_with_delimiter(prefix).await
    }
    async fn copy_opts(&self, from: &Path, to: &Path, opts: object_store::CopyOptions) -> object_store::Result<()> {
        crate::metrics::add(&crate::metrics::OBJECT_WRITES, 1);
        self.0.copy_opts(from, to, opts).await
    }
}

/// A multipart upload's parts and its completion, each a request the bucket bills as a write.
#[derive(Debug)]
struct CountedParts(Box<dyn object_store::MultipartUpload>);

#[async_trait::async_trait]
impl object_store::MultipartUpload for CountedParts {
    fn put_part(&mut self, data: object_store::PutPayload) -> object_store::UploadPart {
        crate::metrics::add(&crate::metrics::OBJECT_WRITES, 1);
        self.0.put_part(data)
    }
    async fn complete(&mut self) -> object_store::Result<object_store::PutResult> {
        crate::metrics::add(&crate::metrics::OBJECT_WRITES, 1);
        self.0.complete().await
    }
    async fn abort(&mut self) -> object_store::Result<()> { self.0.abort().await }
}

/// A local folder's store. `LocalFileSystem` refuses a conditional overwrite (`PutMode::Update`), and
/// SlateDB's collector advances its boundary files with one: from the collector's second pass on, its
/// manifest and compactions passes failed and deleted nothing. So an `Update` here is a head and an
/// overwrite with the e-tag checked between them (a folder's version is its e-tag alone: `version` is
/// None there); every other call is the folder's own.
#[cfg(not(target_arch = "wasm32"))]
#[derive(Debug)]
pub(crate) struct Local(pub(crate) LocalFileSystem);

/// One process's conditional updates of a folder take turns here, so a check and the overwrite after it
/// never interleave. (Not `Lake::replace`'s `ONE`, which is held across its own put.)
#[cfg(not(target_arch = "wasm32"))]
static UPDATES: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

#[cfg(not(target_arch = "wasm32"))]
impl std::fmt::Display for Local {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { self.0.fmt(f) }
}

#[cfg(not(target_arch = "wasm32"))]
#[async_trait::async_trait]
impl ObjectStore for Local {
    async fn put_opts(&self, at: &Path, body: object_store::PutPayload, opts: PutOptions) -> object_store::Result<object_store::PutResult> {
        let PutMode::Update(version) = &opts.mode else { return self.0.put_opts(at, body, opts).await };
        let _turn = UPDATES.lock().await;
        let refused = |why: String| object_store::Error::Precondition { path: at.to_string(), source: why.into() };
        let current = match self.0.head(at).await {
            Ok(meta) => meta.e_tag,
            Err(object_store::Error::NotFound { .. }) => return Err(refused("there is no object to update".into())),
            Err(e) => return Err(e),
        };
        if current != version.e_tag {
            return Err(refused(format!("its e-tag is {current:?}, not {:?}", version.e_tag)));
        }
        // (the attributes stay: LocalFileSystem refuses them by name, before it writes anything)
        self.0.put_opts(at, body, PutOptions { mode: PutMode::Overwrite, ..opts }).await
    }
    async fn put_multipart_opts(&self, at: &Path, opts: object_store::PutMultipartOptions) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
        self.0.put_multipart_opts(at, opts).await
    }
    async fn get_opts(&self, at: &Path, opts: object_store::GetOptions) -> object_store::Result<object_store::GetResult> { self.0.get_opts(at, opts).await }
    async fn get_ranges(&self, at: &Path, ranges: &[std::ops::Range<u64>]) -> object_store::Result<Vec<bytes::Bytes>> { self.0.get_ranges(at, ranges).await }
    fn delete_stream(&self, at: futures::stream::BoxStream<'static, object_store::Result<Path>>) -> futures::stream::BoxStream<'static, object_store::Result<Path>> { self.0.delete_stream(at) }
    fn list(&self, prefix: Option<&Path>) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>> { self.0.list(prefix) }
    fn list_with_offset(&self, prefix: Option<&Path>, offset: &Path) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>> { self.0.list_with_offset(prefix, offset) }
    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<object_store::ListResult> { self.0.list_with_delimiter(prefix).await }
    async fn copy_opts(&self, from: &Path, to: &Path, opts: object_store::CopyOptions) -> object_store::Result<()> { self.0.copy_opts(from, to, opts).await }
    async fn rename_opts(&self, from: &Path, to: &Path, opts: object_store::RenameOptions) -> object_store::Result<()> { self.0.rename_opts(from, to, opts).await }
}

/// What a node still catching up with its leader answers (57P03: it ran nothing, ask again or
/// another node; `Lake::caught_up`).
pub const CATCHING_UP: &str = "this node just started and is still catching up with its leader: try again in a moment, or ask another node";

/// A lake read with a read-only key (ADR-058): nothing in this process writes or deletes there,
/// whatever the key allows (the bucket would refuse too; this says why first).
#[derive(Debug)]
struct ReadOnly(Store, String);

/// What a write to a lake read with a read-only key gets back.
fn refused(lake: &str, path: &str) -> object_store::Error {
    object_store::Error::PermissionDenied { path: path.to_string(), source: format!("{lake} is read with a read-only key here: nothing on this server writes it (ADR-058)").into() }
}

impl std::fmt::Display for ReadOnly {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { write!(f, "{} (read only)", self.0) }
}

#[async_trait::async_trait]
impl ObjectStore for ReadOnly {
    async fn put_opts(&self, at: &Path, _: object_store::PutPayload, _: PutOptions) -> object_store::Result<object_store::PutResult> { Err(refused(&self.1, &at.to_string())) }
    async fn put_multipart_opts(&self, at: &Path, _: object_store::PutMultipartOptions) -> object_store::Result<Box<dyn object_store::MultipartUpload>> { Err(refused(&self.1, &at.to_string())) }
    async fn get_opts(&self, at: &Path, opts: object_store::GetOptions) -> object_store::Result<object_store::GetResult> { self.0.get_opts(at, opts).await }
    fn delete_stream(&self, at: futures::stream::BoxStream<'static, object_store::Result<Path>>) -> futures::stream::BoxStream<'static, object_store::Result<Path>> {
        use futures::StreamExt;
        let lake = self.1.clone();
        at.map(move |p| Err(refused(&lake, &p.map(|p| p.to_string()).unwrap_or_default()))).boxed()
    }
    fn list(&self, prefix: Option<&Path>) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>> { self.0.list(prefix) }
    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<object_store::ListResult> { self.0.list_with_delimiter(prefix).await }
    async fn copy_opts(&self, _: &Path, to: &Path, _: object_store::CopyOptions) -> object_store::Result<()> { Err(refused(&self.1, &to.to_string())) }
}

/// How often a reader polls the bucket for newer manifests and WAL: a node's, and the CLI's.
pub const READER_POLL: Duration = Duration::from_millis(250);

/// How often a node that follows the leader's commit stream polls the bucket for newer manifests. Once seeded it answers
/// every read from the stream (`Catalog::seed`): its own view only lets it drop the inline data it holds, and catches it up
/// after a gap in the stream. Each poll is two GETs (the next manifest, and SlateDB's GC boundary), so every 250 ms was
/// 480 GETs a minute a follower, idle or not (reviews/storage-requests-2026-11-01.md); every second is 120.
pub const FOLLOWER_POLL: Duration = Duration::from_secs(1);

/// A guest that reads reads one snapshot, for one statement, and never polls: its life is far shorter than this.
pub const READER_ONE_SNAPSHOT: Duration = Duration::from_secs(3600);

impl Lake {
    /// `writer`: the leader. `streamed`: a follower, which also gets every commit streamed from
    /// the leader, so its own catalog view only needs the leader's checkpoints (no log replay).
    /// What opening a folder with no catalog says (a follower of a leader still making one waits).
    pub const NO_LAKE: &'static str = "holds no lake yet";

    pub async fn open(url: &str, writer: bool, streamed: bool) -> Result<Arc<Lake>> {
        Self::open_polled(url, writer, streamed, READER_POLL).await
    }

    /// `open`, a reader polling the bucket every `poll` (`READER_POLL`, or `READER_ONE_SNAPSHOT` for a guest's one statement).
    pub async fn open_polled(url: &str, writer: bool, streamed: bool, poll: Duration) -> Result<Arc<Lake>> {
        Self::open_with(url, writer, streamed, poll, None).await
    }

    /// A reader of the lake at `url` (a memory URL whose store is given: `memory::give`) whose catalog is `kv`, a node's
    /// over `kv::remote` for a joined page (designs/l2-page-joins-cluster.md, J5). Nothing polls `kv`, and the lake's torn
    /// move is not checked here: the node that serves the catalog checks it.
    pub(crate) async fn open_on(url: &str, streamed: bool, kv: Arc<dyn Kv>) -> Result<Arc<Lake>> {
        Self::open_with(url, false, streamed, READER_POLL, Some(kv)).await
    }

    /// `open_polled` and `open_on`: a reader's catalog is `given` when one is, else `kv::reader` on the lake's store.
    async fn open_with(url: &str, writer: bool, streamed: bool, poll: Duration, given: Option<Arc<dyn Kv>>) -> Result<Arc<Lake>> {
        let (url, store, bucket) = open_store(url)?;
        let memory = in_memory(&url); // (no bucket to bill or cache: its catalog's garbage goes as a local disk's)
        let local = bucket.is_none() || memory;
        let pool = lake_pool();
        let ledger = pool.ledger().clone();
        // (spills go to `spill_disk`; no listing kept past its statement: DataFusion's own cache of
        // them would hide a file added to a folder outside the lake, ADR-026)
        // (Parquet footers and files' statistics kept by caches of our own, so `cached_bytes` can say
        // what they hold: DataFusion's own, at its own limits)
        use datafusion::execution::cache::cache_manager::{CacheManagerConfig, DEFAULT_FILE_STATISTICS_MEMORY_LIMIT, DEFAULT_METADATA_CACHE_LIMIT};
        let kept = (Arc::new(Footers::new(DEFAULT_METADATA_CACHE_LIMIT).with_name("DefaultFileMetadataCache")), Arc::new(FileStatistics::new(DEFAULT_FILE_STATISTICS_MEMORY_LIMIT).with_name("DefaultFileStatisticsCache")));
        let caches = CacheManagerConfig::default().with_list_files_cache_limit(0).with_file_metadata_cache(Some(kept.0.clone())).with_file_statistics_cache(Some(kept.1.clone()));
        let rt = RuntimeEnvBuilder::new().with_memory_pool(pool).with_cache_manager(caches).with_disk_manager_builder(spill_disk()).build_arc()?;
        let disk = bucket.as_ref().filter(|_| !memory).and_then(|_| disk_tier(&url, &store));
        let mut cached_store = None;
        if let Some((bucket_url, held)) = bucket.as_ref().filter(|_| memory) {
            rt.register_object_store(&url::Url::parse(bucket_url)?, Arc::new(crate::bridge::Bridge(held.clone()))); // (in memory already: read as it is)
        } else if let Some((bucket_url, s3)) = bucket {
            let prefix = url.trim_start_matches(&bucket_url).trim_start_matches('/').to_string();
            let cached = Arc::new(crate::cache::CachedStore::new(s3, cache_mb() << 20, disk.clone().map(|d| (d, prefix.clone())), &prefix));
            rt.register_object_store(&url::Url::parse(&bucket_url)?, cached.clone());
            cached_store = Some(cached);
        }
        // The catalog's own files go on the SSD tier too (next to the lake's objects), so catalog
        // reads — the leader's, a new node's, a reader's without a live leader — are local.
        let cache_dir = disk.as_ref().map(|d| d.dir.with_extension("catalog"));
        let check_torn = given.is_none();
        let opening = async {
            Ok::<_, anyhow::Error>(match writer {
                true => {
                    let t0 = datafusion::common::instant::Instant::now();
                    let kv = kv::writer(store.clone(), cache_dir, local).await?;
                    trace_since("the catalog's writer open", t0);
                    Catalog::writer(kv, t0).await?
                }
                // (a given catalog is a node's: no bucket to wait for, and the store a page holds lists nothing under `catalog/`,
                // so `opened`'s listing after 3 s would fail a slow open)
                false => match given {
                    Some(kv) => Catalog::reader(kv, streamed).await?,
                    None => match Catalog::opened(store.clone(), streamed, kv::reader(store.clone(), streamed, cache_dir, poll)).await {
                        Ok(cat) => cat,
                        // (no catalog at all: say so, rather than the database's own words for it)
                        Err(e) => match futures::StreamExt::next(&mut store.list(Some(&object_store::path::Path::from("catalog/manifest")))).await {
                            None => anyhow::bail!("{url} {}: start one there (`pondra {url}`, or `pondra serve --dir {url}`), or make a table in it", Lake::NO_LAKE),
                            Some(_) => return Err(e),
                        },
                    },
                },
            })
        };
        // (the place's torn check runs beside the catalog's opening, so a bucket pays no extra round trip; `join`
        // and not `try_join`: a failure of one must not drop the other half way, with SlateDB's tasks still running)
        let (torn, cat) = tokio::join!(async { if check_torn { crate::memory::refuse_torn(&store, &url).await } else { Ok(()) } }, opening);
        torn?;
        let cat = cat?;
        crate::format::check(&cat, &url, writer).await?; // (a lake a newer Pondra wrote: refused before its tables are read, ADR-039)
        let hwm = watch::Sender::new(cat.get::<u64>("n").await?.unwrap_or(1) - 1);
        cat.frozen.store(cat.get::<serde_json::Value>(crate::backup::KEY).await?.is_some(), Relaxed); // (a backup: ADR-070)
        let lake = Arc::new_cyclic(|me| Lake { url, store, cat, hwm, backlog: Default::default(), rt, tail: Default::default(), tails: Arc::new(Mutex::new(crate::query::tails())), kept, disk, groups: Arc::new(crate::serve::Groups::new(cache_mb() << 19)), hot: Arc::new(crate::hot::Hot::new(memory)), attached: Default::default(), ids: Default::default(), cached: cached_store, caught: watch::channel(true).0, to: Default::default(), owner_key: Default::default(), sequences: Default::default(), bases: Default::default(), sessions: Default::default(), me: me.clone() });
        for (name, cache) in lake.caches() {
            ledger.charge(name, Arc::downgrade(&cache)); // (they give memory back when the node needs it: the one watch takes it, `oom.rs`)
        }
        ledger.charge("facts", Arc::downgrade(&crate::query::facts_kept())); // (the node's, charged once whatever lakes it opens)
        ledger.charge("manifests", Arc::downgrade(&crate::manifest::kept())); // (the node's too: see `node::start_app` for the rest)
        ledger.charge("files", Arc::downgrade(&crate::cache::outside())); // (the process's, of files outside the lakes)
        crate::oom::wake();
        crate::branch::load(&lake).await?; // (a branch's files listed in its bases: ADR-047)
        if let Some(writes) = lake.cat.unstarted.lock().unwrap().take() {
            crate::panics::spawn(Lake::commits(Arc::downgrade(&lake), writes)); // (weak: a lake opened in a process goes when its last handle does)
        }
        Ok(lake)
    }

    /// Leader: commits, in write order. A write is committed — acknowledged, visible here, and
    /// streamed as committed — once it is in the bucket, or (`--ack replicated`) once
    /// `replicas - 1` member followers hold it, whichever comes first. It reaches the bucket a
    /// moment later either way. A write that fails to reach the bucket (a newer leader fenced
    /// us out) has an unknown outcome: restart and rejoin; the restart reloads the state.
    /// Both loops hold the lake weakly: the writes' sender is the lake's own (`Catalog::writes`), so
    /// a strong hold would keep a lake opened in a process (a backup taken in it) for ever.
    async fn commits(me: std::sync::Weak<Self>, mut writes: mpsc::UnboundedReceiver<Write>) {
        let (to_bucket, mut in_bucket) = mpsc::unbounded_channel::<(Arc<dyn Written>, u64)>();
        let lake = me.clone();
        crate::panics::spawn(async move {
            while let Some((handle, id)) = in_bucket.recv().await {
                if let Err(e) = handle.durable().await {
                    eprintln!("catalog commit failed: {e}");
                    crate::node::restart("a catalog commit failed");
                }
                let Some(lake) = lake.upgrade() else { return };
                lake.cat.durable.send_replace(id);
                lake.cat.send(Frame::Durable(id));
            }
        });
        while let Some((handle, d, done, sent)) = writes.recv().await {
            let t = commit_stage("queued", sent);
            let Some(lake) = me.upgrade() else { return };
            let cat = &lake.cat;
            let need = cat.replicas.saturating_sub(1);
            if need > 0 {
                cat.send(Frame::Change(d.clone())); // followers keep it and say so
            }
            let mut acked = cat.acked.subscribe();
            // (At most AHEAD commits are acknowledged ahead of the bucket: if it stalls, acks wait
            // for it, so what a failover has to recover — and could lose — stays bounded.)
            let close = *cat.durable.borrow() + AHEAD >= d.id;
            loop {
                if need > 0 && close && cat.holders(d.id) >= need {
                    break;
                }
                tokio::select! {
                    r = handle.durable() => {
                        if let Err(e) = r {
                            eprintln!("catalog commit failed: {e}");
                            crate::node::restart("a catalog commit failed");
                        }
                        break;
                    }
                    _ = acked.changed() => {}
                }
            }
            let t = commit_stage("durable", t);
            if need == 0 {
                cat.send(Frame::Change(d.clone()));
            }
            cat.apply(&d); // our in-memory catalog
            cat.committed.store(d.id, Relaxed);
            cat.send(Frame::Committed(d.id));
            let _ = to_bucket.send((handle, d.id));
            let _ = done.send(());
            commit_stage("applied", t);
        }
    }

    /// Follower: a change streamed from the leader; it takes effect once committed.
    pub fn hold(&self, d: Arc<Delta>) {
        self.arrived(&d);
        self.cat.pending.lock().unwrap().insert(d.id, d);
    }

    /// Follower: a new leader's term began (`members::follow`): the changes the old one streamed and never committed are
    /// not the new one's, so they go.
    pub fn drop_pending(&self) {
        self.cat.pending.lock().unwrap().clear();
    }

    /// Follower: lay the committed changes (up to `upto`) over our view of the catalog.
    pub fn commit_upto(&self, upto: u64) {
        let ready: Vec<Arc<Delta>> = {
            let mut p = self.cat.pending.lock().unwrap();
            let later = p.split_off(&(upto + 1));
            std::mem::replace(&mut *p, later).into_values().collect()
        };
        ready.iter().for_each(|d| self.cat.apply(d));
        self.advance(self.cat.visible_n() - 1);
    }

    /// A commit seen here. The recent lake is kept on this node's SSD: every object it brings in (a
    /// log segment another node wrote, a new Parquet file) is fetched in the background, so a query
    /// on any node reads recent data from local disk, not from the bucket. And the files it
    /// replaced leave the hot columns.
    pub fn arrived(&self, d: &Delta) {
        for (key, value) in &d.puts {
            match (key.get(..2), &self.disk) {
                (Some("s/"), Some(disk)) => {
                    if let Ok(s) = serde_json::from_slice::<Segment>(value) {
                        if !s.path.is_empty() && !s.path.starts_with("_base/") {
                            disk.fetch_later(s.path);
                        }
                    }
                }
                (Some("t/"), disk) => {
                    let decoded = self.tail.lock().unwrap().rows.contains_key(&key[2..]);
                    if !(decoded || disk.is_some() || self.hot.on()) {
                        continue;
                    }
                    let Ok(m) = serde_json::from_slice::<TableMeta>(value) else { continue };
                    self.tail.lock().unwrap().forget(&key[2..], m.tiered); // (rows now in files: no read asks for them again)
                    self.hot.forget(&m.garbage);
                    // (files a tiering round writes; not a bulk load's big files, which only the queries that need them read)
                    if let Some(disk) = disk {
                        m.files.into_iter().filter(|f| f.bytes <= 256 << 20 && !f.path.is_empty() && !f.path.starts_with("_base/")).for_each(|f| disk.fetch_later(f.path));
                    }
                }
                _ => {} // (keys like "c" and "n" are one character long)
            }
        }
    }

    /// A node starting on a lake in object storage: copy the log tail and the newest table
    /// files (up to half the SSD tier) to local disk in the background, so a first query on a
    /// fresh node doesn't wait on the bucket either.
    pub async fn warm(&self) -> Result<()> {
        let Some(disk) = &self.disk else { return Ok(()) };
        let tables = self.cat.scan::<TableMeta>("t/", "t0").await?;
        let tiered = tables.iter().map(|(_, m)| m.tiered).min().unwrap_or(0);
        for (_, seg) in self.cat.segments(&seg_key(tiered + 1), "s0").await? {
            if !seg.path.is_empty() {
                disk.fetch_later(seg.path.clone());
            }
        }
        let mut budget = disk.max / 2;
        for f in tables.iter().flat_map(|(_, m)| m.files.iter().rev()) {
            if f.bytes <= budget && !f.path.starts_with("_base/") {
                budget -= f.bytes;
                disk.fetch_later(f.path.clone());
            }
        }
        Ok(())
    }

    /// Wait, 10 s at most, until this node holds what its leader had committed when it started
    /// (`cluster::catch_up`), and refuse (57P03) if it doesn't yet. A node restarted after a
    /// failover would otherwise answer from an older catalog than it did before: its view reads no
    /// WAL, and the new leader flushes what it took over a moment after it leads (a table just
    /// made was "not found").
    pub async fn caught_up(&self) -> Result<()> {
        if !*self.caught.borrow() {
            let _ = tokio::time::timeout(std::time::Duration::from_secs(10), self.caught.subscribe().wait_for(|c| *c)).await;
        }
        anyhow::ensure!(*self.caught.borrow(), "{CATCHING_UP}");
        Ok(())
    }

    /// Table `name`'s entry; None only once this node holds what its leader had committed when it started (`caught_up`):
    /// a node just started, or a follower whose leader was just restarted, may not hold a table yet, and says so (57P03:
    /// a client asks again) rather than that there is none. An entry it holds answers at once.
    pub async fn table_meta_if(&self, name: &str) -> Result<Option<Arc<TableMeta>>> {
        if let Some(meta) = self.cat.meta(&table_key(name)).await? {
            return Ok(Some(meta));
        }
        self.caught_up().await?;
        self.cat.meta(&table_key(name)).await
    }

    /// Table `name`'s entry, or "no table" (42P01), as [`Lake::table_meta_if`] decides it.
    pub async fn table_meta(&self, name: &str) -> Result<Arc<TableMeta>> {
        self.table_meta_if(name).await?.ok_or_else(|| anyhow::anyhow!("no table {name}"))
    }

    /// Non-leaders: catch up with our own catalog view (and prune what it now holds).
    pub async fn refresh(&self) -> Result<()> {
        self.cat.refresh().await?;
        self.advance(self.cat.visible_n() - 1);
        Ok(())
    }

    /// The highest segment this node's reads include right now: on the leader everything that is
    /// durable, on a follower what its own view plus the streamed commits hold. (`hwm` only wakes
    /// readers up; it can be ahead of what a follower can actually read.)
    pub fn visible(&self) -> u64 {
        match self.cat.is_writer() {
            true => *self.hwm.borrow(),
            false => self.cat.visible_n().saturating_sub(1),
        }
    }

    /// Note that segments up to `hwm` are committed (wakes tasks and watchers).
    pub fn advance(&self, hwm: u64) {
        self.hwm.send_if_modified(|h| std::mem::replace(h, (*h).max(hwm)) < hwm);
    }

    /// What a remembered answer to `sql` depends on: this lake's catalog, and those of the attached
    /// lakes it may read, itself or through the views it reads (and which lakes are attached).
    /// None: nothing to remember it by.
    pub async fn version_for(&self, sql: &str) -> Option<u64> {
        use std::hash::{Hash, Hasher};
        let mut h = std::collections::hash_map::DefaultHasher::new();
        self.cat.version()?.hash(&mut h);
        let mut text = sql.to_string();
        if !self.attached.read().unwrap().is_empty() {
            crate::query::stored_views(self, sql, false).await.ok()?.iter().for_each(|(_, v)| text.push_str(&format!(" {v}")));
        }
        for (name, other) in self.attached.read().unwrap().iter().filter(|(n, _)| crate::text::mentions(&text, n)) {
            (name, other.cat.version()?, crate::auth::operator()).hash(&mut h); // (whoever runs the nodes may read what a user may not: `users::across`)
        }
        Some(h.finish())
    }

    /// Read another lake as `name.table` in this one's queries (its bucket's reader goes into our
    /// runtime too: a query here reads its files).
    pub fn attach(&self, name: &str, other: Arc<Lake>) -> Result<()> {
        let name = &name.to_lowercase(); // (as SQL reads an unquoted name)
        crate::ddl::check(name)?;
        anyhow::ensure!(*name != crate::ddl::lake_name(self), "this lake is called {name} already: attach the other under another name");
        match (bucket_url(&other.url), &self.cached) {
            (Some(bucket), Some(ours)) if bucket_url(&self.url).as_ref() == Some(&bucket) => ours.add_lake(other.url[bucket.len()..].trim_start_matches('/')), // (one bucket: our reader, its files cached too)
            (Some(bucket), _) => {
                let url = url::Url::parse(&bucket)?;
                self.rt.register_object_store(&url, other.rt.object_store(datafusion::execution::object_store::ObjectStoreUrl::parse(&bucket)?)?);
            }
            _ => {}
        }
        // (the catalog's sync and an ATTACH may both open it at once, on a bucket for seconds: one is kept)
        let mut attached = self.attached.write().unwrap();
        if !attached.iter().any(|(n, _)| n == name) {
            attached.push((name.to_string(), other));
        }
        Ok(())
    }

    /// A fresh SQL session on the shared runtime (`partitions()` of them).
    pub fn session(&self) -> SessionContext {
        self.session_with(partitions())
    }

    /// A session with a fixed number of partitions: 1 for point lookups, where splitting the work
    /// costs more than it saves and many queries run at once. Its functions, planners and rules
    /// are the same for every query, so they are made once (`made`) and copied; each copy gets
    /// catalogs of its own, so the tables a query registers are its alone.
    pub fn session_with(&self, partitions: usize) -> SessionContext {
        spill_room(); // (the spill disk's room as it is now: `Spills` looked at it once, when the first spill made the disk)
        let state = self.sessions.lock().unwrap().entry(partitions).or_insert_with(|| self.made(partitions).state()).clone();
        let catalog = datafusion::execution::SessionStateDefaults::default_catalog(state.config(), state.table_factories(), state.runtime_env());
        let ctx = crate::engine::with_catalog(state, Arc::new(catalog));
        crate::files::register(&ctx, self.arc()); // files('…'), file_read(path) (here, not in what is kept: the lake would keep itself)
        crate::seq::register(&ctx, self.arc()); // nextval, currval, setval
        crate::ext::register_secrets(&ctx, self.arc()); // secrets()
        crate::extension::register(&ctx, self.arc()); // extensions() (SHOW EXTENSIONS)
        ctx
    }

    /// A session made from nothing, with everything every query's session has: every function set in the registry
    /// (`engine::session`). On a page every plan it makes runs its partitions in the page's task, as the page's own
    /// session's do (`in_task::planned`): a page has no runtime to start a partition's task on, and a plan of one
    /// partition still merges more than one below it. Plans made by hand from it (`fresh.rs`) are planned by it too.
    fn made(&self, partitions: usize) -> SessionContext {
        let config = SessionConfig::new().with_target_partitions(partitions).with_default_catalog_and_schema(crate::ddl::lake_name(self), crate::text::PUBLIC);
        let ctx = crate::engine::session(config, self.rt.clone());
        #[cfg(target_arch = "wasm32")]
        let ctx = crate::in_task::planned(ctx);
        ctx
    }

    /// Query memory in use on the node's line, every lake's together, and the line (`ledger::Ledger`).
    pub fn memory(&self) -> (usize, usize) {
        match self.ledger() {
            Some(ledger) => (ledger.reserved(), ledger.line()),
            None => (self.rt.memory_pool.reserved(), memory_limit()),
        }
    }

    /// The node's line this lake's queries and caches are on (`lake_pool`).
    pub fn ledger(&self) -> Option<&Arc<crate::ledger::Ledger>> {
        Some(self.rt.memory_pool.downcast_ref::<crate::ledger::LakePool>()?.ledger())
    }

    /// This lake's caches, each by the name `/metrics` gives it, in the order they give memory back when the node needs it
    /// (`ledger::Charged`; each is charged to the lake's line as it opens). The hot columns go first, decoded again from
    /// the files when they are read; then the row groups for lookups and the tables' tails, read again from the files and
    /// the log; then the bucket's byte ranges, fetched again. The log's decoded rows, the catalog's blocks and DataFusion's
    /// footers and statistics keep what they hold by rules of their own, and say what that is.
    pub fn caches(&self) -> Vec<(&'static str, Arc<dyn crate::ledger::Charged>)> {
        let mut caches: Vec<(&'static str, Arc<dyn crate::ledger::Charged>)> = vec![("hot", self.hot.clone()), ("lookups", self.groups.clone()), ("tails", self.tails.clone())];
        if let Some(ranges) = &self.cached {
            caches.push(("ranges", ranges.clone())); // (a lake on a bucket: a local or in-memory one reads its store directly)
        }
        caches.extend([("log", self.tail.clone() as _), ("catalog_blocks", self.cat.blocks()), ("footers", self.kept.0.clone() as _), ("statistics", self.kept.1.clone() as _)]);
        caches.extend(self.cat.kept_parsed());
        caches
    }

    /// What each of the node's caches on this lake's line holds now, by name, in bytes (`/metrics`' `cache_bytes`; the hot
    /// columns are its `hot_bytes`): what fills up to its limit is not memory the node is losing (`tools/soak.py`). A cache
    /// charged to the line (`ledger::Ledger::charge`) is one of them.
    pub fn cached_bytes(&self) -> Vec<(&'static str, usize)> {
        match self.ledger() {
            Some(ledger) => ledger.held_by_name().into_iter().filter(|(name, _)| *name != "hot").collect(),
            None => self.caches().into_iter().filter(|(name, _)| *name != "hot").map(|(name, cache)| (name, cache.held())).collect(), // (a pool not the ledger's: none here)
        }
    }

    /// This lake as an `Arc` (for query plans that outlive the call that made them).
    pub fn arc(&self) -> Arc<Lake> { self.me.upgrade().expect("a lake outlives its queries") }

    /// Full URL of an object, for DataFusion.
    pub fn full(&self, path: &str) -> String {
        if path.contains("://") {
            return path.to_string(); // (a file outside the lake: `ext.rs`)
        }
        if let Some((id, rest)) = crate::branch::split(path) {
            if let Some((url, _)) = self.bases.read().unwrap().get(id) {
                return format!("{url}/{rest}"); // (a branch's file in its base: ADR-047)
            }
        }
        format!("{}/{path}", self.url)
    }

    /// A branch's base (ADR-047), so the paths it lists there resolve: DataFusion reads its files
    /// through this lake's bucket reader when they share a bucket, or one of its own.
    pub fn add_base(&self, id: &str, url: &str) -> Result<()> {
        if self.bases.read().unwrap().contains_key(id) {
            return Ok(());
        }
        crate::budget::share(url); // (its objects read with a share of the bucket's turns: prod's own nodes come first)
        match (bucket_url(url), &self.cached) {
            (Some(bucket), Some(ours)) if bucket_url(&self.url).as_ref() == Some(&bucket) => ours.add_lake(url[bucket.len()..].trim_start_matches('/')),
            (Some(bucket), _) => {
                if let Ok((_, _, Some((root, whole)))) = open_store(url) {
                    let prefix = url[root.len()..].trim_start_matches('/').to_string();
                    self.rt.register_object_store(&url::Url::parse(&bucket)?, Arc::new(crate::cache::CachedStore::new(whole, cache_mb() << 20, None, &prefix)));
                }
            }
            _ => {}
        }
        self.bases.write().unwrap().insert(id.to_string(), (url.to_string(), None));
        Ok(())
    }

    /// A branch's base's store, for the objects of the base it reads whole (manifests, segments).
    fn base_store(&self, id: &str) -> Result<Store> {
        let url = match self.bases.read().unwrap().get(id) {
            Some((_, Some(store))) => return Ok(store.clone()),
            Some((url, None)) => url.clone(),
            None => anyhow::bail!("no base {id} in this branch"),
        };
        anyhow::ensure!(url.contains("://") || std::path::Path::new(&url).exists(), "this branch's base {url} is gone");
        let store = open_store(&url)?.1;
        if let Some(b) = self.bases.write().unwrap().get_mut(id) {
            b.1 = Some(store.clone());
        }
        Ok(store)
    }

    /// The store DataFusion reads `url` through (for lakes on object storage: the read cache and
    /// the SSD tier in front of the bucket).
    pub fn object_store(&self, url: &datafusion::datasource::listing::ListingTableUrl) -> Result<Arc<dyn object_store_df::ObjectStore>> {
        Ok(self.rt.object_store(url)?)
    }

    /// Write an object only if it does not exist yet: data is never overwritten.
    pub async fn put(&self, path: &str, bytes: impl Into<Bytes>) -> Result<()> {
        if self.cat.frozen.load(Relaxed) {
            return Err(crate::backup::refused(Some(&self.url))); // (a backup's objects are its runs': ADR-070)
        }
        let opts = PutOptions { mode: PutMode::Create, ..Default::default() };
        let bytes: Bytes = bytes.into(); // (a `Vec` or `Bytes` as it is: a page's 232 MB file was held twice while it was put)
        self.store.put_opts(&Path::from(path), bytes.clone().into(), opts).await?;
        if let (Some(disk), false) = (&self.disk, crate::delta::open_format(path) || replaceable(path)) {
            disk.put(path, &bytes); // what a node writes, it keeps
        }
        Ok(())
    }

    /// A whole lake object: from the SSD tier if it's there, else from the bucket (and kept). A
    /// user's file (`files/`) may be replaced, so it is always the bucket's (ADR-034).
    pub async fn object(&self, path: &str) -> Result<Bytes> {
        let disk = self.disk.as_ref().filter(|_| !replaceable(path));
        if let Some((file, _)) = disk.and_then(|d| d.get(path)) {
            if let Ok(bytes) = tokio::fs::read(file).await {
                return Ok(bytes.into());
            }
        }
        let bytes = match crate::branch::split(path) {
            Some((id, rest)) => self.base_store(id)?.get(&Path::from(rest)).await?.bytes().await?, // (a branch's base's: ADR-047)
            None => self.store.get(&Path::from(path)).await?.bytes().await?,
        };
        if let Some(disk) = disk {
            disk.put(path, &bytes);
        }
        Ok(bytes)
    }

    /// Where `path` is read whole, past the caches: this lake's bucket, or a branch's base's (a
    /// backup copies through it, a piece at a time).
    pub fn reader(&self, path: &str) -> Result<(Store, Path)> {
        Ok(match crate::branch::split(path) {
            Some((id, rest)) => (self.base_store(id)?, Path::from(rest)),
            None => (self.store.clone(), Path::from(path)),
        })
    }

    /// A user's file's version, for `If-Match` (`GET /files` gives it): its e-tag and size, or
    /// (a store without e-tags) its time and size.
    pub async fn version(&self, path: &str) -> Result<String> { Ok(version_of(&self.store.head(&Path::from(path)).await?)) }

    /// A user's file and the version of those very bytes (one request: `GET /files`).
    pub async fn file(&self, path: &str) -> Result<(Bytes, String)> {
        let r = self.store.get(&Path::from(path)).await?;
        let v = version_of(&r.meta);
        Ok((r.bytes().await?, v))
    }

    /// Replace a user's file (`files/`) that is still the version `expect` (ADR-034): a newer
    /// one, saved meanwhile, is refused (`Changed`), so two editors never overwrite each other
    /// blind. Where the store can, the check and the write are one request (a conditional put).
    pub async fn replace(&self, path: &str, bytes: impl Into<Bytes>, expect: &str) -> Result<String> {
        anyhow::ensure!(replaceable(path), "only the lake's files (files/…) are replaced");
        if self.cat.frozen.load(Relaxed) {
            return Err(crate::backup::refused(Some(&self.url))); // (ADR-070)
        }
        static ONE: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(()); // (a store with no conditional put: this node's saves one at a time)
        let _one = ONE.lock().await;
        let at = Path::from(path);
        let m = match self.store.head(&at).await {
            Ok(m) => m,
            Err(object_store::Error::NotFound { .. }) => return Err(Changed.into()), // (no file is not the version the caller read: 412)
            Err(e) => return Err(e.into()),
        };
        if version_of(&m) != expect {
            return Err(Changed.into());
        }
        let bytes: Bytes = bytes.into();
        let (len, opts) = (bytes.len() as u64, |mode| PutOptions { mode, ..Default::default() });
        let update = PutMode::Update(object_store::UpdateVersion { e_tag: m.e_tag.clone(), version: m.version.clone() });
        let put = match self.store.put_opts(&at, bytes.clone().into(), opts(update)).await {
            Ok(r) => r,
            Err(object_store::Error::Precondition { .. }) => return Err(Changed.into()),
            Err(object_store::Error::NotImplemented { .. } | object_store::Error::NotSupported { .. }) => {
                self.store.put_opts(&at, bytes.into(), opts(PutMode::Overwrite)).await? // (a store with no conditional put: checked just above)
            }
            Err(e) => return Err(e.into()),
        };
        match put.e_tag {
            Some(e) => Ok(format!("{}:{len}", e.trim_matches('"'))), // (the version written: not one another save made since)
            None => self.version(path).await,
        }
    }

    /// Remove a user's file (`files/`): the tables' own objects are removed by Pondra alone.
    pub async fn remove_file(&self, path: &str) -> Result<()> {
        anyhow::ensure!(replaceable(path), "only the lake's files (files/…) are removed this way");
        if self.cat.frozen.load(Relaxed) {
            return Err(crate::backup::refused(Some(&self.url))); // (ADR-070)
        }
        self.store.head(&Path::from(path)).await?; // (not there: an error that says so)
        self.store.delete(&Path::from(path)).await?;
        Ok(())
    }

    /// The log's segments after `after`, to the newest visible, by number (`Catalog::segments`, so each is
    /// parsed once a node). A key lookup asks only for what was committed since its last one (`serve::Tail`).
    pub async fn segments_after(&self, after: u64) -> Result<Vec<(u64, Arc<Segment>)>> {
        let now = self.visible();
        if after >= now {
            return Ok(vec![]);
        }
        let segs = self.cat.segments(&seg_key(after + 1), &seg_key(now + 1)).await?;
        segs.into_iter().map(|(k, s)| Ok((k[2..].parse::<u64>()?, s))).collect()
    }

    pub async fn segment_rows(&self, n: u64, seg: &Segment, table: &str) -> Result<Rows> {
        if let Some(rows) = self.tail.lock().unwrap().rows.get(table).and_then(|t| t.get(&n)) {
            return Ok(rows.clone());
        }
        let Some(parts) = seg.parts.get(table) else { return Ok(Rows::default()) };
        let bytes = match seg.path.is_empty() {
            true => self.cat.inline(&data_key(n)).await?,
            false => self.object(&seg.path).await?,
        };
        let mut rows = vec![];
        for &(off, len, _) in parts {
            rows.extend(crate::log::decode_part(&bytes[off as usize..(off + len) as usize])?); // (fresh row ids: one number in the log)
        }
        let rows: Rows = Arc::new(rows);
        let tiered = self.cat.meta(&table_key(table)).await?.map_or(0, |t| t.tiered);
        let mut c = self.tail.lock().unwrap();
        c.forget(table, tiered);
        if n > tiered {
            c.keep(table, n, rows.clone());
        }
        Ok(rows)
    }

    /// Rows of `table` a file commit gives the log's readers (`Segment::files`): its files' rows as
    /// they were then, with their system columns (stored names), from the `from`-th on and `rows`
    /// of them at most; `removed`: the rows of the files it took out.
    pub async fn filed_rows(&self, seg: &Segment, table: &str, removed: bool, from: u64, rows: u64) -> Result<Vec<RecordBatch>> {
        let (Some(filed), Some(meta)) = (seg.files.get(table), self.cat.meta(&table_key(table)).await?) else { return Ok(vec![]) };
        let meta = crate::sys::with_sys(&meta);
        let schema = meta.schema()?;
        let (ctx, end, mut at, mut out) = (self.session(), from.saturating_add(rows), 0u64, vec![]);
        for f in if removed { &filed.removed } else { &filed.added } {
            let (lo, hi) = (at.max(from), (at + f.rows).min(end));
            if lo < hi {
                out.extend(crate::lake_scan::file_rows(self, &ctx, f, &meta, &schema, crate::lake_scan::Pick::Range(lo - at, hi - at)).await?.collect().await?);
            }
            at += f.rows;
            if at >= end {
                break;
            }
        }
        out.iter().map(|b| crate::query::conform(b, &schema)).collect()
    }

    /// Rows of `table` a file commit deleted by position (`Filed::deleted`), with their system
    /// columns (stored names): each file's rows at the places its new deletes name.
    pub async fn deleted_rows(&self, seg: &Segment, table: &str) -> Result<Vec<RecordBatch>> {
        let (Some(filed), Some(meta)) = (seg.files.get(table), self.cat.meta(&table_key(table)).await?) else { return Ok(vec![]) };
        let meta = crate::sys::with_sys(&meta);
        let schema = meta.schema()?;
        let mut out = vec![];
        for (f, deletes) in &filed.deleted {
            out.extend(crate::adopt::rows_deleted(self, &meta, f, deletes, &schema).await?);
        }
        out.iter().map(|b| crate::query::conform(b, &schema)).collect()
    }

    pub async fn delete(&self, path: &str) {
        if path.starts_with("_base/") {
            return; // a branch never deletes its base's files (ADR-047): the base lets them go
        }
        self.store.delete(&Path::from(path)).await.ok();
    }
}

/// Catalog entries as parsed, by key, each with the raw bytes it was parsed from: tables' entries
/// (`Catalog::parsed`) and log segments' (`Catalog::segments`). Bounded by count and by bytes, the raw ones
/// and their parsed form's (`charge`): an entry of 128 files' statistics is large, and every query fills the
/// cache, so the least recently used go first.
struct Parsed<T> {
    by_key: crate::ledger::ByteLru<String, (Bytes, Arc<T>)>, // (at most `count` entries and `most` bytes charged)
    charge: fn(usize) -> usize,                              // an entry's bytes, raw and parsed, by its raw length
}

const PARSED_COUNT: usize = 256;
const PARSED_BYTES: usize = 16 << 20;
/// The most segment entries kept parsed: more than a tiering round leaves untiered at a few thousand
/// commits a second (`tier_secs`, 10 s). Past it a pass over every untiered segment parses each again,
/// as it did before this cache.
const SEGMENTS_COUNT: usize = 65_536;
/// The most bytes they are charged, ~1 KB each for the common entry (`segment_charge`).
const SEGMENTS_BYTES: usize = 64 << 20;

/// A segment entry's bytes, raw and parsed, by its raw length: parsed, an entry of one table's part is
/// ~750 bytes for ~80 raw (a map's node is 544), of ten tables ~1,700 for ~400 (measured with a counting
/// allocator, 11-03). This charges a little more than either.
fn segment_charge(raw: usize) -> usize { 640 + 4 * raw }

impl<T> Parsed<T> {
    fn new(count: usize, most: usize, charge: fn(usize) -> usize) -> Self {
        Parsed { by_key: crate::ledger::ByteLru::new(most).with_count(count), charge }
    }

    /// The entry parsed from these bytes, if it was parsed from bytes that are these.
    fn get(&mut self, key: &str, raw: &Bytes) -> Option<Arc<T>> {
        let (held, m) = self.by_key.get(key)?;
        same_bytes(held, raw).then(|| m.clone())
    }

    /// Keeps `m`, parsed from `raw`, unless it alone is charged more than the byte bound.
    fn put(&mut self, key: &str, raw: &Bytes, m: &Arc<T>) {
        let _ = self.by_key.put(key.to_string(), (raw.clone(), m.clone()), (self.charge)(raw.len()));
    }
}

/// The entries kept parsed give memory back when the node needs it, the least recently used first: each is parsed again.
impl<T: Send + Sync> crate::ledger::Charged for Mutex<Parsed<T>> {
    fn held(&self) -> usize { self.lock().unwrap_or_else(std::sync::PoisonError::into_inner).by_key.bytes() }

    fn give_back(&self, bytes: usize) -> usize { crate::ledger::give_back_from(self, |p| &mut p.by_key, bytes) }

    fn gives_back(&self) -> bool { true }
}

/// Whether two buffers hold the same bytes. A buffer held by `Parsed` is never freed, and `Bytes` never
/// change, so the same start and length is the same content; any other pair is compared, which is cheap
/// next to a parse.
fn same_bytes(a: &Bytes, b: &Bytes) -> bool {
    (a.as_ptr() == b.as_ptr() && a.len() == b.len()) || a == b
}

/// The keys the node holds, each with the commit that put it and its value (None: deleted, kept as a tombstone where the
/// node doesn't hold the whole catalog). It counts the live producers' marks (`p/`) and expiries (`g/`, not the old marks'
/// `g/~` pass) as they change, under the same lock, so a scrape reads two numbers instead of scanning the range
/// (`Catalog::tallies`). Every change goes through the methods below, which keep the counts.
#[derive(Default)]
struct Overlay {
    map: BTreeMap<String, (u64, Option<Bytes>)>,
    marks: usize,
    expiries: usize,
}

impl std::ops::Deref for Overlay {
    type Target = BTreeMap<String, (u64, Option<Bytes>)>;
    fn deref(&self) -> &Self::Target { &self.map }
}

impl Overlay {
    /// Puts `k` as `v`, whatever was there.
    fn set(&mut self, k: String, v: (u64, Option<Bytes>)) {
        let (was, is) = (self.map.get(&k).is_some_and(|(_, old)| old.is_some()), v.1.is_some());
        self.map.insert(k.clone(), v);
        self.tally(&k, was, is);
    }

    /// Puts `k` as `v` if nothing is there.
    fn insert_absent(&mut self, k: String, v: (u64, Option<Bytes>)) {
        if !self.map.contains_key(&k) {
            self.set(k, v);
        }
    }

    fn remove(&mut self, k: &str) {
        if let Some((_, v)) = self.map.remove(k) {
            self.tally(k, v.is_some(), false);
        }
    }

    /// Keeps the entries `keep` says to keep.
    fn retain(&mut self, mut keep: impl FnMut(&str, &(u64, Option<Bytes>)) -> bool) {
        let (mut marks, mut expiries) = (0, 0);
        self.map.retain(|k, v| {
            let kept = keep(k, v);
            if !kept && v.1.is_some() {
                match Self::family(k) {
                    Some(true) => marks += 1,
                    Some(false) => expiries += 1,
                    None => {}
                }
            }
            kept
        });
        self.marks -= marks;
        self.expiries -= expiries;
    }

    fn clear(&mut self) {
        self.map.clear();
        (self.marks, self.expiries) = (0, 0);
    }

    /// Whether `k` is a producer's mark (`Some(true)`), an expiry (`Some(false)`), or neither.
    fn family(k: &str) -> Option<bool> {
        if k.starts_with("p/") {
            Some(true)
        } else if k.starts_with(crate::log::EXPIRY) && !k.starts_with("g/~") {
            Some(false)
        } else {
            None
        }
    }

    /// A key went from live (`was`) to live or deleted (`is`): counted if it is one of the two families.
    fn tally(&mut self, k: &str, was: bool, is: bool) {
        if was == is {
            return;
        }
        let count = match Self::family(k) {
            Some(true) => &mut self.marks,
            Some(false) => &mut self.expiries,
            None => return,
        };
        if is {
            *count += 1;
        } else {
            *count -= 1;
        }
    }
}

/// The catalog is a key-value store inside the lake (`kv`: SlateDB on a node).
/// One process holds the writer (the store fences out any other); any number can read.
pub struct Catalog {
    kv: Arc<dyn Kv>,
    order: tokio::sync::Mutex<u64>, // leader: next commit id; held while writing, so ids follow write order
    flushed: AtomicU64,             // leader: `order` at the last memtable flush
    flushed_at: Mutex<Instant>,     // leader: when the catalog last had nothing unflushed (`checkpoint`)
    writes: Option<mpsc::UnboundedSender<Write>>, // leader: writes waiting to commit, in order
    unstarted: Mutex<Option<mpsc::UnboundedReceiver<Write>>>, // (until `Lake::open` starts `commits`)
    committed: AtomicU64,           // leader: the last committed write (what reads see)
    durable: watch::Sender<u64>,    // leader: the last write that is in the bucket
    pub replicas: usize,            // leader: copies that commit a write (1: the bucket's alone)
    acks: Mutex<(BTreeMap<String, (u64, u64)>, Vec<String>)>, // leader: follower -> the run of changes it holds; the members whose copies count
    acked: watch::Sender<()>,       // leader: an ack arrived
    pending: Mutex<BTreeMap<u64, Arc<Delta>>>, // follower: changes streamed but not yet committed
    feed: broadcast::Sender<Frame>, // leader: the commit stream
    recent: Recent, // leader: its last RECENT (and their bytes), for (re)connecting followers
    last_n: AtomicU64, // the lake's "n" (next segment) after the latest commit written / streamed to us
    // Every commit also writes "c", its number. Follower: the leader's streamed changes (None =
    // deleted), each with its commit number, are laid over our own view of the catalog, which is
    // a few seconds behind. A read uses them only if our view isn't ahead of the stream (else
    // they could be older than what the view has) and, after a gap, once the view passed `hold`.
    overlay: Mutex<Overlay>,
    view: (AtomicU64, AtomicU64), // our own view's "c" and the "n" it had at or before that "c"
    pruned: AtomicU64,            // streamed changes up to this commit were dropped (the view has them)
    pins: Mutex<BTreeMap<u64, usize>>, // scans in progress, by the view "c" they started from
    streamed: AtomicU64,          // the last commit streamed to us; with the view: the whole lake
    hold: AtomicU64,              // commits before this one never arrived: read from the view alone
    follows: bool,                // follower / read-only node that gets the commit stream
    mirror: AtomicBool,           // the overlay holds the whole catalog (but inline data): read only it
    loud: AtomicU64,              // the last commit a remembered answer may depend on (`quiet`, `version`)
    metas: Arc<Mutex<Parsed<TableMeta>>>, // tables' entries as parsed (`parsed`)
    segs: Arc<Mutex<Parsed<Segment>>>,    // log segments' entries as parsed (`segments`)
    pub frozen: AtomicBool,       // a backup (ADR-070): written only by its runs (`backup::may_write`)
    closing: AtomicBool,          // its own node's stop closed it (`close`): an error from it is no fence
}

/// A scan in progress: the streamed changes it may still lay over its (older) view stay.
struct Pin<'a>(&'a Catalog, u64);

impl Drop for Pin<'_> {
    fn drop(&mut self) {
        let mut pins = self.0.pins.lock().unwrap();
        if let Some(n) = pins.get_mut(&self.1) {
            *n -= 1;
            if *n == 0 {
                pins.remove(&self.1);
            }
        }
    }
}

type Recent = Arc<Mutex<(VecDeque<(Instant, Frame)>, usize)>>;
type Write = (Arc<dyn Written>, Arc<Delta>, oneshot::Sender<()>, Instant); // (when it was handed to `commits`: `commit_stage`)

/// One committed catalog write.
pub struct Delta {
    pub id: u64, // the commit number, "c"
    pub puts: Vec<(String, Bytes)>,
    pub deletes: Vec<String>,
}

impl Delta {
    fn bytes(&self) -> usize { self.puts.iter().map(|(k, v)| k.len() + v.len()).sum() }

    /// The same change with only the puts and deletes `keep` names. The id stays, so a stream that filters
    /// its changes shows no gap (`server::feed`, `objects::readable`).
    pub fn only(&self, keep: impl Fn(&str) -> bool) -> Delta {
        Delta { id: self.id, puts: self.puts.iter().filter(|(k, _)| keep(k)).cloned().collect(), deletes: self.deletes.iter().filter(|k| keep(k)).cloned().collect() }
    }
}

/// A commit's key that says nothing a read sees changed (retention's marks: `tier::expire`).
pub const QUIET: &str = "quiet";

/// The node's own records, which its writer appends (`history::writer`) and no remembered answer
/// reads (`App::remembers`): the statements' history and the audit log.
const RECORDS: [&str; 2] = [crate::history::TABLE, crate::audit::TABLE];

/// A commit no remembered answer depends on (`Catalog::version`): only the records' rows, their
/// producers' progress and their tables' entries (`history.rs`, every second on every node; the
/// audit log 50 ms after a DDL statement or a refusal), the lake's own keys (`users::KEYS`, written
/// beside serving when the lake is made, and again when sealed), retention letting segments go and
/// marked `QUIET`, or numbers taken (sequences' and row ids' blocks, a bulk write's commit:
/// `log::commit`, marked too). Counting them, every remembered answer was forgotten every second on
/// a lake nobody wrote to, again 50 ms after each `CREATE TABLE`, and once just after a new lake served.
/// Producers' expiries and the old marks' pass (`log::EXPIRY`), and the marks they delete (`p/`), count
/// neither way: a commit is as quiet as it is without them.
fn quiet(d: &Delta) -> bool {
    let hex = |id: &str| id.len() == 32 && id.bytes().all(|b| b.is_ascii_hexdigit());
    let expiry = |k: &str| k.starts_with(crate::log::EXPIRY);
    let unread = |k: &str| {
        k == crate::users::KEYS
            || RECORDS.iter().any(|t| k.strip_prefix("t/") == Some(t) || k.strip_prefix("p/").and_then(|p| p.strip_prefix(t.trim_start_matches("pondra$"))).and_then(|p| p.strip_prefix('-')).is_some_and(hex))
    };
    let marked = d.puts.iter().any(|(k, _)| k == QUIET);
    let only = |s: &Segment| s.files.is_empty() && s.parts.keys().all(|t| RECORDS.contains(&t.as_str()));
    d.deletes.iter().all(|k| k.starts_with("s/") || k.starts_with("d/") || k.starts_with("p/") || expiry(k))
        && (marked || d.puts.iter().any(|(k, _)| unread(k)))
        && d.puts.iter().all(|(k, v)| {
            matches!(k.as_str(), "c" | "n" | "b" | QUIET) || k.starts_with("d/") || unread(k) || expiry(k) || (marked && (k.starts_with("t/") || k.starts_with("sq/")))
                || (k.starts_with("s/") && serde_json::from_slice::<Segment>(v).is_ok_and(|s| only(&s)))
        })
}

/// What the leader streams to the other nodes (`GET /cluster/log`), in order.
#[derive(Clone)]
pub enum Frame {
    /// First on every connection: the leader's term, and whether commits are replicated
    /// (followers then keep each change on disk and acknowledge it; see `replica.rs`).
    Start { term: u64, replicated: bool },
    /// A write, in order. It takes effect with the `Committed` notice that covers it.
    Change(Arc<Delta>),
    /// Every change up to here is committed: apply it.
    Committed(u64),
    /// … and is in the bucket: a follower no longer needs its copy.
    Durable(u64),
}

impl Frame {
    /// Wire format: u32 length | u8 kind | body. A change's body: u64 id | puts: u32 count,
    /// (key, value)* | deletes: u32 count, key*; every key and value is a u32 length and bytes.
    pub fn encode(&self) -> Bytes {
        let mut b = vec![0u8; 4];
        let field = |b: &mut Vec<u8>, x: &[u8]| {
            b.extend((x.len() as u32).to_le_bytes());
            b.extend(x);
        };
        match self {
            Frame::Start { term, replicated } => {
                b.push(0);
                b.extend(term.to_le_bytes());
                b.push(*replicated as u8);
            }
            Frame::Change(d) => {
                b.push(1);
                b.extend(d.id.to_le_bytes());
                b.extend((d.puts.len() as u32).to_le_bytes());
                for (k, v) in &d.puts {
                    field(&mut b, k.as_bytes());
                    field(&mut b, v);
                }
                b.extend((d.deletes.len() as u32).to_le_bytes());
                for k in &d.deletes {
                    field(&mut b, k.as_bytes());
                }
            }
            Frame::Committed(id) => (b.push(2), b.extend(id.to_le_bytes())).1,
            Frame::Durable(id) => (b.push(3), b.extend(id.to_le_bytes())).1,
        }
        let n = (b.len() - 4) as u32;
        b[..4].copy_from_slice(&n.to_le_bytes());
        b.into()
    }

    /// Take one whole frame off the front of `buf`, if it has one.
    pub fn take(buf: &mut bytes::BytesMut) -> Result<Option<Frame>> {
        if buf.len() < 4 || buf.len() < 4 + u32::from_le_bytes(buf[..4].try_into()?) as usize {
            return Ok(None);
        }
        let n = u32::from_le_bytes(buf[..4].try_into()?) as usize;
        let mut f = buf.split_to(4 + n).freeze().slice(4..);
        let u64_ = |f: &mut Bytes| -> Result<u64> { Ok(u64::from_le_bytes(next(f, 8)?[..].try_into()?)) };
        Ok(Some(match next(&mut f, 1)?[0] {
            0 => Frame::Start { term: u64_(&mut f)?, replicated: next(&mut f, 1)?[0] == 1 },
            1 => {
                let id = u64_(&mut f)?;
                let puts = (0..count(&mut f)?).map(|_| Ok((String::from_utf8(field(&mut f)?.to_vec())?, field(&mut f)?))).collect::<Result<_>>()?;
                let deletes = (0..count(&mut f)?).map(|_| Ok(String::from_utf8(field(&mut f)?.to_vec())?)).collect::<Result<_>>()?;
                Frame::Change(Arc::new(Delta { id, puts, deletes }))
            }
            2 => Frame::Committed(u64_(&mut f)?),
            3 => Frame::Durable(u64_(&mut f)?),
            k => bail!("unknown frame kind {k}"),
        }))
    }
}

fn next(f: &mut Bytes, n: usize) -> Result<Bytes> {
    anyhow::ensure!(f.len() >= n, "truncated frame");
    Ok(f.split_to(n))
}
fn count(f: &mut Bytes) -> Result<u32> { Ok(u32::from_le_bytes(next(f, 4)?[..].try_into()?)) }
fn field(f: &mut Bytes) -> Result<Bytes> {
    let n = count(f)? as usize;
    next(f, n)
}

crate::per_node!(
    /// When the node open now began its start (`trace_from_now`): what `trace`'s times count from.
    static TRACED_FROM: std::sync::Mutex<Option<datafusion::common::instant::Instant>> = std::sync::Mutex::new(None));

/// The node's start begins now, for `trace` (`Node::start`).
pub fn trace_from_now() { *TRACED_FROM.lock().unwrap_or_else(std::sync::PoisonError::into_inner) = Some(datafusion::common::instant::Instant::now()); }

/// A cold start's steps and their times, with `PONDRA_TRACE_START` (`tools/cold_trace.sh`, C5): since the node's start
/// began (`trace_from_now`), nothing before it.
pub fn trace(what: &str) {
    let from = *TRACED_FROM.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
    if let (Some(since), true) = (from, crate::env::is_set("PONDRA_TRACE_START")) {
        eprintln!("start: {what} {:.2} s", since.elapsed().as_secs_f64());
    }
}

/// `trace`'s line, timed from `since`: the catalog's open times its own steps.
pub fn trace_since(what: &str, since: datafusion::common::instant::Instant) {
    if crate::env::is_set("PONDRA_TRACE_START") {
        eprintln!("start: {what} {:.2} s", since.elapsed().as_secs_f64());
    }
}

crate::per_node!(
    /// Has the node begun to serve? (Then the catalog's upkeep starts: `kv::slate`.)
    pub(crate) static SERVING: watch::Sender<bool> = watch::Sender::new(false));

/// The node serves now (`main.rs`).
pub fn serving() { SERVING.send_replace(true); }

/// DataFusion's caches of Parquet footers and of files' statistics (`Lake::kept`).
type Footers = datafusion::execution::cache::default_cache::DefaultCache<object_store_df::path::Path, datafusion::execution::cache::cache_manager::CachedFileMetadataEntry>;
type FileStatistics = datafusion::execution::cache::default_cache::DefaultCache<datafusion::execution::cache::TableScopedPath, datafusion::execution::cache::cache_manager::CachedFileMetadata>;

/// DataFusion's caches keep what they hold at their own limits, and are counted.
impl crate::ledger::Charged for Footers {
    fn held(&self) -> usize { self.memory_used() }
}

impl crate::ledger::Charged for FileStatistics {
    fn held(&self) -> usize { self.memory_used() }
}

/// A catalog let go stops what its store runs for it: a reader's polling, a writer's flushing and its
/// upkeep. SlateDB's tasks outlive its handles, so a lake opened for a moment (a branch's base, a
/// lake detached, a node stopped in a host's process) went on reading its manifest four times a
/// second for the life of the process. (`close` does the same and waits for it: `Node::stop`.)
impl Drop for Catalog {
    fn drop(&mut self) {
        if tokio::runtime::Handle::try_current().is_err() {
            return; // (no runtime: it ended, and SlateDB's tasks with it)
        }
        let kv = self.kv.clone();
        crate::panics::task(async move {
            let _ = kv.close().await;
        });
    }
}

impl Catalog {
    /// The catalog as its leader opens it, on `kv` (the leader's store, `kv::writer`). `since`: when its
    /// opening began, for `PONDRA_TRACE_START`.
    async fn writer(kv: Arc<dyn Kv>, since: Instant) -> Result<Self> {
        let mut cat = Self::new(kv);
        cat.replicas = crate::env::value("PONDRA_REPLICAS");
        cat.last_n.store(cat.get::<u64>("n").await?.unwrap_or(1), Relaxed);
        let c = cat.get::<u64>("c").await?.unwrap_or(0);
        *cat.order.get_mut() = c + 1;
        let (tx, rx) = mpsc::unbounded_channel();
        (cat.writes, *cat.unstarted.get_mut().unwrap()) = (Some(tx), Some(rx));
        cat.committed.store(c, Relaxed);
        cat.durable.send_replace(c);
        // (what we inherited, replayed from the WAL, reaches every node's view at the first
        // checkpoint: right after the node serves, `main.rs`)
        // The leader too reads the catalog from memory: everything committed, nothing in flight.
        let mut all = cat.kv.scan(b"".to_vec()..b"d/".to_vec(), false).await.map_err(fatal)?;
        all.extend(cat.kv.scan(b"d0".to_vec()..vec![0xff], false).await.map_err(fatal)?);
        trace_since("the catalog in memory", since);
        let overlay = cat.overlay.get_mut().unwrap();
        for (k, v) in all {
            overlay.set(k, (c, Some(v)));
        }
        cat.streamed.store(c, Relaxed);
        cat.loud.store(c, Relaxed);
        cat.mirror.store(true, Relaxed);
        Ok(cat)
    }

    /// The catalog as a follower or a reader opens it, on `kv` (`kv::reader`).
    async fn reader(kv: Arc<dyn Kv>, streamed: bool) -> Result<Self> {
        let mut cat = Self::new(kv);
        cat.follows = streamed;
        cat.refresh().await?; // (one try at the in-memory catalog before serving; later refreshes retry)
        Ok(cat)
    }

    /// A reader, or why there can't be one: SlateDB tries again for as long as the bucket says no,
    /// so a bucket that isn't there, or that refuses the credentials, left `pondra sql` waiting
    /// for ever. When opening takes a while, a listing (after the store's own retries) says
    /// whether there is a catalog to wait for. `reader_open` is the reader's opening: this waits
    /// for it, and for the catalog's first view of the bucket, under the one timeout.
    async fn opened(store: Store, streamed: bool, reader_open: impl std::future::Future<Output = Result<Arc<dyn Kv>, kv::Error>>) -> Result<Self> {
        let open = async { Self::reader(reader_open.await?, streamed).await };
        tokio::pin!(open);
        if let Ok(cat) = tokio::time::timeout(Duration::from_secs(3), &mut open).await {
            return cat;
        }
        match futures::StreamExt::next(&mut store.list(Some(&object_store::path::Path::from("catalog/manifest")))).await {
            Some(Err(e)) => Err(anyhow::Error::new(e).context("the lake's bucket")),
            Some(Ok(_)) => open.await, // (a catalog, slow to open)
            None => bail!("no catalog"), // (the caller says what to do)
        }
    }

    fn new(kv: Arc<dyn Kv>) -> Self {
        let (feed, order) = (broadcast::channel(4096).0, tokio::sync::Mutex::new(1));
        let (last_n, view, pruned, pins, streamed, hold, mirror, flushed, committed, loud) = Default::default();
        let (durable, acked, acks, pending, unstarted) = (watch::Sender::new(0), watch::Sender::new(()), Default::default(), Default::default(), Default::default());
        Catalog { kv, order, flushed, flushed_at: Mutex::new(Instant::now()), writes: None, unstarted, committed, durable, replicas: 1, acks, acked, pending, feed, recent: Default::default(), last_n, overlay: Default::default(), view, pruned, pins, streamed, hold, follows: false, mirror, loud, metas: Arc::new(Mutex::new(Parsed::new(PARSED_COUNT, PARSED_BYTES, |raw| raw))), segs: Arc::new(Mutex::new(Parsed::new(SEGMENTS_COUNT, SEGMENTS_BYTES, segment_charge))), frozen: Default::default(), closing: Default::default() }
    }

    /// What the catalog's block caches hold, in bytes.
    /// Its store's block caches (`/metrics`' `catalog_blocks`), which keep what they hold at their own limits.
    pub fn blocks(&self) -> Arc<dyn crate::ledger::Charged> { self.kv.clone() }

    /// The entries kept parsed, tables' and log segments', as the node's line counts them (`cache_bytes{cache=
    /// "catalog_entries"}`, `"catalog_segments"`; 11-07 audit M4).
    pub fn kept_parsed(&self) -> [(&'static str, Arc<dyn crate::ledger::Charged>); 2] { [("catalog_entries", self.metas.clone() as _), ("catalog_segments", self.segs.clone() as _)] }

    /// The recent frames plus a receiver for every frame from now on: the leader's own stream, or a follower's, which
    /// holds the leader's frames as it relayed them (`relay`). Either serves `GET /cluster/log` the same way.
    pub fn subscribe(&self) -> (Vec<Frame>, broadcast::Receiver<Frame>) {
        let rx = self.feed.subscribe();
        (self.recent.lock().unwrap().0.iter().map(|(_, f)| f.clone()).collect(), rx)
    }

    /// Follower: relay a frame of the leader's stream as it came (`members::follow`, with `resend`), so the pages that
    /// follow this node get it from `subscribe`, as the leader's followers get its own. A new term's `Start` says whether
    /// this node's own commits are replicated, as its feed's first `Start` does, never the leader's.
    pub(crate) fn relay(&self, f: Frame) {
        self.send(match f {
            Frame::Start { term, .. } => Frame::Start { term, replicated: self.replicas > 1 },
            f => f,
        });
    }

    /// Inline data (`d/`) that one of the last 64 commits wrote: it never changes, so the frame's copy is always good.
    fn recent_inline(&self, key: &str) -> Option<Bytes> {
        let r = self.recent.lock().unwrap();
        let found = r.0.iter().rev().filter_map(|(_, f)| match f {
            Frame::Change(d) => Some(d),
            _ => None,
        }).take(64).find_map(|d| d.puts.iter().find(|(k, _)| k == key).map(|(_, v)| v.clone()));
        found
    }

    /// Leader: stream a frame (and keep it for followers that connect later).
    fn send(&self, f: Frame) {
        let mut r = self.recent.lock().unwrap();
        r.1 += if let Frame::Change(d) = &f { d.bytes() } else { 0 };
        r.0.push_back((Instant::now(), f.clone()));
        while r.0.len() > 1 && r.0.front().is_some_and(|(t, _)| t.elapsed() > RECENT || r.1 > RECENT_BYTES) {
            if let Some((_, Frame::Change(old))) = r.0.pop_front() {
                r.1 -= old.bytes();
            }
        }
        drop(r);
        let _ = self.feed.send(f); // no followers listening is fine
    }

    /// Leader: a follower holds every change from `first` to `upto` (replicated mode).
    pub fn ack(&self, from: &str, first: u64, upto: u64) {
        self.acks.lock().unwrap().0.insert(from.to_string(), (first, upto));
        self.acked.send_replace(());
    }

    /// Leader: the followers whose copies count towards committing (listed in the catalog as
    /// "m", so a new leader knows whom to ask for them; see `replica::recover`).
    pub fn set_members(&self, members: Vec<String>) { self.acks.lock().unwrap().1 = members; }

    /// Leader: how many members hold change `id`.
    fn holders(&self, id: u64) -> usize {
        let a = self.acks.lock().unwrap();
        a.1.iter().filter(|m| a.0.get(*m).is_some_and(|&(first, upto)| (first..=upto).contains(&id))).count()
    }

    /// Leader: wait until write `id` is in the bucket.
    pub async fn wait_durable(&self, id: u64) {
        let _ = self.durable.subscribe().wait_for(|&d| d >= id).await;
    }

    /// Leader: the last committed write.
    pub fn committed(&self) -> u64 { self.committed.load(Relaxed) }

    /// Do this node's reads include commit `c` (its leader's `committed`)? The leader's, once it committed it; a follower's
    /// whole catalog in memory, once the stream brought it; between seeds, once our view has it, or the stream laid over the
    /// view does (`overlay_ok`). A view's mark moves at `settle`. (Commits, not log segments: a CREATE TABLE adds none.)
    pub fn holds(&self, c: u64) -> bool {
        if self.kv.is_writer() {
            return self.committed() >= c;
        }
        let _o = self.overlay.lock().unwrap(); // (under the lock: see `apply`)
        let (view, streamed) = (self.view.0.load(Relaxed), self.streamed.load(Relaxed));
        match self.mirror.load(Relaxed) {
            true => streamed >= c,
            false => view >= c || (self.overlay_ok(view) && streamed >= c),
        }
    }

    fn apply(&self, d: &Delta) {
        let mut o = self.overlay.lock().unwrap();
        if d.id <= self.pruned.load(Relaxed) {
            return; // our own view has it, and newer changes to its keys may already be dropped
        }
        // A gap: commits between what we hold — the mirror, or the streamed commits plus our view
        // — and this one never arrived.
        let streamed = self.streamed.load(Relaxed);
        let covered = if self.mirror.load(Relaxed) { streamed } else { streamed.max(self.view.0.load(Relaxed)) };
        if d.id > covered + 1 {
            // Some commits never arrived. What we hold is no longer a prefix of the lake, so
            // drop it and read from our own view alone until the view has passed the gap.
            o.clear();
            self.mirror.store(false, Relaxed);
            self.hold.fetch_max(d.id - 1, Relaxed);
        }
        if let Some((_, v)) = d.puts.iter().find(|(k, _)| k == "n") {
            self.last_n.fetch_max(serde_json::from_slice(v).unwrap_or(1), Relaxed);
        }
        // (The leader reads inline data from its own store; everyone else keeps it until their
        // view has it. With the whole catalog here, a deleted key is simply gone.)
        let writer = self.is_writer();
        for (k, v) in d.puts.iter().filter(|(k, _)| !(writer && k.starts_with("d/"))) {
            o.set(k.clone(), (d.id, Some(v.clone())));
        }
        match self.mirror.load(Relaxed) {
            true => d.deletes.iter().for_each(|k| o.remove(k)),
            false => d.deletes.iter().for_each(|k| o.set(k.clone(), (d.id, None))),
        }
        self.streamed.fetch_max(d.id, Relaxed);
        if !quiet(d) {
            self.loud.fetch_max(d.id, Relaxed);
        }
    }

    /// Whether a read that saw our own view at commit `c` may lay the streamed changes over it.
    /// (Call it while holding the overlay lock: `apply` changes both under it.)
    fn overlay_ok(&self, c: u64) -> bool { self.hold.load(Relaxed) <= c && c <= self.streamed.load(Relaxed) }

    /// Our own view's commit number right now.
    async fn view_now(&self) -> Result<u64> {
        if self.kv.is_writer() {
            return Ok(0);
        }
        Ok(self.kv.get("c", false).await?.map(|v| serde_json::from_slice(&v)).transpose()?.unwrap_or(0))
    }

    /// Non-leader: move our view's marks to where the reader is now. Returns its commit and the
    /// oldest commit a scan still in progress (it started from an older view) holds.
    async fn mark(&self) -> Result<(u64, u64)> {
        if self.kv.is_writer() {
            return Ok((0, 0));
        }
        let get = |k: &'static str| async move { Ok::<u64, anyhow::Error>(self.kv.get(k, false).await?.map(|v| serde_json::from_slice(&v)).transpose()?.unwrap_or(0)) };
        // "n" BEFORE "c": our view only moves forward, so this `n` is one our view already had
        // at commit `c` (the other way round it could be from a newer view than `c`, and reads
        // held back to an older commit would miss segments this node thought it could read).
        let (n, c) = (get("n").await?.max(1), get("c").await?);
        let pins = self.pins.lock().unwrap();
        self.view.0.fetch_max(c, Relaxed); // (queries move them too, `settle`: an older read never wins)
        self.view.1.fetch_max(n, Relaxed);
        Ok((c, pins.keys().next().map_or(c, |&p| p.min(c))))
    }

    /// Right after a query read table entries, on a node with no in-memory catalog (an attached
    /// lake, a follower between seeds): those entries are the reader as it is now, which moves on
    /// between refreshes, so the log's end the query reads to (`visible`) is brought up to them.
    /// Behind them, a change's old row was left out while its new one wasn't read yet (an attached
    /// lake's changed row missing from half the reads: `harness.py attached`). Marks only: pruning
    /// and seeding stay `refresh`'s, once a period (invariant 8).
    pub async fn settle(&self) -> Result<()> {
        match self.mirror.load(Relaxed) {
            true => Ok(()),
            false => self.mark().await.map(|_| ()),
        }
    }

    /// Non-leader: how far our own catalog view is; drop the streamed changes it now has.
    async fn refresh(&self) -> Result<()> {
        if self.kv.is_writer() {
            return Ok(());
        }
        // Drop the streamed copies our view now has, except those a scan still in progress
        // (it started from an older view) may lay over its data.
        let (c, upto) = self.mark().await?;
        {
            let mut o = self.overlay.lock().unwrap();
            if self.mirror.load(Relaxed) {
                // The mirror answers every read but inline data without our view, so commits the
                // view already has must still arrive through the stream: `pruned` stays put.
                o.retain(|k, v| v.0 > upto || !k.starts_with("d/"));
            } else {
                self.pruned.fetch_max(upto, Relaxed);
                o.retain(|_, v| v.0 > upto);
            }
        }
        if self.follows && !self.mirror.load(Relaxed) && self.hold.load(Relaxed) <= c {
            self.seed().await?;
        }
        Ok(())
    }

    /// Follower: load the whole catalog, but inline segment data, into memory. From then on the
    /// commit stream keeps it current and reads are answered from memory, as of the last streamed
    /// commit: the bucket is out of the query path. (Redone after a gap in the stream.)
    async fn seed(&self) -> Result<()> {
        if self.kv.is_writer() {
            return Ok(());
        }
        let _pin = self.pin(); // (the streamed changes after `before` stay while we read)
        let before = self.view_now().await?;
        let mut all = self.kv.scan(b"".to_vec()..b"d/".to_vec(), false).await?;
        all.extend(self.kv.scan(b"d0".to_vec()..vec![0xff], false).await?);
        let after = self.view_now().await?;
        let n = all.get("n").map_or(Ok(1), |v| serde_json::from_slice(v))?;
        let mut o = self.overlay.lock().unwrap();
        // One consistent picture: the scan saw one view, or the streamed commits we hold cover
        // every change it may have picked up after `before` (they win over it, below). A gap
        // after `before`, or neither: try again at the next refresh (one try each, never a loop
        // that a busy lake on slow storage could keep failing).
        let c = before;
        let covered = after <= self.streamed.load(Relaxed);
        if self.hold.load(Relaxed) > c || self.mirror.load(Relaxed) || (after != before && !covered) {
            return Ok(());
        }
        // Streamed changes newer than the snapshot win; everything older gives way to it.
        o.retain(|k, v| v.0 > c || k.starts_with("d/"));
        for (k, v) in all {
            o.insert_absent(k, (c, Some(v)));
        }
        o.retain(|_, v| v.1.is_some()); // (deleted keys: absent from the whole catalog)
        self.pruned.fetch_max(c, Relaxed); // older streamed commits are in the snapshot
        self.streamed.fetch_max(c, Relaxed);
        self.loud.fetch_max(c, Relaxed);
        self.last_n.fetch_max(n, Relaxed);
        self.mirror.store(true, Relaxed);
        Ok(())
    }

    /// Before a scan: see `Pin`. Its data will come from a view at least this new.
    fn pin(&self) -> Pin<'_> {
        let mut pins = self.pins.lock().unwrap();
        let c = self.view.0.load(Relaxed);
        *pins.entry(c).or_default() += 1;
        Pin(self, c)
    }

    fn overlaid(&self) -> bool {
        let _o = self.overlay.lock().unwrap();
        self.overlay_ok(self.view.0.load(Relaxed))
    }

    /// The "n" of the newest commit this node's reads include.
    pub fn visible_n(&self) -> u64 {
        if self.mirror.load(Relaxed) {
            return self.last_n.load(Relaxed); // the mirror is the lake as of the last streamed commit
        }
        if self.kv.is_writer() {
            return self.last_n.load(Relaxed);
        }
        match self.overlaid() {
            true => self.view.1.load(Relaxed).max(self.last_n.load(Relaxed)),
            false => self.view.1.load(Relaxed),
        }
    }

    pub fn is_writer(&self) -> bool { self.kv.is_writer() }

    /// The catalog version every read here reflects right now — on the leader (durable commits)
    /// or a node that holds the whole catalog in memory — or `None` where reads mix our view with
    /// the stream. A read that starts after this sees at least this version (never older). Quiet
    /// commits don't count: every read but the history's is the same across them.
    pub fn version(&self) -> Option<u64> {
        match self.kv.is_writer() {
            true => Some(self.loud.load(Relaxed).min(self.committed.load(Relaxed))), // (`apply` comes just before `committed`)
            false => self.mirror.load(Relaxed).then(|| self.loud.load(Relaxed)),
        }
    }

    pub async fn get<T: DeserializeOwned>(&self, key: &str) -> Result<Option<T>> {
        self.get_raw(key).await?.map(|v| serde_json::from_slice(&v).context(key.to_string())).transpose()
    }

    /// A table's entry (`t/…`) as parsed, for a read that runs per query or request: see `parsed`.
    pub async fn meta(&self, key: &str) -> Result<Option<Arc<TableMeta>>> {
        let raw = if self.mirror.load(Relaxed) {
            self.overlay.lock().unwrap().get(key).and_then(|(_, v)| v.clone()) // (the overlay holds the whole catalog)
        } else {
            self.get_raw(key).await?
        };
        raw.map(|raw| self.parsed(key, &raw)).transpose()
    }

    /// The table entry `raw` (read for `key`) as parsed: the copy kept from the last parse when these
    /// are the bytes it was parsed from, else a new parse, which is kept. An entry of 128 files'
    /// statistics is large, and every query reads the entries of the tables it names.
    pub fn parsed(&self, key: &str, raw: &Bytes) -> Result<Arc<TableMeta>> {
        if let Some(m) = self.metas.lock().unwrap().get(key, raw) {
            return Ok(m);
        }
        let m = Arc::new(serde_json::from_slice::<TableMeta>(raw).context(key.to_string())?);
        self.metas.lock().unwrap().put(key, raw, &m);
        Ok(m)
    }

    /// `key` from this node's own view of the bucket, past the mirror: for what a follower needs
    /// before its commit stream opens (the lake's own keys, which signing in to that stream takes:
    /// `users::keys`), or when the key it signed in with is refused (`users::sealed_node_key`).
    pub async fn from_view(&self, key: &str) -> Result<Option<Bytes>> {
        match self.kv.is_writer() {
            true => self.get_raw(key).await,
            false => Ok(self.kv.get(key, false).await?),
        }
    }

    pub async fn get_raw(&self, key: &str) -> Result<Option<Bytes>> {
        if self.kv.is_writer() {
            return Ok(match key.starts_with("d/") {
                // The leader answers from its in-memory catalog: committed writes only, never ones
                // in flight. Inline segment data is only asked for once its segment is committed.
                true => match self.recent_inline(key) {
                    Some(bytes) => Some(bytes),
                    None => self.kv.get(key, false).await.map_err(|e| self.fenced(e))?,
                },
                false => match self.mirror.load(Relaxed) {
                    true => self.overlay.lock().unwrap().get(key).and_then(|(_, v)| v.clone()),
                    false => self.kv.get(key, true).await.map_err(|e| self.fenced(e))?,
                },
            });
        }
        let inline = key.starts_with("d/");
        {
            let o = self.overlay.lock().unwrap();
            if self.mirror.load(Relaxed) && !inline {
                return Ok(o.get(key).and_then(|(_, v)| v.clone())); // the in-memory catalog
            }
        }
        // A streamed copy is newer than our view's (unless the view is ahead of the
        // stream); inline segment data never changes, so its streamed copy is always good.
        let c = if inline { 0 } else { self.view_now().await? };
        let copy = {
            let o = self.overlay.lock().unwrap(); // (under the lock: see `apply`)
            o.get(key).filter(|_| inline || self.overlay_ok(c)).map(|(_, v)| v.clone())
        };
        Ok(match copy {
            Some(v) => v,
            None => self.kv.get(key, false).await?, // (read after the check: at least as new)
        })
    }

    /// The inline data (`d/`) of a segment the catalog lists, which always arrives. A reader's view can
    /// hold less than its commit number says for a moment while a new writer opens the lake, so a miss
    /// waits here (20 ms, doubling to 500 ms, 10 s in all) rather than failing the read.
    pub async fn inline(&self, key: &str) -> Result<Bytes> {
        let started = Instant::now();
        let mut wait = Duration::from_millis(20);
        loop {
            if let Some(bytes) = self.get_raw(key).await? {
                return Ok(bytes);
            }
            if self.is_writer() || started.elapsed() >= Duration::from_secs(10) {
                let n = key.strip_prefix("d/").and_then(|n| n.parse::<u64>().ok()).unwrap_or(0);
                bail!("missing inline segment {n}");
            }
            tokio::time::sleep(wait).await;
            self.mark().await?; // (our view's marks move on, so the next read sees what it has now)
            wait = (wait * 2).min(Duration::from_millis(500));
        }
    }

    /// The log's segment entries with keys in [from, to) (`seg_key`), as `scan` gives them, each parsed once
    /// while its bytes stay the same: a committed segment never changes, and the tier loop (every second), a
    /// table's log tail at each query, key lookups, Kafka fetches, the change feed and live queries read the
    /// same ones again. Every reader of the log's segments reads them here: one cache, one parse each.
    pub async fn segments(&self, from: &str, to: &str) -> Result<Vec<(String, Arc<Segment>)>> {
        self.segments_of(self.scan_raw(from, to).await?)
    }

    /// `segments` over entries already read. A pass over more entries than the cache keeps (a sweep of the
    /// whole log) parses those it hasn't without keeping them, so it doesn't push out the log tail's.
    fn segments_of(&self, raw: BTreeMap<String, Bytes>) -> Result<Vec<(String, Arc<Segment>)>> {
        let keep = raw.len() <= SEGMENTS_COUNT;
        raw.into_iter()
            .map(|(k, v)| {
                let hit = self.segs.lock().unwrap().get(&k, &v);
                let seg = match hit {
                    Some(seg) => seg,
                    None => {
                        let seg = Arc::new(serde_json::from_slice::<Segment>(&v)?);
                        if keep {
                            self.segs.lock().unwrap().put(&k, &v, &seg);
                        }
                        seg
                    }
                };
                Ok((k, seg))
            })
            .collect()
    }

    /// All entries with keys in [from, to).
    pub async fn scan<T: DeserializeOwned>(&self, from: &str, to: &str) -> Result<Vec<(String, T)>> {
        self.scan_raw(from, to).await?.into_iter().map(|(k, v)| Ok((k, serde_json::from_slice(&v)?))).collect()
    }

    /// `scan`, the values as they are kept (JSON), for a caller that decodes only what it hasn't.
    pub async fn scan_raw(&self, from: &str, to: &str) -> Result<BTreeMap<String, Bytes>> {
        Ok(self.scan_raw_at(from, to).await?.0)
    }

    /// `scan_raw`, and the commit the entries are read as of (`at`): the lake as of commit `at`, which a page of a range
    /// is checked against (`kv`, `Remote::scan`). A mirror answers as of its last streamed commit; a view alone as of its own.
    pub async fn scan_raw_at(&self, from: &str, to: &str) -> Result<(BTreeMap<String, Bytes>, u64)> {
        if from >= to {
            return Ok((BTreeMap::new(), self.at_now().await?));
        }
        let range = from.as_bytes().to_vec()..to.as_bytes().to_vec();
        if self.kv.is_writer() && !self.mirror.load(Relaxed) {
            // (`at` first: a commit after it that the scan sees is one a later page's `moved_since` counts)
            let at = self.committed.load(Relaxed);
            let all = self.kv.scan(range, true).await.map_err(|e| self.fenced(e))?;
            return Ok((all, at));
        }
        {
            let o = self.overlay.lock().unwrap();
            if self.mirror.load(Relaxed) {
                // The in-memory catalog (scans never cover inline segment data).
                let hits = o.range(from.to_string()..to.to_string()).filter_map(|(k, (_, v))| Some((k.clone(), v.clone()?)));
                return Ok((hits.collect(), self.streamed.load(Relaxed)));
            }
        }
        let _pin = self.pin();
        loop {
            // Our view is somewhere between `before` and `after` while we read it.
            let before = self.view_now().await?;
            let mut all = self.kv.scan(range.clone(), false).await?;
            let after = self.view_now().await?;
            // Under the lock, so the stream can't move while we decide (see `apply`).
            let streamed = {
                let o = self.overlay.lock().unwrap();
                let streamed = self.streamed.load(Relaxed);
                if self.hold.load(Relaxed) <= before && after <= streamed {
                    for (k, (_, v)) in o.range(from.to_string()..to.to_string()) {
                        match v {
                            Some(v) => all.insert(k.clone(), v.clone()),
                            None => all.remove(k),
                        };
                    }
                    return Ok((all, streamed)); // = the lake as of commit `streamed`
                }
                streamed
            };
            // Our view alone, when the stream is newer than it or has a gap, and the view stood still while we read it (as
            // the loop below requires: a view that moved mid-read is read again).
            if (before >= streamed || before < self.hold.load(Relaxed)) && before == after {
                return Ok((all, before)); // our view alone: newer than the stream (or the stream has a gap)
            }
            tokio::time::sleep(Duration::from_millis(1)).await; // our view passed the stream mid-read: again
        }
    }

    /// The commit a read takes right now, when it lays nothing over our view: the mirror's last streamed commit, else the view's.
    async fn at_now(&self) -> Result<u64> {
        if self.mirror.load(Relaxed) {
            return Ok(self.streamed.load(Relaxed));
        }
        self.view_now().await
    }

    /// The first `n` entries with keys in [from, to) that `keep` allows, in key order, and the commit they are read as of
    /// (`scan_raw_at`'s `at`): one page of a range (`kv`). In memory it takes only those `n` (the overlay's range, filtered
    /// and `take`n), so keys `keep` refuses don't cost a page; without the whole catalog here, it reads all of the range.
    pub async fn scan_page(&self, from: &str, to: &str, n: usize, keep: impl Fn(&str) -> bool) -> Result<(Vec<(String, Bytes)>, u64)> {
        if from >= to {
            return Ok((Vec::new(), self.at_now().await?));
        }
        if self.mirror.load(Relaxed) {
            let o = self.overlay.lock().unwrap();
            let page = o.range(from.to_string()..to.to_string()).filter(|(k, _)| keep(k.as_str())).filter_map(|(k, (_, v))| Some((k.clone(), v.clone()?))).take(n).collect();
            return Ok((page, self.streamed.load(Relaxed)));
        }
        let (all, at) = self.scan_raw_at(from, to).await?;
        Ok((all.into_iter().filter(|(k, _)| keep(k.as_str())).take(n).collect(), at))
    }

    /// Whether a commit between `since` and `at` put or deleted a key in [from, to) that `keep` allows, as far as the changes
    /// `recent` holds say (the leader's own, or a follower's relayed ones): a range read partly as of `since` and partly as of
    /// `at` is then torn (`kv`'s `since`). Only the commits after the older of the two and up to the newer count: later ones
    /// are in neither read. One change per commit, so a commit missing from `recent` (aged out, or a follower's reconnect
    /// that left a hole) counts as moved: it can't tell.
    pub fn moved_since(&self, from: &str, to: &str, since: u64, at: u64, keep: impl Fn(&str) -> bool) -> bool {
        let (after, upto) = (since.min(at), since.max(at));
        if after == upto {
            return false;
        }
        let within = |k: &str| k >= from && k < to && keep(k);
        let r = self.recent.lock().unwrap();
        let mut ids = std::collections::BTreeSet::new();
        let mut touched = false;
        for (_, f) in r.0.iter() {
            if let Frame::Change(d) = f {
                if d.id > after && d.id <= upto {
                    ids.insert(d.id);
                    touched = touched || d.puts.iter().any(|(k, _)| within(k)) || d.deletes.iter().any(|k| within(k));
                }
            }
        }
        touched || (ids.len() as u64) < upto - after
    }

    /// The first `n` entries with keys in [from, to), in key order: a commit's bounded look at a range
    /// (producers' expiries, the old marks' pass). In memory it takes only those `n` (the overlay's range,
    /// `take`), never a copy of the whole range; without the whole catalog here, it reads all of it.
    pub async fn scan_first(&self, from: &str, to: &str, n: usize) -> Result<Vec<(String, Bytes)>> {
        Ok(self.scan_page(from, to, n, |_| true).await?.0)
    }

    /// The producers' marks (`p/` keys) and the expiries (`g/` keys) the catalog holds, when it holds the whole catalog in
    /// memory (the leader's, and a follower's once seeded); `None` otherwise. Counted as the overlay changes, so this is two
    /// numbers, not a scan under the lock (`Overlay`).
    pub fn tallies(&self) -> Option<(usize, usize)> {
        self.mirror.load(Relaxed).then(|| {
            let o = self.overlay.lock().unwrap();
            (o.marks, o.expiries)
        })
    }

    /// Leader: persist the catalog memtable if anything was committed since the last time. A
    /// restart then replays only the WAL written since, and the other nodes' views — which read
    /// no WAL — catch up (a follower whose commit stream broke reads from its view until then).
    pub async fn checkpoint(&self) -> Result<()> {
        if !self.kv.is_writer() {
            return Ok(());
        }
        let next = *self.order.lock().await; // every commit before it is in the memtable
        let flushed = self.flushed.load(Relaxed);
        if flushed != next {
            let flush = self.kv.checkpoint();
            tokio::pin!(flush);
            // (a flush that takes 30 s is still waited for, not dropped: the line says why it is slow)
            if tokio::time::timeout(Duration::from_secs(30), &mut flush).await.is_err() {
                eprintln!("the catalog's checkpoint has waited 30 s: its level-0 files wait for compaction ({} commits not yet in the bucket's catalog)", next - flushed);
                flush.await?;
            }
            self.flushed.store(next, Relaxed);
        }
        // (a catalog with nothing unflushed counts from here, so a commit after a quiet spell isn't seen as one that waited all of it)
        *self.flushed_at.lock().unwrap() = Instant::now();
        Ok(())
    }

    /// Leader: the catalog's level-0 files waiting for compaction.
    pub fn waiting(&self) -> usize { self.kv.waiting() }

    /// A leader that lives for one statement (`pondra sql`): runs the compactor until fewer than `below` level-0 files
    /// wait (`cap` at most), so a flush doesn't wait on it. Each one-off writer's stop leaves one level-0 file, and none
    /// runs the compactor alone (it starts 10 s on): left so, the catalog's level 0 filled, and every write after it
    /// waited out its flush (10 s a write). `collect`: then the memtable written out, and the garbage collector run once
    /// (old WAL and manifests: a lake no node serves had nobody else to clear them).
    pub async fn tidy(&self, below: usize, cap: Duration, collect: bool) -> Result<()> {
        self.kv.compact(below, cap).await?;
        if collect {
            self.checkpoint().await?;
            self.kv.collect().await;
        }
        Ok(())
    }

    /// Leader: how long, in seconds, the catalog has held commits that no flush has persisted (0 when it has none).
    /// `/metrics` reads it. It reads `committed` and not `order`, which a commit holds while it writes: a stalled
    /// bucket must not stall the scrape that says so. `order` is `committed` + 1 once no commit is in flight, so
    /// commits are unflushed exactly when `committed` has reached `flushed`.
    pub fn unflushed_seconds(&self) -> f64 {
        // (a reader's `committed` and `flushed` are both 0: without this it would count from its open)
        if !self.is_writer() || self.committed.load(Relaxed) < self.flushed.load(Relaxed) {
            return 0.0;
        }
        self.flushed_at.lock().unwrap().elapsed().as_secs_f64()
    }

    /// Lets go of the catalog: its writer flushes what it holds and stops its background tasks, a
    /// reader its polling (`Node::stop`, ADR-061 §4: one process may open another lake after it).
    pub async fn close(&self) -> Result<()> {
        self.closing.store(true, Relaxed);
        Ok(self.kv.close().await?)
    }

    /// A new branch's leader (ADR-047): its commits number after its base's, whose rows it holds
    /// with their `_version`s.
    pub async fn start_after(&self, c: u64) {
        let mut order = self.order.lock().await;
        *order = (*order).max(c + 1);
    }

    /// Write puts and deletes atomically and wait until committed (see `Lake::commits`).
    pub async fn commit(&self, puts: Vec<(String, Vec<u8>)>, deletes: &[String]) -> Result<()> {
        Ok(self.write(puts, deletes).await?.await?)
    }

    /// Write puts and deletes atomically. The returned receiver fires once the write is committed;
    /// writes commit in the order they were made, so a caller can make the next write without
    /// waiting (pipelined commits: one round trip no longer blocks the next).
    pub async fn write(&self, puts: Vec<(String, Vec<u8>)>, deletes: &[String]) -> Result<oneshot::Receiver<()>> {
        let (true, Some(writes)) = (self.kv.is_writer(), &self.writes) else { bail!("read-only node") };
        if self.frozen.load(Relaxed) && !crate::backup::may_write(puts.iter().map(|(k, _)| k).chain(deletes)) {
            return Err(crate::backup::refused(None)); // (whatever door a write came through: ADR-070)
        }
        let t0 = Instant::now();
        let puts = listed(self, puts).await?;
        let mut puts: Vec<(String, Bytes)> = puts.into_iter().map(|(k, v)| (k, v.into())).collect();
        let mut id = self.order.lock().await;
        let t1 = commit_stage("order", t0);
        // The commit number is the last put: the store takes the puts as they are, and the `Delta` keeps the same list.
        puts.push(("c".to_string(), Bytes::from(json(&*id))));
        let handle = self.kv.write(&puts, deletes).await.map_err(|e| self.fenced(e))?;
        let t2 = commit_stage("kv write", t1);
        if let Some((_, v)) = puts.iter().find(|(k, _)| k == "n") {
            self.last_n.store(serde_json::from_slice(v)?, Relaxed);
        }
        let (done, rx) = oneshot::channel();
        let _ = writes.send((handle, Arc::new(Delta { id: *id, puts, deletes: deletes.to_vec() }), done, t2));
        *id += 1;
        Ok(rx)
    }
}


/// Each table entry is written in its lake's shape (redesign 1b, piece 6b): the one place that is chosen. Once the lake is
/// format 6 (`format::LISTS`), an entry still in format 5's (read before the raise, or new: `Record`) is written as its
/// list; below it, every entry is written in format 5's, so a 0.80 node reads it (one read as a list there came from
/// another lake). An entry begins with its column record (`TableMeta`'s first field): `{"list":` or `{"columns":`.
async fn listed(cat: &Catalog, mut puts: Vec<(String, Vec<u8>)>) -> Result<Vec<(String, Vec<u8>)>> {
    let shape = |k: &str, v: &[u8]| match k.starts_with("t/") {
        true if v.starts_with(br#"{"list":"#) => Some(true),
        true if v.starts_with(br#"{"columns":"#) => Some(false),
        _ => None,
    };
    if !puts.iter().any(|(k, v)| shape(k, v).is_some()) {
        return Ok(puts);
    }
    let lists = crate::format::of(cat).await?.format >= crate::format::LISTS;
    for (k, v) in puts.iter_mut() {
        if shape(k, v) == Some(!lists) {
            if let Ok(mut t) = serde_json::from_slice::<TableMeta>(v) {
                t.record.listed = lists;
                *v = json(&t);
            }
        }
    }
    Ok(puts)
}

/// `PONDRA_TRACE_COMMIT=1`: where a leader's commits spend their time. Each stage is the time since the one before it
/// (`since`); once a minute one line on stderr gives each stage's p50 and p95 over that minute, and how late a 10 ms
/// sleep woke (`lag`: the runtime's own wait, which every stage pays when the node's cores are busy). Returns now, the
/// next stage's start.
pub fn commit_stage(stage: &'static str, since: Instant) -> Instant {
    type Stages = Mutex<(Instant, Vec<(&'static str, Vec<f64>)>)>;
    static ON: std::sync::LazyLock<bool> = std::sync::LazyLock::new(|| {
        let on = crate::env::is_set("PONDRA_TRACE_COMMIT");
        if on {
            crate::panics::spawn(async {
                loop {
                    let t = Instant::now();
                    tokio::time::sleep(Duration::from_millis(10)).await;
                    commit_stage("lag", t + Duration::from_millis(10));
                }
            });
        }
        on
    });
    crate::per_node!(static STAGES: Stages = Mutex::new((Instant::now(), vec![])); clear = |s| *s.lock().unwrap_or_else(|e| e.into_inner()) = (Instant::now(), vec![]));
    let now = Instant::now();
    if !*ON {
        return now;
    }
    let mut s = STAGES.lock().unwrap();
    let ms = now.saturating_duration_since(since).as_secs_f64() * 1e3;
    match s.1.iter_mut().find(|(n, _)| *n == stage) {
        Some((_, v)) => v.push(ms),
        None => s.1.push((stage, vec![ms])),
    }
    if now - s.0 >= Duration::from_secs(60) {
        let line: Vec<String> = s.1.iter_mut().map(|(n, v)| {
            v.sort_by(f64::total_cmp);
            let q = |p: f64| v[((v.len() - 1) as f64 * p) as usize];
            format!("{n} {:.3}/{:.3} ({})", q(0.5), q(0.95), v.len())
        }).collect();
        eprintln!("commit stages p50/p95 ms (count): {}", line.join(", "));
        *s = (now, vec![]);
    }
    now
}

impl Catalog {
    /// `fatal`, unless this node's own stop closed the catalog: then the write or read in flight
    /// fails and the stop goes on to give up the term. Restarting there led the term again, and its
    /// followers waited out the lease (`upgrade_check.py rolling`: a batch waited 5.8 s).
    fn fenced(&self, e: kv::Error) -> anyhow::Error {
        if self.closing.load(Relaxed) {
            return e.into();
        }
        fatal(e)
    }
}

/// A writer that was fenced out (a new leader took over) must stop at once: it restarts and
/// rejoins the cluster as a follower. (The upkeep's failures take this too: `kv::slate`.)
pub(crate) fn fatal(e: kv::Error) -> anyhow::Error {
    if e.closed() {
        eprintln!("catalog closed: {e}");
        crate::node::restart("the catalog was closed (fenced by a newer leader)");
    }
    e.into()
}

/// Test hook: `PONDRA_CRASH=after_seg_put:0.05,after_parquet_put:0.2` aborts the process at
/// that point with that probability, to prove crashes never lose or duplicate rows.
pub fn maybe_crash(point: &str) {
    if crash_rolled(point) {
        if crate::node::hosted() {
            panic!("PONDRA_CRASH at {point}"); // (a library's node fails, its host stays: a vital loop's panic)
        }
        eprintln!("PONDRA_CRASH: aborting at {point}");
        std::process::abort();
    }
}

/// Whether `PONDRA_CRASH` names `point` and its roll comes up: `maybe_crash`'s test, for a point that
/// fails an operation (`memory::settle`'s second pass) rather than the process.
pub fn crash_rolled(point: &str) -> bool {
    let Some(spec) = crate::env::get("PONDRA_CRASH") else { return false };
    spec.split(',').filter_map(|p| p.split_once(':')).any(|(name, prob)| {
        let roll = (uuid::Uuid::new_v4().as_u128() % 1_000_000) as f64 / 1e6;
        name == point && roll < prob.parse().unwrap_or(0.0)
    })
}

/// How many partitions a query runs in here: one per core (`PONDRA_CORES` stands for another
/// machine's count), at least 2, so every node plans aggregations as partial + final (see
/// spmd.rs), and at most one per 24 MB of query memory. Each partition's sort keeps 10 MB aside to
/// merge what it spilled (DataFusion's default); on 4 cores with 50 MB the reserves took the
/// budget and the merge above them failed, and smaller reserves can't merge at all.
pub fn partitions() -> usize {
    static PARTS: std::sync::OnceLock<usize> = std::sync::OnceLock::new(); // (every session asks: the cores are read from the cgroup's files)
    *PARTS.get_or_init(|| {
        #[cfg(target_arch = "wasm32")]
        return 1; // (a page has one thread and no runtime to start a partition's task on: `in_task.rs`)
        #[cfg(not(target_arch = "wasm32"))]
        {
            let cores = crate::env::parse("PONDRA_CORES").unwrap_or_else(crate::env::cores);
            cores.min(memory_limit() / (24 << 20)).max(2)
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn placed_changes_with_the_order_an_insert_fills() {
        // (11-03 audit M9: the leader refuses rows a node put by another order, `log::Ack::moved`)
        let m = TableMeta::with_columns(vec![("a".into(), "Int32".into()), ("b".into(), "Int32".into())]);
        let mut moved = m.clone();
        moved.set_order(vec!["b".into(), "a".into()]);
        let mut back = moved.clone();
        back.set_order(vec!["a".into(), "b".into()]);
        let mut renamed = m.clone();
        renamed.rename_column("a", "x");
        let mut added = m.clone();
        added.add_column("c", "Int32", None, None);
        let mut dropped = added.clone();
        dropped.drop_column("c");
        assert_ne!(m.placed(), moved.placed(), "a move");
        assert_eq!(m.placed(), back.placed(), "moved back: the same places");
        assert_eq!(m.placed(), renamed.placed(), "a rename puts nothing elsewhere");
        assert_ne!(m.placed(), added.placed(), "a column more");
        assert_eq!(m.placed(), dropped.placed(), "a dropped column isn't filled");
        // (11-07 audit L3: values a node put by an order that is the first columns of the table's are where they go)
        assert!(m.places(m.placed()) && moved.places(moved.placed()), "its own order");
        assert!(added.places(m.placed()), "the order before a column was added at the end: its first columns");
        assert!(!m.places(added.placed()), "an order with a column more");
        assert!(!moved.places(m.placed()) && !m.places(moved.placed()), "a move");
        let mut first = m.clone();
        let c = first.add_column("c", "Int32", None, None);
        first.set_order(vec![c, "a".into(), "b".into()]);
        assert!(!first.places(m.placed()), "a column added before the others");
        let mut gone = added.clone();
        gone.drop_column("b");
        assert!(!gone.places(m.placed()) && !gone.places(added.placed()), "a column dropped from among them");
    }

    #[test]
    fn a_place_is_a_folder_a_bucket_memory_or_a_node_and_no_other_scheme() {
        assert_eq!(place("/a").unwrap(), Place::Folder);
        assert_eq!(place("a/b").unwrap(), Place::Folder);
        assert_eq!(place("s3://b/x").unwrap(), Place::Bucket);
        assert_eq!(place("memory://x").unwrap(), Place::Memory);
        #[cfg(feature = "doors")]
        assert_eq!(place("pondra://h:1").unwrap(), Place::Served);
        for url in ["https://../../x", "file:///a", "r2://b"] {
            assert_eq!(crate::codes::of(&place(url).unwrap_err()), "22023", "{url} is no place a lake can be");
            assert!(open_store(url).is_err(), "{url} is refused before a folder is made of it");
        }
    }

    #[test]
    fn merges_read_as_many_runs_as_their_memory_and_files_allow() {
        // (B50, 10-31: under a 2 GB cap a GROUP BY's merge took 5 runs at once and made several passes; its runs number
        // up to 64 a partition there. A window's sorts take half a share's worth: at the window check's 256 MB it passes
        // at 16 and is refused at 32)
        assert_eq!(merge_files(4), 64);
        assert_eq!(merge_files(16), 32, "512 files over the partitions");
        assert_eq!(merge_files(256), 4, "4 at least");
        assert_eq!(merge_fan_in(683 << 20, 4), 21);
        assert_eq!(merge_fan_in(256 << 20, 4), 8);
        assert_eq!(merge_fan_in(64 << 20, 4), 4, "4 at least");
        assert_eq!(merge_fan_in(5 << 30, 4), 64, "64 at most");
    }

    /// `resident` counts bytes: `/proc/self/statm` counts pages, and the page size is the OS's (64 KB on
    /// some arm64 builds), so it agrees with `VmRSS` in kilobytes. (On 4 KB pages the old constant passed
    /// too; it is the arm64 case this guards.)
    #[cfg(target_os = "linux")]
    #[test]
    fn resident_is_in_bytes_as_the_page_size_says() {
        let block = vec![1u8; 100 << 20]; // (written, so resident)
        std::hint::black_box(&block);
        let ours = resident().expect("resident memory");
        let status = std::fs::read_to_string("/proc/self/status").expect("the process's status");
        let kb: usize = status.lines().find_map(|l| l.strip_prefix("VmRSS:")?.trim().strip_suffix("kB")?.trim().parse().ok()).expect("VmRSS");
        let theirs = kb << 10;
        assert!(ours.abs_diff(theirs) * 4 <= theirs, "resident {ours} bytes, VmRSS {theirs} bytes");
    }

    #[test]
    fn catching_up_is_cannot_connect_now() {
        assert_eq!(crate::codes::of(&anyhow::anyhow!("{}", CATCHING_UP)), "57P03");
    }

    /// What a read never sees is quiet, and anything else in the same commit makes it loud
    /// (invariant 224): `harness.py remembered` asks around the audit log's line and a new lake's keys.
    #[test]
    fn quiet_is_what_no_read_sees() {
        let id = "0123456789abcdef0123456789abcdef";
        let seg = |table: &str| serde_json::json!({"path": "", "parts": {table: [[0, 10, 1]]}, "ts_ms": 1}).to_string();
        let quiet_of = |puts: &[(&str, &str)]| quiet(&Delta { id: 9, puts: puts.iter().map(|(k, v)| (k.to_string(), Bytes::from(v.to_string()))).collect(), deletes: vec![] });
        let audit = seg(crate::audit::TABLE);
        let (history, user) = (seg(crate::history::TABLE), seg("r_t"));
        let (audit_p, history_p) = (format!("p/audit-{id}"), format!("p/history-{id}"));
        assert!(quiet_of(&[("t/pondra$audit", "{}"), ("c", "9")]), "the audit log's table made");
        assert!(quiet_of(&[("d/3", "x"), ("s/3", &audit), (&audit_p, "1"), ("n", "4"), ("c", "9")]), "the audit log's rows");
        assert!(quiet_of(&[("d/4", "x"), ("s/4", &history), (&history_p, "1"), ("n", "5"), ("c", "9")]), "the history's rows");
        assert!(quiet_of(&[("n", "4"), ("b", "2"), (QUIET, "true"), ("c", "9")]), "a row-id block taken");
        assert!(quiet_of(&[(crate::users::KEYS, "{}"), ("c", "9")]), "the lake's own keys");
        assert!(!quiet_of(&[("n", "4"), ("b", "2"), ("c", "9")]), "unmarked numbers are loud");
        assert!(!quiet_of(&[("t/r_t", "{}"), ("c", "9")]), "a table's entry");
        assert!(!quiet_of(&[("d/3", "x"), ("s/3", &user), ("p/sql:1", "1"), ("n", "4"), ("c", "9")]), "a table's rows");
        assert!(!quiet_of(&[("d/3", "x"), ("s/3", &user), (QUIET, "true"), ("c", "9")]), "a table's rows, marked");
        assert!(!quiet_of(&[("p/audit-x", "1"), ("c", "9")]), "a producer only named like the audit log's");
        assert!(!quiet_of(&[("d/3", "x"), ("s/3", &audit), (&audit_p, "1"), ("v/a_view", "{}"), ("c", "9")]), "the audit log's rows with a view's entry");
        // Expiries, the old marks' marker and the marks they delete count neither way (`log::EXPIRY`).
        let expiry = |due: u64, p: &str| format!("{}{due:020}/{p}", crate::log::EXPIRY);
        let quiet_with = |puts: &[(&str, &str)], deletes: &[&str]| quiet(&Delta { id: 9, puts: puts.iter().map(|(k, v)| (k.to_string(), Bytes::from(v.to_string()))).collect(), deletes: deletes.iter().map(|k| k.to_string()).collect() });
        let (expiring, marker) = (expiry(5, "sql:auto-x"), format!("{}~old", crate::log::EXPIRY));
        assert!(quiet_with(&[("d/4", "x"), ("s/4", &history), (&history_p, "1"), ("n", "5"), ("c", "9")], &[&expiring, "p/sql:auto-y"]), "the history's rows, with marks expiring and gone");
        assert!(quiet_with(&[("d/4", "x"), ("s/4", &history), (&history_p, "1"), (&expiring, "1"), (&marker, "\"Done\""), ("n", "5"), ("c", "9")], &[]), "the history's rows, with expiries and the marker put");
        assert!(quiet_with(&[(QUIET, "true"), (&expiring, "1"), ("n", "4"), ("b", "2"), ("c", "9")], &[]), "a row-id block taken, with an expiry put");
        assert!(!quiet_with(&[("d/3", "x"), ("s/3", &user), (&expiring, "1"), ("n", "4"), ("c", "9")], &[]), "a table's rows keep the commit loud");
        assert!(!quiet_with(&[("d/3", "x"), ("s/3", &user), ("c", "9")], &["p/sql:auto-y"]), "a table's rows keep the commit loud, with a mark gone");
        assert!(!quiet_with(&[(&expiring, "1"), (&marker, "\"Done\""), ("n", "4"), ("c", "9")], &[]), "expiries alone are as loud as nothing: unmarked numbers are loud");
    }

    /// A change a page's caller sees keeps its id and only the keys it may hold: a change with nothing kept is still
    /// a change, so the ids the page sees run on without a gap.
    #[test]
    fn only_keeps_the_keys_asked_and_the_id() {
        let d = Delta { id: 7, puts: vec![("t/a".into(), Bytes::from("1")), ("z/auth".into(), Bytes::from("2"))], deletes: vec!["z/old".into(), "t/b".into()] };
        let kept = d.only(|k| !k.starts_with("z/"));
        assert_eq!(kept.id, 7);
        assert_eq!(kept.puts.iter().map(|(k, _)| k.as_str()).collect::<Vec<_>>(), ["t/a"]);
        assert_eq!(kept.deletes, ["t/b"]);
        assert_eq!(d.only(|_| false).id, 7, "an empty change keeps its id");
        assert!(d.only(|_| false).puts.is_empty() && d.only(|_| false).deletes.is_empty());
    }

    /// `scan_first` is the first keys of a range, in key order, and `count` counts the range as the catalog holds it.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn scan_first_takes_the_first_keys_of_a_range() -> Result<()> {
        // (a lake opened to write: its commits are applied by `Lake::open`'s `commits`, which a bare
        // `Catalog::writer` never starts, so its `commit` would wait for ever)
        let dir = std::env::temp_dir().join(format!("pondra-scan-first-{}", uuid::Uuid::new_v4()));
        let lake = Lake::open(dir.to_str().unwrap(), true, false).await?;
        lake.cat.commit((0..5u8).map(|i| (format!("g/{i}"), vec![i])).chain([("h/x".to_string(), vec![9])]).collect(), &[]).await?;
        let first: Vec<String> = lake.cat.scan_first("g/", "g0", 3).await?.into_iter().map(|(k, _)| k).collect();
        assert_eq!(first, ["g/0", "g/1", "g/2"]);
        assert_eq!(lake.cat.scan_first("g/", "g0", 99).await?.len(), 5);
        assert_eq!(lake.cat.tallies().map(|t| t.1), Some(5), "five expiries; `h/x` is neither");
        lake.cat.close().await?;
        std::fs::remove_dir_all(&dir)?;
        Ok(())
    }

    /// `scan_page` pages a range by the keys `keep` allows: nothing lost or doubled, the keys it refuses between the pages
    /// don't stop it, and every page reads the one commit (on a memory lake, which the overlay answers).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn scan_page_pages_a_range_past_the_keys_it_refuses() -> Result<()> {
        let url = format!("memory://scan-page-{}", uuid::Uuid::new_v4().simple());
        let lake = Lake::open(&url, true, false).await?;
        lake.cat.commit((0..12u8).map(|i| (format!("g/{i:02}"), vec![i])).collect(), &[]).await?;
        let may = |k: &str| !matches!(k, "g/03" | "g/04" | "g/05" | "g/10");
        let (mut got, mut from, mut ats) = (Vec::new(), "g/".to_string(), Vec::new());
        loop {
            let (page, at) = lake.cat.scan_page(&from, "g0", 3, may).await?;
            ats.push(at);
            let Some(last) = page.last().map(|(k, _)| k.clone()) else { break };
            from = format!("{last}\0"); // (the next key past this page's last)
            got.extend(page.into_iter().map(|(k, _)| k));
        }
        assert_eq!(got, ["g/00", "g/01", "g/02", "g/06", "g/07", "g/08", "g/09", "g/11"], "the kept keys, once each, in order");
        assert!(ats.iter().all(|&a| a == lake.cat.committed()) && ats.len() == 4, "three pages and the empty one, all as of the one commit");
        crate::memory::forget_memory(&url);
        Ok(())
    }

    /// `moved_since`: a commit outside the range doesn't move a read of it since an earlier commit, and one inside does.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn moved_since_sees_a_commit_in_the_range_and_not_one_outside() -> Result<()> {
        let url = format!("memory://moved-since-{}", uuid::Uuid::new_v4().simple());
        let lake = Lake::open(&url, true, false).await?;
        lake.cat.commit(vec![("g/1".to_string(), vec![1])], &[]).await?;
        let since = lake.cat.committed();
        lake.cat.commit(vec![("h/x".to_string(), vec![2])], &[]).await?;
        let all = |_: &str| true;
        let mid = lake.cat.committed();
        assert!(!lake.cat.moved_since("g/", "g0", since, mid, all), "a commit outside the range doesn't move it");
        lake.cat.commit(vec![("g/2".to_string(), vec![3])], &[]).await?;
        let now = lake.cat.committed();
        assert!(lake.cat.moved_since("g/", "g0", since, now, all), "a commit inside it does");
        assert!(lake.cat.moved_since("g/", "g0", now, since, all), "read the other way round too");
        assert!(!lake.cat.moved_since("g/", "g0", since, mid, all), "one past both reads is in neither");
        assert!(!lake.cat.moved_since("g/", "g0", since, now, |k: &str| k != "g/2"), "one the reader may not see doesn't");
        assert!(lake.cat.moved_since("x/", "x0", now, now + 2, all), "commits `recent` doesn't hold can't be told apart");
        crate::memory::forget_memory(&url);
        Ok(())
    }

    /// A catalog its own node's stop closed answers an error, never a restart (invariant 300):
    /// taken for a fence, the write below would exec this test again.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn closed_by_its_own_stop_is_no_fence() -> Result<()> {
        let dir = std::env::temp_dir().join(format!("pondra-closed-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir)?;
        let store: Store = Arc::new(Local(LocalFileSystem::new_with_prefix(&dir)?));
        let cat = Catalog::writer(kv::writer(store, None, true).await?, Instant::now()).await?;
        let _ = cat.write(vec![("t/a".into(), b"1".to_vec())], &[]).await?;
        cat.close().await?;
        assert!(cat.write(vec![("t/b".into(), b"2".to_vec())], &[]).await.is_err());
        assert!(cat.get_raw("d/1").await.is_err());
        std::fs::remove_dir_all(&dir)?;
        Ok(())
    }

    /// A folder's store takes the conditional overwrites SlateDB's collector makes (`Local`): an update with the
    /// e-tag a put or a head gave goes through, and the next with the e-tag that one gave too; an old e-tag, a
    /// missing object and two updates racing on one e-tag are refused as S3 refuses them; an update with an
    /// attribute is refused by name and changes nothing; creates and overwrites are as they were.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_folder_takes_conditional_overwrites() -> Result<()> {
        let dir = std::env::temp_dir().join(format!("pondra-local-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir)?;
        let store: Store = Arc::new(Local(LocalFileSystem::new_with_prefix(&dir)?));
        let at = Path::from("catalog/gc/manifest.boundary");
        let bytes = |b: &'static [u8]| object_store::PutPayload::from_static(b);
        let update = |e_tag: &str| PutOptions { mode: PutMode::Update(object_store::UpdateVersion { e_tag: Some(e_tag.into()), version: None }), ..Default::default() };
        let refused = |r: object_store::Result<object_store::PutResult>| matches!(r, Err(object_store::Error::Precondition { .. }));

        let created = store.put_opts(&at, bytes(b"1"), PutOptions { mode: PutMode::Create, ..Default::default() }).await?;
        let again = store.put_opts(&at, bytes(b"x"), PutOptions { mode: PutMode::Create, ..Default::default() }).await;
        assert!(matches!(again, Err(object_store::Error::AlreadyExists { .. })), "a create over an object: already there");
        let first = created.e_tag.expect("an e-tag");
        assert_eq!(store.head(&at).await?.e_tag.as_deref(), Some(first.as_str()), "a put's e-tag is the one a head gives");

        let second = store.put_opts(&at, bytes(b"2"), update(&first)).await?;
        assert_eq!(second.e_tag, store.head(&at).await?.e_tag, "an update's e-tag is the new object's");
        let second = second.e_tag.expect("an e-tag");
        let third = store.put_opts(&at, bytes(b"3"), update(&second)).await?.e_tag.expect("an e-tag");
        assert_eq!(store.get(&at).await?.bytes().await?, Bytes::from_static(b"3"), "the second update went through");

        assert!(refused(store.put_opts(&at, bytes(b"4"), update(&first)).await), "an old e-tag: a precondition");
        let missing = Path::from("catalog/gc/none.boundary");
        assert!(refused(store.put_opts(&missing, bytes(b"5"), update("any")).await), "a missing object: a precondition");
        assert!(store.head(&missing).await.is_err(), "and nothing was created");

        let mut attributes = object_store::Attributes::new();
        attributes.insert(object_store::Attribute::ContentType, "text/plain".into());
        let tagged = PutOptions { attributes, ..update(&third) };
        assert!(matches!(store.put_opts(&at, bytes(b"6"), tagged).await, Err(object_store::Error::NotImplemented { .. })), "an update with an attribute: refused by name");
        assert_eq!(store.get(&at).await?.bytes().await?, Bytes::from_static(b"3"), "and nothing changed");

        let (a, b) = tokio::join!(store.put_opts(&at, bytes(b"7"), update(&third)), store.put_opts(&at, bytes(b"8"), update(&third)));
        assert_eq!([a.is_ok(), b.is_ok()].iter().filter(|ok| **ok).count(), 1, "of two updates on one e-tag, one is made");
        assert!(refused(if a.is_err() { a } else { b }), "the other is a precondition");
        store.put_opts(&at, bytes(b"9"), PutOptions::default()).await?;
        assert_eq!(store.get(&at).await?.bytes().await?, Bytes::from_static(b"9"), "an overwrite is as it was");
        std::fs::remove_dir_all(&dir)?;
        Ok(())
    }

    /// A table entry is parsed once for its bytes: the same bytes (in the same buffer or another) give the
    /// same parse, changed bytes a new one, and what is kept stays under its count and byte bounds.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_table_entry_is_parsed_once_per_version() -> Result<()> {
        let dir = std::env::temp_dir().join(format!("pondra-parsed-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir)?;
        let store: Store = Arc::new(Local(LocalFileSystem::new_with_prefix(&dir)?));
        let cat = Catalog::writer(kv::writer(store, None, true).await?, Instant::now()).await?;
        let entry = |folder: &str| -> Result<Bytes> { Ok(Bytes::from(serde_json::to_vec(&TableMeta { folder: Some(folder.to_string()), ..Default::default() })?)) };
        let raw = entry("x")?;
        let first = cat.parsed("t/a", &raw)?;
        assert!(Arc::ptr_eq(&first, &cat.parsed("t/a", &raw)?), "the same bytes: the same parse");
        assert!(Arc::ptr_eq(&first, &cat.parsed("t/a", &Bytes::copy_from_slice(&raw))?), "equal bytes in another buffer: no parse");
        let second = cat.parsed("t/a", &entry("y")?)?;
        assert!(!Arc::ptr_eq(&first, &second) && second.folder.as_deref() == Some("y"), "changed bytes: a new parse");
        assert!(cat.parsed("t/a", &Bytes::from_static(b"not an entry")).is_err(), "bytes that aren't an entry are an error");
        // Entries of 1 MiB: 16 MiB of raw bytes stay at most, the least recently used out first; one of
        // more than 16 MiB is parsed for its caller and not kept.
        let big = |n: usize| -> Result<Bytes> { Ok(Bytes::from(serde_json::to_vec(&TableMeta { folder: Some(format!("{n}{}", "f".repeat(1 << 20))), ..Default::default() })?)) };
        for n in 0..40 {
            cat.parsed(&format!("t/big{n}"), &big(n)?)?;
        }
        let kept = |cat: &Catalog, key: &str| cat.metas.lock().unwrap().by_key.contains(key);
        assert!(cat.metas.lock().unwrap().by_key.bytes() <= PARSED_BYTES, "the raw bytes kept are within the bound");
        assert!(kept(&cat, "t/big39") && !kept(&cat, "t/big0"), "the least recently used went first");
        let before = cat.metas.lock().unwrap().by_key.bytes();
        cat.parsed("t/huge", &Bytes::from(serde_json::to_vec(&TableMeta { folder: Some("h".repeat(PARSED_BYTES + 1)), ..Default::default() })?))?;
        assert!(!kept(&cat, "t/huge") && cat.metas.lock().unwrap().by_key.bytes() == before, "an entry over the bound is not kept");
        for n in 0..300 {
            cat.parsed(&format!("t/small{n}"), &entry(&n.to_string())?)?;
        }
        assert!(cat.metas.lock().unwrap().by_key.len() <= PARSED_COUNT, "the count bound holds");
        std::fs::remove_dir_all(&dir)?;
        Ok(())
    }

    /// A log segment's entry is parsed once for its bytes (`Catalog::segments`): a second pass over the same
    /// entries, in other buffers, gives the same parses; changed bytes a new one; bad bytes an error; each entry
    /// is charged its parsed form too, within the bounds; and a sweep bigger than the cache leaves it as it was.
    /// Prints what a pass over 5,000 entries costs parsed and kept.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_segment_entry_is_parsed_once_per_version() -> Result<()> {
        let dir = std::env::temp_dir().join(format!("pondra-segments-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir)?;
        let store: Store = Arc::new(Local(LocalFileSystem::new_with_prefix(&dir)?));
        let cat = Catalog::writer(kv::writer(store, None, true).await?, Instant::now()).await?;
        let seg = |n: u64, rows: u64| -> Result<Bytes> {
            let parts = ["pgbench_accounts", "pgbench_tellers", "pgbench_branches", "pgbench_history"].map(|t| (t.to_string(), vec![(n, 300, rows)]));
            Ok(Bytes::from(serde_json::to_vec(&Segment { path: String::new(), parts: parts.into_iter().collect(), ts_ms: n, files: BTreeMap::new() })?))
        };
        let entries = |rows: u64| -> Result<BTreeMap<String, Bytes>> { (1..=5000).map(|n| Ok((seg_key(n), seg(n, rows)?))).collect() };
        let t = Instant::now();
        let first = cat.segments_of(entries(2)?)?;
        let parsed = t.elapsed();
        let again = entries(2)?;
        let t = Instant::now();
        let second = cat.segments_of(again)?;
        let kept = t.elapsed();
        eprintln!("5,000 segment entries: {parsed:?} parsed, {kept:?} kept");
        assert_eq!(first.len(), 5000);
        assert!(first.iter().zip(&second).all(|((a, x), (b, y))| a == b && Arc::ptr_eq(x, y)), "equal bytes in other buffers: no parse");
        assert_eq!(second[41].1.parts["pgbench_history"], vec![(42, 300, 2)], "the entry as written");
        let changed = cat.segments_of([(seg_key(42), seg(42, 3)?)].into_iter().collect())?;
        assert!(!Arc::ptr_eq(&changed[0].1, &second[41].1) && changed[0].1.parts["pgbench_history"] == vec![(42, 300, 3)], "changed bytes: a new parse");
        assert!(cat.segments_of([(seg_key(7), Bytes::from_static(b"not a segment"))].into_iter().collect()).is_err(), "bytes that aren't a segment are an error");
        {
            let held = cat.segs.lock().unwrap();
            assert_eq!(held.by_key.bytes(), held.by_key.iter().map(|(_, (raw, _))| segment_charge(raw.len())).sum::<usize>(), "each entry charged raw and parsed");
            assert!(held.by_key.len() <= SEGMENTS_COUNT && held.by_key.bytes() <= SEGMENTS_BYTES, "the bounds hold");
        }
        // A sweep of more entries than the cache keeps parses the ones it hasn't without keeping them: the tail's stay.
        let sweep: BTreeMap<String, Bytes> = (1..=SEGMENTS_COUNT as u64 + 1).map(|n| Ok((seg_key(n), seg(n, 2)?))).collect::<Result<_>>()?;
        let swept = cat.segments_of(sweep)?;
        assert_eq!(swept.len(), SEGMENTS_COUNT + 1);
        assert!(Arc::ptr_eq(&swept[0].1, &second[0].1), "a kept entry is a hit");
        let held = cat.segs.lock().unwrap();
        assert_eq!(held.by_key.len(), 5000, "nothing of the sweep's own kept");
        assert!(held.by_key.peek(&seg_key(1)).is_some() && held.by_key.peek(&seg_key(60_000)).is_none());
        drop(held);
        std::fs::remove_dir_all(&dir)?;
        Ok(())
    }

    /// `recent_inline` answers a `d/` key that one of the last 64 commits wrote, and no other key: the
    /// frames are sent by hand, as the commit loop would send them, so no commit has to wait for it.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn recent_inline_answers_the_last_64_commits() -> Result<()> {
        let dir = std::env::temp_dir().join(format!("pondra-recent-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir)?;
        let store: Store = Arc::new(object_store::local::LocalFileSystem::new_with_prefix(&dir)?);
        let cat = Catalog::writer(kv::writer(store, None, true).await?, Instant::now()).await?;
        let change = |id: u64, key: String, bytes: &'static [u8]| Frame::Change(Arc::new(Delta { id, puts: vec![(key, Bytes::from_static(bytes))], deletes: vec![] }));
        cat.send(change(1, data_key(1), b"rows"));
        assert_eq!(cat.recent_inline(&data_key(1)).as_deref(), Some(&b"rows"[..]));
        assert_eq!(cat.recent_inline(&data_key(2)), None);
        assert_eq!(cat.get_raw(&data_key(1)).await?.as_deref(), Some(&b"rows"[..]));
        for i in 0..64u64 {
            cat.send(change(2 + i, data_key(100 + i), b"other"));
        }
        assert_eq!(cat.recent_inline(&data_key(1)), None, "the window is the last 64 commits");
        assert_eq!(cat.recent_inline(&data_key(163)).as_deref(), Some(&b"other"[..]));
        cat.close().await?;
        std::fs::remove_dir_all(&dir)?;
        Ok(())
    }

    /// Two lakes a node opens share its one line for query memory (`ledger::Ledger`): each lake's pool is its share of
    /// it, and each lake's `memory` reads the line and what every lake holds on it.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn two_lakes_of_a_node_share_its_one_line_for_queries() -> Result<()> {
        use datafusion::execution::memory_pool::MemoryConsumer;
        let dirs = [0, 1].map(|_| std::env::temp_dir().join(format!("pondra-ledger-{}", uuid::Uuid::new_v4())));
        let a = Lake::open(dirs[0].to_str().unwrap(), true, false).await?;
        let b = Lake::open(dirs[1].to_str().unwrap(), true, false).await?;
        let ledger = |l: &Lake| l.rt.memory_pool.downcast_ref::<crate::ledger::LakePool>().expect("a lake's pool is its share of the line").ledger().clone();
        assert!(Arc::ptr_eq(&ledger(&a), &ledger(&b)), "one line for both");
        let held = MemoryConsumer::new("a test's").register(&a.rt.memory_pool);
        held.try_grow(1 << 20)?;
        // (other tests' lakes may hold some of the same line meanwhile: what a holds is the least it reads)
        assert_eq!(b.memory().1, memory_limit());
        assert!(b.memory().0 >= 1 << 20, "a's reservation counts on the line b reads");
        assert_eq!(b.rt.memory_pool.reserved(), 0, "and not in b's own pool");
        drop(held);
        for (lake, dir) in [a, b].into_iter().zip(&dirs) {
            lake.cat.close().await?;
            let _ = std::fs::remove_dir_all(dir);
        }
        Ok(())
    }

    /// A lake's caches are charged to its line as it opens, so the node's memory watch takes memory back from them all:
    /// here the tables' tails, after the hot columns and the lookups' row groups, which hold nothing yet.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_lakes_caches_are_charged_to_its_line_and_give_memory_back() -> Result<()> {
        use crate::ledger::Charged;
        let dir = std::env::temp_dir().join(format!("pondra-charged-{}", uuid::Uuid::new_v4()));
        let lake = Lake::open(dir.to_str().unwrap(), true, false).await?;
        let ledger = lake.rt.memory_pool.downcast_ref::<crate::ledger::LakePool>().expect("a lake's pool is its share of the line").ledger().clone();
        let names: Vec<&str> = lake.caches().iter().map(|(name, _)| *name).collect();
        assert_eq!(names, ["hot", "lookups", "tails", "log", "catalog_blocks", "footers", "statistics", "catalog_entries", "catalog_segments"], "a local lake has no cache of a bucket's ranges");
        let _ = lake.tails.lock().unwrap().put("t".into(), vec![], 4096);
        // (other tests' lakes may be charged to the same line meanwhile: what this lake holds is the least it reads)
        assert!(ledger.held() >= 4096, "the line counts the tails");
        // (taken back from this lake's caches alone, in the line's order: from the whole line, other tests' would go too)
        let mut freed = 0;
        for (_, cache) in lake.caches() {
            freed += if freed < 4096 { cache.give_back(4096 - freed) } else { 0 };
        }
        assert!(freed >= 4096 && lake.tails.held() == 0, "the tails give their memory back");
        assert!(!lake.cached_bytes().iter().any(|(name, _)| *name == "hot"), "the hot columns are `hot_bytes`");
        // (`/metrics` reads the line's list by name: the node's planning facts and manifests are on it beside the lake's caches;
        // a node's start charges its kept pages and outside tables' logs and manifests, `node::start_app`)
        let metrics: Vec<&str> = lake.cached_bytes().iter().map(|(name, _)| *name).collect();
        assert!(["lookups", "tails", "log", "catalog_blocks", "footers", "statistics", "catalog_entries", "catalog_segments", "facts", "manifests", "files"].iter().all(|n| metrics.contains(n)), "{metrics:?}");
        lake.cat.close().await?;
        let _ = std::fs::remove_dir_all(&dir);
        Ok(())
    }

    /// A lake a process opens to write (a backup taken in it: `backup::take_here`) is let go with its
    /// last handle: the loops `Lake::open` starts hold it weakly.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_lake_opened_to_write_goes_with_its_last_handle() -> Result<()> {
        let dir = std::env::temp_dir().join(format!("pondra-dropped-{}", uuid::Uuid::new_v4()));
        let lake = Lake::open(dir.to_str().unwrap(), true, false).await?;
        lake.cat.commit(vec![("t/a".into(), b"1".to_vec())], &[]).await?;
        lake.cat.close().await?;
        let gone = Arc::downgrade(&lake);
        drop(lake);
        for _ in 0..100 {
            if gone.upgrade().is_none() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert!(gone.upgrade().is_none(), "the lake is still held");
        let _ = std::fs::remove_dir_all(&dir);
        Ok(())
    }

    /// A joined page's reader (`open_on`): its catalog is the kv it is given (a node's, over `kv::remote`), and its tables
    /// are listed from that. Here the kv holds a folder lake's catalog as the writer sees it, and the folder is the files.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn open_on_lists_the_tables_of_the_catalog_it_is_given() -> Result<()> {
        /// A catalog that holds what it was given and takes no writes, as a node's catalog reads to a joined page.
        struct Given(BTreeMap<String, Bytes>);
        #[async_trait::async_trait]
        impl Kv for Given {
            async fn get(&self, key: &str, _: bool) -> Result<Option<Bytes>, kv::Error> {
                Ok(self.0.get(key).cloned())
            }
            async fn scan(&self, range: std::ops::Range<Vec<u8>>, _: bool) -> Result<BTreeMap<String, Bytes>, kv::Error> {
                Ok(self.0.iter().filter(|(k, _)| range.contains(&k.as_bytes().to_vec())).map(|(k, v)| (k.clone(), v.clone())).collect())
            }
            fn is_writer(&self) -> bool {
                false
            }
            async fn write(&self, _: &[(String, Bytes)], _: &[String]) -> Result<Arc<dyn Written>, kv::Error> {
                Err(kv::Error::new(false, "a joined page's catalog takes no writes"))
            }
            async fn checkpoint(&self) -> Result<(), kv::Error> {
                Ok(())
            }
            async fn close(&self) -> Result<(), kv::Error> {
                Ok(())
            }
        }
        impl crate::ledger::Charged for Given {
            fn held(&self) -> usize {
                0
            }
        }

        let dir = std::env::temp_dir().join(format!("pondra-joined-{}", uuid::Uuid::new_v4()));
        let folder = dir.to_str().unwrap().to_string();
        let writer = Lake::open(&folder, true, false).await?;
        crate::write::create_table(&writer, "t", r#"{"columns": [["a", "Int64"]]}"#).await?;
        let copied = writer.cat.kv.scan(b"".to_vec()..vec![0xff], false).await?;
        assert!(copied.contains_key(&table_key("t")), "the writer's catalog holds its table");

        let reader = Lake::open_on(&folder, false, Arc::new(Given(copied))).await?;
        let tables = reader.cat.scan::<TableMeta>("t/", "t0").await?;
        assert_eq!(tables.iter().map(|(k, _)| k.as_str()).collect::<Vec<_>>(), vec![table_key("t").as_str()]);

        reader.cat.close().await?;
        writer.cat.close().await?;
        std::fs::remove_dir_all(&dir)?;
        Ok(())
    }

    /// A listed segment's inline rows can reach a reader a moment late, while its view is behind the
    /// commit number it has streamed (a new writer opening the lake): the read waits for them. A writer's
    /// miss is an error at once (`Catalog::inline`; a reader's after 10 s, which no test waits out).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_listed_segment_waits_for_its_inline_rows() -> Result<()> {
        use datafusion::arrow::array::{ArrayRef, Int64Array};
        let dir = std::env::temp_dir().join(format!("pondra-inline-{}", uuid::Uuid::new_v4()));
        let writer = Lake::open(dir.to_str().unwrap(), true, false).await?;
        writer.cat.commit(vec![("t/x".into(), b"1".to_vec())], &[]).await?;
        writer.cat.checkpoint().await?; // (a reader's view sees only what is flushed)
        let reader = Lake::open(dir.to_str().unwrap(), false, true).await?;
        assert!(reader.cat.mirror.load(Relaxed), "a streamed reader holds the whole catalog once it opens");

        // The next commit, streamed to the reader with its segment and inline rows.
        let batch = RecordBatch::try_from_iter([("a", Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef)])?;
        let ipc = crate::log::encode_ipc(&[batch])?;
        let n = writer.cat.committed() + 1;
        let seg = Segment { parts: BTreeMap::from([("t".to_string(), vec![(0, ipc.len() as u64, 3)])]), ..Default::default() };
        let streamed = vec![
            (seg_key(n), Bytes::from(json(&seg))),
            (data_key(n), Bytes::from(ipc.clone())),
            ("n".to_string(), Bytes::from(json(&(n + 1)))),
            ("c".to_string(), Bytes::from(json(&n))),
        ];
        reader.cat.apply(&Delta { id: n, puts: streamed, deletes: vec![] });

        // Commit n reaches the writer's next flush without its inline rows, so the reader's view takes
        // its commit number and not the rows.
        writer.cat.commit(vec![("t/y".into(), b"2".to_vec())], &[]).await?;
        writer.cat.checkpoint().await?;
        let since = Instant::now();
        while reader.cat.view_now().await? < n {
            anyhow::ensure!(since.elapsed() < Duration::from_secs(10), "the reader's view never reached commit {n}");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        reader.cat.refresh().await?; // (the streamed copy of `d/n` goes: the view has its commit)
        assert!(reader.cat.get_raw(&data_key(n)).await?.is_none(), "the view holds no inline rows of commit {n} yet");

        // The rows land 300 ms later. The read below starts first, so it has to wait for them.
        let late = writer.clone();
        let before = Instant::now();
        let landing = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(300)).await;
            late.cat.commit(vec![(data_key(n), ipc.clone())], &[]).await?;
            late.cat.checkpoint().await
        });
        let rows = reader.segment_rows(n, &seg, "t").await?;
        assert_eq!(rows.iter().map(|b| b.num_rows()).sum::<usize>(), 3, "the segment's rows, once they arrive");
        assert!(before.elapsed() >= Duration::from_millis(300), "the read answered before its rows were written");
        landing.await??;

        // A writer has the whole catalog: its miss is an error at once.
        let quick = Instant::now();
        let miss = writer.cat.inline(&data_key(999_999)).await.unwrap_err();
        assert_eq!(miss.to_string(), "missing inline segment 999999");
        assert!(quick.elapsed() < Duration::from_secs(1), "a writer's miss waited");

        reader.cat.close().await?;
        writer.cat.close().await?;
        let _ = std::fs::remove_dir_all(&dir);
        Ok(())
    }

    /// A node started on a full disk (its cap 0 at open) spills again once room is freed: `spill_room` gives a
    /// manager what it holds plus the disk's room beyond a lake's keep. The setting is passed in, not read, so
    /// the test doesn't depend on the environment it runs in.
    #[test]
    fn spill_room_follows_the_disk() {
        let folder = spill_folder().expect("this process's spill folder");
        let manager = DiskManagerBuilder::default().with_mode(DiskManagerMode::Directories(vec![folder.to_path_buf()])).with_max_temp_directory_size(0).build().expect("a disk manager");
        let manager = Arc::new(manager);
        SPILLERS.lock().unwrap().push(Arc::downgrade(&manager));
        set_room(None);
        let cap = manager.max_temp_directory_size();
        assert!(cap >= manager.used_disk_space() + (1 << 20), "a cap of {cap} with the disk's room");
        set_room(Some(1.0));
        assert!(manager.max_temp_directory_size() <= gb_bytes(1.0), "the setting caps the spills");
    }

    /// A lake's spill disk shows no directory of DataFusion's until something spills: its first spill file is made
    /// in the spill folder, and the folder is the one the spill made (`spill_folder`, this process's).
    #[test]
    fn a_lake_makes_no_spill_folder_until_it_spills() {
        if spill_gb() == Some(0.0) {
            return; // (spilling is off: there is no spill disk to make)
        }
        let m = Arc::new(spill_disk().build().expect("a spill disk"));
        assert!(m.temp_dir_paths().is_empty(), "no directory before a spill");
        assert!(m.tmp_files_enabled());
        let f = m.create_tmp_file("a test").expect("a spill file");
        let path = f.path().expect("a local file").to_path_buf();
        assert!(path.exists(), "{path:?}");
        if let Ok(folder) = spill_folder() {
            assert!(path.starts_with(folder), "{path:?} is not in {folder:?}");
        }
        // (DataFusion counts a write's bytes on the manager that made the file, through the file's own writer: not asserted here)
        let mut writer = f.open_writer().expect("a writer");
        std::io::Write::write_all(&mut writer, &[7u8; 10]).expect("10 bytes written");
        writer.finish().expect("finished");
    }

    /// A shuffle's piece asks for its room first (`spill_fits`): what doesn't fit is refused with 53100, and the
    /// setting is named only when it is set.
    #[test]
    fn a_piece_that_does_not_fit_is_refused_by_name() {
        assert!(!piece_fits(Some(0.5), 0, 1 << 30, None), "a gigabyte past half a gigabyte");
        assert!(piece_fits(Some(0.5), 1 << 20, 1 << 20, None), "a megabyte more under the setting");
        assert!(!piece_fits(None, 0, 1, Some((0, 1 << 30))), "no free room");
        assert!(piece_fits(None, 0, 1 << 20, Some((1 << 30, 1 << 30))), "a gigabyte free, a lake keeps 256 MB");
        let set = no_room(1 << 20, Some(0.5));
        assert_eq!(crate::codes::of(&set), "53100");
        assert!(set.to_string().contains("PONDRA_SPILL_GB") && !set.to_string().contains("datafusion"), "{set}");
        let unset = no_room(1 << 20, None);
        assert_eq!(crate::codes::of(&unset), "53100");
        assert!(!unset.to_string().contains("PONDRA_SPILL_GB") && !unset.to_string().contains("datafusion"), "{unset}");
        let err = spill_fits(u64::MAX / 2).expect_err("no disk holds that");
        assert_eq!(crate::codes::of(&err), "53100");
        assert_eq!(err.to_string().contains("PONDRA_SPILL_GB"), spill_gb().is_some(), "{err}");
    }

    /// A keyed table with every rule kind: a renamed and a dropped column, both identity kinds with their
    /// defaults, an enum, a uuid, a varchar(3), NOT NULL, DEFAULT, CHECK, an enforced and a NOT ENFORCED UNIQUE.
    fn with_rules() -> TableMeta {
        let col = |c: &str, t: &str| (c.to_string(), t.to_string());
        let set = |v: &[(&str, &str)]| v.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect::<BTreeMap<String, String>>();
        let id = |always: bool, seq: &str| Identity { always, declared: vec![], sequence: seq.into() };
        let unique = |name: &str, c: &str, enforced: bool| crate::constraints::Constraint { name: name.into(), kind: crate::constraints::Kind::Unique, columns: vec![c.into()], references: None, enforced };
        let mut t = TableMeta::made(Made {
            columns: vec![col("id", "Int64"), col("seq_no", "Int64"), col("c_code", "Utf8"), col("c_status", "Utf8"), col("owner", "Utf8"), col("total", "Int64"), col("doc", "Utf8"), col("gone", "Utf8"), col("_deleted", "Boolean")],
            names: set(&[("c_code", "code"), ("c_status", "status")]),
            dropped: vec!["gone".into()],
            not_null: vec!["id".into(), "c_code".into(), "gone".into()],
            defaults: set(&[("id", "nextval('orders_id_seq')"), ("seq_no", "nextval('orders_seq_no_seq')"), ("total", "0"), ("gone", "'x'")]),
            identity: [("id".to_string(), id(false, "orders_id_seq")), ("seq_no".to_string(), id(true, "orders_seq_no_seq"))].into(),
            enums: [("c_status".to_string(), Enum { of: Some("order_status".into()), labels: vec!["open".into(), "shipped".into()] })].into(),
            sql_types: set(&[("owner", "uuid"), ("c_code", "varchar(3)"), ("doc", "json"), ("gone", "json")]),
            ..Default::default()
        });
        t.key = vec!["id".into()];
        t.checks = vec![("orders_total_check".into(), "total >= 0".into())];
        t.constraints = vec![unique("orders_code_key", "c_code", true), unique("orders_owner_key", "owner", false)];
        t
    }

    /// `logical()` of `with_rules`, as JSON: the golden is the unchanged code's.
    #[test]
    fn logical_is_as_before_rules() {
        // (as JSON values: an entry written as format 5's fields has its column fields first now, the keys the same)
        let json = serde_json::to_value(&with_rules().logical()).unwrap();
        assert!(serde_json::to_string(&with_rules().logical()).unwrap().starts_with(r#"{"columns":"#));
        assert_eq!(json, serde_json::from_str::<serde_json::Value>(r#"{"columns":[["id","Int64"],["seq_no","Int64"],["code","Utf8"],["status","Utf8"],["owner","Utf8"],["total","Int64"],["doc","Utf8"],["_deleted","Boolean"]],"key":["id"],"merge":{},"files":[],"sealed":null,"tiered":0,"garbage":[],"rows_at":0,"shadows":false,"publish":[],"cluster":[],"ttl":null,"partition":null,"ids":false,"changed":false,"not_null":["id","code"],"defaults":{"id":"nextval('orders_id_seq')","seq_no":"nextval('orders_seq_no_seq')","total":"0"},"checks":[["orders_total_check","total >= 0"]],"identity":{"id":{"always":false,"sequence":"orders_id_seq"},"seq_no":{"always":true,"sequence":"orders_seq_no_seq"}},"constraints":[{"name":"orders_code_key","kind":"unique","columns":["code"],"enforced":true},{"name":"orders_owner_key","kind":"unique","columns":["owner"]}],"enums":{"status":{"of":"order_status","labels":["open","shipped"]}},"sql_types":{"code":"varchar(3)","doc":"json","owner":"uuid"}}"#).unwrap());
    }

    /// `to_rules` names everything SQL's way: the dropped column is absent, the renamed one keeps its stored name, and
    /// each rule field is what `logical()` gives under the same names.
    #[test]
    fn to_rules_maps_the_rule_fields_as_logical_does() -> Result<()> {
        let m = with_rules();
        let (t, l) = (m.to_rules()?, m.logical());
        let plain = |name: &str, stored: &str| crate::rules::Column { name: name.into(), stored: stored.into(), not_null: false, default: None, always: false, held: None, labels: None };
        let want = vec![
            crate::rules::Column { not_null: true, default: Some("nextval('orders_id_seq')".into()), ..plain("id", "id") },
            crate::rules::Column { default: Some("nextval('orders_seq_no_seq')".into()), always: true, ..plain("seq_no", "seq_no") },
            crate::rules::Column { not_null: true, held: Some("varchar(3)".into()), ..plain("code", "c_code") },
            crate::rules::Column { labels: Some((Some("order_status".into()), vec!["open".into(), "shipped".into()])), ..plain("status", "c_status") },
            crate::rules::Column { held: Some("uuid".into()), ..plain("owner", "owner") },
            crate::rules::Column { default: Some("0".into()), ..plain("total", "total") },
            crate::rules::Column { held: Some("json".into()), ..plain("doc", "doc") },
            plain("_deleted", "_deleted"),
        ];
        assert_eq!(t.columns, want);
        let names: Vec<&str> = t.schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(names, ["id", "seq_no", "code", "status", "owner", "total", "doc", "_deleted"], "SQL's names, _deleted among them");
        assert_eq!(t.key, ["id"]);
        assert!(t.key_checked && t.mapped && t.checked() && t.width() == 7);
        assert_eq!(t.unique, [("orders_code_key".to_string(), vec!["code".to_string()])]);
        assert_eq!(t.checks, [("orders_total_check".to_string(), "total >= 0".to_string())]);
        assert!(t.column("gone").is_none() && t.marker("_deleted") && !t.marker("total"));
        for c in &t.columns {
            assert_eq!(l.fields().not_null.contains(&c.name), c.not_null, "{}", c.name);
            assert_eq!(l.fields().defaults.get(&c.name).cloned(), c.default, "{}", c.name);
            assert_eq!(l.fields().identity.get(&c.name).is_some_and(|i| i.always), c.always, "{}", c.name);
            assert_eq!(l.fields().sql_types.get(&c.name).cloned(), c.held, "{}", c.name);
            assert_eq!(l.fields().enums.get(&c.name).map(|e| (e.of.clone(), e.labels.clone())), c.labels, "{}", c.name);
        }
        assert_eq!(l.key, t.key);
        assert_eq!(l.columns().map(|(n, _)| n).collect::<Vec<_>>(), names);
        assert!(!l.fields().defaults.contains_key("gone") && !l.fields().sql_types.contains_key("gone") && !l.fields().not_null.iter().any(|c| c == "gone"));
        Ok(())
    }

    /// A merge table (a GROUP BY view's) has a key, but its plain INSERTs' keys are not checked.
    #[test]
    fn a_merge_table_is_not_key_checked() -> Result<()> {
        let mut m = TableMeta::with_columns(vec![("day".into(), "Int64".into()), ("n".into(), "Int64".into())]);
        (m.key, m.merge) = (vec!["day".into()], [("n".to_string(), "sum".to_string())].into());
        let t = m.to_rules()?;
        assert!(!t.key_checked && !t.checked() && !t.mapped);
        assert_eq!(t.key, ["day"]);
        assert_eq!(t.key, m.logical().key);
        Ok(())
    }

    /// `rules()` makes a parse's table once: the same `Arc` gives the same tables, a new parse makes its own, and a
    /// clone starts with none.
    #[test]
    fn the_rules_are_made_once_per_parse() -> Result<()> {
        let a = Arc::new(with_rules());
        let (x, y) = (a.rules()?, a.rules()?);
        assert!(Arc::ptr_eq(&x, &y), "the same parse gives the same tables");
        assert_eq!(*x, a.to_rules()?, "and they are the rules of its entry");
        let b = Arc::new((*a).clone());
        assert!(!Arc::ptr_eq(&b.rules()?, &x), "a new parse makes its own");
        let c = (*a).clone();
        assert!(c.rules.get().is_none(), "a clone's memo is empty");
        let made = Arc::new(c).rules()?;
        assert_eq!(*made, *x, "and the clone's rules are the same");
        Ok(())
    }

    /// The one column list (redesign step 1b) agrees with the order the entry's own fields gave before it, for any mix
    /// of renames, drops and moves: SQL's order, ids as stored places + 1, a renamed column's stored name kept, and
    /// dropped columns marked, last. `star` and `sql_names` are the list's, a keyed table's markers out, and the same of
    /// `logical()`'s copy; `sql_key`, `sql_partition` and `logical_columns` are its fields. `cols()` makes it once per parse, and a clone's memo is empty.
    #[test]
    fn the_column_list_is_the_entrys_columns_in_sqls_order() {
        // (the order as `in_order` made it before the list: `shown`'s columns, then the rest as stored, the dropped out)
        fn was(m: &TableMeta) -> Vec<String> {
            let f = m.fields();
            let (shown, dropped) = (&f.shown, &f.dropped);
            let first = shown.iter().filter_map(|s| m.columns().find(|(c, _)| c == s));
            let rest = m.columns().filter(|(c, _)| !shown.iter().any(|s| s == c));
            first.chain(rest).filter(|(c, _)| !dropped.iter().any(|d| d == c)).map(|(c, _)| m.name_of(c).to_string()).collect()
        }
        let mut seed = 0x9e37_79b9_7f4a_7c15u64;
        let mut next = |n: u64| {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed % n
        };
        for _ in 0..2000 {
            let n = 1 + next(8) as usize;
            let mut f = Made { columns: (0..n).map(|i| (format!("c{i}"), if i % 2 == 0 { "Int64" } else { "Utf8" }.to_string())).collect(), ..Default::default() };
            for i in 0..n {
                match next(6) {
                    0 => f.dropped.push(format!("c{i}")),
                    1 => _ = f.names.insert(format!("c{i}"), format!("r{i}")),
                    _ => {}
                }
            }
            // (delete markers: a keyed table's, or a column of that name in a table without a key)
            let keyed = next(3) == 0;
            if keyed || next(4) == 0 {
                f.columns.push(("_deleted".into(), "Boolean".into()));
            }
            let partition = match next(3) {
                0 => Some(format!("day(c{})", next(n as u64))),
                1 => Some(format!("c{}", next(n as u64))),
                _ => None,
            };
            let n = f.columns.len();
            // (`shown`: some of the columns, each once, in any order, dropped ones among them)
            let mut shown: Vec<String> = (0..n).filter(|_| next(2) == 0).map(|i| format!("c{i}")).collect();
            for i in (1..shown.len()).rev() {
                shown.swap(i, next(i as u64 + 1) as usize);
            }
            f.shown = shown;
            let mut m = TableMeta::made(f);
            if keyed {
                m.key = vec!["c0".into()];
            }
            m.partition = partition;
            let list = m.column_list();
            // (the same entry read back as its list answers the same)
            let back: TableMeta = serde_json::from_slice(&json(&m.clone().as_list())).unwrap();
            assert_eq!((back.column_list(), back.sql_order(), back.star(), back.logical().list()), (list.clone(), m.sql_order(), m.star(), m.logical().list()));
            let sql: Vec<String> = list.sql().map(|c| c.name.clone()).collect();
            assert_eq!(sql, was(&m), "{:?} {:?} {:?}", m.fields().shown, m.fields().dropped, m.fields().names);
            assert_eq!(m.sql_order(), sql);
            let stored: Vec<String> = list.stored_order().map(|c| c.name.clone()).collect();
            assert_eq!(m.moved(), sql != stored, "moved: SQL's order isn't the stored one");
            let star: Vec<(String, String)> = list.sql().filter(|c| !m.marker(&c.stored)).map(|c| (c.name.clone(), c.dtype.clone())).collect();
            assert_eq!(m.star(), star, "what `SELECT *` lists");
            assert_eq!(m.sql_names(), sql.iter().filter(|c| *c != "_deleted").cloned().collect::<Vec<_>>());
            let l = m.logical();
            assert_eq!((l.star(), l.sql_names(), l.sql_order()), (m.star(), m.sql_names(), m.sql_order()), "the same of the logical copy");
            assert_eq!((m.sql_key(), m.sql_partition(), m.logical_columns()), (l.key.clone(), l.partition.clone(), l.to_columns()), "a field without the copy");
            for c in list.all() {
                assert_eq!(m.columns().nth(c.id as usize - 1).map(|(s, _)| s), Some(c.stored.as_str()), "an id is a stored place + 1");
                assert_eq!((c.name.as_str(), c.dropped), (m.name_of(&c.stored), m.fields().dropped.contains(&c.stored)));
            }
            assert_eq!(list.all().len(), n, "every column, the dropped too");
            assert!(list.stored_order().collect::<Vec<_>>().windows(2).all(|w| w[0].id < w[1].id));
        }
        let a = Arc::new(TableMeta::made(Made { columns: vec![("a".into(), "Int64".into()), ("b".into(), "Utf8".into())], shown: vec!["b".into()], ..Default::default() }));
        assert!(Arc::ptr_eq(&a.cols(), &a.cols()), "made once per parse");
        assert_eq!(*a.cols(), a.column_list());
        assert!((*a).clone().column_memo.get().is_none(), "a clone's memo is empty");
    }

    /// The parse and size measures' wide entry: 500 columns, renames, drops, a moved order, NOT NULLs, defaults, held
    /// types and 200 files.
    fn wide_entry() -> TableMeta {
        let mut m = TableMeta::made(Made {
            columns: (0..500).map(|i| (format!("column_{i}"), if i % 3 == 0 { "Int64" } else { "Utf8" }.to_string())).collect(),
            names: (0..500).step_by(10).map(|i| (format!("column_{i}"), format!("renamed_{i}"))).collect(),
            dropped: (5..500).step_by(25).map(|i| format!("column_{i}")).collect(),
            shown: ["column_499", "column_1", "column_2"].map(String::from).to_vec(),
            not_null: (0..100).map(|i| format!("column_{i}")).collect(),
            defaults: (100..150).map(|i| (format!("column_{i}"), "'x'".to_string())).collect(),
            sql_types: (200..230).map(|i| (format!("column_{i}"), "uuid".to_string())).collect(),
            ..Default::default()
        });
        m.key = vec!["column_0".into()];
        m.files = (0..200).map(|i| crate::data_file::DataFile { path: format!("data/t/{i:08}.parquet"), rows: 1000, bytes: 1 << 20, ..Default::default() }).collect();
        m
    }

    /// Their 2,000 small entries: 1 to 8 columns, some renamed, some with a column dropped.
    fn small_entries() -> Vec<TableMeta> {
        (0..2000u64)
            .map(|i| {
                let n = 1 + (i % 8) as usize;
                let mut f = Made { columns: (0..n).map(|c| (format!("c{c}"), "Int64".to_string())).collect(), ..Default::default() };
                if i % 3 == 0 {
                    f.names.insert("c0".into(), "r0".into());
                }
                if i % 5 == 0 && n > 1 {
                    f.dropped.push(format!("c{}", n - 1));
                }
                TableMeta::made(f)
            })
            .collect()
    }

    /// Format 6's list is no bigger than format 5's fields: the wide entry at or under its 35,088 bytes, and the small
    /// ones at or under their 269 bytes on average (the redesign thread's gate for 6b).
    #[test]
    fn entry_size() {
        let (wide, small) = (wide_entry(), small_entries());
        let avg = |ts: &[TableMeta], listed: bool| ts.iter().map(|t| json(&if listed { t.clone().as_list() } else { t.clone() }).len()).sum::<usize>() / ts.len();
        let (w5, w6) = (json(&wide).len(), json(&wide.clone().as_list()).len());
        let (s5, s6) = (avg(&small, false), avg(&small, true));
        println!("wide: {w5} bytes in format 5's fields, {w6} as format 6's list; small: {s5} and {s6} on average");
        assert!(w5 == 35_088 && s5 == 269, "the measures' entries changed: wide {w5}, small {s5}");
        assert!(w6 <= w5 && s6 <= s5, "format 6's list is bigger: wide {w6}, small {s6}");
        for t in std::iter::once(&wide).chain(&small) {
            assert_eq!(serde_json::to_string(&t.record).unwrap(), written(&t.fields()), "format 5's fields, borrowed from the list");
        }
    }

    /// Format 5's fields as 0.80 wrote them, `columns` first and always, each other one only when it holds anything: what
    /// `Record` writes from the list, without making the fields.
    fn written(f: &Made) -> String {
        let mut parts = vec![format!("\"columns\":{}", serde_json::to_string(&f.columns).unwrap())];
        macro_rules! unless_empty {
            ($($field:ident),*) => { $(if !f.$field.is_empty() { parts.push(format!("\"{}\":{}", stringify!($field), serde_json::to_string(&f.$field).unwrap())); })* };
        }
        unless_empty!(names, dropped, shown, not_null, defaults, identity, enums, sql_types, logical_types);
        format!("{{{}}}", parts.join(","))
    }

    /// `described` makes from the list what it made from the fields by stored name before piece 7e: SQL's columns with
    /// the logical copy's facts and no order, or a finished view's answer columns, each with the facts of its name, and
    /// the copy's order of the names still there.
    #[test]
    fn a_description_is_made_from_the_list_as_from_the_fields() {
        let was = |t: &TableMeta, columns: Vec<(String, String)>, order: bool| {
            let mut f = t.logical().fields();
            f.columns = columns;
            if !order {
                f.shown = vec![];
            }
            Stored::from_fields(f)
        };
        for t in [wide_entry(), with_rules()].into_iter().chain(small_entries()) {
            if !t.list().order.is_empty() {
                assert_eq!(t.described().list(), &was(&t, t.sql_columns(), false), "{:?}", t.list());
            }
            let mut v = t.clone();
            let answers: Vec<(String, String)> = t.sql_columns().into_iter().rev().chain([("x".into(), "Utf8".into())]).collect();
            v.finish = Some(Finish { sql: String::new(), columns: answers.clone() });
            let d = v.described();
            assert_eq!(d.list(), &was(&v, answers, true), "{:?}", t.list());
            assert!(d.merge.is_empty());
        }
    }

    /// What writing an entry back costs in each shape (6b-3's before and after): each entry decoded as a lake keeps it and
    /// written as it was read, against decoding it alone. A measure, not a check: `cargo test --release --lib
    /// entry_write_time -- --ignored --nocapture`.
    #[test]
    #[ignore]
    fn entry_write_time() {
        let time = |raws: &[Vec<u8>], rounds: u32, write: bool| {
            let best = (0..5)
                .map(|_| {
                    let t = std::time::Instant::now();
                    for _ in 0..rounds {
                        for r in raws {
                            let m: TableMeta = serde_json::from_slice(r).unwrap();
                            std::hint::black_box(if write { serde_json::to_vec(&m).unwrap().len() } else { m.key.len() });
                        }
                    }
                    t.elapsed().as_nanos() as f64 / (rounds as f64 * raws.len() as f64)
                })
                .fold(f64::MAX, f64::min);
            best / 1000.0
        };
        let (wide, small) = (wide_entry(), small_entries());
        for (what, raws, rounds) in [
            ("wide, format 5's fields", vec![json(&wide)], 2000),
            ("small, format 5's fields", small.iter().map(json).collect::<Vec<_>>(), 20),
            ("wide, format 6's list", vec![json(&wide.clone().as_list())], 2000),
            ("small, format 6's list", small.iter().map(|m| json(&m.clone().as_list())).collect(), 20),
        ] {
            let (read, both) = (time(&raws, rounds, false), time(&raws, rounds, true));
            // (and written again: each entry decoded once, then written `rounds` times)
            let entries: Vec<TableMeta> = raws.iter().map(|r| serde_json::from_slice(r).unwrap()).collect();
            entries.iter().for_each(|m| _ = serde_json::to_vec(m).unwrap());
            let again = (0..5)
                .map(|_| {
                    let t = std::time::Instant::now();
                    for _ in 0..rounds {
                        entries.iter().for_each(|m| _ = std::hint::black_box(serde_json::to_vec(m).unwrap().len()));
                    }
                    t.elapsed().as_nanos() as f64 / (rounds as f64 * entries.len() as f64) / 1000.0
                })
                .fold(f64::MAX, f64::min);
            println!("{what}: {both:.1} µs an entry read and written, {read:.1} µs read alone, {again:.1} µs written again, best of 5");
        }
    }

    /// What decoding an entry and making its column list cost (redesign 1b, piece 6b's before and after): a wide table
    /// (500 columns, renames, drops, a move, per-column facts and 200 files) and 2,000 small random ones. A measure, not a
    /// check: `cargo test --release --lib entry_parse_time -- --ignored --nocapture`.
    #[test]
    #[ignore]
    fn entry_parse_time() {
        let (wide, small) = (json(&wide_entry()), small_entries().iter().map(json).collect::<Vec<_>>());
        let time = |what: &str, raws: &[&[u8]], rounds: u32| {
            let best = (0..5)
                .map(|_| {
                    let t = std::time::Instant::now();
                    for _ in 0..rounds {
                        for r in raws {
                            let m: TableMeta = serde_json::from_slice(r).unwrap();
                            std::hint::black_box(m.column_list().len());
                        }
                    }
                    t.elapsed().as_nanos() as f64 / (rounds as f64 * raws.len() as f64)
                })
                .fold(f64::MAX, f64::min);
            println!("{what}: {:.1} µs an entry, decoded and listed, best of 5 ({} bytes)", best / 1000.0, raws.iter().map(|r| r.len()).sum::<usize>() / raws.len());
        };
        // (format 6's shape: the same entries kept as the list)
        let listed = |r: &[u8]| json(&serde_json::from_slice::<TableMeta>(r).unwrap().as_list());
        let (wide6, small6): (Vec<u8>, Vec<Vec<u8>>) = (listed(&wide), small.iter().map(|r| listed(r)).collect());
        time("wide (500 columns, 200 files)", &[&wide], 2000);
        time("small (2,000 random)", &small.iter().map(Vec::as_slice).collect::<Vec<_>>(), 20);
        time("wide, format 6's list", &[&wide6], 2000);
        time("small, format 6's list", &small6.iter().map(Vec::as_slice).collect::<Vec<_>>(), 20);
    }

    /// What the entry's own column helpers cost (redesign 1b, piece 7a's before and after: from the fields by stored name
    /// to the list): each on an entry read as format 6's list, asked again ("warm") and first after the read ("first",
    /// the read's own cost taken off). The wide table and the 2,000 small ones of `entry_parse_time`. A measure, not a
    /// check: `cargo test --release --lib column_helpers_time -- --ignored --nocapture`.
    #[test]
    #[ignore]
    fn column_helpers_time() {
        let best = |rounds: u32, f: &mut dyn FnMut()| {
            (0..5)
                .map(|_| {
                    let t = std::time::Instant::now();
                    (0..rounds).for_each(|_| f());
                    t.elapsed().as_nanos() as f64 / rounds as f64 / 1000.0
                })
                .fold(f64::MAX, f64::min)
        };
        let helpers: [(&str, fn(&TableMeta) -> usize); 13] = [
            ("columns()", |m| m.columns().map(|(c, t)| c.len() + t.len()).sum()),
            ("described()", |m| m.described().columns().len()),
            ("schema()", |m| m.schema().unwrap().fields().len()),
            ("read_schema()", |m| m.read_schema().unwrap().fields().len()),
            ("table_sql()", |m| crate::objects::table_sql("t", m).len()),
            ("types::shown(), each column", |m| m.list().columns.iter().filter_map(|c| m.col(c.stored_name())).map(|c| crate::types::shown(c).len()).sum()),
            ("default_sql(), each column", |m| m.live().filter_map(|(_, n, _)| crate::rules::rows::default_sql(m, n)).count()),
            ("logical()", |m| m.logical().columns().len()),
            ("to_rules()", |m| m.to_rules().unwrap().columns.len()),
            ("sql_columns()", |m| m.sql_columns().len()),
            ("placed()", |m| m.placed() as usize),
            ("live()", |m| m.live().count()),
            ("name_of() each column", |m| m.list().columns.iter().map(|c| m.name_of(c.stored_name()).len()).sum()),
        ];
        let (wide, small) = (json(&wide_entry().as_list()), small_entries().iter().map(|m| json(&m.clone().as_list())).collect::<Vec<_>>());
        for (what, raws, rounds) in [("wide", vec![wide], 2000), ("small", small, 20)] {
            let read = || raws.iter().map(|r| serde_json::from_slice::<TableMeta>(r).unwrap()).collect::<Vec<_>>();
            let n = raws.len() as f64;
            let decode = best(rounds, &mut || { std::hint::black_box(read()); }) / n;
            for (helper, f) in helpers {
                let first = best(rounds, &mut || { std::hint::black_box(read().iter().map(f).sum::<usize>()); }) / n - decode;
                let entries = read();
                entries.iter().for_each(|m| _ = f(m));
                let warm = best(rounds, &mut || { std::hint::black_box(entries.iter().map(f).sum::<usize>()); }) / n;
                println!("{what}, {helper}: {warm:.2} µs warm, {first:.2} µs first, an entry, best of 5");
            }
            let m = read().remove(0);
            let b = RecordBatch::new_empty(m.schema().unwrap());
            let warm = best(rounds, &mut || { std::hint::black_box(m.to_logical(&b).unwrap().num_columns()); });
            println!("{what}, to_logical() of a batch of every stored column: {warm:.2} µs warm, the first entry");
        }
    }

    /// A column's *about* (`About`) is kept on its record: written with format 6's facts and read back, through a rename
    /// and a drop, in a `logical()` copy and in a description. Format 5's fields leave it out: they have no place for it.
    #[test]
    fn a_columns_about_is_kept_on_its_record() {
        let about: About = [("label".to_string(), serde_json::json!("Net revenue")), ("synonyms".to_string(), serde_json::json!(["sales", "turnover"]))].into();
        let mut t = TableMeta::with_columns(vec![("a".into(), "Int64".into()), ("b".into(), "Float64".into()), ("c".into(), "Utf8".into())]).as_list();
        t.edit(|l| {
            for s in ["b", "c"] {
                l.at(s).unwrap().about = Some(Box::new(about.clone()));
            }
        });
        t.rename_column("b", "revenue");
        t.drop_column("c");
        let back: TableMeta = serde_json::from_slice(&json(&t)).unwrap();
        assert_eq!(back.list(), t.list(), "format 6's list, read back");
        assert_eq!([("b", true), ("c", true), ("a", false)].map(|(s, has)| back.col(s).unwrap().about.as_deref() == has.then_some(&about)), [true; 3], "through a rename and a drop");
        assert_eq!(t.logical().col("revenue").and_then(|c| c.about.as_deref()), Some(&about), "SQL's copy");
        let mut v = t.clone();
        v.finish = Some(Finish { sql: String::new(), columns: vec![("revenue".into(), "Float64".into())] });
        assert_eq!(v.described().col("revenue").and_then(|c| c.about.as_deref()), Some(&about), "a description");
        let mut five = t.clone();
        five.record.listed = false;
        assert!(!String::from_utf8(json(&five)).unwrap().contains("synonyms"), "format 5's fields leave it out");
    }

    /// A list's `order` names columns by id, and an id is its place + 1 only as lists are written so far (the format writes
    /// one where it isn't): SQL's order, `logical()`, format 5's fields as written, the column list and a new column's id all
    /// take an id as an id (`Stored::place`).
    #[test]
    fn an_id_that_isnt_a_place_is_read_as_an_id() {
        let col = |id: u32, name: &str| Col { id, name: name.into(), dtype: "Int64".into(), ..Default::default() };
        let list = Stored { columns: vec![col(3, "a"), col(1, "b"), col(2, "c"), Col { dropped: true, ..col(4, "d") }], order: vec![2, 3] };
        let m = TableMeta { record: Record::new(list, true), ..Default::default() };
        assert_eq!(m.sql_order(), ["c", "a", "b"]);
        assert!(m.moved());
        assert_eq!(m.logical().sql_order(), ["c", "a", "b"]);
        assert_eq!(m.list().fields().shown, ["c", "a"]);
        let mut five = m.clone();
        five.record.listed = false;
        assert!(serde_json::to_string(&five.record).unwrap().contains(r#""shown":["c","a"]"#));
        let cols = m.column_list();
        assert_eq!(cols.sql().map(|c| c.name.as_str()).collect::<Vec<_>>(), ["c", "a", "b"]);
        assert_eq!([3, 1, 4].map(|id| cols.by_id(id).map(|c| c.name.as_str())), [Some("a"), Some("b"), Some("d")]);
        let mut m = m;
        m.add_column("e", "Utf8", None, None);
        m.set_order(vec!["e".into(), "a".into()]);
        assert_eq!((m.col("e").map(|c| c.id), m.list().order.clone(), m.sql_order()), (Some(5), vec![5, 3], vec!["e".to_string(), "a".into(), "b".into(), "c".into()]));
    }

    /// A column SQL reads as another type than its files hold (a mapped Delta table's struct: `Col::sql_type`) keeps it in
    /// either shape, written only where it has one; SQL's columns and the description give SQL's type, the files' stays.
    #[test]
    fn a_columns_sql_type_is_kept_in_either_shape() {
        let (files, sql) = (r#"Struct("col-9": Utf8)"#, r#"Struct("city": Utf8)"#);
        let made = Made {
            columns: vec![("col-1".into(), "Int64".into()), ("col-2".into(), files.into())],
            names: [("col-1", "id"), ("col-2", "addr")].iter().map(|(s, n)| (s.to_string(), n.to_string())).collect(),
            logical_types: [("col-2".to_string(), sql.to_string())].into(),
            ..Default::default()
        };
        let t = TableMeta::made(made.clone());
        assert_eq!((t.col("col-2").map(|c| (c.dtype.as_str(), c.sql_type())), t.relabeled()), (Some((files, sql)), true));
        let (five, six) = (serde_json::to_value(&t).unwrap(), serde_json::to_value(t.clone().as_list()).unwrap());
        assert_eq!(five["logical_types"], serde_json::json!({"col-2": sql}), "{five}");
        assert_eq!(six["list"][0][1][2]["sql_type"], serde_json::json!(sql), "{six}");
        for raw in [five, six] {
            let back: TableMeta = serde_json::from_value(raw.clone()).unwrap();
            assert_eq!(back.list(), t.list(), "{raw}");
        }
        assert_eq!(t.fields(), made, "the fields by stored name again");
        let want = vec![("id".to_string(), "Int64".to_string()), ("addr".into(), sql.into())];
        assert_eq!((t.sql_columns(), t.star()), (want.clone(), want.clone()));
        let described: Vec<(String, String)> = t.described().columns().map(|(c, ty)| (c.to_string(), ty.to_string())).collect();
        assert_eq!((described, t.described().relabeled()), (want, false));
        assert_eq!(t.columns().map(|(_, ty)| ty).collect::<Vec<_>>(), ["Int64", files], "the files' types, as a scan reads them");
        let plain = TableMeta::made(Made { columns: vec![("a".into(), "Int64".into())], ..Default::default() });
        let (five, six) = (serde_json::to_string(&plain).unwrap(), serde_json::to_string(&plain.clone().as_list()).unwrap());
        assert!(!five.contains("logical_types") && !six.contains("sql_type") && !plain.relabeled(), "{five} {six}");
    }

    /// A table's replaced rows (`sys::deleted`) take its columns as stored, ids, names and drop marks kept, and none of
    /// its facts or its order: no write is checked there, and nothing reads it by SQL's order.
    #[test]
    fn the_replaced_rows_take_the_columns_and_nothing_else() {
        let col = |id: u32, name: &str| Col { id, name: name.into(), dtype: "Int64".into(), ..Default::default() };
        let facts = Col { not_null: true, default: Some("1".into()), held: Some("uuid".into()), stored: Some("b0".into()), ..col(1, "b") };
        let of = Stored { columns: vec![col(3, "a"), facts, Col { dropped: true, ..col(2, "c") }], order: vec![1, 3] };
        let r = Stored::replaced(&of);
        let want = [(3, "a", "a", false), (1, "b", "b0", false), (2, "c", "c", true), (4, "_old_version", "_old_version", false)];
        assert_eq!(r.columns.iter().map(|c| (c.id, c.name.as_str(), c.stored_name(), c.dropped)).collect::<Vec<_>>(), want);
        assert!(r.order.is_empty() && r.columns.iter().all(|c| !c.not_null && c.default.is_none() && c.held.is_none()));
        let mut d = TableMeta::default();
        d.follow_columns(&TableMeta::with_list(of));
        assert_eq!(d.list(), &r);
    }

    /// An entry is written in the shape it was read in (`Record`): format 6's as the list alone, format 5's as its fields,
    /// `columns` always (a 0.80 node needs the field), each read back as it was; an edit keeps the shape, and a new entry
    /// and a `logical()` copy are written as format 5's fields.
    #[test]
    fn an_entry_is_written_in_the_shape_it_was_read_in() {
        let t = with_rules();
        let (old, new) = (serde_json::to_value(&t).unwrap(), serde_json::to_value(t.clone().as_list()).unwrap());
        assert!(old.get("columns").is_some() && old.get("names").is_some() && old.get("list").is_none(), "{old}");
        assert!(["columns", "names", "dropped", "not_null", "defaults", "identity", "enums", "sql_types"].iter().all(|k| new.get(k).is_none()), "{new}");
        let back: TableMeta = serde_json::from_value(new.clone()).unwrap();
        assert!(back.record.listed && serde_json::to_value(&back).unwrap() == new);
        assert_eq!((back.to_columns(), &back.fields().names, &back.fields().not_null, &back.fields().identity), (t.to_columns(), &t.fields().names, &t.fields().not_null, &t.fields().identity));
        let again: TableMeta = serde_json::from_value(old.clone()).unwrap();
        assert!(!again.record.listed && serde_json::to_value(&again).unwrap() == old, "format 5's, field for field");
        let mut edited = back.clone();
        edited.add_column("more", "Utf8", None, None);
        assert!(json(&edited).starts_with(br#"{"list":"#), "an edit keeps the shape");
        assert!(json(&back.logical()).starts_with(br#"{"columns":"#), "a logical copy: SQL's names, so never a list");
        assert!(json(&TableMeta::default()).starts_with(br#"{"columns":[]"#), "a new entry, with no columns: still written");
    }

    /// The column edits on the list (piece 6b-3) leave the entry as piece 6a's edits on the fields by stored name did,
    /// over random sequences of ADD, RENAME, DROP, a type, a held type, a default, NOT NULL, a move, an enum's new label
    /// and system columns appended: the list those fields make, the same answers, and the same JSON in either shape.
    #[test]
    fn the_list_edits_leave_the_entry_as_the_field_edits_did() {
        let mut seed = 0x2545_f491_4f6c_dd1du64;
        let mut next = |n: u64| {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed % n.max(1)
        };
        let mood = || Enum { of: Some("mood".into()), labels: vec!["sad".into(), "ok".into()] };
        for round in 0..500 {
            // (`a` edits its list; `r` is the reference, the fields edited as 6a did; `c` is an entry read as its list)
            let (mut a, mut r, mut c) = (TableMeta::default(), Made::default(), TableMeta::default().as_list());
            for step in 0..12 {
                let live: Vec<String> = a.live().map(|(s, _, _)| s.to_string()).collect();
                let pick = |i: u64| live.get(i as usize).cloned();
                match next(10) {
                    0 => {
                        let name = format!("c{}", next(4));
                        if a.live().any(|(_, n, _)| n == name) {
                            continue;
                        }
                        let (held, labels) = match next(4) {
                            0 => (Some("uuid".to_string()), None),
                            1 => (None, Some(mood())),
                            _ => (None, None),
                        };
                        let s = a.add_column(&name, "Utf8", held.clone(), labels.clone());
                        assert_eq!(c.add_column(&name, "Utf8", held.clone(), labels.clone()), s);
                        assert_eq!(r.add_column(&name, "Utf8", held, labels), s, "the same stored name");
                    }
                    1 => {
                        let (Some(s), to) = (pick(next(live.len() as u64)), format!("r{}", next(4))) else { continue };
                        if a.live().any(|(_, n, _)| n == to) {
                            continue;
                        }
                        a.rename_column(&s, &to);
                        c.rename_column(&s, &to);
                        r.rename_column(&s, &to);
                    }
                    2 => {
                        let Some(s) = pick(next(live.len() as u64)).filter(|_| live.len() > 1) else { continue };
                        a.drop_column(&s);
                        c.drop_column(&s);
                        r.drop_column(&s);
                    }
                    3 => {
                        let Some(s) = pick(next(live.len() as u64)) else { continue };
                        a.retype_column(&s, "LargeUtf8".into());
                        c.retype_column(&s, "LargeUtf8".into());
                        r.retype_column(&s, "LargeUtf8".into());
                    }
                    4 => {
                        let Some(s) = pick(next(live.len() as u64)) else { continue };
                        let held = (next(2) == 0).then(|| "varchar(9)".to_string());
                        a.hold_column(&s, held.clone());
                        c.hold_column(&s, held.clone());
                        r.hold_column(&s, held);
                    }
                    5 => {
                        let Some(s) = pick(next(live.len() as u64)) else { continue };
                        let expr = (next(2) == 0).then(|| "'x'".to_string());
                        a.set_default(&s, expr.clone());
                        c.set_default(&s, expr.clone());
                        r.set_default(&s, expr);
                    }
                    6 => {
                        // (SET NOT NULL only on a column that isn't: defaults.rs refuses the rest by the rows it finds)
                        let Some(s) = pick(next(live.len() as u64)) else { continue };
                        let on = !r.not_null.contains(&s);
                        a.set_not_null(&s, on);
                        c.set_not_null(&s, on);
                        r.set_not_null(&s, on);
                    }
                    7 => {
                        // (a move: every live column in a random order, as ddl.rs's `place_column` gives it)
                        let mut order = live.clone();
                        for i in (1..order.len()).rev() {
                            order.swap(i, next(i as u64 + 1) as usize);
                        }
                        a.set_order(order.clone());
                        c.set_order(order.clone());
                        r.set_order(order);
                    }
                    8 => {
                        // (ALTER TYPE mood ADD VALUE: every column of the type takes it)
                        let label = format!("l{step}");
                        a.edit_enums("mood", |e| e.labels.push(label.clone()));
                        c.edit_enums("mood", |e| e.labels.push(label.clone()));
                        r.enums.values_mut().filter(|e| e.of.as_deref() == Some("mood")).for_each(|e| e.labels.push(label.clone()));
                    }
                    _ => {
                        // (a system table's columns, kept as they are named: none is stored under them yet)
                        let more = vec![(format!("_sys{round}_{step}"), "Int64".to_string())];
                        a.append_columns(more.clone());
                        c.append_columns(more.clone());
                        r.columns.extend(more);
                    }
                }
                let want = TableMeta::made(r.clone());
                assert_eq!(a.list(), &Stored::of(&r), "the list of the fields 6a's edits leave: {:?}", r);
                assert_eq!(a.fields(), Stored::of(&r).fields(), "and its fields are theirs, each set in id order");
                assert_eq!(serde_json::to_string(&a.record).unwrap(), written(&a.fields()), "written as those fields, borrowed from the list");
                assert_eq!((a.sql_columns(), a.star(), a.sql_names(), a.column_list()), (want.sql_columns(), want.star(), want.sql_names(), want.column_list()));
                assert_eq!(a.to_rules().unwrap(), want.to_rules().unwrap(), "the rules");
                assert_eq!(a.logical().list(), want.logical().list(), "the logical copy");
                assert_eq!(json(&a.clone().as_list()), json(&want.clone().as_list()), "the same list, as written");
                assert!(!a.record.listed && c.record.listed, "an edit keeps the shape");
                assert_eq!(json(&c), json(&a.clone().as_list()), "an entry read as its list, edited the same: the same list");
                assert_eq!(Stored::of(&a.list().fields()), *a.list(), "list -> fields -> list is the identity");
            }
            let mut d = TableMeta::default();
            d.follow_columns(&a);
            assert_eq!(d.columns().last().map(|(c, _)| c), Some("_old_version"));
            assert_eq!((&d.fields().names, &d.fields().dropped, d.columns().len()), (&a.fields().names, &a.fields().dropped, a.columns().len() + 1));
        }
    }

    /// The entries of every release's lake since 0.22, as each release wrote them (`keep_old_entries`): a line each,
    /// `{"release", "key", "entry"}`.
    const OLD_ENTRIES: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/old-entries.jsonl");

    /// Keeps every table entry (`t/`) and dropped table's record (`dt/`) of each release's lake since 0.22 as it wrote
    /// them, for `old_entries_read_as_before`: `python3 tools/upgrade_check.py lakes --work DIR`, then
    /// `PONDRA_OLD_LAKES=DIR cargo test --release --lib keep_old_entries -- --ignored` (its `lake-{v}-as-released`).
    #[tokio::test]
    #[ignore]
    async fn keep_old_entries() -> Result<()> {
        let dir = std::env::var("PONDRA_OLD_LAKES").context("PONDRA_OLD_LAKES: upgrade_check.py's --work")?;
        let version = |v: &str| v.split('.').map(|p| p.parse::<u32>().unwrap_or(0)).collect::<Vec<_>>();
        let mut lakes: Vec<(String, std::path::PathBuf)> = std::fs::read_dir(&dir)?
            .filter_map(|e| {
                let p = e.ok()?.path();
                let v = p.file_name()?.to_str()?.strip_prefix("lake-")?.strip_suffix("-as-released")?.to_string();
                Some((v, p))
            })
            .collect();
        lakes.sort_by_key(|(v, _)| version(v));
        let mut out = String::new();
        for (v, at) in lakes {
            // (a copy: opening a lake may write to it)
            let copy = std::env::temp_dir().join(format!("pondra-old-{v}-{}", uuid::Uuid::new_v4()));
            anyhow::ensure!(std::process::Command::new("cp").arg("-r").arg(&at).arg(&copy).status()?.success(), "copying {at:?}");
            let lake = Lake::open(copy.to_str().expect("a path"), true, false).await?;
            for (from, to) in [("t/", "t0"), ("dt/", "dt0")] {
                for (k, raw) in lake.cat.scan_raw(from, to).await? {
                    out += &format!("{{\"release\":\"{v}\",\"key\":{},\"entry\":{}}}\n", serde_json::to_string(&k)?, std::str::from_utf8(&raw)?);
                }
            }
            lake.cat.close().await?;
            let _ = std::fs::remove_dir_all(&copy);
        }
        std::fs::write(OLD_ENTRIES, out)?;
        Ok(())
    }

    /// The entries every release since 0.22 wrote (`keep_old_entries`) read as they did (redesign 1b, piece 6b-3). Each
    /// decodes to the list its fields make, read on their own as 6b-2 and 0.80 read them; written again below format 6 its
    /// column fields are the release's; old → list → old is the identity, each set of columns (NOT NULL, dropped) in id
    /// order; and the list answers as the fields did. A dropped table's record holds its entry: the same of that.
    #[test]
    fn old_entries_read_as_before() {
        let text = std::fs::read_to_string(OLD_ENTRIES).expect("the old entries");
        let (mut releases, mut entries) = (std::collections::BTreeSet::new(), 0);
        for line in text.lines() {
            let l: serde_json::Value = serde_json::from_str(line).unwrap();
            let (v, k) = (l["release"].as_str().unwrap(), l["key"].as_str().unwrap());
            releases.insert(v.to_string());
            let at = format!("{v} {k}");
            let metas = match k.starts_with("dt/") {
                true => {
                    let _: crate::ddl::Dropped = serde_json::from_value(l["entry"].clone()).expect(&at);
                    [Some(&l["entry"]["meta"]), l["entry"].get("deleted").filter(|d| !d.is_null())].into_iter().flatten().collect()
                }
                false => vec![&l["entry"]],
            };
            for raw in metas {
                entries += 1;
                old_entry_reads_as_before(&at, raw);
            }
        }
        println!("{entries} entries of {} releases: {releases:?}", releases.len());
        assert!(releases.contains("0.22.0") && releases.len() >= 10 && entries >= 100, "{releases:?}, {entries} entries");
    }

    fn old_entry_reads_as_before(at: &str, raw: &serde_json::Value) {
        const FIELDS: [&str; 9] = ["columns", "names", "dropped", "shown", "not_null", "defaults", "identity", "enums", "sql_types"];
        let t: TableMeta = serde_json::from_value(raw.clone()).expect(at);
        assert!(!t.record.listed, "{at}: format 5's shape");
        // (its column fields, read on their own: each missing one empty)
        fn field<T: serde::de::DeserializeOwned + Default>(raw: &serde_json::Value, k: &str) -> T {
            raw.get(k).filter(|v| !v.is_null()).map(|v| serde_json::from_value(v.clone()).unwrap()).unwrap_or_default()
        }
        let f = Made {
            columns: field(raw, "columns"),
            names: field(raw, "names"),
            dropped: field(raw, "dropped"),
            shown: field(raw, "shown"),
            not_null: field(raw, "not_null"),
            defaults: field(raw, "defaults"),
            identity: field(raw, "identity"),
            enums: field(raw, "enums"),
            sql_types: field(raw, "sql_types"),
            logical_types: field(raw, "logical_types"),
        };
        assert_eq!(t.list(), &Stored::of(&f), "{at}: the list its fields make");
        let empty = |v: Option<&serde_json::Value>| v.is_none_or(|v| v.is_null() || v.as_array().is_some_and(Vec::is_empty) || v.as_object().is_some_and(|o| o.is_empty()));
        let again = serde_json::to_value(&t).unwrap();
        for k in FIELDS {
            assert!(again.get(k) == raw.get(k) || empty(again.get(k)) && empty(raw.get(k)) && k != "columns", "{at}: {k} written again as the release wrote it");
        }
        // (old -> list -> old: the fields with each set of columns in id order, and no fact of a column the entry hasn't)
        let place = |s: &String| f.columns.iter().position(|(c, _)| c == s);
        let mut sorted = f.clone();
        sorted.not_null.sort_by_key(place);
        sorted.dropped.sort_by_key(place);
        let listed: TableMeta = serde_json::from_slice(&json(&t.clone().as_list())).unwrap();
        let mut back = listed.clone();
        back.record.listed = false;
        assert_eq!(back.fields(), sorted, "{at}: old -> list -> old is the identity");
        assert_eq!(
            (listed.sql_columns(), listed.star(), listed.column_list(), listed.logical().list(), listed.to_rules().ok()),
            (t.sql_columns(), t.star(), t.column_list(), t.logical().list(), t.to_rules().ok()),
            "{at}: every answer the same from the list"
        );
    }

    /// `past_ttl` keeps out what `ttl_sql` does: a timestamp older than the TTL's seconds, a NULL, a row without the column.
    /// A row exactly the TTL old is kept (`>=`), a date counts from its midnight, and a table with no TTL keeps every row.
    #[test]
    fn a_row_past_its_ttl_is_the_one_ttl_sql_keeps_out() -> Result<()> {
        use datafusion::arrow::array::{ArrayRef, Date32Array, Int64Array, TimestampMicrosecondArray};
        let hour = 3_600_000_000i64; // µs
        let now = 1_800_000_000_000_000i64; // µs since the epoch: 08:00 UTC
        let ttl = |secs: u64| TableMeta { ttl: Some(("seen".into(), secs)), ..Default::default() };
        let one = |a: ArrayRef| RecordBatch::try_from_iter([("seen", a)]);
        let at = |v: Option<i64>| one(Arc::new(TimestampMicrosecondArray::from(vec![v])) as ArrayRef);
        let old = at(Some(now - 2 * hour))?;
        assert!(ttl(3600).past_ttl(&old, now)?, "two hours old, a one hour TTL");
        assert!(!ttl(3600).past_ttl(&at(Some(now - hour / 2))?, now)?, "half an hour old");
        assert!(!ttl(3600).past_ttl(&at(Some(now - hour))?, now)?, "exactly the TTL old is kept");
        assert!(ttl(3600).past_ttl(&at(None)?, now)?, "a NULL is gone: SQL keeps no NULL");
        let no_column = RecordBatch::try_from_iter([("k", Arc::new(Int64Array::from(vec![1i64])) as ArrayRef)])?;
        assert!(ttl(3600).past_ttl(&no_column, now)?, "a row without the column reads it as NULL");
        assert!(!TableMeta::default().past_ttl(&old, now)?, "no TTL, no row is past it");
        let date = one(Arc::new(Date32Array::from(vec![Some(20833)])) as ArrayRef)?; // 08:00 UTC is eight hours past its midnight
        assert!(ttl(3600).past_ttl(&date, now)?, "a date is its midnight: eight hours old");
        assert!(!ttl(864_000).past_ttl(&date, now)?, "ten days' TTL keeps it");
        Ok(())
    }
}
