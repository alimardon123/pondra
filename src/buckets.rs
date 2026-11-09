//! What opens a lake's bucket (redesign step 5, piece 8g): the platform's object stores, which only the hosts name (the
//! floors: a platform's implementation of a base trait is reached by hosts alone). A lake's store (`store::open_store`)
//! opens a folder and a lake in memory itself, and asks this section for the rest: a cloud bucket, a node's door
//! (`pondra://`), a branch's base read with a share of its bucket's turns, and how DataFusion reads a lake in memory.
//! The hosts fill it (`hosted`); a form that fills a part in no way refuses it by name.
use anyhow::Result;
use std::collections::BTreeMap;
use std::sync::Arc;

/// An object store, as the catalog keeps one (`store::Store`).
pub type Store = Arc<dyn object_store::ObjectStore>;

/// A bucket a lake is in: `s3://bucket/prefix` is scheme `s3`, bucket `bucket`, root `s3://bucket`.
pub struct At<'a> {
    pub scheme: &'a str,
    pub bucket: &'a str,
    pub root: &'a str,
}

/// The platform's stores, by what a lake's URL says.
#[derive(Default)]
pub struct Buckets {
    /// A cloud bucket (s3, gs, az, abfss), with the environment's credentials or a secret's settings (`params`): the
    /// whole bucket, which the lake's store narrows to its prefix.
    pub open: Option<fn(&At, Option<&BTreeMap<String, String>>) -> Result<Store>>,
    /// The store a node serves through its door (`pondra://host:port`).
    pub served: Option<fn(&str) -> Store>,
    /// A branch's base in a bucket (ADR-047): its reads take a share of the bucket's turns, the base's own nodes first.
    pub share: Option<fn(&str)>,
    /// How DataFusion reads a store of ours (a lake in memory): its object store's version is its own.
    pub reader: Option<fn(Store) -> Arc<dyn object_store_df::ObjectStore>>,
}

impl crate::registry::Section for Buckets {
    const NAME: &'static str = "buckets";
}

/// The section, as the hosts filled it (`hosted`, in every form).
pub fn get() -> &'static Buckets { crate::registry::get().section::<Buckets>() }
