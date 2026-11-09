//! How a node reaches lakes on other servers (ADR-058; redesign step 5, piece 7d): for each place a statement named
//! (`ext::reach`: an attachment, a branch's base, a backup's bucket), the bucket key it is read with, whether nothing here
//! may write it, and where its leader is asked. The node keeps them (`NodeState::reaches`), each store reads its own as it
//! opens (`store::open_store`), and a process the node starts to lead a place for a moment is handed them (`handed`,
//! `PONDRA_REACH`), so it reaches the place as the node does. Each node has its own: two nodes in one process (a test's,
//! a library's after a restart) never see each other's keys. (`reach.rs` is another thing: where another engine's files
//! are read from.)
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::{Arc, PoisonError, RwLock};

/// How a node reaches one lake on another server: the settings of the bucket key it is read with (a secret's,
/// `ext::build`; None: the environment's), whether nothing here may write it, and where its leader is asked (a URL, with a
/// TYPE pondra secret's token).
#[derive(Clone, Default, Debug, Serialize, Deserialize)]
pub struct Reach {
    pub read_only: bool,
    pub params: Option<BTreeMap<String, String>>,
    pub endpoint: Option<String>,
    pub token: Option<String>,
    pub secret: Option<String>, // (the bucket secret's name: who may use it reads the lake here, `users::across`)
}

/// The setting a process started to lead a place for a moment is handed its starter's reaches in (`Reaches::handed`).
pub const REACH_VAR: &str = "PONDRA_REACH";

/// The places a node reaches on other servers, by URL. A clone is the same places (a task the node starts takes one).
#[derive(Clone, Default, Debug)]
pub struct Reaches(Arc<RwLock<BTreeMap<String, Reach>>>);

impl Reaches {
    /// A node's reaches as it starts: the ones its starter handed it (`REACH_VAR`, in `handed`'s form), or none.
    pub fn inherited(handed: Option<&str>) -> Reaches {
        let reaches = Reaches::default();
        if let Some(text) = handed {
            match serde_json::from_str::<BTreeMap<String, Reach>>(text) {
                Ok(all) => all.into_iter().for_each(|(url, r)| reaches.by(&url, r)),
                Err(e) => eprintln!("{REACH_VAR}: {e}"),
            }
        }
        reaches
    }

    /// Says how `url` (a lake) is reached. Asked again (an attachment and a branch of it on one node), what the new one
    /// leaves out is kept, and read-only stays read-only.
    pub fn by(&self, url: &str, r: Reach) {
        let mut all = self.0.write().unwrap_or_else(PoisonError::into_inner);
        let old = all.remove(url.trim_end_matches('/')).unwrap_or_default();
        let read_only = old.read_only || r.read_only;
        all.insert(url.trim_end_matches('/').to_string(), Reach { read_only, params: r.params.or(old.params), endpoint: r.endpoint.or(old.endpoint), token: r.token.or(old.token), secret: r.secret.or(old.secret) });
    }

    /// How `url`, or a lake it is in, is reached: the longest place said, `url` itself or a path under it.
    pub fn of(&self, url: &str) -> Option<Reach> {
        let url = url.trim_end_matches('/');
        let all = self.0.read().unwrap_or_else(PoisonError::into_inner);
        all.iter().filter(|(k, _)| url.strip_prefix(k.as_str()).is_some_and(|rest| rest.is_empty() || rest.starts_with('/')))
            .max_by_key(|(k, _)| k.len()).map(|(_, r)| r.clone())
    }

    /// Every place, for a process the node starts to lead one of them for a moment (`node::lead_once`): it holds the
    /// same keys, so a backup in another bucket is written with the secret that reaches it (ADR-070).
    pub fn handed(&self) -> String { serde_json::to_string(&*self.0.read().unwrap_or_else(PoisonError::into_inner)).unwrap_or_default() }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A node's reaches are its own: what one says, another doesn't see; a place under a said one is reached as it is;
    /// said again, read-only stays and what is left out is kept; and a started process gets them all back.
    #[test]
    fn each_node_reaches_what_it_was_told_of() {
        let (one, other) = (Reaches::default(), Reaches::default());
        one.by("s3://b/lake/", Reach { read_only: true, endpoint: Some("https://there".into()), ..Default::default() });
        assert!(one.of("s3://b/lake").is_some_and(|r| r.read_only));
        assert!(one.of("s3://b/lake/data/x.parquet").is_some(), "a path under the place");
        assert!(one.of("s3://b/lakes").is_none(), "a place beside it");
        assert!(other.of("s3://b/lake").is_none(), "another node's");
        one.by("s3://b/lake", Reach { secret: Some("k".into()), ..Default::default() });
        let r = one.of("s3://b/lake").unwrap();
        assert!(r.read_only && r.endpoint.as_deref() == Some("https://there") && r.secret.as_deref() == Some("k"));
        let started = Reaches::inherited(Some(&one.handed()));
        assert_eq!(format!("{:?}", started.of("s3://b/lake")), format!("{:?}", one.of("s3://b/lake")));
        assert!(Reaches::inherited(None).of("s3://b/lake").is_none());
        let (a, b) = (crate::node_state::NodeState::unhosted(), crate::node_state::NodeState::unhosted());
        a.reaches.by("s3://b/x", Reach::default());
        assert!(a.reaches.of("s3://b/x").is_some() && b.reaches.of("s3://b/x").is_none(), "two nodes in one process each have their own");
    }
}
