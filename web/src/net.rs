//! How the page's calls out reach the network, as the core asks a node's (`pondra::net::Net`, redesign §8, step 2): every
//! one is the browser's `fetch`, whoever it goes to. The browser resolves names and follows redirects under its own rules
//! (CORS, Private Network Access) and keeps its own time limits, so a policy changes nothing here. A member joined to a
//! node signs in with the Bearer its open sets on each request (`open_joined`), never the nodes' token, which a page never
//! holds. A web origin's store is the page's own (`fetch::Fetch`), which reads a part from the range it asked when the
//! server hides `Content-Range`. The page's platform installs it once a module, before any page opens (`install`).
use std::sync::{Arc, OnceLock};

/// The page's calls out: one fetch client for every policy.
#[derive(Default)]
struct Browser {
    client: OnceLock<reqwest::Client>,
}

impl pondra::net::Net for Browser {
    fn client(&self, _: pondra::net::Policy) -> reqwest::Client {
        self.client.get_or_init(reqwest::Client::new).clone()
    }

    fn web(&self, _: pondra::net::Policy, origin: &str) -> Option<Arc<dyn object_store::ObjectStore>> {
        Some(Arc::new(crate::fetch::Fetch::new(origin)))
    }
}

/// Installs the page's `Net` as the process's (`net::install`): the first install stays, so a later one changes nothing.
pub fn install() {
    pondra::net::install(Arc::new(Browser::default()));
}
