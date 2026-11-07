//! What a node's host does when the node must stop or start again, and the facts about the form it runs in that the code
//! below the hosts asks (redesign §8-9, step 2). Whoever opens a lake hands it one (`Lake::open`), and the cluster's
//! member is given the same (`Cluster::join`): storage and the engine ask it, never the host's module, and never which
//! form it is. The host implements it: the program execs itself again, a library fails its node and lives on, a page
//! stops (`node::Native`); a test's lake panics instead (`Unhosted`).
//!
//! A task a library spawns for us gets a clone of the lake's when it is spawned, and DataFusion's own tasks reach it through
//! the lake they were given: nothing below the hosts reads a static for it.
use std::path::PathBuf;
use std::sync::Arc;

/// A node's lifecycle, shared by its lakes and its member.
pub type Life = Arc<dyn Lifecycle>;

pub trait Lifecycle: Send + Sync {
    /// The node stops for good, with this reason, when its own work can't go on (a vital loop's panic, the catalog's
    /// compactor, a lake a newer Pondra wrote). Where the node has its process to itself the process ends; where it
    /// doesn't (a library's host) the node fails, says why to every request until it is opened again, and this returns.
    fn stop(&self, why: String);

    /// The node starts again, to rejoin its cluster, with `carried` settings for the next run (`pondra sql`'s job and its
    /// tries) on top of what it was given. It never returns: the process runs again, or the task that asked unwinds with
    /// [`Restarting`] while the host starts the node again in place.
    fn restart(&self, why: &str, carried: &[(&str, String)]) -> !;

    /// A line said when all goes well (a node is up, a lake's format moved on), where someone reads it.
    fn say(&self, line: &str);

    /// Does the node end with its statement (a guest: `pondra sql`, a CI step)? It runs no loops of its own, so it tidies
    /// its catalog as it goes and waits for what it started.
    fn one_off(&self) -> bool;

    /// Is the process the node's own? Not a library's host's: there the memory and an abort are the host's.
    fn owns_process(&self) -> bool;

    /// The program a node starts helpers with (`pondra lead`, the Python workers found beside it).
    fn program(&self) -> anyhow::Result<PathBuf>;

    /// Leads the lake at `dir` for a moment, making it if nothing is there yet, in a process of its own: for a lake nobody
    /// leads, and for a writer whose leader is gone (`inbox::send`).
    fn lead_elsewhere<'a>(&'a self, dir: &'a str) -> futures::future::BoxFuture<'a, anyhow::Result<()>>;
}

/// What a node restarting in place unwinds with ([`Lifecycle::restart`]): `panics::spawn` lets it go, and `panics::door`
/// answers the request 57P03, as a node catching up does.
#[derive(Debug)]
pub struct Restarting(pub String);

impl std::fmt::Display for Restarting {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "the node is restarting to rejoin the cluster ({}), catching up with its leader: try again, or ask another node", self.0)
    }
}

/// What a guest is for. A guest that reads claims nothing; one that writes may record, so it leads a lake nobody leads.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Guest {
    /// It only reads: it follows a leader that answers, or reads beside one that doesn't, or nobody's, and claims nothing.
    Reads,
    /// It may record writes, so it leads a lake nobody leads (`Cluster::guest`).
    Writes,
}

/// The lifecycle of a lake a test opens, with no node around it: a stop or a restart panics, so the test sees it, and
/// the process is the test's.
pub struct Unhosted;

impl Lifecycle for Unhosted {
    fn stop(&self, why: String) {
        panic!("stopping: {why}")
    }
    fn restart(&self, why: &str, _carried: &[(&str, String)]) -> ! {
        std::panic::resume_unwind(Box::new(Restarting(why.to_string())))
    }
    fn say(&self, line: &str) {
        eprintln!("{line}")
    }
    fn one_off(&self) -> bool {
        false
    }
    fn owns_process(&self) -> bool {
        true
    }
    fn program(&self) -> anyhow::Result<PathBuf> {
        anyhow::bail!("a lake opened with no node around it starts no helpers")
    }
    fn lead_elsewhere<'a>(&'a self, dir: &'a str) -> futures::future::BoxFuture<'a, anyhow::Result<()>> {
        Box::pin(async move { anyhow::bail!("a lake opened with no node around it leads no other lake ({dir})") })
    }
}

/// [`Unhosted`], as a lake takes it.
pub fn unhosted() -> Life {
    Arc::new(Unhosted)
}
