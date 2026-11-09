//! What the machine a node runs on has (redesign §8, step 2): its memory (with its container's limit), its cores, the
//! process's resident memory and a disk's free room. The code below the hosts reads it here, never `/proc` or the OS:
//! the host installs the machine once, at its start (`install`; natively `os::Os`, on a page its device's), and a node's
//! state holds it (`NodeState::machine`), so a node reads its own and the sim can give each of its members one of its
//! own. What reads it with no node at hand (a setting's default, the memory watch, a query's budget) reads the process's
//! (`process`). A unit test's lakes, whose process installs none, read a fixed machine (`Testing`).
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering::SeqCst};
use std::sync::{Arc, OnceLock};

/// A machine, as its host reads it.
pub trait Machine: Send + Sync {
    /// The memory this process may use, if the machine says: its RAM, less where a container or a cgroup limits the
    /// process. None where it isn't read (a page, whose memory is its device's line: `line`).
    fn ram(&self) -> Option<usize>;

    /// The memory line the platform holds this process to, where it draws one itself: a page's device, since a phone's
    /// browser gives a module less than a desktop's. None natively, where the core draws the line from `ram` (`oom::line`);
    /// on a page whose device doesn't say, the line is the page's own (`env::PAGE_LINE`).
    fn line(&self) -> Option<usize> {
        None
    }

    /// This process's resident memory now, if the OS says (on a page, the bytes its program reads).
    fn resident(&self) -> Option<usize>;

    /// The cores this process has: every count of threads, turns or partitions starts from it.
    fn cores(&self) -> usize;

    /// The free and the whole bytes of the disk that holds `dir` (its nearest ancestor that exists), if the OS says.
    fn space(&self, dir: &Path) -> Option<(u64, u64)>;
}

/// A machine as a node's state holds it.
pub type Mach = Arc<dyn Machine>;

/// The machine of a process whose host installed none (a unit test's lakes): it reads nothing but the cores the
/// standard library sees. Memory then has its defaults for a machine that doesn't say (`store::memory_limit`).
struct Unknown;

impl Machine for Unknown {
    fn ram(&self) -> Option<usize> { None }
    fn resident(&self) -> Option<usize> { None }
    fn cores(&self) -> usize {
        #[cfg(target_arch = "wasm32")]
        return 1;
        #[cfg(not(target_arch = "wasm32"))]
        std::thread::available_parallelism().map_or(2, |n| n.get())
    }
    fn space(&self, _: &Path) -> Option<(u64, u64)> { None }
}

/// A unit test's machine, when its test installed none: fixed, so no test depends on the machine it runs on. As CI's
/// runner is: 16 GB, the cores the standard library sees, and a disk with 64 GB free of 128. It reads no resident
/// memory, so no memory watch runs beside a test.
#[cfg(test)]
struct Testing;

#[cfg(test)]
impl Machine for Testing {
    fn ram(&self) -> Option<usize> { Some(16 << 30) }
    fn resident(&self) -> Option<usize> { None }
    fn cores(&self) -> usize { Unknown.cores() }
    fn space(&self, _: &Path) -> Option<(u64, u64)> { Some((64 << 30, 128 << 30)) }
}

// (the process's machine: installed once by its host, as the allocator is chosen once)
static INSTALLED: OnceLock<Mach> = OnceLock::new();

// (whether this process is a host program's, a library's: set by its first open, never by the program)
static HOSTED: AtomicBool = AtomicBool::new(false);

/// Installs the process's machine, once: a second install keeps the first, which it answers.
pub fn install(machine: Mach) -> Mach { INSTALLED.get_or_init(|| machine).clone() }

/// The process's machine: the one its host installed, else one that reads nothing (`Unknown`; a unit test's, `Testing`).
pub fn process() -> &'static dyn Machine {
    match INSTALLED.get() {
        Some(m) => m.as_ref(),
        #[cfg(test)]
        None => &Testing,
        #[cfg(not(test))]
        None => &Unknown,
    }
}

/// The process's machine, to hold (`NodeState::new`).
pub fn installed() -> Mach {
    match INSTALLED.get() {
        Some(m) => m.clone(),
        #[cfg(test)]
        None => Arc::new(Testing),
        #[cfg(not(test))]
        None => Arc::new(Unknown),
    }
}

/// Whether this process is a host program's (a library's, from its first open): its memory and its process are the
/// host's, so a node draws no memory line of its own (`oom::line`) and never ends the process.
pub fn hosted() -> bool { HOSTED.load(SeqCst) }

/// This process is a host program's from now on (`library::claim`, a library's first open).
pub fn set_hosted() { HOSTED.store(true, SeqCst) }

#[cfg(test)]
mod tests {
    use super::*;

    /// A machine that says nothing still counts its cores, and a node's state holds the process's machine.
    #[test]
    fn a_process_with_no_machine_installed_reads_nothing_but_its_cores() {
        let unknown = Unknown;
        assert!(unknown.ram().is_none() && unknown.resident().is_none() && unknown.space(Path::new(".")).is_none());
        assert!(unknown.cores() >= 1);
    }
}
