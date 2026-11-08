//! Space of this process's own on this machine's disk (redesign §8, step 2): the folders its spills go to (a query's,
//! a shuffle's, a lake in memory's past its budget), each locked while it is used and removed when it is let go, and
//! the folders of processes that ended swept. Plain folders of the user's own (`own_dir`), under the disk that has the
//! room (`spill_base`).
use std::collections::BTreeSet;
use std::path::{Path as FsPath, PathBuf};
use std::sync::{Mutex, PoisonError};
use std::time::Duration;

/// The lock file in a spill folder. The process that made the folder holds it, so a folder whose
/// lock another process can take is one its process left behind. It is not an object of the lake.
pub(crate) const LOCK_FILE: &str = ".lock";

/// The spill folders this process made and has not yet removed (`Folder`). A process static, as `MEMORY` is: a
/// folder of a static (`store::SPILLS`) outlives a node until the program ends, and `leave` removes what is left.
/// `sweep` skips these (on NFS a lock is per process, so opening a folder's lock again would drop ours). Not the
/// pid: an exec restart keeps it, and the container image runs `pondra` as PID 1 on a shared volume.
static MADE: Mutex<BTreeSet<PathBuf>> = Mutex::new(BTreeSet::new());

/// A folder of this process's own under the user's spill folder for one kind of spill (a lake held in
/// memory's, or a query's and a shuffle's: `store::spill_folder`), with its lock file held. The folders of
/// processes that ended are swept first (`sweep`); dropping this one removes it.
#[derive(Debug)]
pub(crate) struct Folder {
    pub(crate) path: PathBuf,
    lock: Option<std::fs::File>,
}

impl Folder {
    /// A folder of this process's for `kind`, with its lock file held. It is in `MADE` from the moment it is made.
    pub(crate) fn make(kind: &str) -> std::io::Result<Folder> {
        let parent = own_dir(&spill_base(), &kind_dir(kind))?;
        sweep(&parent);
        let path = parent.join(format!("{}-{}", std::process::id(), uuid::Uuid::new_v4().simple()));
        std::fs::create_dir(&path)?;
        MADE.lock().unwrap_or_else(PoisonError::into_inner).insert(path.clone());
        // (until its lock is held the folder is a `Folder` with no lock: a failure below drops it, and its folder goes)
        let mut folder = Folder { path, lock: None };
        let lock = std::fs::OpenOptions::new().read(true).write(true).create_new(true).open(folder.path.join(LOCK_FILE))?;
        if lock.try_lock().is_err() {
            return Err(std::io::Error::other("its lock file is held by another process"));
        }
        folder.lock = Some(lock);
        Ok(folder)
    }
}

impl Drop for Folder {
    fn drop(&mut self) {
        drop(self.lock.take()); // (Windows can't delete a folder with an open file in it)
        let _ = std::fs::remove_dir_all(&self.path);
        MADE.lock().unwrap_or_else(PoisonError::into_inner).remove(&self.path); // (after its folder is gone: `sweep` skipped it meanwhile)
    }
}

/// Where a node's spills go: `PONDRA_SPILL_DIR` when set, else `PONDRA_CACHE_DIR` (with the node's other
/// scratch), else the temporary directory, unless that is itself held in memory (tmpfs: /tmp on Fedora
/// and Arch), where a spill would free nothing: then the user's cache folder. A lake held in memory
/// spills here too, and so do a query's spills (`store.rs`) and a shuffle's (`spill.rs`).
pub(crate) fn spill_base() -> PathBuf {
    if let Some(dir) = crate::env::get("PONDRA_SPILL_DIR").or_else(|| crate::env::get("PONDRA_CACHE_DIR")) {
        return PathBuf::from(dir);
    }
    let temp = std::env::temp_dir();
    #[cfg(target_os = "linux")]
    if held_in_ram(&temp) {
        let cache = std::env::var_os("XDG_CACHE_HOME").map(PathBuf::from).or_else(|| std::env::var_os("HOME").map(|h| PathBuf::from(h).join(".cache")));
        if let Some(cache) = cache.filter(|c| c.is_absolute()) {
            if !held_in_ram(&cache) {
                return cache;
            }
        }
    }
    temp
}

/// Is `dir` on a filesystem held in memory (tmpfs, ramfs)?
#[cfg(target_os = "linux")]
fn held_in_ram(dir: &FsPath) -> bool {
    use std::os::unix::ffi::OsStrExt;
    // (a folder not made yet is judged by its nearest existing parent: the filesystem it would be made on)
    let Some(dir) = dir.ancestors().find(|p| p.exists()) else { return false };
    let Ok(path) = std::ffi::CString::new(dir.as_os_str().as_bytes()) else { return false };
    let mut fs: libc::statfs = unsafe { std::mem::zeroed() };
    unsafe { libc::statfs(path.as_ptr(), &mut fs) == 0 && matches!(fs.f_type as u32, 0x0102_1994 | 0x8584_58f6) } // (TMPFS_MAGIC, RAMFS_MAGIC)
}

/// A folder named `name` in `base` that is this user's alone: `{name}-{uid}` on Unix (where /tmp is shared),
/// made 0700 and used only while it is ours alone, so no other user can read what is in it or take a lock
/// away; `name` elsewhere, where the temporary folder is the user's own. `base` is made first, parents and all
/// (a PONDRA_SPILL_DIR not made yet). Refused (PermissionDenied) when it is someone else's or open to others.
pub(crate) fn own_dir(base: &FsPath, name: &str) -> std::io::Result<PathBuf> {
    let dir = own_path(base, name);
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        std::fs::create_dir_all(base)?;
        let _ = std::fs::DirBuilder::new().mode(0o700).create(&dir); // (it may be there already)
    }
    #[cfg(not(unix))]
    std::fs::create_dir_all(&dir)?;
    check_alone(&dir)?;
    Ok(dir)
}

/// `name` in `base` as `own_dir` has it, if it is there and this user's alone. Makes nothing: what a node's start
/// asks (`sweep_all`), where nothing may be made.
pub(crate) fn own_dir_if_there(base: &FsPath, name: &str) -> Option<PathBuf> {
    let dir = own_path(base, name);
    check_alone(&dir).ok()?;
    Some(dir)
}

/// The folder `name` is in `base`: `{name}-{uid}` on Unix, where `/tmp` is shared, `name` elsewhere.
fn own_path(base: &FsPath, name: &str) -> PathBuf {
    #[cfg(unix)]
    {
        base.join(format!("{name}-{}", unsafe { libc::getuid() }))
    }
    #[cfg(not(unix))]
    {
        base.join(name)
    }
}

/// The one ownership check of `own_dir` and `own_dir_if_there`: `dir` is a folder, not a link, owned by this user
/// and open to no one else (Unix). Refused (PermissionDenied) otherwise.
fn check_alone(dir: &FsPath) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        let m = std::fs::symlink_metadata(dir)?;
        if m.is_dir() && m.uid() == unsafe { libc::getuid() } && m.mode() & 0o077 == 0 {
            return Ok(());
        }
        Err(std::io::Error::new(std::io::ErrorKind::PermissionDenied, format!("{} is not this user's alone", dir.display())))
    }
    #[cfg(not(unix))]
    {
        if dir.is_dir() { Ok(()) } else { Err(std::io::Error::from(std::io::ErrorKind::NotFound)) }
    }
}

/// The name of the folders of one kind of spill in the user's spill folder: `pondra-{kind}`, `own_dir`'s.
fn kind_dir(kind: &str) -> String {
    format!("pondra-{kind}")
}

/// Removes the spill folders whose process has ended: a folder whose lock file this process can take
/// is one nobody holds, and a folder with no lock file at all is one whose process died between making
/// the folder and its lock (made at once, so none a minute on). Best effort. A folder made a minute ago is
/// left alone: its lock may not be taken yet (another lake in this process may be making it now). This
/// process's own folders are skipped before their lock is opened (on NFS a lock is per process: opening it
/// again would drop ours).
fn sweep(parent: &FsPath) {
    let Ok(entries) = std::fs::read_dir(parent) else { return };
    for entry in entries.flatten() {
        let dir = entry.path();
        if MADE.lock().unwrap_or_else(PoisonError::into_inner).contains(&dir) {
            continue;
        }
        let lock_path = dir.join(LOCK_FILE);
        match std::fs::metadata(&lock_path) {
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                if age(&dir).is_some_and(|a| a > Duration::from_secs(60)) {
                    let _ = std::fs::remove_dir_all(&dir);
                }
            }
            Ok(_) if age(&lock_path).is_some_and(|a| a > Duration::from_secs(60)) => {
                let Ok(lock) = std::fs::OpenOptions::new().read(true).write(true).open(&lock_path) else { continue };
                if lock.try_lock().is_ok() {
                    drop(lock); // (Windows can't delete a folder with an open file in it)
                    let _ = std::fs::remove_dir_all(&dir);
                }
            }
            _ => {}
        }
    }
}

/// How long ago `path` was last changed, if it can say.
fn age(path: &FsPath) -> Option<Duration> {
    std::fs::metadata(path).and_then(|m| m.modified()).ok()?.elapsed().ok()
}

/// Removes the spill folders that processes which ended left, of both kinds (`sweep`). Run beside a node's start
/// (`node.rs`), on a thread of its own: a folder left by a crash may be gigabytes, and nothing is made here.
pub fn sweep_all() {
    for kind in ["memory", "spill"] {
        if let Some(parent) = own_dir_if_there(&spill_base(), &kind_dir(kind)) {
            sweep(&parent);
        }
    }
}

/// The program's end, for the ends that run no destructor: every folder this process made that is still there goes.
/// Best effort, no error (`memory::leave`).
pub fn leave() {
    let made = std::mem::take(&mut *MADE.lock().unwrap_or_else(PoisonError::into_inner));
    for path in made {
        let _ = std::fs::remove_dir_all(&path);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(target_os = "linux")]
    #[test]
    fn a_folder_held_in_memory_is_told_apart() {
        if FsPath::new("/dev/shm").is_dir() {
            assert!(held_in_ram(FsPath::new("/dev/shm")), "/dev/shm is tmpfs");
        }
        assert!(!held_in_ram(FsPath::new(env!("CARGO_MANIFEST_DIR"))), "the checkout is on disk");
    }

    /// A folder under `parent` with its lock file made, returned open. `old` sets the lock's time two minutes back.
    fn lock_in(parent: &FsPath, name: &str, old: bool) -> std::fs::File {
        let dir = parent.join(name);
        std::fs::create_dir_all(&dir).unwrap();
        let lock = std::fs::OpenOptions::new().read(true).write(true).create_new(true).open(dir.join(LOCK_FILE)).unwrap();
        if old {
            lock.set_modified(std::time::SystemTime::now() - Duration::from_secs(120)).unwrap();
        }
        lock
    }

    #[test]
    fn sweep_removes_a_folder_whose_process_ended_and_keeps_the_rest() {
        let parent = std::env::temp_dir().join(format!("pondra-sweep-{}", uuid::Uuid::new_v4().simple()));
        drop(lock_in(&parent, "ended", true)); // its lock is free, and it was last touched two minutes ago
        let alive = lock_in(&parent, "alive", true); // as old, but its lock is held
        alive.try_lock().unwrap();
        drop(lock_in(&parent, "new", false)); // free, but made just now: its lock may not be taken yet
        sweep(&parent);
        assert!(!parent.join("ended").exists(), "a folder whose process ended is removed");
        assert!(parent.join("alive").exists(), "a folder whose lock is held is kept");
        assert!(parent.join("new").exists(), "a folder made just now is kept");
        drop(alive);
        let _ = std::fs::remove_dir_all(&parent);
    }

    #[cfg(unix)]
    #[test]
    fn own_dir_is_this_users_alone_and_refused_once_others_can_open_it() {
        use std::os::unix::fs::PermissionsExt;
        let root = std::env::temp_dir().join(format!("pondra-own-{}", uuid::Uuid::new_v4().simple()));
        let base = root.join("a").join("b"); // not made yet: `own_dir` makes it, parents and all
        let uid = unsafe { libc::getuid() };
        let dir = own_dir(&base, "x").unwrap();
        assert_eq!(dir, base.join(format!("x-{uid}")));
        assert_eq!(std::fs::metadata(&dir).unwrap().permissions().mode() & 0o777, 0o700);
        assert_eq!(own_dir(&base, "x").unwrap(), dir, "called again, it gives the same folder");
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o755)).unwrap();
        let refused = own_dir(&base, "x").unwrap_err();
        assert_eq!(refused.kind(), std::io::ErrorKind::PermissionDenied);
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o700)).unwrap();
        let _ = std::fs::remove_dir_all(&root);
    }

    #[cfg(unix)]
    #[test]
    fn own_dir_if_there_makes_nothing() {
        use std::os::unix::fs::PermissionsExt;
        let root = std::env::temp_dir().join(format!("pondra-own-if-{}", uuid::Uuid::new_v4().simple()));
        let base = root.join("a");
        assert!(own_dir_if_there(&base, "x").is_none(), "not there: none");
        assert!(!root.exists(), "and nothing is made");
        let dir = own_dir(&base, "x").unwrap();
        assert_eq!(own_dir_if_there(&base, "x"), Some(dir.clone()));
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o755)).unwrap();
        assert!(own_dir_if_there(&base, "x").is_none(), "open to others: none");
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o700)).unwrap();
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn a_folder_this_process_made_is_kept_by_sweep() {
        let parent = std::env::temp_dir().join(format!("pondra-sweep-made-{}", uuid::Uuid::new_v4().simple()));
        drop(lock_in(&parent, "mine", true)); // old, and its lock free: only this process's own list keeps it
        let path = parent.join("mine");
        MADE.lock().unwrap_or_else(PoisonError::into_inner).insert(path.clone());
        sweep(&parent);
        assert!(path.exists(), "a folder this process made is kept, whatever its lock");
        MADE.lock().unwrap_or_else(PoisonError::into_inner).remove(&path);
        let _ = std::fs::remove_dir_all(&parent);
    }

    #[cfg(unix)]
    #[test]
    fn a_folder_with_no_lock_file_goes_once_a_minute_old() {
        let parent = std::env::temp_dir().join(format!("pondra-sweep-bare-{}", uuid::Uuid::new_v4().simple()));
        let bare = parent.join("bare");
        std::fs::create_dir_all(&bare).unwrap(); // no lock file: its process died before it made one
        std::fs::File::open(&bare).unwrap().set_modified(std::time::SystemTime::now() - Duration::from_secs(120)).unwrap();
        std::fs::create_dir_all(parent.join("new")).unwrap(); // made just now: its lock may not be made yet
        sweep(&parent);
        assert!(!bare.exists(), "a folder with no lock file, a minute old, is removed");
        assert!(parent.join("new").exists(), "a folder made just now is kept");
        let _ = std::fs::remove_dir_all(&parent);
    }

    #[test]
    fn a_folder_is_in_made_until_it_goes() {
        let folder = Folder::make("test").unwrap();
        let path = folder.path.clone();
        assert!(MADE.lock().unwrap_or_else(PoisonError::into_inner).contains(&path));
        drop(folder);
        assert!(!path.exists(), "dropping it removes it");
        assert!(!MADE.lock().unwrap_or_else(PoisonError::into_inner).contains(&path));
    }
}
