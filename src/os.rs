//! This machine as its OS says (redesign §8, step 2): the native [`Machine`](crate::machine::Machine), which the program
//! and a library's process install at their start (`cli::main`, `Node::start`). Linux reads `/proc` and the cgroup's
//! limit; elsewhere the OS's own calls (sysinfo); the free disk is `statvfs` on Unix.
use crate::machine::Machine;
use std::path::Path;
use std::sync::OnceLock;

/// The machine this process runs on.
#[derive(Default)]
pub struct Os {
    // (read once: the machine's memory doesn't change while it runs)
    ram: OnceLock<Option<usize>>,
}

impl Machine for Os {
    /// The machine's memory: `/proc/meminfo` on Linux, the OS's own call elsewhere. Less when a cgroup limits this
    /// process (a container, a service with `MemoryMax`): `/proc/meminfo` is the host's, and a node in a 4 GB container
    /// on a 64 GB host sized its queries for 64 and was killed.
    fn ram(&self) -> Option<usize> { *self.ram.get_or_init(read_ram) }

    /// This process's resident memory: `/proc/self/statm` on Linux, the OS's own call elsewhere.
    fn resident(&self) -> Option<usize> {
        let proc = std::fs::read_to_string("/proc/self/statm").ok().and_then(|s| s.split_whitespace().nth(1)?.parse::<usize>().ok());
        proc.map(|pages| pages * page_size()).or_else(|| {
            use sysinfo::{ProcessRefreshKind, ProcessesToUpdate, System};
            let (pid, mut s) = (sysinfo::get_current_pid().ok()?, System::new());
            s.refresh_processes_specifics(ProcessesToUpdate::Some(&[pid]), false, ProcessRefreshKind::nothing().with_memory());
            Some(s.process(pid)?.memory() as usize).filter(|&b| b > 0)
        })
    }

    /// The machine's cores (its cgroup's, as the standard library reads them), else 2.
    fn cores(&self) -> usize { std::thread::available_parallelism().map_or(2, |n| n.get()) }

    fn space(&self, dir: &Path) -> Option<(u64, u64)> {
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
}

fn read_ram() -> Option<usize> {
    let proc = std::fs::read_to_string("/proc/meminfo").ok().and_then(|m| m.lines().find_map(|l| l.strip_prefix("MemTotal:")?.trim().strip_suffix("kB")?.trim().parse::<usize>().ok()));
    proc.map(|kb| (kb << 10).min(cgroup_memory().unwrap_or(usize::MAX))).or_else(|| {
        let s = sysinfo::System::new_with_specifics(sysinfo::RefreshKind::nothing().with_memory(sysinfo::MemoryRefreshKind::nothing().with_ram()));
        Some(s.total_memory() as usize).filter(|&b| b > 0)
    })
}

/// The least memory limit of this process's cgroups and their parents (v2 `memory.max`, v1
/// `memory.limit_in_bytes`; "max" or none: no limit). In a container the cgroup is its root.
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

/// The size of a page in bytes, which `/proc/self/statm` counts in: 4 KB, or 64 KB on some arm64 builds
/// (where a count of pages read as 4 KB pages was 16 times too little). Asked each time: glibc answers
/// from memory, and a static would be a node's state for nothing.
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

#[cfg(test)]
mod tests {
    use super::*;

    /// `resident` counts bytes: `/proc/self/statm` counts pages, and the page size is the OS's (64 KB on
    /// some arm64 builds), so it agrees with `VmRSS` in kilobytes. (On 4 KB pages the old constant passed
    /// too; it is the arm64 case this guards.)
    #[cfg(target_os = "linux")]
    #[test]
    fn resident_is_in_bytes_as_the_page_size_says() {
        let block = vec![1u8; 100 << 20]; // (written, so resident)
        std::hint::black_box(&block);
        let ours = Os::default().resident().expect("resident memory");
        let status = std::fs::read_to_string("/proc/self/status").expect("the process's status");
        let kb: usize = status.lines().find_map(|l| l.strip_prefix("VmRSS:")?.trim().strip_suffix("kB")?.trim().parse().ok()).expect("VmRSS");
        let theirs = kb << 10;
        assert!(ours.abs_diff(theirs) * 4 <= theirs, "resident {ours} bytes, VmRSS {theirs} bytes");
    }

    /// The machine reads its memory, its cores and the free room of a folder's disk.
    #[cfg(unix)]
    #[test]
    fn this_machine_says_its_memory_its_cores_and_a_disks_room() {
        let os = Os::default();
        assert!(os.ram().is_some_and(|b| b > 0) && os.cores() >= 1);
        let (free, total) = os.space(&std::env::temp_dir().join("not-made-yet")).expect("the temp folder's disk");
        assert!(total > 0 && free <= total, "free {free} of {total}");
    }
}
