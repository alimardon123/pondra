//! What the page's device has, as the core reads a machine (`pondra::machine::Machine`, redesign §8, step 2). The page's
//! platform installs it before the module's page is claimed (`install`), so the core's memory watch, its hot columns and its
//! counts of threads and partitions read the page's device, never a guess the core makes for a target it can't see.
use std::path::Path;
use std::sync::Arc;

/// The page's device. Its memory is the line its device has (`device_line` in lib.rs), not a machine's, so it reads no RAM
/// (4 GiB is past a 32-bit `usize` anyway). Its resident memory is the bytes the module's allocator holds (`heap::live`). It
/// has one core, and a page has no disk.
struct Device;

impl pondra::machine::Machine for Device {
    fn ram(&self) -> Option<usize> {
        None
    }

    fn resident(&self) -> Option<usize> {
        Some(crate::heap::live())
    }

    fn cores(&self) -> usize {
        1
    }

    fn space(&self, _: &Path) -> Option<(u64, u64)> {
        None
    }
}

/// Installs the page's device as the module's machine: once, as its allocator is chosen once (a second call keeps it).
pub(crate) fn install() {
    pondra::machine::install(Arc::new(Device));
}
