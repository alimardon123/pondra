//! What the page's device has, as the core reads a machine (`pondra::machine::Machine`, redesign §8, step 2). The page's
//! platform installs it before the module's page is claimed (`install`), so the core's memory watch, its query pool, its hot
//! columns and its counts of threads and partitions read the page's device, never a guess the core makes for a target it
//! can't see, nor a value handed along with each page.
use crate::js;
use std::path::Path;
use std::sync::Arc;
use wasm_bindgen::JsValue;

/// The page's device. It reads no RAM, since a browser doesn't say it (and 4 GiB is past a 32-bit `usize`): the memory it
/// gives the module is its line (`line`). Its resident memory is the bytes the module's allocator holds (`heap::live`). It
/// has one core, and a page has no disk.
struct Device {
    line: Option<usize>, // (the device's memory line, read once as it is installed: `line`)
}

impl pondra::machine::Machine for Device {
    fn ram(&self) -> Option<usize> {
        None
    }

    fn line(&self) -> Option<usize> {
        self.line
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
    pondra::machine::install(Arc::new(Device { line: line() }));
}

/// The memory line a page has on this device (`memory_max_gb` is taken over it), or None for the page's own 3 GiB
/// (`env::PAGE_LINE`). A phone's browser refuses to grow a module's memory well before 3 GiB, and the page would trap there
/// instead of stopping the query that grew it (53200). A browser that says how much memory its device has
/// (`navigator.deviceMemory`, in GiB, which Chrome rounds down to a power of two and gives as 8 at most) gives three
/// eighths of it, so 8 keeps 3 GiB; one that doesn't say (Safari, Firefox) on a device whose pointer is a finger (`(pointer:
/// coarse)`: a phone or a tablet), or in a worker (no `matchMedia`) whose browser says it is a phone's or a tablet's
/// (`navigator.userAgent`), gives 1 GiB. Elsewhere (a desktop's Firefox or Safari, Node) the page keeps its own.
fn line() -> Option<usize> {
    let global = JsValue::from(js_sys::global());
    let said = js::get(&global, "navigator").and_then(|navigator| js::get(&navigator, "deviceMemory")).ok().and_then(|gib| gib.as_f64());
    let gib = match said {
        Some(gib) if gib > 0.0 => gib * 3.0 / 8.0,
        _ if coarse_pointer(&global) || mobile_agent(&global) => 1.0,
        _ => return None,
    };
    Some((gib.min(3.0) * crate::GIB) as usize)
}

/// Whether the device's main pointer is a finger (`matchMedia('(pointer: coarse)')`). A worker has no `matchMedia`: false.
fn coarse_pointer(global: &JsValue) -> bool {
    let Ok(match_media) = js::function(global, "matchMedia") else { return false };
    let Ok(query) = match_media.call1(global, &"(pointer: coarse)".into()) else { return false };
    js::get(&query, "matches").ok().and_then(|matches| matches.as_bool()) == Some(true)
}

/// Whether the browser says it is a phone's or a tablet's (`navigator.userAgent`), which a worker can read where it has no
/// `matchMedia`. (An iPad's Safari says it is a desktop's.)
fn mobile_agent(global: &JsValue) -> bool {
    let agent = js::get(global, "navigator").and_then(|navigator| js::get(&navigator, "userAgent")).ok().and_then(|a| a.as_string()).unwrap_or_default();
    ["Mobi", "Android", "iPhone", "iPad"].iter().any(|m| agent.contains(m))
}
