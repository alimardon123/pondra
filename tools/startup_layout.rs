//! The startup layout (designs/l2-startup-layout.md), shared by the program's and the library's build scripts.
//!
//! A node runs about 12 MB of its code from its start to its first answer, spread through all of it, and the
//! kernel reads code around every page a function touches: an idle node kept 67 MB of code resident. With
//! `PONDRA_STARTUP_LAYOUT` set, a Linux build puts the functions listed in `tools/startup.txt` (written by
//! `tools/startup_order.py`) next to each other ahead of the rest, and keeps 16–20 MB. lld matches every
//! section against every name, which makes the link about 45 s longer, so builds that nobody ships leave it out.
use std::{env, fs, path::PathBuf};

/// The linker script for this build, from the list at `list`, or None when the layout isn't asked for or the
/// target isn't Linux.
pub fn script(list: &str) -> Option<PathBuf> {
    println!("cargo:rerun-if-env-changed=PONDRA_STARTUP_LAYOUT");
    // (an empty value is no value, as for every setting: CI gives it empty to pull requests)
    if env::var("PONDRA_STARTUP_LAYOUT").unwrap_or_default().is_empty() || env::var("CARGO_CFG_TARGET_OS").ok()? != "linux" {
        return None;
    }
    println!("cargo:rerun-if-changed={list}");
    let mut out = String::from("SECTIONS {\n  .text.startup : {\n    *(\n");
    for line in fs::read_to_string(list).expect(list).lines().map(str::trim).filter(|l| !l.is_empty() && !l.starts_with('#')) {
        // A function LLVM took for cold (`unlikely.` in the list) has its section under `.text.unlikely.`; it
        // may not be cold in the next build, so both. Both for every function made the link twice as long.
        let name = line.strip_prefix("unlikely.").unwrap_or(line);
        if name != line {
            out += &format!("      .text.unlikely.{name}\n");
        }
        out += &format!("      .text.{name}\n");
    }
    out += "    )\n  }\n} INSERT BEFORE .text;\n";
    let path = PathBuf::from(env::var("OUT_DIR").expect("OUT_DIR")).join("startup.ld");
    fs::write(&path, out).expect("startup.ld");
    Some(path)
}
