//! The `pondra` command of the packages (ADR-061 §10).
//!
//! The wheel and the npm platform packages carry this program and `libpondra`, the same engine as a
//! library (`capi/`). The program loads the library at run time and calls its `pondra_main`, so one copy
//! of the engine serves both the command line and `pondra.open()`. It is never linked to the library: the
//! library is looked for where the packages put it, and a missing one is refused by name.

use std::ffi::{c_char, c_void, OsString};
#[cfg(unix)]
use std::ffi::{CStr, CString};
use std::path::{Path, PathBuf};

const VERSION: &str = env!("CARGO_PKG_VERSION");
const ENTRY: &[u8] = b"pondra_main\0";
/// The shell's code for a program it can't run; the launcher's refusals exit with it too.
const EXIT_CANNOT_RUN: i32 = 127;

/// The library's `int32_t pondra_main(int32_t argc, const char **argv)`.
type Main = unsafe extern "C" fn(i32, *const *const c_char) -> i32;

fn main() {
    let folder = program_folder();
    let Some(path) = candidates(&folder, VERSION, std::env::var_os("PONDRA_LIBRARY")).into_iter().next() else {
        refuse_missing(&places(&folder, VERSION));
    };
    let path = std::path::absolute(&path).unwrap_or(path);
    if !path.is_file() {
        // Only PONDRA_LIBRARY can name a missing file: the other places are checked before they are kept.
        fail(format!("PONDRA_LIBRARY names {}, which isn't a file", path.display()));
    }
    let handle = open(&path).unwrap_or_else(|why| fail(format!("cannot load {}: {why}", path.display())));
    let entry = find_entry(handle);
    if entry.is_null() {
        fail(format!("{} has no pondra_main: it is from another version", path.display()));
    }
    // The address is the library's `pondra_main`, which has the signature `Main`.
    let main = unsafe { std::mem::transmute::<*mut c_void, Main>(entry) };
    std::process::exit(call(main));
}

/// The library's file name on this platform.
fn library_name() -> &'static str {
    if cfg!(windows) {
        "pondra.dll"
    } else if cfg!(target_os = "macos") {
        "libpondra.dylib"
    } else {
        "libpondra.so"
    }
}

/// Every place the library is looked for, in order, whether or not it is there: beside the program, then
/// the site-packages folders a scripts folder can belong to. A site folder counts only with its own
/// `pondra-<version>.dist-info`, so two Pythons in one prefix never mix versions.
fn places(exe_dir: &Path, version: &str) -> Vec<PathBuf> {
    let name = library_name();
    let mut places = vec![exe_dir.join(name)];
    // `parent`, not `..`: a path with `..` in it isn't normalised, and the folders are named the same
    // way on every platform.
    let up = exe_dir.parent().unwrap_or(exe_dir);
    let lib = up.join("lib");
    // Unix venvs, system and user installs, conda, PyPy, Debian. macOS's framework user installs
    // (`lib/python/site-packages`) are one of these.
    let mut sites: Vec<PathBuf> = subfolders(&lib)
        .into_iter()
        .flat_map(|folder| [folder.join("site-packages"), folder.join("dist-packages")])
        .collect();
    sites.push(up.join("Lib").join("site-packages")); // Windows venvs and system installs
    sites.push(up.join("site-packages")); // Windows user installs
    sites.push(up.to_path_buf()); // `pip install --target dir`: dir/bin/pondra beside dir/pondra/_lib
    for site in sites {
        if site.join(format!("pondra-{version}.dist-info")).is_dir() {
            places.push(site.join("pondra").join("_lib").join(name));
        }
    }
    places
}

/// The places to try, in order; the first one is loaded. `PONDRA_LIBRARY` (a development build, a test)
/// comes first and is kept even when it is missing, so that loading it names the path. The other places
/// count only when they are files.
fn candidates(exe_dir: &Path, version: &str, variable: Option<OsString>) -> Vec<PathBuf> {
    let mut found = Vec::new();
    if let Some(path) = variable.filter(|path| !path.is_empty()) {
        found.push(PathBuf::from(path));
    }
    found.extend(places(exe_dir, version).into_iter().filter(|path| path.is_file()));
    found
}

/// The sub-folders of `dir`, by name, so the order of the places is the same on every run.
fn subfolders(dir: &Path) -> Vec<PathBuf> {
    let mut folders: Vec<PathBuf> = std::fs::read_dir(dir)
        .into_iter()
        .flatten()
        .flatten()
        .map(|entry| entry.path())
        .filter(|path| path.is_dir())
        .collect();
    folders.sort();
    folders
}

/// The folder of the program, with links followed: a `pondra` linked into a bin folder (npm's `.bin`)
/// finds the library beside the real one, not beside the link.
fn program_folder() -> PathBuf {
    let exe = std::env::current_exe().unwrap_or_else(|e| fail(format!("cannot find its own program: {e}")));
    let real = std::fs::canonicalize(&exe).unwrap_or(exe);
    real.parent().map(Path::to_path_buf).unwrap_or_default()
}

/// The program's arguments, as the library's `main` takes them: C strings, the list ending with a null.
#[cfg(unix)]
fn call(main: Main) -> i32 {
    use std::os::unix::ffi::OsStringExt;
    let args: Vec<CString> = std::env::args_os()
        .map(|arg| CString::new(arg.into_vec()).expect("an argument has no NUL byte"))
        .collect();
    let mut argv: Vec<*const c_char> = args.iter().map(|arg| arg.as_ptr()).collect();
    argv.push(std::ptr::null());
    // `args` owns the strings and lives until the program returns.
    unsafe { main(args.len() as i32, argv.as_ptr()) }
}

/// Windows: the library reads the process's own arguments, which the OS gives it as one command line.
#[cfg(windows)]
fn call(main: Main) -> i32 {
    unsafe { main(0, std::ptr::null()) }
}

/// Loads the library at `path`. On failure, the loader's own words say why.
#[cfg(unix)]
fn open(path: &Path) -> Result<*mut c_void, String> {
    use std::os::unix::ffi::OsStrExt;
    let name = CString::new(path.as_os_str().as_bytes()).map_err(|e| e.to_string())?;
    // RTLD_LOCAL: the library's symbols stay its own, not joining those of a host process (a Python).
    let handle = unsafe { libc::dlopen(name.as_ptr(), libc::RTLD_NOW | libc::RTLD_LOCAL) };
    if handle.is_null() {
        return Err(last_dl_error());
    }
    Ok(handle)
}

#[cfg(unix)]
fn last_dl_error() -> String {
    let message = unsafe { libc::dlerror() };
    if message.is_null() {
        return "unknown error".to_string();
    }
    unsafe { CStr::from_ptr(message) }.to_string_lossy().into_owned()
}

#[cfg(unix)]
fn find_entry(handle: *mut c_void) -> *mut c_void {
    unsafe { libc::dlsym(handle, ENTRY.as_ptr().cast()) }
}

// The two kernel32 functions, declared here: kernel32 is linked into every Windows program.
#[cfg(windows)]
#[allow(non_snake_case)]
extern "system" {
    fn LoadLibraryW(name: *const u16) -> *mut c_void;
    fn GetProcAddress(module: *mut c_void, name: *const u8) -> *mut c_void;
}

#[cfg(windows)]
fn open(path: &Path) -> Result<*mut c_void, String> {
    use std::os::windows::ffi::OsStrExt;
    // The full path in UTF-16, with its terminating NUL.
    let wide: Vec<u16> = path.as_os_str().encode_wide().chain(Some(0)).collect();
    let handle = unsafe { LoadLibraryW(wide.as_ptr()) };
    if handle.is_null() {
        return Err(std::io::Error::last_os_error().to_string());
    }
    Ok(handle)
}

#[cfg(windows)]
fn find_entry(handle: *mut c_void) -> *mut c_void {
    unsafe { GetProcAddress(handle, ENTRY.as_ptr()) }
}

/// The library is in none of the places: say where it was looked for, and how to point at it.
fn refuse_missing(looked: &[PathBuf]) -> ! {
    let mut message = format!("libpondra wasn't found ({}). Looked in:", library_name());
    for place in looked {
        message.push_str(&format!("\n  {}", place.display()));
    }
    message.push_str("\nSet PONDRA_LIBRARY to the library's path to use another one.");
    fail(message)
}

/// Says what went wrong, on a line that starts with `pondra: `, and exits with the shell's "can't run" code.
fn fail(message: String) -> ! {
    eprintln!("pondra: {message}");
    std::process::exit(EXIT_CANNOT_RUN)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::{create_dir_all, File};

    const V: &str = "0.33.0";

    /// A folder tree under the temporary folder, named by its test and removed when the test ends.
    struct Tree(PathBuf);

    impl Tree {
        fn new(test: &str) -> Tree {
            let root = std::env::temp_dir().join(format!("pondra-launcher-{}-{test}", std::process::id()));
            let _ = std::fs::remove_dir_all(&root);
            create_dir_all(&root).unwrap();
            Tree(root)
        }

        /// A folder under the tree, with its parents.
        fn dir(&self, relative: &str) -> PathBuf {
            let path = self.0.join(relative);
            create_dir_all(&path).unwrap();
            path
        }

        /// An empty file under the tree, with its folders.
        fn file(&self, relative: &str) -> PathBuf {
            let path = self.0.join(relative);
            create_dir_all(path.parent().unwrap()).unwrap();
            File::create(&path).unwrap();
            path
        }
    }

    impl Drop for Tree {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    /// `<folder>/pondra/_lib/<library>`, the path the wheel puts the library at.
    fn in_wheel(folder: &str) -> String {
        format!("{folder}/pondra/_lib/{}", library_name())
    }

    #[test]
    fn finds_the_library_beside_the_program() {
        let tree = Tree::new("beside");
        let bin = tree.dir("bin");
        let library = tree.file(&format!("bin/{}", library_name()));
        assert_eq!(candidates(&bin, V, None), vec![library]);
    }

    #[test]
    fn finds_the_wheel_of_its_own_version_only() {
        let tree = Tree::new("own-version");
        let bin = tree.dir("prefix/bin");
        // Two Pythons in one prefix: the 3.11 wheel is another version, so only 3.12's counts.
        tree.dir("prefix/lib/python3.11/site-packages/pondra-0.32.0.dist-info");
        tree.file(&in_wheel("prefix/lib/python3.11/site-packages"));
        tree.dir("prefix/lib/python3.12/site-packages/pondra-0.33.0.dist-info");
        let library = tree.file(&in_wheel("prefix/lib/python3.12/site-packages"));
        assert_eq!(candidates(&bin, V, None), vec![library]);
    }

    #[test]
    fn ignores_a_wheel_of_another_version_alone() {
        let tree = Tree::new("other-version");
        let bin = tree.dir("prefix/bin");
        tree.dir("prefix/lib/python3.12/site-packages/pondra-0.32.0.dist-info");
        tree.file(&in_wheel("prefix/lib/python3.12/site-packages"));
        assert!(candidates(&bin, V, None).is_empty());
    }

    #[test]
    fn finds_windows_venvs() {
        let tree = Tree::new("windows-venv");
        let scripts = tree.dir("venv/Scripts");
        tree.dir("venv/Lib/site-packages/pondra-0.33.0.dist-info");
        let library = tree.file(&in_wheel("venv/Lib/site-packages"));
        assert_eq!(candidates(&scripts, V, None), vec![library]);
    }

    #[test]
    fn finds_windows_user_installs() {
        let tree = Tree::new("windows-user");
        let scripts = tree.dir("Python312/Scripts");
        tree.dir("Python312/site-packages/pondra-0.33.0.dist-info");
        let library = tree.file(&in_wheel("Python312/site-packages"));
        assert_eq!(candidates(&scripts, V, None), vec![library]);
    }

    #[test]
    fn finds_a_target_folder() {
        let tree = Tree::new("target");
        let bin = tree.dir("dir/bin");
        tree.dir("dir/pondra-0.33.0.dist-info");
        let library = tree.file(&in_wheel("dir"));
        assert_eq!(candidates(&bin, V, None), vec![library]);
    }

    #[test]
    fn the_variable_comes_first_even_when_missing() {
        let tree = Tree::new("variable");
        let bin = tree.dir("bin");
        let beside = tree.file(&format!("bin/{}", library_name()));
        let missing = PathBuf::from("/nonexistent/dev/libpondra.so");
        let given = candidates(&bin, V, Some(missing.clone().into_os_string()));
        assert_eq!(given, vec![missing, beside.clone()]);
        // An empty variable is no variable.
        assert_eq!(candidates(&bin, V, Some(OsString::new())), vec![beside]);
    }

    #[test]
    fn lists_every_place_including_missing_ones() {
        let tree = Tree::new("missing");
        let bin = tree.dir("prefix/bin");
        assert_eq!(places(&bin, V), vec![bin.join(library_name())]);
    }
}
