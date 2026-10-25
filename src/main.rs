//! The `pondra` command (`cli.rs`): a node, the SQL shell, and the commands around them.

// The program's allocator, not the library's: a program built on the library picks its own.
// On Linux jemalloc, whose own threads give back what has been free for ten seconds: under 240
// small commits a second a node on mimalloc grew about 50 MB a minute and never stopped, one on
// jemalloc levels off within three minutes (`harness.py memory`), and TPC-H and TPC-DS run faster
// on it. Elsewhere mimalloc.
#[cfg(all(target_os = "linux", target_env = "gnu"))]
#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc; // (its options are built in: `.cargo/config.toml`)
#[cfg(not(all(target_os = "linux", target_env = "gnu")))]
#[global_allocator]
static ALLOC: mimalloc::MiMalloc = mimalloc::MiMalloc;

fn main() {
    pondra::cli::main()
}
