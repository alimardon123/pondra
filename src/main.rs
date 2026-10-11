//! The `pondra` command (`cli.rs`): a node, the SQL shell, and the commands around them.

// The program's allocator, not the library's: a program built on the library picks its own.
#[global_allocator]
static ALLOC: mimalloc::MiMalloc = mimalloc::MiMalloc; // returns freed memory to the OS promptly (glibc malloc holds on to it)

fn main() {
    pondra::cli::main()
}
