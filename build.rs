#[path = "tools/startup_layout.rs"]
mod startup_layout;

fn main() {
    if let Some(script) = startup_layout::script("tools/startup.txt") {
        println!("cargo:rustc-link-arg-bins=-Wl,-T,{}", script.display());
    }
}
