#[path = "../tools/startup_layout.rs"]
mod startup_layout;

fn main() {
    if let Some(script) = startup_layout::script("../tools/startup.txt") {
        println!("cargo:rustc-cdylib-link-arg=-Wl,-T,{}", script.display());
    }
}
