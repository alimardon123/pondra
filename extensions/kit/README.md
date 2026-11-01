# pondra-extension

Write a Pondra extension in Rust: SQL functions and table functions over plain Rust values, built once to a WebAssembly
component that every Pondra 1.x loads (ADR-075).

```rust
use pondra_extension::Extension;

fn hello() -> Extension {
    Extension::new("hello", "0.1")
        .scalar("greet", |name: String| format!("hello, {name}"))
        .scalar("add", |a: i64, b: i64| a + b)
        .table("count_to", ["i", "square"], |n: i64| (1..=n).map(|i| (i, i * i)))
}

pondra_extension::export!(hello);
```

```bash
cargo build --release --target wasm32-unknown-unknown
wasm-tools component new target/wasm32-unknown-unknown/release/hello.wasm -o hello-0.1.wasm
```

```sql
INSTALL hello FROM '/path/to/hello-0.1.wasm';
SELECT greet('Ada'), add(2, 3);
SELECT * FROM count_to(3);
```

The guide is [Write an extension](https://alimardon123.github.io/pondra/guides/extensions/); `../hello` is the example
above, built by `tools/extension_fixture.sh` and run on every platform Pondra is built for (`tools/smoke.py`).
