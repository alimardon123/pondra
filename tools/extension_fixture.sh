#!/usr/bin/env bash
# Builds tests/extensions/words to tests/fixtures/extensions/words-0.1.wasm, the extension `src/wasm.rs`'s tests load.
# Needs the wasm32-unknown-unknown target (rustup target add wasm32-unknown-unknown) and wasm-tools (cargo install
# wasm-tools). Built for the bare target and made a component here, so it imports nothing: no WASI, as ADR-075 §2 asks.
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd)"
target="${CARGO_TARGET_DIR:-$root/target}/extensions"
(cd "$root/tests/extensions/words" && CARGO_TARGET_DIR="$target" cargo build --release --target wasm32-unknown-unknown)
mkdir -p "$root/tests/fixtures/extensions"
wasm-tools component new "$target/wasm32-unknown-unknown/release/words.wasm" -o "$root/tests/fixtures/extensions/words-0.1.wasm"
wasm-tools validate "$root/tests/fixtures/extensions/words-0.1.wasm"
ls -l "$root/tests/fixtures/extensions/words-0.1.wasm"
