#!/usr/bin/env bash
# Builds the extensions `src/wasm.rs`'s tests load, to tests/fixtures/extensions/: tests/extensions/words (words-0.1.wasm,
# scalars and tables), tests/extensions/clocked (clocked-0.1.wasm, the clock and the log: E5), and needs-http.wasm from
# tests/extensions/needs-http.wat (a part needing a capability 1.0 doesn't offer). Needs the wasm32-unknown-unknown target
# (rustup target add wasm32-unknown-unknown) and wasm-tools (cargo install wasm-tools). Built for the bare target and made a
# component here, so a part imports only the capabilities it uses: no WASI, as ADR-075 §2 asks.
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd)"
target="${CARGO_TARGET_DIR:-$root/target}/extensions"
out="$root/tests/fixtures/extensions"
mkdir -p "$out"
for part in words clocked; do
  (cd "$root/tests/extensions/$part" && CARGO_TARGET_DIR="$target" cargo build --release --target wasm32-unknown-unknown)
  wasm-tools component new "$target/wasm32-unknown-unknown/release/$part.wasm" -o "$out/$part-0.1.wasm"
  wasm-tools validate "$out/$part-0.1.wasm"
done
wasm-tools parse "$root/tests/extensions/needs-http.wat" -o "$out/needs-http.wasm"
wasm-tools validate "$out/needs-http.wasm"
ls -l "$out"
