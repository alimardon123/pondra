#!/usr/bin/env bash
# The page form's module (web/, designs/l2-b4-web.md) into web/pkg/: pondra_web.js and pondra_web_bg.wasm, an ES
# module for a browser or Node (wasm-bindgen's `--target web`). Needs the wasm32-unknown-unknown target and
# wasm-bindgen's command line at web/Cargo.lock's version; wasm-opt (binaryen) shrinks it further when it is there.
set -euo pipefail
cd "$(dirname "$0")/../web"
want=$(grep -A1 '^name = "wasm-bindgen"$' Cargo.lock | sed -n 's/^version = "\(.*\)"/\1/p')
have=$(wasm-bindgen --version 2>/dev/null | awk '{print $2}' || true)
[ "$have" = "$want" ] || { echo "wasm-bindgen $want is needed (have: ${have:-none}): cargo install wasm-bindgen-cli --version $want --locked" >&2; exit 1; }
cargo build --release
wasm-bindgen --target web --remove-name-section --remove-producers-section --out-dir pkg "${CARGO_TARGET_DIR:-target}/wasm32-unknown-unknown/release/pondra_web.wasm"
if command -v wasm-opt >/dev/null; then
  wasm-opt -Oz --enable-bulk-memory --enable-nontrapping-float-to-int --enable-sign-ext pkg/pondra_web_bg.wasm -o pkg/pondra_web_bg.wasm
fi
ls -l pkg/pondra_web_bg.wasm
