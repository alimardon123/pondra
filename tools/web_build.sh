#!/usr/bin/env bash
# The page form's module (web/, designs/l2-b4-web.md) into web/pkg/: pondra_web.js and pondra_web_bg.wasm, an ES
# module for a browser or Node (wasm-bindgen's `--target web`). Needs the wasm32-unknown-unknown target and
# wasm-bindgen's command line at web/Cargo.lock's version. No wasm-opt: binaryen 108's -Oz took the module from 62.5 to
# 55.2 MB but its brotli from 8.98 to 9.86 MB (gzip 15.1 to 15.9), and a page downloads the compressed bytes (10-31).
# The module is the core's lake (web/Cargo.toml's `pondra` with `lake`), and it links with thin LTO (16 codegen units, its
# dependencies as built): fat LTO of it needs more than the 15 GB a CI runner or a laptop has, and was killed.
set -euo pipefail
cd "$(dirname "$0")/../web"
out=pkg
want=$(grep -A1 '^name = "wasm-bindgen"$' Cargo.lock | sed -n 's/^version = "\(.*\)"/\1/p')
have=$(wasm-bindgen --version 2>/dev/null | awk '{print $2}' || true)
[ "$have" = "$want" ] || { echo "wasm-bindgen $want is needed (have: ${have:-none}): cargo install wasm-bindgen-cli --version $want --locked" >&2; exit 1; }
cargo rustc --release --lib -- -C lto=thin -C codegen-units=16
wasm-bindgen --target web --remove-name-section --remove-producers-section --out-dir "$out" "${CARGO_TARGET_DIR:-target}/wasm32-unknown-unknown/release/pondra_web.wasm"
ls -l "$out/pondra_web_bg.wasm"
