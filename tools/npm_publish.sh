#!/usr/bin/env bash
# The npm packages tools/package.py made in dist/, published: the platform packages first, then
# `pondra`, which depends on them. A version npm has already is skipped, so a release started
# again publishes only what's missing. Its arguments go to `npm publish`: the release passes
# --provenance; CI passes --dry-run on every push, so the command is tried before a tag needs it.
#
#   bash tools/npm_publish.sh --dry-run
set -eo pipefail
publish() {
  local file=$1 v; shift
  v=$(tar -xzOf "$file" package/package.json | node -p 'const p = JSON.parse(require("fs").readFileSync(0)); p.name + "@" + p.version')
  if [[ " $* " != *" --dry-run "* ]] && npm view "$v" version > /dev/null 2>&1; then echo "$v is published already"
  else npm publish "$file" --access public "$@"; fi
}
# (./dist/…, not dist/…: npm reads `dist/x.tgz` as the GitHub repository "dist/x.tgz", and refuses it)
for p in ./dist/pondra-*-*.tgz; do publish "$p" "$@"; done
publish ./dist/pondra-[0-9]*.tgz "$@"
