#!/usr/bin/env bash
# The npm packages tools/package.py made in dist/, published: the platform packages first, then
# `pondra`, which depends on them. A version npm has already is skipped, so a release started
# again publishes only what's missing. Its arguments go to `npm publish`: the release passes
# --provenance; CI passes --dry-run on every push, so the command is tried before a tag needs it
# (skipped too once that version is out: npm refuses even a dry run over it, so every push after
# a release failed until the version moved on).
#
#   bash tools/npm_publish.sh --dry-run
set -eo pipefail
publish() {
  local file=$1 v tag=(); shift
  v=$(tar -xzOf "$file" package/package.json | node -p 'const p = JSON.parse(require("fs").readFileSync(0)); p.name + "@" + p.version')
  # (a release candidate, 0.80.0-rc.1, goes under the tag `next`: `npm install pondra` keeps the release, and
  # `npm install pondra@next` takes the candidate. npm refuses a pre-release with no tag)
  case ${v##*@} in *-*) tag=(--tag next) ;; esac
  if npm view "$v" version > /dev/null 2>&1; then echo "$v is published already"
  else npm publish "$file" --access public "${tag[@]}" "$@"; fi
}
# (./dist/…, not dist/…: npm reads `dist/x.tgz` as the GitHub repository "dist/x.tgz", and refuses it)
for p in ./dist/pondra-[a-z]*-*.tgz; do publish "$p" "$@"; done # (pondra-linux-x64-…: a platform starts with a letter, a version with a digit)
publish ./dist/pondra-[0-9]*.tgz "$@"
