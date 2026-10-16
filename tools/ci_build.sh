#!/usr/bin/env bash
# The build workflow's run whose packages are this commit's: `tools/ci_build.sh [SHA [NAMES]]` prints
# its id, or nothing. NAMES are the packages a caller needs (`pondra-linux-x64`: a soak, a bench; the
# ones a push to main would make: build.yml); without them, every platform's, which only a version
# commit's run or one started by hand has (macOS Intel is built there alone).
# A run of this very commit if it built (and passed), else a passed run of a commit with the same tree: a pull request's run, when the merge to main changed nothing in it (the branch was
# up to date with main), so main never builds it twice (build.yml's `reuse`), and a release
# (release.yml) or the cluster bench (cluster-bench.yml) takes the pull request's packages, which
# its suite tested. Needs `gh` with GH_TOKEN (actions: read) and GITHUB_REPOSITORY. Never fails:
# nothing found prints nothing.
set -u
sha=${1:-$GITHUB_SHA}
name=${2:-}
repo=$GITHUB_REPOSITORY
tree_of() { gh api "repos/$repo/commits/$1" --jq .commit.tree.sha 2>/dev/null; }
# (a run that built: the five platforms' packages and `pondra` itself, or NAMES', not yet expired)
built() {
  local live
  live=$(gh api "repos/$repo/actions/runs/$1/artifacts" --jq '[.artifacts[] | select(.name | startswith("pondra-")) | select(.expired | not) | .name] | join(" ")' 2>/dev/null)
  if [ -z "$name" ]; then [ "$(wc -w <<< "$live")" -eq 6 ]; return; fi
  local n
  for n in $name; do [[ " $live " == *" $n "* ]] || return 1; done
}
tree=$(tree_of "$sha")
[ -n "$tree" ] || exit 0
declare -A trees=(["$sha"]="$tree")
runs=$(gh run list --repo "$repo" --workflow build.yml --status success --limit 60 --json databaseId,headSha --jq '.[] | "\(.databaseId) \(.headSha)"' 2>/dev/null)
for pass in own same; do # (this commit's own runs first)
  while read -r run head; do
    [ -n "$run" ] || continue
    if [ $pass = own ]; then [ "$head" = "$sha" ] || continue; else [ "$head" != "$sha" ] || continue; fi
    [ -n "${trees[$head]+x}" ] || trees[$head]=$(tree_of "$head")
    if [ "${trees[$head]}" = "$tree" ] && built "$run"; then echo "$run"; exit 0; fi
  done <<< "$runs"
done
exit 0
