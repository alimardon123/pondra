#!/usr/bin/env bash
# One platform's packages from the build workflow's run of a commit, into dist/:
#   tools/ci_artifact.sh linux-x64 [SHA]
# For a workflow that tries what the build made, started apart from it (deploy-check.yml on a tag,
# a test started by hand), without building it again. The build uploads each platform's packages
# as soon as they are made, before its suite runs, so this waits for them while that run goes on
# (`WAIT_MINUTES`, 75), or takes a passed run of the same tree
# (`ci_build.sh`: a merge or a tag whose code a pull request's run built). A pull request's `ci`
# run names its packages `pr-linux-x64` (the release profile, for the runner's glibc), so they are
# never taken for what ships. Needs `gh` with GH_TOKEN (actions: read) and GITHUB_REPOSITORY.
set -euo pipefail
platform=$1 sha=${2:-$GITHUB_SHA} repo=$GITHUB_REPOSITORY
artifact() { gh api "repos/$repo/actions/runs/$1/artifacts" --jq ".artifacts[] | select(.name == \"pondra-$platform\" and (.expired | not)) | .id" 2>/dev/null | head -1; }
# (once a minute: every wait polls the API, and the repository's allowance is 1,000 calls an hour)
gone=0
for _ in $(seq ${WAIT_MINUTES:-75}); do
  # (the newest run that builds: one stopped by a newer push, or skipped for a label, has nothing; and how many were stopped)
  read -r run status stopped < <(gh run list --repo "$repo" --workflow build.yml --commit "$sha" --limit 10 --json databaseId,status,conclusion --jq '([.[] | select(.conclusion != "cancelled" and .conclusion != "skipped")][0]) as $r | "\($r.databaseId) \($r.status) \([.[] | select(.conclusion == "cancelled")] | length)"' 2>/dev/null || true) || true
  [ -n "${run:-}" ] && [ "$run" != null ] || { run=$(bash "$(dirname "$0")/ci_build.sh" "$sha" "pondra-$platform"); status=completed; }
  id=$([ -n "$run" ] && artifact "$run" || true)
  if [ -z "$id" ] && [ "$status" = completed ]; then # (a push to main that built nothing, reusing a pull request's run: that run's)
    reused=$(bash "$(dirname "$0")/ci_build.sh" "$sha" "pondra-$platform"); [ -z "$reused" ] || { run=$reused; id=$(artifact "$run" || true); }
  fi
  if [ -n "$id" ]; then
    mkdir -p dist
    gh api "repos/$repo/actions/artifacts/$id/zip" > "pondra-$platform.zip"
    unzip -qo "pondra-$platform.zip" -d dist && rm "pondra-$platform.zip"
    echo "pondra-$platform from https://github.com/$repo/actions/runs/$run:" && ls dist
    exit 0
  fi
  # (a run that doesn't build this platform: a pull request's `ci` builds linux-x64 alone, its
  # `full-ci` all but macOS Intel, and main builds macOS Intel only on a version commit or by hand.
  # build.yml's `plan` has made its jobs, and not this one)
  jobs=$([ -n "$run" ] && gh api "repos/$repo/actions/runs/$run/jobs" --jq '.jobs[] | "\(.name) \(.status)"' 2>/dev/null || true)
  if grep -qx 'plan completed' <<< "$jobs" && grep -q '^linux-x64' <<< "$jobs" && ! grep -q "^$platform " <<< "$jobs"; then
    echo "::error::build run $run doesn't build $platform: a pull request's full-ci builds all but macos-x64, and the build workflow started by hand builds every platform" && exit 1
  fi
  if [ -n "$run" ] && [ "$status" = completed ]; then
    echo "::error::build run $run of $sha ended without pondra-$platform's packages" && exit 1
  fi
  # (main builds only its newest commit: a run still waiting when a newer push comes is stopped, and that commit
  # never builds. Five minutes of nothing but stopped runs, and no passed run of its tree, is that: say so, rather
  # than wait out the 75 minutes. 10-26's soak was a few minutes from it)
  if [ -z "$run" ] && [ "${stopped:-0}" -gt 0 ]; then gone=$((gone + 1)); else gone=0; fi
  if [ "$gone" -ge 5 ]; then
    echo "::error::every build run of $sha was stopped (main builds only its newest commit, and a newer push came before this one's run started), so it has no packages: start this on a commit whose build runs, such as main's newest" && exit 1
  fi
  sleep 60
done
echo "::error::no pondra-$platform packages for $sha after ${WAIT_MINUTES:-75} minutes" && exit 1
