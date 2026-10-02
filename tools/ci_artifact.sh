#!/usr/bin/env bash
# One platform's packages from the build workflow's run of a commit, into dist/:
#   tools/ci_artifact.sh linux-x64 [SHA]
# For a workflow that tries what the build made (deploy.yml) without building it again. The build
# uploads each platform's packages as soon as they are made, before its suite runs, so this waits
# for them while that run goes on (75 minutes at most), or takes a passed run of the same tree
# (`ci_build.sh`: a merge or a tag whose code a pull request's run built). Needs `gh` with
# GH_TOKEN (actions: read) and GITHUB_REPOSITORY.
set -euo pipefail
platform=$1 sha=${2:-$GITHUB_SHA} repo=$GITHUB_REPOSITORY
artifact() { gh api "repos/$repo/actions/runs/$1/artifacts" --jq ".artifacts[] | select(.name == \"pondra-$platform\" and (.expired | not)) | .id" 2>/dev/null | head -1; }
for _ in $(seq 150); do
  # (the newest run that builds: one stopped by a newer push, or skipped for a label, has nothing)
  read -r run status < <(gh run list --repo "$repo" --workflow build.yml --commit "$sha" --limit 10 --json databaseId,status,conclusion --jq '[.[] | select(.conclusion != "cancelled" and .conclusion != "skipped")][0] | "\(.databaseId) \(.status)"' 2>/dev/null || true) || true
  [ -n "${run:-}" ] && [ "$run" != null ] || { run=$(bash "$(dirname "$0")/ci_build.sh" "$sha"); status=completed; }
  id=$([ -n "$run" ] && artifact "$run" || true)
  if [ -z "$id" ] && [ "$status" = completed ]; then # (a push to main that built nothing, reusing a pull request's run: that run's)
    reused=$(bash "$(dirname "$0")/ci_build.sh" "$sha"); [ -z "$reused" ] || { run=$reused; id=$(artifact "$run" || true); }
  fi
  if [ -n "$id" ]; then
    mkdir -p dist
    gh api "repos/$repo/actions/artifacts/$id/zip" > "pondra-$platform.zip"
    unzip -qo "pondra-$platform.zip" -d dist && rm "pondra-$platform.zip"
    echo "pondra-$platform from https://github.com/$repo/actions/runs/$run:" && ls dist
    exit 0
  fi
  # (a pull request's push builds linux-x64 alone: build.yml's `plan` has made its jobs, and not this one)
  jobs=$([ -n "$run" ] && gh api "repos/$repo/actions/runs/$run/jobs" --jq '.jobs[] | "\(.name) \(.status)"' 2>/dev/null || true)
  if grep -qx 'plan completed' <<< "$jobs" && grep -q '^linux-x64' <<< "$jobs" && ! grep -q "^$platform " <<< "$jobs"; then
    echo "::error::build run $run builds linux-x64 alone: add the label full-ci to the pull request (or [full-ci] to a commit's message) to build $platform" && exit 1
  fi
  if [ -n "$run" ] && [ "$status" = completed ]; then
    echo "::error::build run $run of $sha ended without pondra-$platform's packages" && exit 1
  fi
  sleep 30
done
echo "::error::no pondra-$platform packages for $sha after 75 minutes" && exit 1
