#!/usr/bin/env bash
# The packages tools/package.py made in dist/ for one platform, installed as users get them and
# tried: the wheel in a new virtualenv (tools/package_check.py), the npm packages in a new project
# (tools/package_check.mjs). CI runs it on each OS (Git Bash on Windows).
#
#   bash tools/try_packages.sh linux-x64
set -eo pipefail
platform=$1 root=$PWD try=$(mktemp -d)  # ($PWD, not $GITHUB_WORKSPACE: a /d/a/… path Git Bash globs and hands on)
python -m venv "$try/venv"
source "$try/venv/bin/activate" 2>/dev/null || source "$try/venv/Scripts/activate"  # (Scripts on Windows)
pip install -q "$root"/dist/*.whl pyarrow
python "$root/tools/package_check.py"
cd "$try" && npm init -y > /dev/null
npm install "$root"/dist/pondra-"$platform"-*.tgz "$root"/dist/pondra-[0-9]*.tgz
cp "$root/tools/package_check.mjs" . && node package_check.mjs
