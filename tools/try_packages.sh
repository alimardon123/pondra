#!/usr/bin/env bash
# The packages tools/package.py made in dist/ for one platform, installed as users get them and
# tried: the wheel in a new virtualenv, without pyarrow and then with it (tools/package_check.py), with
# uv, and with pip's --target;
# the npm packages in a new project (tools/package_check.mjs); and the one-line installer with
# this build's binary, which must leave `pondra` on PATH. CI runs it on each OS (Git Bash on Windows).
#
#   bash tools/try_packages.sh linux-x64
set -eo pipefail
platform=$1 root=$PWD try=$(mktemp -d)  # ($PWD, not $GITHUB_WORKSPACE: a /d/a/… path Git Bash globs and hands on)
python -m venv "$try/venv"
# The virtualenv's own Python, named, not "activated": activating it in Git Bash on CI's Windows
# (Python 3.11) didn't take, so python and pip were the runner's own and the wheel went there.
py=$try/venv/bin/python && [ -x "$py" ] || py=$try/venv/Scripts/python.exe
"$py" -m pip install -q "$root"/dist/*.whl
"$py" "$root/tools/package_check.py"  # (no pyarrow: rows as JSON)
# The wheel registers SQLAlchemy's dialects (pondra/dialect.py) without SQLAlchemy installed: only their names
"$py" -c "from importlib.metadata import entry_points; names = {e.name for e in entry_points(group='sqlalchemy.dialects')}; assert {'pondra', 'pondra.http', 'pondra.psycopg'} <= names, names"
"$py" -m pip install -q pyarrow
"$py" "$root/tools/package_check.py"
# uv installs the same wheel, and uvx runs its `pondra` without installing it
python -m pip install -q uv
python -m uv venv -q "$try/uvenv"
uvpy=$try/uvenv/bin/python && [ -x "$uvpy" ] || uvpy=$try/uvenv/Scripts/python.exe
python -m uv pip install -q --python "$uvpy" "$root"/dist/*.whl
"$uvpy" -c "import pondra, subprocess; print('uv:', subprocess.run([pondra.binary(), '--version'], capture_output=True, text=True, check=True).stdout.strip())"
python -m uv tool run --from "$(ls "$root"/dist/*.whl)" pondra --version
# pip install --target: the launcher, in target/bin, finds the library in target/pondra/_lib
python -m pip install -q --target "$try/target" "$root"/dist/*.whl
launcher=
for candidate in "$try"/target/bin/pondra "$try"/target/bin/pondra.exe "$try"/target/Scripts/pondra.exe; do
  [ -f "$candidate" ] && launcher=$candidate && break
done
[ -n "$launcher" ] && "$launcher" --version || { echo "pip install --target: no pondra that answers ($(ls "$try/target"))" >&2; exit 1; }

(cd "$try" && npm init -y > /dev/null &&
  npm install "$root"/dist/pondra-"$platform"-*.tgz "$root"/dist/pondra-[0-9]*.tgz &&
  cp "$root/tools/package_check.mjs" . && node package_check.mjs)

# The installer, from this build's archive: `pondra` found on PATH afterwards, as the installed one.
if [ "$platform" = windows-x64 ]; then
  [ -z "$CI" ] || powershell -NoProfile -ExecutionPolicy Bypass -File "$(cygpath -w "$root/tools/try_install.ps1")" \
    "$(cygpath -w "$root")" "$(cygpath -w "$try/installed")"  # (it sets the user's PATH: in CI only)
else
  # an archive whose SHA-256 isn't the one given is refused, and nothing is installed
  if PONDRA_SHA256=0000000000000000000000000000000000000000000000000000000000000000 SHELL=/bin/bash HOME="$try" PONDRA_INSTALL="$try/refused" PONDRA_ARCHIVE="$root/dist/pondra-$platform.tar.gz" sh "$root/install.sh" 2> "$try/refused.err"; then
    echo "install.sh installed an archive whose SHA-256 was wrong" >&2; exit 1
  fi
  grep -q "not installed" "$try/refused.err" && [ ! -e "$try/refused/pondra" ] || { echo "install.sh didn't refuse the wrong SHA-256 as it should:" >&2; cat "$try/refused.err" >&2; exit 1; }
  PONDRA_SHA256="$( (sha256sum "$root/dist/pondra-$platform.tar.gz" 2>/dev/null || shasum -a 256 "$root/dist/pondra-$platform.tar.gz") | cut -d' ' -f1)" SHELL=/bin/bash HOME="$try" PONDRA_INSTALL="$try/installed" PONDRA_ARCHIVE="$root/dist/pondra-$platform.tar.gz" sh "$root/install.sh"
  rc=$try/.bashrc; [ "$(uname -s)" != Darwin ] || rc=$try/.bash_profile
  found=$(env -i HOME="$try" PATH=/usr/bin:/bin bash -c ". '$rc' && command -v pondra && pondra --version")
  [ "$(echo "$found" | head -1)" = "$try/installed/pondra" ] || { echo "the installer left pondra off PATH: $found"; exit 1; }
fi
echo "packages and installer ok"
