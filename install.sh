#!/bin/sh
# Pondra for Linux and macOS, with nothing to set up: the latest release's binary into
# ~/.local/bin, and that folder on your PATH.
#
#   curl -fsSL https://github.com/alimardon123/pondra/releases/latest/download/install.sh | sh
#
# PONDRA_VERSION=0.22.1 for a given release; PONDRA_INSTALL=<folder> for another place;
# PONDRA_ARCHIVE=<file or URL> for a build of your own (CI tries this script that way);
# PONDRA_SHA256=<hex> to check an archive of your own.
set -eu
case "$(uname -s)-$(uname -m)" in
  Linux-x86_64) p=linux-x64 ;;
  Linux-aarch64 | Linux-arm64) p=linux-arm64 ;;
  Darwin-x86_64) p=macos-x64 ;;
  Darwin-arm64) p=macos-arm64 ;;
  *) echo "pondra: no build for $(uname -s) $(uname -m)" >&2; exit 1 ;;
esac
releases=https://github.com/alimardon123/pondra/releases
if [ -n "${PONDRA_VERSION:-}" ]; then at=download/v$PONDRA_VERSION; else at=latest/download; fi
archive=${PONDRA_ARCHIVE:-$releases/$at/pondra-$p.tar.gz}
dir=${PONDRA_INSTALL:-$HOME/.local/bin}

# fetch URL FILE: a download with curl, or with wget where there is no curl
fetch() {
  if command -v curl > /dev/null; then curl -fsSL "$1" -o "$2"; else wget -qO "$2" "$1"; fi
}

tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
case "$archive" in
  http*://*) fetch "$archive" "$tmp/p.tar.gz" ;;
  *) cp "$archive" "$tmp/p.tar.gz" ;;
esac

# The SHA-256 the archive must have: PONDRA_SHA256, or else the release's own line of its SHA256SUMS
# (release.yml makes the file). An archive of your own, with no hash given, isn't checked.
want=${PONDRA_SHA256:-}
if [ -z "$want" ] && [ -z "${PONDRA_ARCHIVE:-}" ]; then
  if fetch "$releases/$at/SHA256SUMS" "$tmp/sums" 2> /dev/null; then
    want=$(awk -v f="pondra-$p.tar.gz" '$2 == f {print $1}' "$tmp/sums")
    [ -n "$want" ] || { echo "pondra: SHA256SUMS lists no pondra-$p.tar.gz" >&2; exit 1; }
  elif [ -n "${PONDRA_VERSION:-}" ]; then
    echo "pondra: v$PONDRA_VERSION has no SHA256SUMS (releases before 0.34.0 carry none): not checked" >&2
  else
    echo "pondra: couldn't download the release's SHA256SUMS: not installed" >&2; exit 1
  fi
fi
if [ -n "$want" ]; then
  if command -v sha256sum > /dev/null; then got=$(sha256sum "$tmp/p.tar.gz" | cut -d' ' -f1)
  elif command -v shasum > /dev/null; then got=$(shasum -a 256 "$tmp/p.tar.gz" | cut -d' ' -f1)
  else echo "pondra: no sha256sum or shasum to check the download with" >&2; exit 1
  fi
  [ "$got" = "$want" ] || { echo "pondra: the download's SHA-256 is $got, not $want: not installed" >&2; exit 1; }
fi
tar -xzf "$tmp/p.tar.gz" -C "$tmp"
mkdir -p "$dir"
mv -f "$tmp/pondra" "$dir/pondra"
chmod +x "$dir/pondra"

# on PATH: in the startup file of the shell you use (for the terminals you open next)
case ":$PATH:" in
  *":$dir:"*) ;;
  *)
    case "$(basename "${SHELL:-sh}")" in
      zsh) rc=$HOME/.zshrc ;;
      bash) if [ "$(uname -s)" = Darwin ]; then rc=$HOME/.bash_profile; else rc=$HOME/.bashrc; fi ;;
      *) rc=$HOME/.profile ;;
    esac
    line="export PATH=\"$dir:\$PATH\"  # pondra"
    grep -qsF "$line" "$rc" || printf '\n%s\n' "$line" >> "$rc"
    echo "Added $dir to your PATH in $rc: open a new terminal, or run: export PATH=\"$dir:\$PATH\""
    ;;
esac
"$dir/pondra" --version
echo "Installed: try \`pondra\` (a SQL shell on ./lake)"
