#!/bin/sh
# Pondra for Linux and macOS, with nothing to set up: the latest release's binary into
# ~/.local/bin, and that folder on your PATH.
#
#   curl -fsSL https://github.com/alimardon123/pondra/releases/latest/download/install.sh | sh
#
# PONDRA_VERSION=0.22.1 for a given release; PONDRA_INSTALL=<folder> for another place;
# PONDRA_ARCHIVE=<file or URL> for a build of your own (CI tries this script that way).
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

tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
case "$archive" in
  http*://*) if command -v curl > /dev/null; then curl -fsSL "$archive" -o "$tmp/p.tar.gz"; else wget -qO "$tmp/p.tar.gz" "$archive"; fi ;;
  *) cp "$archive" "$tmp/p.tar.gz" ;;
esac
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
