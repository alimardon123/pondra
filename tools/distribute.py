#!/usr/bin/env python3
"""The files that put a Pondra release into Homebrew, winget and Scoop: a formula for the tap
alimardon123/homebrew-pondra, the manifests for microsoft/winget-pkgs and a manifest for a Scoop
bucket. Each names the release's archives (`archive()` in tools/package.py) by URL and SHA-256.
Nothing is published or built here: it writes files, and whoever releases puts them where they go.

  distribute.py --version 0.30.0 --release --out out/      # SHA-256s from the release's own archives (downloaded)
  distribute.py --version 0.32.0 --dist dist --out out/    # from a folder holding the five archives (the build workflow's packages)
  distribute.py --version 0.32.0 --dist dist --url-base file:///tmp/dist   # URLs (and --release's downloads) from there, to try the files before a release exists

Writes out/homebrew/pondra.rb (`brew install alimardon123/pondra/pondra`), out/winget/manifests/p/Pondra/
Pondra/<version>/*.yaml (`winget install Pondra.Pondra`) and out/scoop/pondra.json. Exit 1, writing
nothing, if an archive is missing.
"""
import argparse, hashlib, json, os, sys, urllib.request
from string import Template

REPO = "https://github.com/alimardon123/pondra"
RELEASES = f"{REPO}/releases/download"
HOME = "https://alimardon123.github.io/pondra/"
DESC = "Streaming store, lakehouse and SQL engine in one binary, on object storage"  # (Homebrew: 80 at most, no article first)
PLATFORMS = ["linux-x64", "linux-arm64", "macos-x64", "macos-arm64", "windows-x64"]  # tools/package.py's names
WINGET = "1.9.0"  # the manifest schema's version


def archive(platform):
    return f"pondra-{platform}.{'zip' if platform.startswith('windows') else 'tar.gz'}"


def digest(f):
    h = hashlib.sha256()
    while chunk := f.read(1 << 20):  # (45 MB archives: never all in memory)
        h.update(chunk)
    return h.hexdigest()


def checksums(base, dist, partial=False):
    """Every archive's SHA-256, from a folder or from where the files are served. A missing one is
    reported with the rest, so one run says everything that is wrong; with `partial` (trying the
    files on one machine, never for a release) it gets zeros instead."""
    sums, missing = {}, []
    for platform in PLATFORMS:
        name = archive(platform)
        try:
            with open(os.path.join(dist, name), "rb") if dist else urllib.request.urlopen(f"{base}/{name}", timeout=60) as f:
                sums[name] = digest(f)
            print(f"sha256  {sums[name]}  {name}", flush=True)
        except OSError as e:  # (HTTP's 404 and a file that isn't there are both OSErrors)
            if partial:
                sums[name] = "0" * 64
                continue
            missing.append(f"{name} ({e})")
    return sums, missing


def on(os_, base, sums):
    """Homebrew's `on_macos` / `on_linux`: the archive for each CPU, picked by the machine installing."""
    lines = [f"  on_{os_} do"]
    for cpu, arch in (("arm", "arm64"), ("intel", "x64")):
        name = archive(f"{os_}-{arch}")
        lines += [f"    on_{cpu} do", f'      url "{base}/{name}"', f'      sha256 "{sums[name]}"', "    end"]
    return "\n".join(lines + ["  end"])


FORMULA = Template('''class Pondra < Formula
  desc "$desc"
  homepage "$home"
  version "$version"
  license any_of: ["MIT", "Apache-2.0"]

$macos

$linux

  def install
    bin.install "pondra"
  end

  # `brew services start pondra`: a node on a lake of its own, at http://127.0.0.1:8080
  service do
    run [opt_bin/"pondra", "serve", "--lake", var/"pondra/lake", "--addr", "127.0.0.1:8080"]
    keep_alive true
    working_dir var/"pondra"
    log_path var/"log/pondra.log"
    error_log_path var/"log/pondra.log"
  end

  test do
    assert_match version.to_s, shell_output("#{bin}/pondra --version")
    # a folder becomes a lake with its first table: `sql` alone refuses one that holds none
    system bin/"pondra", "sql", "--lake", testpath/"lake", "CREATE TABLE t AS SELECT 42 AS x"
    assert_match "42", shell_output("#{bin}/pondra sql --lake #{testpath}/lake 'SELECT x FROM t'")
  end
end
''')


def homebrew(version, base, sums):
    return FORMULA.substitute(desc=DESC, home=HOME, version=version, macos=on("macos", base, sums), linux=on("linux", base, sums))


def winget(version, url, sha, date):
    """The three files winget-pkgs wants: the version, the installer and the default locale."""
    def manifest(kind, body):
        return (f"# yaml-language-server: $schema=https://aka.ms/winget-manifest.{kind}.{WINGET}.schema.json\n\n"
                f"PackageIdentifier: Pondra.Pondra\nPackageVersion: {version}\n{body}ManifestType: {kind}\nManifestVersion: {WINGET}\n")
    installer = ("InstallerType: zip\nNestedInstallerType: portable\nNestedInstallerFiles:\n"  # (a zip holding one portable exe, put on the PATH as `pondra`)
                 "- RelativeFilePath: pondra.exe\n  PortableCommandAlias: pondra\n"
                 f"Installers:\n- Architecture: x64\n  InstallerUrl: {url}\n  InstallerSha256: {sha.upper()}\n"
                 + (f"ReleaseDate: {date}\n" if date else ""))
    tags = ["database", "sql", "lakehouse", "streaming", "kafka", "parquet", "iceberg", "delta-lake", "cli"]
    locale = ("PackageLocale: en-US\nPublisher: Pondra\nPackageName: Pondra\n"
              f"PackageUrl: {HOME}\nLicense: MIT OR Apache-2.0\nLicenseUrl: {REPO}/blob/main/LICENSE-MIT\n"
              f"ShortDescription: {json.dumps(DESC)}\n"  # (quoted: a colon and a space would start a mapping)
              f"ReleaseNotesUrl: {REPO}/releases/tag/v{version}\nMoniker: pondra\nTags:\n" + "".join(f"- {t}\n" for t in tags))
    return {"Pondra.Pondra.yaml": manifest("version", "DefaultLocale: en-US\n"),
            "Pondra.Pondra.installer.yaml": manifest("installer", installer),
            "Pondra.Pondra.locale.en-US.yaml": manifest("defaultLocale", locale)}


def scoop(version, url, sha):
    # (`$version` is Scoop's own, filled in by its autoupdate; a dual license is written with a bar)
    update = {"architecture": {"64bit": {"url": f"{RELEASES}/v$version/{archive('windows-x64')}"}}}
    return json.dumps({"version": version, "description": DESC, "homepage": HOME, "license": "MIT|Apache-2.0",
                       "architecture": {"64bit": {"url": url, "hash": sha}}, "bin": "pondra.exe",
                       "checkver": {"github": REPO}, "autoupdate": update}, indent=4) + "\n"


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--version", required=True, help="the release's version, e.g. 0.30.0")
    src = ap.add_mutually_exclusive_group(required=True)
    src.add_argument("--release", action="store_true", help="take the SHA-256s from the release's archives, downloaded")
    src.add_argument("--dist", help="a folder holding the five archives (tools/package.py's, the build workflow's)")
    ap.add_argument("--url-base", help="where the archives are served, instead of the release's page (file:///… works)")
    ap.add_argument("--date", help="the release's date (YYYY-MM-DD), for winget's ReleaseDate; left out if not given")
    ap.add_argument("--partial", action="store_true", help="archives not there get a SHA-256 of zeros: to try the files on one machine (deploy.yml), never for a release")
    ap.add_argument("--out", default="out")
    a = ap.parse_args()
    version = a.version.lstrip("v")
    base = (a.url_base or f"{RELEASES}/v{version}").rstrip("/")
    sums, missing = checksums(base, a.dist, a.partial)
    if missing:
        sys.exit("missing archives:\n  " + "\n  ".join(missing))
    win = archive("windows-x64")
    files = {os.path.join("homebrew", "pondra.rb"): homebrew(version, base, sums),
             os.path.join("scoop", "pondra.json"): scoop(version, f"{base}/{win}", sums[win]),
             **{os.path.join("winget", "manifests", "p", "Pondra", "Pondra", version, n): t
                for n, t in winget(version, f"{base}/{win}", sums[win], a.date).items()}}
    for path, text in files.items():
        path = os.path.join(a.out, path)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", newline="\n") as f:
            f.write(text)
        print("wrote", path)


if __name__ == "__main__":
    main()
