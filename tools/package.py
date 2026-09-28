#!/usr/bin/env python3
"""Pondra's packages, from a built binary: a Python wheel, npm packages and the binary alone (what
the one-line installers, install.sh and install.ps1, download) for one platform.

  package.py --bin target/x86_64-unknown-linux-gnu/dist/pondra --platform linux-x64 --out dist/
  package.py --npm-main --out dist/        # the `pondra` npm package itself (once, any platform)

The wheel is what `pip install pondra` gets: the Python client (`python/pondra`) and the binary,
installed next to Python as the `pondra` command (a "scripts" entry, as maturin's bin wheels do),
where `pondra.local()` finds it. The npm packages follow esbuild's pattern: `pondra` holds the
JavaScript client and depends, optionally, on `pondra-<platform>` packages that each hold one
binary, so npm installs only the one that fits. No build tools beyond Python and npm.
"""
import argparse, base64, hashlib, json, os, shutil, subprocess, sys, tarfile, tempfile, zipfile

ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")
# platform -> (wheel tags, npm os, npm cpu)
PLATFORMS = {
    "linux-x64": ("manylinux_2_17_x86_64.manylinux2014_x86_64", "linux", "x64"),
    "linux-arm64": ("manylinux_2_17_aarch64.manylinux2014_aarch64", "linux", "arm64"),
    "macos-x64": ("macosx_10_12_x86_64", "darwin", "x64"),
    "macos-arm64": ("macosx_11_0_arm64", "darwin", "arm64"),
    "windows-x64": ("win_amd64", "win32", "x64"),
}


def version():
    for line in open(os.path.join(ROOT, "Cargo.toml")):
        if line.startswith("version"):
            return line.split('"')[1]


LICENSES = ["LICENSE-APACHE", "LICENSE-MIT"]  # (MIT OR Apache-2.0, at the user's choice)
REPO = {"type": "git", "url": "git+https://github.com/alimardon123/pondra.git"}  # (npm's provenance checks it's this repository)


def wheel(binary, platform, out):
    v, tags = version(), PLATFORMS[platform][0]
    name = f"pondra-{v}-py3-none-{tags}.whl"
    exe = "pondra.exe" if platform.startswith("windows") else "pondra"
    readme = open(os.path.join(ROOT, "README.md"), encoding="utf-8").read().split("\n## ")[0]
    meta = "\n".join([
        "Metadata-Version: 2.4", "Name: pondra", f"Version: {v}",
        "Summary: Pondra, a streamhouse in one binary: the binary, and a Python client", "Author: Alimardon",
        "License-Expression: MIT OR Apache-2.0", "License-File: LICENSE-APACHE", "License-File: LICENSE-MIT",
        "Project-URL: Repository, https://github.com/alimardon123/pondra",
        "Requires-Python: >=3.9", "Provides-Extra: arrow", 'Requires-Dist: pyarrow; extra == "arrow"',
        "Description-Content-Type: text/markdown", "", readme])
    wheel_file = "\n".join(["Wheel-Version: 1.0", "Generator: pondra tools/package.py", "Root-Is-Purelib: false"] + [f"Tag: py3-none-{t}" for t in tags.split(".")]) + "\n"
    src = os.path.join(ROOT, "python")
    client = sorted(os.path.relpath(os.path.join(d, f), src).replace(os.sep, "/") for d, _, fs in os.walk(os.path.join(src, "pondra")) for f in fs if f.endswith(".py"))
    files = {**{p: open(os.path.join(src, p), "rb").read() for p in client},  # (the client, its frames, pondra.spark, the worker and plpy)
             f"pondra-{v}.data/scripts/{exe}": open(binary, "rb").read(),
             f"pondra-{v}.dist-info/METADATA": meta.encode(), f"pondra-{v}.dist-info/WHEEL": wheel_file.encode(),
             **{f"pondra-{v}.dist-info/licenses/{n}": open(os.path.join(ROOT, n), "rb").read() for n in LICENSES}}
    digest = lambda b: "sha256=" + base64.urlsafe_b64encode(hashlib.sha256(b).digest()).rstrip(b"=").decode()
    record = "".join(f"{p},{digest(b)},{len(b)}\n" for p, b in files.items()) + f"pondra-{v}.dist-info/RECORD,,\n"
    files[f"pondra-{v}.dist-info/RECORD"] = record.encode()
    with zipfile.ZipFile(os.path.join(out, name), "w", zipfile.ZIP_DEFLATED) as z:
        for path, data in files.items():
            info = zipfile.ZipInfo(path, (2026, 1, 1, 0, 0, 0))
            info.external_attr = (0o100755 if path.endswith(exe) else 0o100644) << 16  # (a regular file; pip keeps the x bit)
            info.compress_type = zipfile.ZIP_DEFLATED
            z.writestr(info, data)
    return name


def archive(binary, platform, out):
    """The binary and its licenses, for install.sh (a .tar.gz) and install.ps1 (a .zip). No version
    in the name: the latest release's URL (`releases/latest/download/…`) stays the same."""
    exe = "pondra.exe" if platform.startswith("windows") else "pondra"
    files = {exe: binary, **{n: os.path.join(ROOT, n) for n in LICENSES}}
    if platform.startswith("windows"):
        name = f"pondra-{platform}.zip"
        with zipfile.ZipFile(os.path.join(out, name), "w", zipfile.ZIP_DEFLATED) as z:
            for n, path in files.items():
                z.write(path, n)
        return name
    name = f"pondra-{platform}.tar.gz"
    with tarfile.open(os.path.join(out, name), "w:gz") as t:
        for n, path in files.items():
            info = t.gettarinfo(path, n)
            info.mode, info.uid, info.gid, info.uname, info.gname = 0o755 if n == exe else 0o644, 0, 0, "", ""
            with open(path, "rb") as f:
                t.addfile(info, f)
    return name


def npm_pack(folder, out):
    npm = shutil.which("npm.cmd" if os.name == "nt" else "npm")  # (on Windows npm is npm.cmd, which a process started without a shell isn't looked up as)
    if not npm:
        sys.exit("npm is needed to make the npm packages")
    return subprocess.run([npm, "pack", "--pack-destination", os.path.abspath(out)], cwd=folder, check=True, capture_output=True, text=True).stdout.strip().splitlines()[-1]


def npm_platform(binary, platform, out):
    _, os_, cpu = PLATFORMS[platform]
    exe = "pondra.exe" if platform.startswith("windows") else "pondra"
    with tempfile.TemporaryDirectory() as d:
        shutil.copy2(binary, os.path.join(d, exe))
        os.chmod(os.path.join(d, exe), 0o755)
        json.dump({"name": f"pondra-{platform}", "version": version(), "description": f"The pondra binary for {platform}: install `pondra` instead",
                   "os": [os_], "cpu": [cpu], "files": [exe, *LICENSES], "license": "MIT OR Apache-2.0", "repository": REPO}, open(os.path.join(d, "package.json"), "w"), indent=2)
        for n in LICENSES:
            shutil.copy2(os.path.join(ROOT, n), d)
        return npm_pack(d, out)


def npm_main(out):
    with tempfile.TemporaryDirectory() as d:
        shutil.copytree(os.path.join(ROOT, "js"), d, dirs_exist_ok=True)
        pkg = json.load(open(os.path.join(d, "package.json")))
        pkg["version"] = version()
        pkg["optionalDependencies"] = {f"pondra-{p}": version() for p in PLATFORMS}
        for n in LICENSES:
            shutil.copy2(os.path.join(ROOT, n), d)
        json.dump(pkg, open(os.path.join(d, "package.json"), "w"), indent=2)
        return npm_pack(d, out)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--bin", help="the built pondra binary")
    ap.add_argument("--platform", choices=PLATFORMS)
    ap.add_argument("--npm-main", action="store_true", help="also (or only) the `pondra` npm package")
    ap.add_argument("--out", default="dist")
    A = ap.parse_args()
    os.makedirs(A.out, exist_ok=True)
    made = [wheel(A.bin, A.platform, A.out), npm_platform(A.bin, A.platform, A.out), archive(A.bin, A.platform, A.out)] if A.bin else []
    made += [npm_main(A.out)] if A.npm_main else []
    print("\n".join(made))
    sys.exit(0 if made else 1)
