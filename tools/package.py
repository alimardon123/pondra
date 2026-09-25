#!/usr/bin/env python3
"""Pondra's packages, from a built binary: a Python wheel and npm packages for one platform.

  package.py --bin target/x86_64-unknown-linux-gnu/dist/pondra --platform linux-x64 --out dist/
  package.py --npm-main --out dist/        # the `pondra` npm package itself (once, any platform)

The wheel is what `pip install pondra` gets: the Python client (`python/pondra`) and the binary,
installed next to Python as the `pondra` command (a "scripts" entry, as maturin's bin wheels do),
where `pondra.local()` finds it. The npm packages follow esbuild's pattern: `pondra` holds the
JavaScript client and depends, optionally, on `pondra-<platform>` packages that each hold one
binary, so npm installs only the one that fits. No build tools beyond Python and npm.
"""
import argparse, base64, hashlib, json, os, shutil, subprocess, sys, tempfile, zipfile

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


def wheel(binary, platform, out):
    v, tags = version(), PLATFORMS[platform][0]
    name = f"pondra-{v}-py3-none-{tags}.whl"
    exe = "pondra.exe" if platform.startswith("windows") else "pondra"
    readme = open(os.path.join(ROOT, "README.md"), encoding="utf-8").read().split("\n## ")[0]
    meta = "\n".join([
        "Metadata-Version: 2.1", "Name: pondra", f"Version: {v}",
        "Summary: Pondra, a streamhouse in one binary: the binary, and a Python client",
        "Requires-Python: >=3.9", "Provides-Extra: arrow", 'Requires-Dist: pyarrow; extra == "arrow"',
        "Description-Content-Type: text/markdown", "", readme])
    wheel_file = "\n".join(["Wheel-Version: 1.0", "Generator: pondra tools/package.py", "Root-Is-Purelib: false"] + [f"Tag: py3-none-{t}" for t in tags.split(".")]) + "\n"
    files = {"pondra/__init__.py": open(os.path.join(ROOT, "python/pondra/__init__.py"), "rb").read(),
             f"pondra-{v}.data/scripts/{exe}": open(binary, "rb").read(),
             f"pondra-{v}.dist-info/METADATA": meta.encode(), f"pondra-{v}.dist-info/WHEEL": wheel_file.encode()}
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


def npm_pack(folder, out):
    return subprocess.run(["npm", "pack", "--pack-destination", os.path.abspath(out)], cwd=folder, check=True, capture_output=True, text=True).stdout.strip().splitlines()[-1]


def npm_platform(binary, platform, out):
    _, os_, cpu = PLATFORMS[platform]
    exe = "pondra.exe" if platform.startswith("windows") else "pondra"
    with tempfile.TemporaryDirectory() as d:
        shutil.copy2(binary, os.path.join(d, exe))
        os.chmod(os.path.join(d, exe), 0o755)
        json.dump({"name": f"pondra-{platform}", "version": version(), "description": f"The pondra binary for {platform}: install `pondra` instead",
                   "os": [os_], "cpu": [cpu], "files": [exe], "license": "UNLICENSED"}, open(os.path.join(d, "package.json"), "w"), indent=2)
        return npm_pack(d, out)


def npm_main(out):
    with tempfile.TemporaryDirectory() as d:
        shutil.copytree(os.path.join(ROOT, "js"), d, dirs_exist_ok=True)
        pkg = json.load(open(os.path.join(d, "package.json")))
        pkg["version"] = version()
        pkg["optionalDependencies"] = {f"pondra-{p}": version() for p in PLATFORMS}
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
    made = [wheel(A.bin, A.platform, A.out), npm_platform(A.bin, A.platform, A.out)] if A.bin else []
    made += [npm_main(A.out)] if A.npm_main else []
    print("\n".join(made))
    sys.exit(0 if made else 1)
