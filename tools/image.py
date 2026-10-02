#!/usr/bin/env python3
"""The build context of Pondra's container image (deploy/docker/Dockerfile): a folder per
architecture, `amd64/` and `arm64/`, each with `pondra` and the wheel, from the packages a build or
a release made, or from a binary of your own.

  image.py --dist dist --out ctx               # the build workflow's packages (tools/package.py's)
  image.py --release 0.30.0 --out ctx          # a release's, from GitHub
  image.py --bin target/release/pondra --out ctx   # yours, for this machine's architecture
  docker buildx build -f deploy/docker/Dockerfile --platform linux/amd64,linux/arm64 ctx

Nothing is compiled: the image holds the binary the build tried and the suite tested (ADR-041).
"""
import argparse, glob, os, platform, shutil, sys, tarfile, urllib.request

RELEASES = "https://github.com/alimardon123/pondra/releases/download"
ARCHES = {"amd64": ("linux-x64", "x86_64"), "arm64": ("linux-arm64", "aarch64")}  # docker's name -> ours, the wheel's


def from_dist(dist, out):
    made = []
    for arch, (ours, wheel_arch) in ARCHES.items():
        archive = os.path.join(dist, f"pondra-{ours}.tar.gz")
        if not os.path.exists(archive):
            continue
        os.makedirs(os.path.join(out, arch), exist_ok=True)
        with tarfile.open(archive) as t:
            t.extract("pondra", os.path.join(out, arch), filter="data")
        for w in glob.glob(os.path.join(dist, f"pondra-*-manylinux_2_17_{wheel_arch}*.whl")):
            shutil.copy2(w, os.path.join(out, arch))
        made.append(arch)
    return made


def from_release(version, out):
    dist = os.path.join(out, ".release")
    os.makedirs(dist, exist_ok=True)
    for arch, (ours, wheel_arch) in ARCHES.items():
        wheel = f"pondra-{version}-py3-none-manylinux_2_17_{wheel_arch}.manylinux2014_{wheel_arch}.whl"
        for name in (f"pondra-{ours}.tar.gz", wheel):
            urllib.request.urlretrieve(f"{RELEASES}/v{version}/{name}", os.path.join(dist, name))
    made = from_dist(dist, out)
    shutil.rmtree(dist)
    return made


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    src = ap.add_mutually_exclusive_group(required=True)
    src.add_argument("--dist", help="a folder of tools/package.py's packages (the build workflow's artifacts)")
    src.add_argument("--release", help="a release's version, e.g. 0.30.0")
    src.add_argument("--bin", help="a Linux pondra binary (this machine's architecture, or --arch)")
    ap.add_argument("--arch", choices=ARCHES, default="arm64" if platform.machine() in ("aarch64", "arm64") else "amd64")
    ap.add_argument("--out", default="ctx")
    a = ap.parse_args()
    os.makedirs(a.out, exist_ok=True)
    if a.bin:
        os.makedirs(os.path.join(a.out, a.arch), exist_ok=True)
        shutil.copy2(a.bin, os.path.join(a.out, a.arch, "pondra"))
        made = [a.arch]
    else:
        made = from_dist(a.dist, a.out) if a.dist else from_release(a.release, a.out)
    print(f"{a.out}: {', '.join(made) or 'nothing'}")
    sys.exit(0 if made else 1)
