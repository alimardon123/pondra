#!/usr/bin/env python3
"""Pondra's mark and colours come from one place, brand/ (ADR-032), and nowhere keeps a copy.

  python3 tools/brand_check.py                 # the repository: one mark, colours in step
  python3 tools/brand_check.py --node          # and the console a node serves shows it
  python3 tools/brand_check.py --site site/dist  # and the docs site built shows it

It fails if:
  - brand/mark.svg's two colours (on light, and its dark `<style>`) aren't brand/colors.css's;
  - a file in the repository other than brand/mark.svg draws the mark or is a logo or favicon;
  - the console's page and style sheet don't take its mark, icon and colours from brand/ (`{{mark}}`,
    `{{favicon}}` in src/console/index.html, `/*{{colors}}*/` in console.css), or what a node serves
    doesn't hold them exactly;
  - the docs site doesn't make its header's marks and favicon from brand/, or the built site's
    aren't brand/mark.svg (its favicon as it is, its header's light and dark in colors.css's).
"""
import argparse, os, re, subprocess, sys, tempfile, time, urllib.parse, urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
MARK = open(os.path.join(ROOT, "brand", "mark.svg"), encoding="utf-8").read()
COLORS = open(os.path.join(ROOT, "brand", "colors.css"), encoding="utf-8").read()


def colour(name):
    m = re.search(rf"--{name}:\s*(#[0-9a-fA-F]{{6}})", COLORS)
    return m.group(1).upper() if m else None


def variant(c):
    """The mark in one colour, for a page that switches it itself (as astro.config.mjs makes it)."""
    return re.sub(r' color="#[0-9a-fA-F]{6}"', f' color="{c}"', re.sub(r"<style>[\s\S]*?</style>\n?", "", MARK, count=1), count=1)


def repository(checks):
    light = re.search(r' color="(#[0-9a-fA-F]{6})"', MARK)
    dark = re.search(r"prefers-color-scheme:\s*dark\)\s*\{\s*\.pondra-mark\s*\{\s*color:\s*(#[0-9a-fA-F]{6})", MARK)
    checks["brand/mark.svg's colours are brand/colors.css's (on light, on dark)"] = bool(light and dark) \
        and light.group(1).upper() == colour("pondra-mark") and dark.group(1).upper() == colour("pondra-mark-dark")
    checks["brand/mark.svg draws in currentColor, its root class pondra-mark"] = 'class="pondra-mark"' in MARK and "currentColor" in MARK
    tracked = subprocess.run(["git", "ls-files"], cwd=ROOT, capture_output=True, text=True, check=True).stdout.split()
    shape = re.findall(r"<(?:ellipse|path|circle|rect|polygon)\b[^>]*>", MARK)
    copies = []
    for f in tracked:
        if f == "brand/mark.svg" or not os.path.exists(os.path.join(ROOT, f)):
            continue
        name = os.path.basename(f).lower()
        if f.endswith(".svg") and ("logo" in name or "favicon" in name or "mark" in name):
            copies.append(f)
            continue
        if f.endswith((".svg", ".html", ".astro", ".mdx", ".md", ".rs", ".js", ".mjs", ".py", ".css")) and shape:
            text = open(os.path.join(ROOT, f), encoding="utf-8", errors="replace").read()
            if f != "tools/brand_check.py" and all(s in text for s in shape):
                copies.append(f)
    checks["no copy of the mark, and no other logo or favicon, anywhere else in the repository"] = not copies
    if copies:
        print("copies:", copies, file=sys.stderr)
    page = open(os.path.join(ROOT, "src", "console", "index.html"), encoding="utf-8").read()
    css = open(os.path.join(ROOT, "src", "console", "console.css"), encoding="utf-8").read()
    rs = open(os.path.join(ROOT, "src", "console.rs"), encoding="utf-8").read()
    checks["the console's page takes its mark, icon and colours from brand/"] = "{{mark}}" in page and "{{favicon}}" in page \
        and css.startswith("/*{{colors}}*/") and 'include_str!("../brand/mark.svg")' in rs and 'include_str!("../brand/colors.css")' in rs \
        and "<svg" not in page.split('class="brand"', 1)[1][:80]
    astro = open(os.path.join(ROOT, "site", "astro.config.mjs"), encoding="utf-8").read()
    checks["the docs site makes its header's marks and favicon from brand/, and its accent from colors.css"] = "brand('mark.svg')" in astro and "brand('colors.css')" in astro \
        and "'../brand/colors.css'" in astro and "public/favicon.svg" in astro and "mark-light.svg" in astro and "mark-dark.svg" in astro


def node(checks):
    """The page a node serves: the mark itself in the header, and as the tab's icon."""
    binary = os.environ.get("PONDRA_BIN") or os.path.join(ROOT, "target", "release", "pondra")
    lake, port = tempfile.mkdtemp(prefix="pondra-brand-"), 8940
    p = subprocess.Popen([binary, "serve", "--lake", os.path.join(lake, "lake"), "--addr", f"127.0.0.1:{port}"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    try:
        html = None
        for _ in range(200):
            try:
                html = urllib.request.urlopen(f"http://127.0.0.1:{port}/", timeout=2).read().decode()
                break
            except Exception:
                time.sleep(0.1)
        icon = re.search(r'<link rel="icon" href="data:image/svg\+xml,([^"]*)"', html or "")
        shown = urllib.parse.unquote(icon.group(1)) if icon else ""
        css = urllib.request.urlopen(f"http://127.0.0.1:{port}/console/console.css", timeout=5).read().decode() if html else ""
        checks["a node's console shows brand/mark.svg in its header and as its icon, and colors.css's colours"] = bool(html) and MARK.strip() in html \
            and shown.replace("'", '"') == MARK.replace("'", '"') and COLORS.strip() in css and "{{" not in html + css
    finally:
        p.terminate()
        p.wait(10)
        subprocess.run(["rm", "-rf", lake])


def site(checks, dist):
    """The built site: its favicon brand/mark.svg itself; its header's marks the light and dark ones."""
    favicon = open(os.path.join(dist, "favicon.svg"), encoding="utf-8").read() if os.path.exists(os.path.join(dist, "favicon.svg")) else ""
    index = open(os.path.join(dist, "index.html"), encoding="utf-8").read()
    base = "/" + index.split('href="/', 1)[1].split("/", 1)[0] + "/" if 'href="/' in index else "/"
    imgs = re.findall(r'<a[^>]*class="site-title[^"]*"[^>]*>(.*?)</a>', index, re.S)
    srcs = re.findall(r'src="([^"]+\.svg)"', imgs[0]) if imgs else []
    made = [open(os.path.join(dist, s[len(base):] if s.startswith(base) else s.lstrip("/")), encoding="utf-8").read() for s in srcs]
    checks["the built site's favicon is brand/mark.svg, and its header shows it in colors.css's light and dark"] = favicon == MARK \
        and sorted(made) == sorted([variant(colour("pondra-mark")), variant(colour("pondra-mark-dark"))])


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--node", action="store_true", help="also start a node and check the console it serves")
    ap.add_argument("--site", help="also check the built docs site in this folder (site/dist)")
    a = ap.parse_args()
    checks = {}
    repository(checks)
    if a.node:
        node(checks)
    if a.site:
        site(checks, a.site)
    for k, v in checks.items():
        print(("ok    " if v else "FAIL  ") + k)
    sys.exit(0 if all(checks.values()) else 1)


if __name__ == "__main__":
    main()
