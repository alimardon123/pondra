#!/usr/bin/env python3
"""One binary, kept small: the shipped program within its size budget (designs/redesign/redesign.md §10, step 0).

  size_check.py --bin PATH --platform linux-x64

Reads the budget for the platform from tools/size-budget.txt, which holds two numbers per platform: the
program's bytes as it ships (the dist profile, stripped), and the bytes it takes to download (gzip at level 6, as
the release's .tar.gz is made). Over either one, this fails and names both numbers. A change that needs more raises
the budget in tools/size-budget.txt in the same pull request, with a line saying why, so every byte added is a choice.
A platform without a line is reported, not checked. One JSON line goes to stdout, and the numbers to the job's summary.
"""
import argparse
import json
import os
import sys
import zlib

HERE = os.path.dirname(os.path.abspath(__file__))
BUDGET = os.path.join(HERE, "size-budget.txt")


def budgets(path=BUDGET):
    """Each platform's (program bytes, download bytes), from lines `platform program download` (`#` starts a comment)."""
    out = {}
    with open(path) as f:
        for line in f:
            line = line.split("#", 1)[0].split()
            if not line:
                continue
            platform, program, download = line
            out[platform] = (int(program), int(download))
    return out


def gzipped(path):
    """The program's bytes at gzip level 6, counted without keeping them."""
    z = zlib.compressobj(6, zlib.DEFLATED, 31)
    n = 0
    with open(path, "rb") as f:
        while chunk := f.read(1 << 20):
            n += len(z.compress(chunk))
    return n + len(z.flush())


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--bin", required=True)
    p.add_argument("--platform", required=True)
    a = p.parse_args()
    program, download = os.path.getsize(a.bin), gzipped(a.bin)
    budget = budgets().get(a.platform)
    out = {"platform": a.platform, "program": program, "download": download, "ok": True, "failed": []}
    if budget:
        out["program_budget"], out["download_budget"] = budget
        if program > budget[0]:
            out["failed"].append(f"the program is {program:,} bytes, over its budget of {budget[0]:,} (tools/size-budget.txt)")
        if download > budget[1]:
            out["failed"].append(f"its download is {download:,} bytes, over its budget of {budget[1]:,} (tools/size-budget.txt)")
        out["ok"] = not out["failed"]
    summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary:
        with open(summary, "a") as s:
            within = "no budget yet" if not budget else ("within its budget" if out["ok"] else "OVER its budget")
            s.write(f"Size ({a.platform}): the program {program / 1e6:.1f} MB, its download {download / 1e6:.1f} MB: {within}.\n")
    print(json.dumps(out))
    for f in out["failed"]:
        print(f"::error title=Size budget::{f}")
    return 0 if out["ok"] else 1


if __name__ == "__main__":
    sys.exit(main())
