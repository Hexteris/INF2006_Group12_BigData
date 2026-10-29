#!/usr/bin/env python3
"""Verify a corpus copy (e.g. downloaded from Google Drive) and arrange it into the raw layout.

Every file is matched by name against evidence/expected_corpus.csv and checked for size and
sha256. Folder structure in --from does not matter (Drive zips, nested folders are fine).

  --from DIR --to RAW      verify, then copy into RAW/<source>/<version>/acq_<date>/<file>
  --move                   move instead of copy (no second 13 GB copy on the laptop)
  --dry-run                verify only, copy nothing
  --check-only RAW         re-check an already arranged RAW tree (laptop or EMR /mnt)
  --sample                 only the fixed dev subset (see SAMPLE below)

Exit code 0 only if every expected file is present and correct. A report is written to
<RAW>/setup_report.txt. Stdlib only; runs on Windows and EMR.

  python scripts\\stage\\setup_corpus.py --from "G:\\My Drive\\INF2006_bigdata_project" --to raw
  python3 scripts/stage/setup_corpus.py --check-only /mnt/g12/raw
"""
import argparse
import csv
import hashlib
import os
import shutil
import socket
import sys
from datetime import datetime, timezone

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
EXPECTED = os.path.join(ROOT, "evidence", "expected_corpus.csv")

# Dev subset for laptops: all small sources, first 20 SEC filings, first GDELT week, smaller Wikinews part.
SAMPLE = {
    "prices": None, "sec_inventory": None, "sec_company_tickers": None,
    "sec_edgar": 20,
    "gdelt": ["2015070%d.export.CSV.zip" % d for d in range(1, 8)],
    "wikinews": ["enwikinews-20261001-pages-meta-history1.xml-p481754p3096188.bz2"],
}


def sha256(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def load_expected(sample):
    with open(EXPECTED, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    if not sample:
        return rows
    out, seen = [], {}
    for r in rows:
        rule = SAMPLE[r["source"]]
        seen[r["source"]] = seen.get(r["source"], 0) + 1
        if rule is None or (isinstance(rule, int) and seen[r["source"]] <= rule) \
                or (isinstance(rule, list) and r["file"] in rule):
            out.append(r)
    return out


def dest_of(raw, r):
    return os.path.join(raw, r["source"], r["version"], f"acq_{r['acq_date']}", r["file"])


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    g = p.add_mutually_exclusive_group(required=True)
    g.add_argument("--from", dest="src", help="folder holding the downloaded files (any nesting)")
    g.add_argument("--check-only", help="arranged raw folder to re-check")
    p.add_argument("--to", default="raw", help="target raw folder (default: ./raw)")
    p.add_argument("--move", action="store_true")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--sample", action="store_true")
    a = p.parse_args()

    expected = load_expected(a.sample)
    raw = os.path.abspath(a.check_only or a.to)

    # Candidate locations per file name.
    cands = {}
    if a.src:
        for d, _, names in os.walk(a.src):
            for n in names:
                cands.setdefault(n, []).append(os.path.join(d, n))
    else:
        for r in expected:
            cands[r["file"]] = [dest_of(raw, r)]

    ok, problems, per_src = 0, [], {}
    for i, r in enumerate(expected, 1):
        size, want = int(r["bytes"]), r["sha256"]
        found = None
        for path in cands.get(r["file"], []):
            if os.path.exists(path) and os.path.getsize(path) == size and sha256(path) == want:
                found = path
                break
        if found is None:
            paths = [x for x in cands.get(r["file"], []) if os.path.exists(x)]
            why = "missing" if not paths else f"wrong size/sha256 ({paths[0]})"
            problems.append(f"{r['source']}/{r['file']}: {why}")
        else:
            ok += 1
            per_src[r["source"]] = per_src.get(r["source"], 0) + 1
            dest = dest_of(raw, r)
            if a.src and not a.dry_run and os.path.abspath(found) != os.path.abspath(dest):
                os.makedirs(os.path.dirname(dest), exist_ok=True)
                (shutil.move if a.move else shutil.copy2)(found, dest)
        if i % 100 == 0:
            print(f"  checked {i}/{len(expected)}", flush=True)

    total = len(expected)
    lines = [f"setup_corpus {'check-only' if a.check_only else 'dry-run' if a.dry_run else 'arrange'}"
             f"{' sample' if a.sample else ''} host={socket.gethostname()} "
             f"utc={datetime.now(timezone.utc):%Y-%m-%dT%H:%M:%SZ}",
             f"target={raw}",
             f"expected_list={os.path.relpath(EXPECTED, ROOT)} sha256={sha256(EXPECTED)}"]
    lines += [f"  {s:<20} {n} ok" for s, n in sorted(per_src.items())]
    lines += [f"PROBLEM {x}" for x in problems]
    lines.append(f"{'OK' if not problems else 'FAILED'} {ok}/{total}")
    print("\n".join(lines))
    if not a.dry_run:
        os.makedirs(raw, exist_ok=True)
        with open(os.path.join(raw, "setup_report.txt"), "w", encoding="utf-8") as f:
            f.write("\n".join(lines) + "\n")
    sys.exit(0 if not problems else 1)


if __name__ == "__main__":
    main()
