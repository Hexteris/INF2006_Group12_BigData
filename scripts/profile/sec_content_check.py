#!/usr/bin/env python3
"""Task 1 read-only content check of the raw SEC complete-submission files.

Download checks proved the bytes arrived intact; this checks what is INSIDE each file
against the course inventory. Nothing is modified. Stdlib only.

Per file (from the SEC header and document blocks):
  - starts with <SEC-DOCUMENT> and ends with </SEC-DOCUMENT>   (truncation)
  - header ACCESSION NUMBER / CONFORMED SUBMISSION TYPE / FILED AS OF DATE /
    CENTRAL INDEX KEY agree with the inventory row
  - ACCEPTANCE-DATETIME (US Eastern) -> after_close flag (>= 16:00), used later for
    information-availability rules
  - PUBLIC DOCUMENT COUNT vs actual <DOCUMENT> blocks
  - main document present (a <TYPE> equal to the form), XBRL present
    (EX-101.INS or inline-XBRL markers), uuencoded binary docs, non-UTF-8 bytes

Outputs: <out>/sec_content_check.csv (one row per filing) and sec_content_summary.txt

Usage:
  python3 sec_content_check.py --manifest <sec_download_manifest.csv> \
      --filings <dir with the .txt files> --out <dir>
"""
import argparse
import csv
import os
import re
import sys
from collections import Counter
from datetime import datetime, timezone

HDR = {
    "accession": re.compile(rb"^ACCESSION NUMBER:\s*(\S+)", re.M),
    "form": re.compile(rb"^CONFORMED SUBMISSION TYPE:\s*(\S+)", re.M),
    "filed": re.compile(rb"^FILED AS OF DATE:\s*(\d{8})", re.M),
    "period": re.compile(rb"^CONFORMED PERIOD OF REPORT:\s*(\d{8})", re.M),
    "doc_count": re.compile(rb"^PUBLIC DOCUMENT COUNT:\s*(\d+)", re.M),
    "accepted": re.compile(rb"<ACCEPTANCE-DATETIME>(\d{14})"),
}
CIK_RE = re.compile(rb"CENTRAL INDEX KEY:\s*(\d+)")
TYPE_RE = re.compile(rb"^<TYPE>([^\r\n]+)", re.M)
UUE_RE = re.compile(rb"^begin \d{3} ", re.M)

FIELDS = ["local_file", "cik", "accession", "form", "filing_date", "bytes",
          "starts_ok", "ends_ok", "header_closed",
          "hdr_accession", "hdr_form", "hdr_filed", "hdr_period", "hdr_ciks",
          "accession_match", "form_match", "filed_match", "cik_match",
          "acceptance_et", "after_close_et",
          "hdr_doc_count", "doc_blocks", "doc_count_match",
          "main_doc_present", "xbrl_instance", "inline_xbrl", "uuencoded_docs",
          "non_utf8_bytes", "issues"]


def iso(d):
    return f"{d[:4]}-{d[4:6]}-{d[6:8]}" if d else ""


def check(path, inv):
    with open(path, "rb") as f:
        b = f.read()
    head = b[:200_000]  # header is always near the top
    g = {k: (m.group(1).decode("ascii", "replace") if (m := rx.search(head)) else "")
         for k, rx in HDR.items()}
    ciks = sorted({c.decode().zfill(10) for c in CIK_RE.findall(head.split(b"</SEC-HEADER>")[0])})
    types = [t.strip().decode("ascii", "replace") for t in TYPE_RE.findall(b)]
    blocks = b.count(b"\n<DOCUMENT>")
    try:
        b.decode("utf-8")
        bad = 0
    except UnicodeDecodeError:
        bad = sum(1 for x in b if x > 127) if len(b) < 50_000_000 else -1  # -1: large, not counted
        bad = bad if bad else 1
    acc = g["accepted"]
    r = {
        "local_file": os.path.basename(path), "cik": inv["cik"], "accession": inv["accession"],
        "form": inv["form"], "filing_date": inv["filing_date"], "bytes": len(b),
        "starts_ok": b.startswith(b"<SEC-DOCUMENT>"),
        "ends_ok": b.rstrip().endswith(b"</SEC-DOCUMENT>"),
        "header_closed": b"</SEC-HEADER>" in head,
        "hdr_accession": g["accession"], "hdr_form": g["form"], "hdr_filed": iso(g["filed"]),
        "hdr_period": iso(g["period"]), "hdr_ciks": "|".join(ciks),
        "acceptance_et": f"{iso(acc[:8])}T{acc[8:10]}:{acc[10:12]}:{acc[12:14]}" if acc else "",
        "after_close_et": (acc[8:12] >= "1600") if acc else "",
        "hdr_doc_count": g["doc_count"], "doc_blocks": blocks,
        "main_doc_present": inv["form"] in types,
        "xbrl_instance": any(t == "EX-101.INS" for t in types),
        "inline_xbrl": b"<ix:header" in b or b"<ix:nonFraction" in b,
        "uuencoded_docs": len(UUE_RE.findall(b)),
        "non_utf8_bytes": bad,
    }
    r["accession_match"] = r["hdr_accession"] == inv["accession"]
    r["form_match"] = r["hdr_form"] == inv["form"]
    r["filed_match"] = r["hdr_filed"] == inv["filing_date"]
    r["cik_match"] = inv["cik"] in ciks
    r["doc_count_match"] = str(blocks) == g["doc_count"]
    issues = [k for k in ("starts_ok", "ends_ok", "header_closed", "accession_match", "form_match",
                          "filed_match", "cik_match", "doc_count_match", "main_doc_present") if not r[k]]
    if not (r["xbrl_instance"] or r["inline_xbrl"]):
        issues.append("no_xbrl")
    r["issues"] = "|".join(issues)
    return r


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--manifest", required=True, help="sec_download_manifest.csv")
    p.add_argument("--filings", required=True)
    p.add_argument("--out", required=True)
    a = p.parse_args()
    os.makedirs(a.out, exist_ok=True)
    started = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    inv = {}
    with open(a.manifest, newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            if r["status"] == "ok":
                inv[r["local_file"]] = r
    rows = []
    for i, (fn, r) in enumerate(sorted(inv.items()), 1):
        path = os.path.join(a.filings, fn)
        if not os.path.exists(path):
            rows.append({"local_file": fn, "cik": r["cik"], "accession": r["accession"],
                         "form": r["form"], "filing_date": r["filing_date"], "issues": "file_missing"})
            continue
        rows.append(check(path, r))
        if i % 50 == 0:
            print(f"{i}/{len(inv)}", file=sys.stderr)

    out_csv = os.path.join(a.out, "sec_content_check.csv")
    with open(out_csv, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS)
        w.writeheader()
        w.writerows({k: r.get(k, "") for k in FIELDS} for r in rows)

    def n(key):
        return sum(1 for r in rows if r.get(key) is True)
    issue_counts = Counter(i for r in rows for i in r.get("issues", "").split("|") if i)
    mismatched = [r for r in rows if r.get("issues") and r["issues"] != "no_xbrl"]
    summary = [
        f"run_started_utc={started}",
        f"filings_checked={len(rows)}",
        f"starts_with_SEC-DOCUMENT={n('starts_ok')} ends_with_/SEC-DOCUMENT={n('ends_ok')} header_closed={n('header_closed')}",
        f"header_matches_inventory: accession={n('accession_match')} form={n('form_match')} "
        f"filing_date={n('filed_match')} cik={n('cik_match')}",
        f"public_document_count_matches_blocks={n('doc_count_match')}",
        f"main_document_present={n('main_doc_present')}",
        f"xbrl_instance_EX-101.INS={n('xbrl_instance')} inline_xbrl={n('inline_xbrl')} "
        f"any_xbrl={sum(1 for r in rows if r.get('xbrl_instance') or r.get('inline_xbrl'))}",
        f"accepted_at_or_after_16:00_ET={n('after_close_et')}",
        f"files_with_uuencoded_binary={sum(1 for r in rows if (r.get('uuencoded_docs') or 0) > 0)}",
        f"files_not_valid_utf8={sum(1 for r in rows if (r.get('non_utf8_bytes') or 0) != 0)}",
        f"multi_filer_headers={sum(1 for r in rows if '|' in str(r.get('hdr_ciks', '')))}",
        f"hdr_form_values={dict(Counter(r.get('hdr_form', '') for r in rows))}",
        f"issue_counts={dict(issue_counts)}",
        f"files_with_issues_other_than_no_xbrl={len(mismatched)}",
        f"run_finished_utc={datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')}",
    ]
    with open(os.path.join(a.out, "sec_content_summary.txt"), "w", encoding="utf-8") as f:
        f.write("\n".join(summary) + "\n")
    print("\n".join(summary))
    for r in mismatched[:15]:
        print("ISSUE", r["local_file"], r["issues"], r.get("hdr_form"), r.get("hdr_filed"), r.get("hdr_accession"))


if __name__ == "__main__":
    main()
