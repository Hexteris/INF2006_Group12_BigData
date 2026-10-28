#!/usr/bin/env python3
"""Validate the course SEC filing inventory and download every listed filing (Task 1).

Inputs: the course inventory as .csv or .xlsx (both are supplied; pass one as
--inventory and the other as --cross-check to prove they list the same filings).
Stdlib only (no pandas/openpyxl), so it runs unchanged on the EMR primary node.

Outputs under --out:
  filings/<CIK10>_<ACCESSION>_<FORM>.txt         original bytes as served by SEC
  _manifests/sec_inventory_as_read.csv          every inventory row: raw values + normalised fields + notes
  _manifests/sec_inventory_checks.csv           one row per failed inventory check (empty = clean)
  _manifests/sec_inventory_crosscheck.csv       differences between --inventory and --cross-check
  _manifests/sec_download_manifest.csv          one row per download attempt: status, bytes, sha256, time
  _manifests/sec_unavailable.csv                URLs that could not be retrieved
  _manifests/sec_acquisition_summary.txt        counts for the report

Identifiers are kept as text. Unavailable URLs are reported, never substituted.
Re-running resumes: URLs whose 'ok' manifest row still matches the file's sha256 are skipped.

SEC access guidance: declare a User-Agent "Name email" and stay under 10 requests/s.
Default pacing is 4 requests/s.

Examples:
  # validate only (no network)
  python3 acquire_sec_filings.py --inventory SEC_Filing_Inventory.csv \
      --cross-check "SEC_Filing_Inventory(SEC filings).xlsx" --out /mnt/raw/sec_edgar --dry-run
  # dev sample, then full run
  export SEC_USER_AGENT="Daniel Tay 2501690@sit.singaporetech.edu.sg"
  python3 acquire_sec_filings.py --inventory SEC_Filing_Inventory.csv --out /mnt/raw/sec_edgar --limit 20
  python3 acquire_sec_filings.py --inventory SEC_Filing_Inventory.csv --out /mnt/raw/sec_edgar
"""
import argparse
import csv
import hashlib
import os
import re
import socket
import sys
import time
import zipfile
import xml.etree.ElementTree as ET
from collections import Counter
from datetime import date, timedelta

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from acquire_url import download, sha256_file, utc_now  # noqa: E402

FIELDS = ["cik", "issuer", "form", "filing_date", "accession", "document_kind", "url"]

# Normalised header text -> logical field. Exact matches only, so a notes row that
# merely mentions "CIK" can never be taken for the header.
HEADER_ALIASES = {
    "cik": "cik",
    "company name": "issuer", "company_name": "issuer", "issuer": "issuer",
    "form": "form", "form type": "form",
    "filing date": "filing_date", "filed_at": "filing_date", "date filed": "filing_date",
    "accession number": "accession", "accession": "accession",
    "document kind": "document_kind", "document_kind": "document_kind",
    "sec source url": "url", "sec url": "url", "url": "url",
}
REQUIRED = ["cik", "form", "filing_date", "accession", "url"]

ACCESSION_RE = re.compile(r"^\d{10}-\d{2}-\d{6}$")
URL_RE = re.compile(r"^https://www\.sec\.gov/Archives/edgar/data/(\d+)/(\d{10}-\d{2}-\d{6})\.txt$")

NS = {"m": "http://schemas.openxmlformats.org/spreadsheetml/2006/main",
      "r": "http://schemas.openxmlformats.org/officeDocument/2006/relationships",
      "pr": "http://schemas.openxmlformats.org/package/2006/relationships"}


# ------------------------------------------------------------------------ readers
def _col_index(ref):
    n = 0
    for ch in re.match(r"[A-Z]+", ref).group(0):
        n = n * 26 + (ord(ch) - 64)
    return n - 1


def read_xlsx_rows(path, sheet=None):
    """Return (sheet_name, rows); each cell is (text, kind), kind in {str, num, bool, empty}."""
    with zipfile.ZipFile(path) as z:
        shared = []
        if "xl/sharedStrings.xml" in z.namelist():
            for si in ET.fromstring(z.read("xl/sharedStrings.xml")).findall("m:si", NS):
                shared.append("".join(t.text or "" for t in si.iter(f"{{{NS['m']}}}t")))
        wb = ET.fromstring(z.read("xl/workbook.xml"))
        rels = ET.fromstring(z.read("xl/_rels/workbook.xml.rels"))
        targets = {r.get("Id"): r.get("Target") for r in rels.findall("pr:Relationship", NS)}
        sheets = [(s.get("name"), targets[s.get(f"{{{NS['r']}}}id")]) for s in wb.find("m:sheets", NS)]
        name, target = next(((n, t) for n, t in sheets if n == sheet), sheets[0])
        target = target.lstrip("/")
        if not target.startswith("xl/"):
            target = "xl/" + target
        root = ET.fromstring(z.read(target))
    rows = []
    for row in root.iter(f"{{{NS['m']}}}row"):
        cells = {}
        for c in row.findall("m:c", NS):
            t, v = c.get("t"), c.find("m:v", NS)
            if t == "s":
                val, kind = shared[int(v.text)], "str"
            elif t == "inlineStr":
                val, kind = "".join(x.text or "" for x in c.iter(f"{{{NS['m']}}}t")), "str"
            elif t == "str":
                val, kind = (v.text or "") if v is not None else "", "str"
            elif t == "b":
                val, kind = v.text or "", "bool"
            elif v is not None:
                val, kind = v.text, "num"
            else:
                val, kind = "", "empty"
            cells[_col_index(c.get("r"))] = (val, kind)
        width = max(cells) + 1 if cells else 0
        rows.append((int(row.get("r", len(rows) + 1)),
                     [cells.get(i, ("", "empty")) for i in range(width)]))
    return name, rows


def read_csv_rows(path):
    with open(path, newline="", encoding="utf-8-sig") as f:
        return "csv", [(i, [(v, "str") for v in r]) for i, r in enumerate(csv.reader(f), 1)]


def excel_serial_to_iso(text):
    try:
        return (date(1899, 12, 30) + timedelta(days=float(text))).isoformat()
    except ValueError:
        return ""


def normalise(field, raw, kind):
    """Return (value, note). Every change from the raw cell is noted, never silent."""
    raw = (raw or "").strip()
    if field == "cik":
        digits = raw.split(".")[0] if kind == "num" else raw
        if digits.isdigit() and len(digits) < 10:
            return digits.zfill(10), f"cik zero-padded from '{raw}' ({kind} cell)"
        return digits, ""
    if field == "filing_date" and kind == "num":
        return excel_serial_to_iso(raw), f"filing_date excel serial '{raw}' -> ISO"
    if field == "accession" and kind == "num":
        return raw, "accession stored as number; dashes/leading zeros lost"
    return raw, ""


def load_inventory(path, sheet=None):
    """Parse a .csv or .xlsx inventory. Returns dict with header, mapping, records."""
    path = os.path.expanduser(path)
    if path.lower().endswith(".xlsx"):
        sheet_name, rows = read_xlsx_rows(path, sheet)
    else:
        sheet_name, rows = read_csv_rows(path)

    hdr = next(((rn, r) for rn, r in rows
                if any(c[0].strip().lower() == "cik" for c in r)), None)
    if hdr is None:
        sys.exit(f"{path}: no header row with a cell equal to 'CIK'")
    hdr_rownum, hdr_cells = hdr
    header = [c[0].strip() for c in hdr_cells]
    mapping = {}
    for i, h in enumerate(header):
        f = HEADER_ALIASES.get(h.lower())
        if f and f not in mapping:
            mapping[f] = i
    missing = [f for f in REQUIRED if f not in mapping]
    if missing:
        sys.exit(f"{path}: header {header} lacks {missing}; extend HEADER_ALIASES")

    records = []
    for rn, r in rows:
        if rn <= hdr_rownum or not any(c[0].strip() for c in r):
            continue
        r = r + [("", "empty")] * (len(header) - len(r))
        rec, notes = {"source_row": rn}, []
        for f in FIELDS:
            if f in mapping:
                val, note = normalise(f, *r[mapping[f]])
            else:
                val, note = "", ""
            rec[f] = val
            if note:
                notes.append(note)
        rec["_raw"] = [c[0] for c in r[:len(header)]]
        rec["_kind"] = [c[1] for c in r[:len(header)]]
        rec["notes"] = "; ".join(notes)
        records.append(rec)
    return {"path": path, "sheet": sheet_name, "header_row": hdr_rownum, "header": header,
            "mapping": mapping, "records": records, "sha256": sha256_file(path),
            "bytes": os.path.getsize(path)}


# ------------------------------------------------------------------------- checks
def check_inventory(records):
    """Return list of (source_row, accession, check, detail). Reported, never fixed."""
    out = []
    acc_counts = Counter(r["accession"] for r in records)
    url_counts = Counter(r["url"] for r in records)
    for r in records:
        row, acc = r["source_row"], r["accession"]
        def bad(check, detail=""):
            out.append((row, acc, check, detail))
        if not (r["cik"].isdigit() and len(r["cik"]) == 10):
            bad("cik_not_10_digits", r["cik"])
        if not ACCESSION_RE.match(acc):
            bad("accession_format", acc)
        if r["form"] not in ("10-K", "10-Q"):
            bad("unexpected_form", r["form"])
        if not re.match(r"^\d{4}-\d{2}-\d{2}$", r["filing_date"]):
            bad("filing_date_format", r["filing_date"])
        elif not ("2013-01-01" <= r["filing_date"] <= "2017-12-31"):
            bad("filing_date_outside_2013_2017", r["filing_date"])
        m = URL_RE.match(r["url"])
        if not m:
            bad("url_not_sec_archives_txt", r["url"])
        else:
            if m.group(1) != r["cik"].lstrip("0"):
                bad("url_cik_mismatch", f"url={m.group(1)} cik={r['cik']}")
            if m.group(2) != acc:
                bad("url_accession_mismatch", f"url={m.group(2)}")
        if acc_counts[acc] > 1:
            bad("duplicate_accession")
        if url_counts[r["url"]] > 1:
            bad("duplicate_url")
    return out


def cross_check(a, b):
    """Compare two inventories by accession. Returns list of (accession, field, a_value, b_value)."""
    ia = {r["accession"]: r for r in a["records"]}
    ib = {r["accession"]: r for r in b["records"]}
    diffs = []
    for acc in sorted(set(ia) | set(ib)):
        if acc not in ib:
            diffs.append((acc, "missing_in_cross_check", "present", ""))
        elif acc not in ia:
            diffs.append((acc, "missing_in_inventory", "", "present"))
        else:
            for f in FIELDS:
                if f in a["mapping"] and f in b["mapping"] and ia[acc][f] != ib[acc][f]:
                    diffs.append((acc, f, ia[acc][f], ib[acc][f]))
    return diffs


# ---------------------------------------------------------------------- download
DL_FIELDS = ["source_row", "cik", "issuer", "form", "filing_date", "accession",
             "source_url", "final_url", "local_file", "http_status", "content_type",
             "content_length_header", "bytes", "sha256", "retrieved_at_utc",
             "elapsed_s", "attempts", "status", "error", "acquired_by_host"]


def safe(s):
    return re.sub(r"[^A-Za-z0-9._-]+", "_", s).strip("_") or "x"


def verified_done(manifest, filings_dir):
    """URLs with an 'ok' manifest row whose file still has the recorded sha256."""
    if not os.path.exists(manifest):
        return set()
    ok = {}
    with open(manifest, newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            if r["status"] == "ok":
                ok[r["source_url"]] = r
    return {u for u, r in ok.items()
            if os.path.exists(os.path.join(filings_dir, r["local_file"]))
            and sha256_file(os.path.join(filings_dir, r["local_file"])) == r["sha256"]}


def write_csv(path, header, rows):
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(rows)


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--inventory", required=True, help="course inventory .csv or .xlsx (drives downloads)")
    p.add_argument("--cross-check", help="the other inventory file, compared by accession")
    p.add_argument("--out", required=True, help="local output root, e.g. /mnt/raw/sec_edgar")
    p.add_argument("--sheet", help="xlsx sheet name (default: first)")
    p.add_argument("--dry-run", action="store_true", help="validate inventory only; no downloads")
    p.add_argument("--rate", type=float, default=4.0, help="max requests/second (must be < 10)")
    p.add_argument("--limit", type=int, help="only the first N distinct URLs (dev sample)")
    p.add_argument("--user-agent", default=os.environ.get("SEC_USER_AGENT"))
    a = p.parse_args()

    if not a.dry_run:
        if not a.user_agent or "@" not in a.user_agent:
            sys.exit("Set SEC_USER_AGENT='Full Name email@domain' (SEC requires a contact).")
        if not 0 < a.rate < 10:
            sys.exit("--rate must be between 0 and 10 requests/second (SEC limit).")

    out = os.path.expanduser(a.out)
    filings_dir = os.path.join(out, "filings")
    man_dir = os.path.join(out, "_manifests")
    os.makedirs(man_dir, exist_ok=True)

    # 1) parse + record inventory exactly as read
    inv = load_inventory(a.inventory, a.sheet)
    recs = inv["records"]
    print(f"inventory={os.path.basename(inv['path'])} header_row={inv['header_row']} "
          f"mapping=" + ", ".join(f"{f}->'{inv['header'][i]}'" for f, i in inv["mapping"].items()))
    write_csv(os.path.join(man_dir, "sec_inventory_as_read.csv"),
              ["source_row"] + [f"raw:{h}" for h in inv["header"]] + [f"kind:{h}" for h in inv["header"]]
              + FIELDS + ["normalisation_notes"],
              [[r["source_row"], *r["_raw"], *r["_kind"], *[r[f] for f in FIELDS], r["notes"]] for r in recs])

    # 2) inventory checks + optional cross-check
    checks = check_inventory(recs)
    write_csv(os.path.join(man_dir, "sec_inventory_checks.csv"),
              ["source_row", "accession", "check", "detail"], checks)
    diffs = None
    if a.cross_check:
        other = load_inventory(a.cross_check, a.sheet)
        diffs = cross_check(inv, other)
        write_csv(os.path.join(man_dir, "sec_inventory_crosscheck.csv"),
                  ["accession", "field", "inventory_value", "cross_check_value"], diffs)

    forms = Counter(r["form"] for r in recs)
    years = Counter(r["filing_date"][:4] for r in recs)
    summary = [
        f"run_started_utc={utc_now()}",  # set before downloads; finished time appended below
        f"inventory_file={os.path.basename(inv['path'])} bytes={inv['bytes']} sha256={inv['sha256']}",
        f"inventory_rows={len(recs)}",
        f"distinct_ciks={len({r['cik'] for r in recs})}",
        f"distinct_accessions={len({r['accession'] for r in recs})}",
        f"distinct_urls={len({r['url'] for r in recs})}",
        f"forms={dict(sorted(forms.items()))}",
        f"filings_by_year={dict(sorted(years.items()))}",
        f"filing_date_range={min(r['filing_date'] for r in recs)}..{max(r['filing_date'] for r in recs)}",
        f"rows_with_normalisation_notes={sum(1 for r in recs if r['notes'])}",
        f"inventory_check_failures={len(checks)} by_check={dict(Counter(c[2] for c in checks))}",
    ]
    if a.cross_check:
        summary.append(f"cross_check_file={os.path.basename(other['path'])} bytes={other['bytes']} "
                       f"sha256={other['sha256']}")
        summary.append(f"cross_check_differences={len(diffs)} "
                       f"({'files agree' if not diffs else 'see sec_inventory_crosscheck.csv'})")

    # 3) downloads
    if not a.dry_run:
        os.makedirs(filings_dir, exist_ok=True)
        manifest = os.path.join(man_dir, "sec_download_manifest.csv")
        done = verified_done(manifest, filings_dir)
        seen, queue = set(), []
        for r in recs:
            if r["url"] and r["url"] not in seen:
                seen.add(r["url"])
                queue.append(r)
        if a.limit:
            queue = queue[:a.limit]
        new_manifest = not os.path.exists(manifest)
        host, gap = socket.gethostname(), 1.0 / a.rate
        with open(manifest, "a", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=DL_FIELDS)
            if new_manifest:
                w.writeheader()
            for n, r in enumerate(queue, 1):
                if r["url"] in done:
                    continue
                ext = os.path.splitext(r["url"].split("?")[0])[1] or ".bin"
                fname = f"{safe(r['cik'])}_{safe(r['accession'])}_{safe(r['form'])}{ext}"
                t0, started = time.time(), utc_now()
                info = download(r["url"], os.path.join(filings_dir, fname), a.user_agent)
                row = {"source_row": r["source_row"], "cik": r["cik"], "issuer": r["issuer"],
                       "form": r["form"], "filing_date": r["filing_date"], "accession": r["accession"],
                       "source_url": r["url"], "local_file": fname if info["status"] == "ok" else "",
                       "retrieved_at_utc": started, "acquired_by_host": host, **info}
                w.writerow({k: row.get(k, "") for k in DL_FIELDS})
                f.flush()
                print(f"[{n}/{len(queue)}] {info['status']:<11} {info.get('http_status', '')} "
                      f"{info.get('bytes', '')} {r['url']}")
                time.sleep(max(0.0, gap - (time.time() - t0)))

        latest = {}
        with open(manifest, newline="", encoding="utf-8") as f:
            for r in csv.DictReader(f):
                latest[r["source_url"]] = r
        ok = [r for r in latest.values() if r["status"] == "ok"]
        bad = [r for r in latest.values() if r["status"] != "ok"]
        with open(os.path.join(man_dir, "sec_unavailable.csv"), "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=DL_FIELDS)
            w.writeheader()
            w.writerows(bad)
        summary += [
            f"download_scope={'first ' + str(a.limit) if a.limit else 'all'} distinct urls",
            f"downloaded_ok={len(ok)}",
            f"unavailable={len(bad)}",
            f"not_attempted={len(seen) - len(ok) - len(bad)}",
            f"bytes_ok_total={sum(int(r['bytes'] or 0) for r in ok)}",
            f"user_agent_declared=yes rate_limit_rps={a.rate}",
        ]
    else:
        summary.append("mode=dry-run (no downloads)")
    summary.append(f"run_finished_utc={utc_now()}")

    with open(os.path.join(man_dir, "sec_acquisition_summary.txt"), "w", encoding="utf-8") as f:
        f.write("\n".join(summary) + "\n")
    print("\n".join(summary))
    if checks or diffs:
        print("NOTE: inventory issues found; see _manifests/sec_inventory_checks.csv / _crosscheck.csv")


if __name__ == "__main__":
    main()
