#!/usr/bin/env python3
"""Download the fixed SEC filing inventory with full provenance (Task 1).

Reads the supplied inventory .xlsx (stdlib only, no openpyxl/pandas), keeps every
identifier as text, downloads each listed SEC URL exactly as served, and writes:

  <out>/filings/<CIK10>_<ACCESSION>_<FORM>__<url-basename>   original bytes
  <out>/_manifests/sec_inventory_as_read.csv    every inventory row, raw + normalised
  <out>/_manifests/sec_download_manifest.csv    one row per URL: status, bytes, sha256, time
  <out>/_manifests/sec_unavailable.csv          URLs that could not be retrieved
  <out>/_manifests/sec_acquisition_summary.txt  counts for the report

Unavailable URLs are reported, never substituted. Re-running resumes: URLs with an
'ok' manifest row whose file still matches its sha256 are skipped.

SEC access guidance: declare a User-Agent of the form "Name email" and stay
under 10 requests/second. Default pacing here is 4 requests/second.

Example (on the EMR primary node):
    export SEC_USER_AGENT="Daniel Tay 2501690@sit.singaporetech.edu.sg"
    python3 acquire_sec_filings.py --inventory ~/raw/sec_inventory/<file>.xlsx --out ~/raw/sec_edgar
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
from datetime import date, timedelta

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from acquire_url import download, utc_now  # noqa: E402

NS = {"m": "http://schemas.openxmlformats.org/spreadsheetml/2006/main",
      "r": "http://schemas.openxmlformats.org/officeDocument/2006/relationships",
      "pr": "http://schemas.openxmlformats.org/package/2006/relationships"}

# Header keyword -> logical field. First matching column wins; mapping is printed.
FIELD_KEYWORDS = {
    "cik": ["cik"],
    "issuer": ["issuer", "company", "entity", "registrant", "name"],
    "form": ["form"],
    "filing_date": ["filing date", "filed", "filing_date", "date"],
    "accession": ["accession"],
    "url": ["url", "link", "href"],
}


# --------------------------------------------------------------------------- xlsx
def _col_index(ref):
    letters = re.match(r"[A-Z]+", ref).group(0)
    n = 0
    for ch in letters:
        n = n * 26 + (ord(ch) - 64)
    return n - 1


def read_xlsx(path, sheet=None):
    """Return list of rows; each cell is (text, kind) with kind in {str, num, bool, empty}."""
    with zipfile.ZipFile(path) as z:
        shared = []
        if "xl/sharedStrings.xml" in z.namelist():
            for si in ET.fromstring(z.read("xl/sharedStrings.xml")).findall("m:si", NS):
                shared.append("".join(t.text or "" for t in si.iter(f"{{{NS['m']}}}t")))
        wb = ET.fromstring(z.read("xl/workbook.xml"))
        rels = ET.fromstring(z.read("xl/_rels/workbook.xml.rels"))
        targets = {r.get("Id"): r.get("Target") for r in rels.findall("pr:Relationship", NS)}
        sheets = [(s.get("name"), targets[s.get(f"{{{NS['r']}}}id")])
                  for s in wb.find("m:sheets", NS)]
        name, target = next(((n, t) for n, t in sheets if n == sheet), sheets[0]) if sheet else sheets[0]
        target = target.lstrip("/")
        if not target.startswith("xl/"):
            target = "xl/" + target
        root = ET.fromstring(z.read(target))
    rows = []
    for row in root.iter(f"{{{NS['m']}}}row"):
        cells = {}
        for c in row.findall("m:c", NS):
            t = c.get("t")
            v = c.find("m:v", NS)
            if t == "s":
                val, kind = shared[int(v.text)], "str"
            elif t == "inlineStr":
                val, kind = "".join(x.text or "" for x in c.iter(f"{{{NS['m']}}}t")), "str"
            elif t == "str":
                val, kind = (v.text or ""), "str"
            elif t == "b":
                val, kind = (v.text or ""), "bool"
            elif v is not None:
                val, kind = v.text, "num"
            else:
                val, kind = "", "empty"
            cells[_col_index(c.get("r"))] = (val, kind)
        width = max(cells) + 1 if cells else 0
        rows.append([cells.get(i, ("", "empty")) for i in range(width)])
    return name, rows


# ---------------------------------------------------------------------- mapping
def map_columns(header):
    norm = [h.strip().lower() for h in header]
    mapping = {}
    for field, keys in FIELD_KEYWORDS.items():
        for i, h in enumerate(norm):
            if i in mapping.values():
                continue
            if any(k in h for k in keys):
                mapping[field] = i
                break
    missing = [f for f in ("cik", "form", "accession", "url") if f not in mapping]
    if missing:
        sys.exit(f"Could not find inventory columns {missing} in header {header}. "
                 f"Edit FIELD_KEYWORDS.")
    return mapping


def excel_serial_to_iso(text):
    try:
        return (date(1899, 12, 30) + timedelta(days=float(text))).isoformat()
    except ValueError:
        return ""


def normalise(raw, kind, field):
    """Return (normalised_text, note). Never guesses silently: every change is noted."""
    raw = (raw or "").strip()
    if field == "cik":
        digits = raw.split(".")[0] if kind == "num" else raw
        if digits.isdigit() and len(digits) < 10:
            return digits.zfill(10), f"zero-padded from '{raw}' ({kind} cell)"
        return digits, ""
    if field == "filing_date" and kind == "num":
        return excel_serial_to_iso(raw), f"excel serial '{raw}' -> ISO date"
    if field == "accession" and kind == "num":
        return raw, "accession stored as number; dashes/leading zeros may be lost"
    return raw, ""


def safe(s):
    return re.sub(r"[^A-Za-z0-9._-]+", "_", s).strip("_") or "x"


# ------------------------------------------------------------------------- main
DL_FIELDS = ["inventory_row", "cik", "issuer", "form", "filing_date", "accession",
             "source_url", "final_url", "local_file", "http_status", "content_type",
             "content_length_header", "bytes", "sha256", "retrieved_at_utc",
             "elapsed_s", "attempts", "status", "error", "acquired_by_host"]


def load_done(manifest, filings_dir):
    done = {}
    if not os.path.exists(manifest):
        return done
    with open(manifest, newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            if r["status"] == "ok":
                done[r["source_url"]] = r
    # keep only entries whose file is still present with the recorded checksum
    ok = {}
    for url, r in done.items():
        p = os.path.join(filings_dir, r["local_file"])
        if os.path.exists(p):
            h = hashlib.sha256()
            with open(p, "rb") as fh:
                for chunk in iter(lambda: fh.read(1 << 20), b""):
                    h.update(chunk)
            if h.hexdigest() == r["sha256"]:
                ok[url] = r
    return ok


def main():
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--inventory", required=True, help="path to SEC filing inventory .xlsx")
    p.add_argument("--out", required=True, help="local output root, e.g. ~/raw/sec_edgar")
    p.add_argument("--sheet", help="sheet name (default: first sheet)")
    p.add_argument("--rate", type=float, default=4.0, help="max requests/second (<10)")
    p.add_argument("--limit", type=int, help="only the first N distinct URLs (dev sample)")
    p.add_argument("--user-agent", default=os.environ.get("SEC_USER_AGENT"))
    a = p.parse_args()

    if not a.user_agent or "@" not in a.user_agent:
        sys.exit("Set SEC_USER_AGENT='Full Name email@domain' (SEC requires a contact).")
    if a.rate >= 10:
        sys.exit("--rate must stay below SEC's 10 requests/second limit.")

    out = os.path.expanduser(a.out)
    filings_dir = os.path.join(out, "filings")
    man_dir = os.path.join(out, "_manifests")
    os.makedirs(filings_dir, exist_ok=True)
    os.makedirs(man_dir, exist_ok=True)

    sheet_name, rows = read_xlsx(os.path.expanduser(a.inventory), a.sheet)
    hdr_idx = next(i for i, r in enumerate(rows) if any("cik" in c[0].lower() for c in r))
    header = [c[0] for c in rows[hdr_idx]]
    cols = map_columns(header)
    print(f"sheet='{sheet_name}' header_row={hdr_idx + 1} mapping="
          + ", ".join(f"{k}->'{header[v]}'" for k, v in cols.items()))

    # 1) record the inventory exactly as read, plus normalised identifiers
    records = []
    as_read = os.path.join(man_dir, "sec_inventory_as_read.csv")
    with open(as_read, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["inventory_row"] + [f"raw:{h}" for h in header]
                   + [f"raw_kind:{h}" for h in header]
                   + [*cols.keys()] + ["normalisation_notes"])
        for i, r in enumerate(rows[hdr_idx + 1:], start=hdr_idx + 2):
            if not any(c[0].strip() for c in r):
                continue
            r = r + [("", "empty")] * (len(header) - len(r))
            rec, notes = {"inventory_row": i}, []
            for field, ci in cols.items():
                val, note = normalise(r[ci][0], r[ci][1], field)
                rec[field] = val
                if note:
                    notes.append(f"{field}: {note}")
            records.append(rec)
            w.writerow([i] + [c[0] for c in r[:len(header)]] + [c[1] for c in r[:len(header)]]
                       + [rec[k] for k in cols] + ["; ".join(notes)])

    # 2) inventory-level checks (reported, not fixed)
    accs = [r["accession"] for r in records]
    urls = [r["url"] for r in records]
    dup_acc = sorted({x for x in accs if accs.count(x) > 1})
    dup_url = sorted({x for x in urls if urls.count(x) > 1})
    blank_url = [r["inventory_row"] for r in records if not r["url"]]

    # 3) download each distinct URL once, in inventory order
    manifest = os.path.join(man_dir, "sec_download_manifest.csv")
    done = load_done(manifest, filings_dir)
    new_manifest = not os.path.exists(manifest)
    seen, queue = set(), []
    for r in records:
        if r["url"] and r["url"] not in seen:
            seen.add(r["url"])
            queue.append(r)
    if a.limit:
        queue = queue[:a.limit]

    min_gap = 1.0 / a.rate
    host = socket.gethostname()
    with open(manifest, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=DL_FIELDS)
        if new_manifest:
            w.writeheader()
        for n, r in enumerate(queue, 1):
            if r["url"] in done:
                continue
            base = safe(os.path.basename(r["url"].split("?")[0]))
            fname = f"{safe(r['cik'])}_{safe(r['accession'])}_{safe(r['form'])}__{base}"
            t0 = time.time()
            started = utc_now()
            info = download(r["url"], os.path.join(filings_dir, fname), a.user_agent)
            row = {"inventory_row": r["inventory_row"], "cik": r["cik"],
                   "issuer": r.get("issuer", ""), "form": r["form"],
                   "filing_date": r.get("filing_date", ""), "accession": r["accession"],
                   "source_url": r["url"], "local_file": fname if info["status"] == "ok" else "",
                   "retrieved_at_utc": started, "acquired_by_host": host, **info}
            w.writerow({k: row.get(k, "") for k in DL_FIELDS})
            f.flush()
            print(f"[{n}/{len(queue)}] {info['status']:<11} {info.get('http_status', '')} "
                  f"{info.get('bytes', '')} {r['url']}")
            time.sleep(max(0.0, min_gap - (time.time() - t0)))

    # 4) summary + unavailable list, rebuilt from the full manifest (latest row per URL)
    latest = {}
    with open(manifest, newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            latest[r["source_url"]] = r
    ok = [r for r in latest.values() if r["status"] == "ok"]
    bad = [r for r in latest.values() if r["status"] != "ok"]
    with open(os.path.join(man_dir, "sec_unavailable.csv"), "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=DL_FIELDS)
        w.writeheader()
        for r in bad:
            w.writerow(r)
    forms = {}
    for r in records:
        forms[r["form"]] = forms.get(r["form"], 0) + 1
    summary = [
        f"generated_at_utc={utc_now()}",
        f"inventory_file={os.path.basename(a.inventory)} sheet={sheet_name}",
        f"inventory_rows={len(records)}",
        f"distinct_ciks={len({r['cik'] for r in records})}",
        f"distinct_accessions={len(set(accs))}",
        f"distinct_urls={len(set(urls))}",
        f"forms={forms}",
        f"duplicate_accessions={dup_acc}",
        f"duplicate_urls={dup_url}",
        f"rows_with_blank_url={blank_url}",
        f"downloaded_ok={len(ok)}",
        f"unavailable={len(bad)}",
        f"bytes_ok_total={sum(int(r['bytes'] or 0) for r in ok)}",
        f"user_agent_declared={'yes' if a.user_agent else 'no'} rate_limit_rps={a.rate}",
    ]
    with open(os.path.join(man_dir, "sec_acquisition_summary.txt"), "w", encoding="utf-8") as f:
        f.write("\n".join(summary) + "\n")
    print("\n".join(summary))


if __name__ == "__main__":
    main()
