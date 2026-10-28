#!/usr/bin/env python3
"""Download one source file with provenance (Task 1).

Used for the daily price CSV, SEC company_tickers.json and any other single-file
source. Bytes are written exactly as served (no decompression or re-encoding) and
one row is appended to a shared acquisition manifest CSV.

--github-pin   For raw.githubusercontent.com URLs on a branch (e.g. .../master/...):
               look up the latest commit that touched the file, download from the
               commit-pinned URL instead, and record the commit SHA. Anyone can later
               fetch exactly the same bytes from pinned_url.
--csv-stats    Count data rows, distinct values of --key-col and min/max of --date-col
               (stdlib csv). Manifest summary only; profiling proper happens in Spark.

Stdlib only, so it runs unchanged on the EMR primary node.

Example:
    python3 acquire_url.py --github-pin --csv-stats --key-col Name --date-col date \
        --url https://raw.githubusercontent.com/plotly/datasets/master/all_stocks_5yr.csv \
        --source prices --version v1_2013-2017 \
        --out-dir /mnt/raw/prices --manifest /mnt/raw/_manifests/acquisition_manifest.csv
"""
import argparse
import csv
import hashlib
import json
import os
import socket
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone

MANIFEST_FIELDS = [
    "source", "version", "source_url", "pinned_url", "commit_sha", "commit_date",
    "final_url", "local_file", "http_status", "content_type", "content_length_header",
    "bytes", "sha256", "retrieved_at_utc", "elapsed_s", "attempts", "status", "error",
    "csv_header", "data_rows", "distinct_keys", "min_date", "max_date", "acquired_by_host",
]


def utc_now():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def sha256_file(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def append_manifest(path, row):
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    new = not os.path.exists(path)
    with open(path, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=MANIFEST_FIELDS)
        if new:
            w.writeheader()
        w.writerow({k: row.get(k, "") for k in MANIFEST_FIELDS})


def download(url, dest, user_agent, timeout=120, retries=4):
    """Stream url to dest; return dict of provenance fields. Never raises."""
    tmp = dest + ".part"
    last_err, status, attempts = "", "", 0
    for attempt in range(1, retries + 1):
        attempts = attempt
        t0 = time.time()
        try:
            req = urllib.request.Request(url, headers={
                "User-Agent": user_agent,
                "Accept-Encoding": "identity",  # keep bytes exactly as stored
            })
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                h, n = hashlib.sha256(), 0
                with open(tmp, "wb") as out:
                    while True:
                        chunk = resp.read(1 << 20)
                        if not chunk:
                            break
                        out.write(chunk)
                        h.update(chunk)
                        n += len(chunk)
                os.replace(tmp, dest)
                return {
                    "final_url": resp.geturl(), "http_status": resp.status,
                    "content_type": resp.headers.get("Content-Type", ""),
                    "content_length_header": resp.headers.get("Content-Length", ""),
                    "bytes": n, "sha256": h.hexdigest(),
                    "elapsed_s": round(time.time() - t0, 3),
                    "attempts": attempts, "status": "ok", "error": "",
                }
        except urllib.error.HTTPError as e:
            status, last_err = e.code, f"HTTPError {e.code} {e.reason}"
            if 400 <= e.code < 500 and e.code != 429:  # won't fix itself
                break
        except (urllib.error.URLError, socket.timeout, ConnectionError, OSError) as e:
            last_err = f"{type(e).__name__}: {e}"
        if os.path.exists(tmp):
            os.remove(tmp)
        time.sleep(min(60, 2 ** attempt))
    return {"http_status": status, "attempts": attempts, "status": "unavailable", "error": last_err}


def github_pin(url, user_agent):
    """raw.githubusercontent.com/<owner>/<repo>/<ref>/<path> -> (pinned_url, sha, date)."""
    u = urllib.parse.urlparse(url)
    if u.netloc != "raw.githubusercontent.com":
        sys.exit("--github-pin only works with raw.githubusercontent.com URLs")
    owner, repo, ref, path = u.path.lstrip("/").split("/", 3)
    api = (f"https://api.github.com/repos/{owner}/{repo}/commits?"
           + urllib.parse.urlencode({"path": path, "sha": ref, "per_page": 1}))
    req = urllib.request.Request(api, headers={"User-Agent": user_agent,
                                               "Accept": "application/vnd.github+json"})
    with urllib.request.urlopen(req, timeout=60) as resp:
        commits = json.load(resp)
    if not commits:
        sys.exit(f"No commits found for {path} on {ref}")
    sha = commits[0]["sha"]
    when = commits[0]["commit"]["committer"]["date"]
    return f"https://raw.githubusercontent.com/{owner}/{repo}/{sha}/{path}", sha, when


def csv_stats(path, key_col=None, date_col=None):
    with open(path, newline="", encoding="utf-8-sig") as f:
        r = csv.reader(f)
        header = next(r)
        ki = header.index(key_col) if key_col in header else None
        di = header.index(date_col) if date_col in header else None
        rows, keys, dmin, dmax = 0, set(), None, None
        for row in r:
            rows += 1
            if ki is not None and ki < len(row):
                keys.add(row[ki])
            if di is not None and di < len(row) and row[di]:
                d = row[di]
                dmin = d if dmin is None or d < dmin else dmin
                dmax = d if dmax is None or d > dmax else dmax
    return {"csv_header": "|".join(header), "data_rows": rows,
            "distinct_keys": len(keys) if ki is not None else "",
            "min_date": dmin or "", "max_date": dmax or ""}


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--url", required=True)
    p.add_argument("--source", required=True, help="e.g. prices, sec_company_tickers")
    p.add_argument("--version", required=True, help="e.g. v1_2013-2017")
    p.add_argument("--out-dir", required=True)
    p.add_argument("--manifest", required=True)
    p.add_argument("--filename", help="local filename (default: from URL)")
    p.add_argument("--github-pin", action="store_true")
    p.add_argument("--csv-stats", action="store_true")
    p.add_argument("--key-col", help="CSV column counted for distinct keys (e.g. Name)")
    p.add_argument("--date-col", help="CSV column for min/max date (ISO dates)")
    p.add_argument("--user-agent", default=os.environ.get("SEC_USER_AGENT", "INF2006-Group12 coursework"),
                   help="SEC requires 'Name email'; set SEC_USER_AGENT")
    a = p.parse_args()

    name = a.filename or os.path.basename(urllib.parse.urlparse(a.url).path) or "download.bin"
    os.makedirs(a.out_dir, exist_ok=True)
    dest = os.path.join(a.out_dir, name)
    if os.path.exists(dest):
        sys.exit(f"Refusing to overwrite existing raw file: {dest}")

    row = {"source": a.source, "version": a.version, "source_url": a.url,
           "local_file": name, "acquired_by_host": socket.gethostname()}
    fetch_url = a.url
    if a.github_pin:
        fetch_url, row["commit_sha"], row["commit_date"] = github_pin(a.url, a.user_agent)
        row["pinned_url"] = fetch_url
        print(f"pinned to commit {row['commit_sha']} ({row['commit_date']})")

    row["retrieved_at_utc"] = utc_now()
    row.update(download(fetch_url, dest, a.user_agent))
    if row["status"] == "ok" and a.csv_stats:
        row.update(csv_stats(dest, a.key_col, a.date_col))
    append_manifest(a.manifest, row)
    for k in ("status", "local_file", "bytes", "sha256", "commit_sha",
              "data_rows", "distinct_keys", "min_date", "max_date", "error"):
        if row.get(k) not in (None, ""):
            print(f"{k}={row[k]}")
    sys.exit(0 if row["status"] == "ok" else 1)


if __name__ == "__main__":
    main()
