#!/usr/bin/env python3
"""Download one source file with provenance (Task 1).

Used for the daily price CSV and the SEC filing inventory (.xlsx), whose URLs
come from the course materials. The file is written byte-for-byte as served
(no decompression, no re-encoding) and one row is appended to a manifest CSV.

Stdlib only, so it runs unchanged on the EMR primary node.

Example (on the EMR primary node):
    python3 acquire_url.py \
        --url "<price CSV URL from course page>" \
        --source prices --version v1_2013-2017 \
        --out-dir ~/raw/prices \
        --manifest ~/raw/_manifests/acquisition_manifest.csv
"""
import argparse
import csv
import hashlib
import os
import socket
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone

MANIFEST_FIELDS = [
    "source", "version", "source_url", "final_url", "local_file",
    "http_status", "content_type", "content_length_header", "bytes", "sha256",
    "retrieved_at_utc", "elapsed_s", "attempts", "status", "error",
    "acquired_by_host",
]


def utc_now():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


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
                h = hashlib.sha256()
                n = 0
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
                    "final_url": resp.geturl(),
                    "http_status": resp.status,
                    "content_type": resp.headers.get("Content-Type", ""),
                    "content_length_header": resp.headers.get("Content-Length", ""),
                    "bytes": n, "sha256": h.hexdigest(),
                    "elapsed_s": round(time.time() - t0, 3),
                    "attempts": attempts, "status": "ok", "error": "",
                }
        except urllib.error.HTTPError as e:
            status, last_err = e.code, f"HTTPError {e.code} {e.reason}"
            # 4xx other than 429 will not fix itself: report, don't retry.
            if 400 <= e.code < 500 and e.code != 429:
                break
        except (urllib.error.URLError, socket.timeout, ConnectionError, OSError) as e:
            last_err = f"{type(e).__name__}: {e}"
        if os.path.exists(tmp):
            os.remove(tmp)
        time.sleep(min(60, 2 ** attempt))
    return {"http_status": status, "attempts": attempts,
            "status": "unavailable", "error": last_err}


def main():
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--url", required=True)
    p.add_argument("--source", required=True, help="e.g. prices, sec_inventory")
    p.add_argument("--version", required=True, help="e.g. v1_2013-2017")
    p.add_argument("--out-dir", required=True)
    p.add_argument("--manifest", required=True)
    p.add_argument("--filename", help="override local filename (default: from URL)")
    p.add_argument("--user-agent",
                   default=os.environ.get("SEC_USER_AGENT", "INF2006-Group12 research"),
                   help="SEC requires 'Name email'; set SEC_USER_AGENT env var")
    a = p.parse_args()

    name = a.filename or os.path.basename(urllib.parse.urlparse(a.url).path) or "download.bin"
    os.makedirs(a.out_dir, exist_ok=True)
    dest = os.path.join(a.out_dir, name)
    if os.path.exists(dest):
        sys.exit(f"Refusing to overwrite existing raw file: {dest}")

    started = utc_now()
    info = download(a.url, dest, a.user_agent)
    row = {"source": a.source, "version": a.version, "source_url": a.url,
           "local_file": name, "retrieved_at_utc": started,
           "acquired_by_host": socket.gethostname(), **info}
    append_manifest(a.manifest, row)
    print(f"{row['status']}: {name} bytes={row.get('bytes', '')} sha256={row.get('sha256', '')}")
    sys.exit(0 if row["status"] == "ok" else 1)


if __name__ == "__main__":
    main()
