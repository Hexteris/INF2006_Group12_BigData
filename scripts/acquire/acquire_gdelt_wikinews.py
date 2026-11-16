#!/usr/bin/env python3
"""Download GDELT 1.0 event files and the English Wikinews dump with provenance.

GDELT   http://data.gdeltproject.org/events/  (GDELT 1.0 Event Database)
        2013-01..2013-03 are monthly files (YYYYMM.zip); from 2013-04-01 there is one
        file per day (YYYYMMDD.export.CSV.zip). 2013-2017 = 1,735 files, about 17.9 GB
        zipped (about 55 GB expanded). Each file is checked against GDELT's published
        md5sums list.
Wikinews https://dumps.wikimedia.org/enwikinews/<dump_date>/  pages-meta-history dump
        (every revision of every page; 2 .bz2 files, about 662 MB for 20261001).
        --wikinews-job articles gets the current-pages-only dump instead.
        Each file is checked against the sha1 in the dump's dumpstatus.json.
        Wikimedia keeps only recent dumps, so the dump date is recorded in the path.

Bytes are kept exactly as served (no unzipping). Layout, same as the rest of the corpus:
  <out>/gdelt/v1_events_<start>-<end>/acq_<date>/*.zip
  <out>/wikinews/<dump_date>_<job>/acq_<date>/*.bz2
  <out>/_manifests/<source>/<version>/acq_<date>/  index files, download manifest, summary

Re-running with the same --acq-date resumes: files already present with the published
checksum are skipped. Stdlib only (Python 3.8+), works on Windows and the EMR primary node.

Examples (PowerShell):
  $env:SEC_USER_AGENT = 'Daniel Tay 2501690@sit.singaporetech.edu.sg'
  python scripts\\acquire\\acquire_gdelt_wikinews.py --dry-run          # list sizes only
  python scripts\\acquire\\acquire_gdelt_wikinews.py --only gdelt --gdelt-start 20150701 --gdelt-end 20170630
  python scripts\\acquire\\acquire_gdelt_wikinews.py --only wikinews --wikinews-dump 20261001
  python scripts\\acquire\\acquire_gdelt_wikinews.py --only gdelt --limit 10   # dev sample
"""
import argparse
import csv
import hashlib
import json
import os
import re
import socket
import sys
import time
import urllib.request
from datetime import date

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from acquire_url import download, utc_now  # noqa: E402

GDELT_BASE = "http://data.gdeltproject.org/events/"
WIKI_BASE = "https://dumps.wikimedia.org/enwikinews/"
# Official mirrors (https://dumps.wikimedia.org/mirrors.html). dumps.wikimedia.org lists the
# large history files but can return 404 for them; mirrors serve the same bytes, and the
# sha1 from dumpstatus.json is checked regardless of which host served the file.
WIKI_MIRRORS = ["https://mirror.accum.se/mirror/wikimedia.org/dumps",
                "https://dumps.wikimedia.your.org"]
GDELT_RE = re.compile(r"^(?:(\d{6})\.zip|(\d{8})\.export\.CSV\.zip)$")

FIELDS = ["source", "version", "file", "source_url", "final_url", "http_status",
          "content_length_header", "expected_bytes", "bytes", "expected_hash_algo",
          "expected_hash", "actual_hash", "hash_match", "sha256", "retrieved_at_utc",
          "elapsed_s", "attempts", "status", "error", "acquired_by_host"]


def fetch_bytes(url, ua):
    req = urllib.request.Request(url, headers={"User-Agent": ua, "Accept-Encoding": "identity"})
    with urllib.request.urlopen(req, timeout=120) as r:
        return r.read()


def file_hash(path, algo):
    h = hashlib.new(algo)
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


# ------------------------------------------------------------------- source plans
def plan_gdelt(start, end, ua, man_dir):
    """Return (version, base_url, [(file, url, size, 'md5', md5)])."""
    md5_raw = fetch_bytes(GDELT_BASE + "md5sums", ua)
    size_raw = fetch_bytes(GDELT_BASE + "filesizes", ua)
    with open(os.path.join(man_dir, "gdelt_md5sums.txt"), "wb") as f:
        f.write(md5_raw)
    with open(os.path.join(man_dir, "gdelt_filesizes.txt"), "wb") as f:
        f.write(size_raw)
    md5 = {}
    for line in md5_raw.decode().splitlines():
        parts = line.split()
        if len(parts) == 2:
            md5[parts[1]] = parts[0].lower()
    sizes = {}
    for line in size_raw.decode().splitlines():
        parts = line.split()
        if len(parts) == 2 and parts[0].isdigit():
            sizes[parts[1]] = int(parts[0])
    files = []
    for name in sorted(md5):
        m = GDELT_RE.match(name)
        if not m:
            continue
        # monthly file covers YYYYMM01..; daily file is one day
        key = (m.group(1) + "01") if m.group(1) else m.group(2)
        if m.group(1):
            if not (start[:6] <= m.group(1) <= end[:6]):
                continue
        elif not (start <= key <= end):
            continue
        files.append((name, GDELT_BASE + name, sizes.get(name, ""), "md5", md5[name]))
    return f"v1_events_{start[:4]}-{end[:4]}", files


# --wikinews-job -> (dumpstatus.json jobs to try in order, folder suffix)
# meta-history: every revision of every page (needed to filter 2013-2017 revisions).
# articles: current version of each page only.
WIKI_JOBS = {
    "meta-history": (("metahistorybz2dump",), "meta-history"),
    "articles": (("articlesdumprecombine", "articlesdump"), "pages-articles"),
}


def job_done(status, job):
    j = status["jobs"].get(job, {})
    return j.get("status") == "done" and bool(j.get("files"))


def latest_wiki_dump(ua, jobs):
    html = fetch_bytes(WIKI_BASE, ua).decode("utf-8", "replace")
    for d in sorted(set(re.findall(r'href="(\d{8})/"', html)), reverse=True):
        try:
            status = json.loads(fetch_bytes(f"{WIKI_BASE}{d}/dumpstatus.json", ua))
        except Exception:
            continue
        if any(job_done(status, j) for j in jobs):
            return d, status
    sys.exit(f"No completed enwikinews dump found for jobs {jobs}")


def plan_wikinews(dump, kind, ua, man_dir):
    jobs, suffix = WIKI_JOBS[kind]
    if dump == "latest":
        dump, status = latest_wiki_dump(ua, jobs)
    else:
        status = json.loads(fetch_bytes(f"{WIKI_BASE}{dump}/dumpstatus.json", ua))
    with open(os.path.join(man_dir, "wikinews_dumpstatus.json"), "w", encoding="utf-8") as f:
        json.dump(status, f, indent=1)
    for job in jobs:
        if job_done(status, job):
            j = status["jobs"][job]
            # meta["url"] is "/enwikinews/<dump>/<file>"; same relative path on every mirror
            files = [(name, ["https://dumps.wikimedia.org" + meta["url"]]
                      + [m + meta["url"] for m in WIKI_MIRRORS], meta.get("size", ""),
                      "sha1", meta["sha1"].lower()) for name, meta in sorted(j["files"].items())]
            return f"{dump}_{suffix}", files
    sys.exit(f"Dump {dump}: no completed job among {jobs}")


# ----------------------------------------------------------------------- download
def run_source(source, version, files, out, man_root, acq, ua, rate, limit):
    data_dir = os.path.join(out, source, version, f"acq_{acq}")
    man_dir = os.path.join(man_root, source, version, f"acq_{acq}")
    os.makedirs(data_dir, exist_ok=True)
    manifest = os.path.join(man_dir, "download_manifest.csv")
    new = not os.path.exists(manifest)
    if limit:
        files = files[:limit]
    host, gap = socket.gethostname(), 1.0 / rate
    ok = skipped = bad = 0
    t_start = time.time()
    with open(manifest, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS)
        if new:
            w.writeheader()
        for n, (name, url, size, algo, expected) in enumerate(files, 1):
            dest = os.path.join(data_dir, name)
            if os.path.exists(dest) and file_hash(dest, algo) == expected:
                skipped += 1
                continue
            t0, started = time.time(), utc_now()
            urls = url if isinstance(url, list) else [url]
            tried = []
            for url in urls:  # primary first, then official mirrors
                info = download(url, dest, ua, timeout=300, retries=2 if len(urls) > 1 else 4)
                if info["status"] == "ok":
                    break
                tried.append(f"{url} -> {info.get('error', '')}")
            if tried and info["status"] == "ok":
                info["error"] = "fallback after: " + " | ".join(tried)
            elif tried:
                info["error"] = " | ".join(tried)
            row = {"source": source, "version": version, "file": name, "source_url": url,
                   "expected_bytes": size, "expected_hash_algo": algo, "expected_hash": expected,
                   "retrieved_at_utc": started, "acquired_by_host": host, **info}
            if info["status"] == "ok":
                row["actual_hash"] = file_hash(dest, algo)
                row["hash_match"] = row["actual_hash"] == expected
                if not row["hash_match"]:
                    row["status"] = "checksum_mismatch"
                    os.replace(dest, dest + ".bad")  # keep for inspection, never accept it
            w.writerow({k: row.get(k, "") for k in FIELDS})
            f.flush()
            if row["status"] == "ok":
                ok += 1
            else:
                bad += 1
            print(f"[{source} {n}/{len(files)}] {row['status']:<17} {row.get('bytes', '')} {name}")
            time.sleep(max(0.0, gap - (time.time() - t0)))

    present = [x for x in files if os.path.exists(os.path.join(data_dir, x[0]))]
    summary = [
        f"source={source} version={version} acq_date={acq}",
        f"finished_utc={utc_now()} elapsed_s={round(time.time() - t_start, 1)}",
        f"planned_files={len(files)} scope={'first ' + str(limit) if limit else 'all'}",
        f"downloaded_ok_this_run={ok} already_present_verified={skipped} failed_this_run={bad}",
        f"files_present={len(present)} missing={len(files) - len(present)}",
        f"bytes_present={sum(os.path.getsize(os.path.join(data_dir, x[0])) for x in present)}",
        f"checksum=published {files[0][3] if files else ''} per file; mismatches renamed *.bad",
        f"user_agent_declared=yes rate_limit_rps={rate}",
    ]
    with open(os.path.join(man_dir, "acquisition_summary.txt"), "w", encoding="utf-8") as f:
        f.write("\n".join(summary) + "\n")
    print("\n".join(summary))
    return bad == 0 and len(present) == len(files)


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--out", default="raw", help="output root (default: ./raw, already git-ignored)")
    p.add_argument("--only", choices=["gdelt", "wikinews"], help="download one source only")
    p.add_argument("--gdelt-start", default="20130101", help="YYYYMMDD, inclusive")
    p.add_argument("--gdelt-end", default="20171231", help="YYYYMMDD, inclusive")
    p.add_argument("--wikinews-dump", default="latest", help="dump date YYYYMMDD or 'latest'")
    p.add_argument("--wikinews-job", choices=sorted(WIKI_JOBS), default="meta-history",
                   help="meta-history = all revisions (default); articles = current pages only")
    p.add_argument("--acq-date", default=date.today().isoformat(),
                   help="acquisition folder date; reuse it to resume on a later day")
    p.add_argument("--rate", type=float, default=2.0, help="max requests/second")
    p.add_argument("--limit", type=int, help="first N files per source (dev sample)")
    p.add_argument("--dry-run", action="store_true", help="show the plan and sizes; no downloads")
    p.add_argument("--user-agent", default=os.environ.get("SEC_USER_AGENT"))
    a = p.parse_args()

    if not a.user_agent or "@" not in a.user_agent:
        sys.exit("Set SEC_USER_AGENT='Full Name email@domain' (Wikimedia asks for a contact).")
    if not 0 < a.rate <= 5:
        sys.exit("--rate must be in (0, 5]")
    for d in (a.gdelt_start, a.gdelt_end):
        if not re.fullmatch(r"\d{8}", d):
            sys.exit("--gdelt-start/--gdelt-end must be YYYYMMDD")

    out = os.path.abspath(a.out)
    man_root = os.path.join(out, "_manifests")
    jobs = []
    if a.only in (None, "gdelt"):
        tmp = os.path.join(man_root, "gdelt", "_index_tmp")
        os.makedirs(tmp, exist_ok=True)
        version, files = plan_gdelt(a.gdelt_start, a.gdelt_end, a.user_agent, tmp)
        jobs.append(("gdelt", version, files, tmp))
    if a.only in (None, "wikinews"):
        tmp = os.path.join(man_root, "wikinews", "_index_tmp")
        os.makedirs(tmp, exist_ok=True)
        version, files = plan_wikinews(a.wikinews_dump, a.wikinews_job, a.user_agent, tmp)
        jobs.append(("wikinews", version, files, tmp))

    for source, version, files, tmp in jobs:
        shown = files[:a.limit] if a.limit else files
        total = sum(x[2] for x in shown if isinstance(x[2], int))
        print(f"{source} {version}: {len(shown)} files, {total / 1e9:.2f} GB "
              f"({shown[0][0] if shown else '-'} .. {shown[-1][0] if shown else '-'})")
    if a.dry_run:
        for _, _, _, tmp in jobs:
            for name in os.listdir(tmp):
                os.remove(os.path.join(tmp, name))
            os.rmdir(tmp)
        return

    all_ok = True
    for source, version, files, tmp in jobs:
        man_dir = os.path.join(man_root, source, version, f"acq_{a.acq_date}")
        os.makedirs(man_dir, exist_ok=True)
        for name in os.listdir(tmp):  # keep the index files the checksums came from
            os.replace(os.path.join(tmp, name), os.path.join(man_dir, name))
        os.rmdir(tmp)
        all_ok &= run_source(source, version, files, out, man_root, a.acq_date,
                             a.user_agent, a.rate, a.limit)
    sys.exit(0 if all_ok else 1)


if __name__ == "__main__":
    main()
