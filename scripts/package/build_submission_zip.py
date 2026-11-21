#!/usr/bin/env python3
"""Build the INF2006 G12 submission ZIP and run the brief's checks on it.

    python3 scripts/package/build_submission_zip.py --report report.pdf [--out INF2006_P2_G12.zip]
            [--report-text report.txt] [--max-mb 50]

Copies the submission files into a ZIP whose root holds report.pdf and the repo folders below, then checks:
  * required files exist (report, notebooks, saved models, driver, scripts, SQL, evidence)
  * every exported notebook has saved outputs
  * no credentials, private keys or machine-specific paths in any text file
  * no file larger than --max-mb, and no raw data (raw/); the prepared Task 2 outputs that Tasks 3 and 4 read are included
  * if --report-text is given (or pdftotext is installed): every repo path the report cites exists in the ZIP

Exit code 1 if any check fails. Standard library only; runs on Windows, macOS and Linux.
"""
import argparse
import json
import os
import re
import shutil
import subprocess
import sys
import zipfile

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))

INCLUDE = [
    "README.md", "install-numpy.sh",
    "project-up.ps1", "project-up.sh", "project-down.ps1", "project-down.sh",
    "config", "docs", "runbooks", "notebooks", "scripts", "sql",
    "task2/code", "task2/cleaned", "task2/model_ready_v1", "task2_driver", "task3_saved_models", "evidence",
]
# Small code-and-log archive that task2/code/README.md and the Task 2 evidence README cite (no data inside).
KEEP_ARCHIVES = {"task2/code/task2_complete_code_backup_20261109.tar.gz"}
# Run cell by cell over SSH, so it has no saved outputs; its evidence is staging_record.csv / staging_summary.txt.
NO_OUTPUT_OK = {"notebooks/INF2006_G12_stage_corpus.ipynb"}
DRIVER_TEMPLATE = "task2_driver/INF2006_G12_task2_driver.ipynb"
EXECUTED_DIR = "evidence/task2_runs/executed_notebooks/"


def executed_driver_copies(files):
    """Executed driver notebooks that actually contain outputs."""
    found = []
    for f in files:
        if f.startswith(EXECUTED_DIR) and f.endswith(".ipynb"):
            nb = json.load(open(os.path.join(ROOT, f), encoding="utf-8-sig"))
            if any(c.get("outputs") for c in nb.get("cells", []) if c.get("cell_type") == "code"):
                found.append(f)
    return found
EXCLUDE_DIRS = {".git", "__pycache__", ".ipynb_checkpoints", "raw", ".idea", ".vscode"}
EXCLUDE_FILES = {".DS_Store", ".project-cluster-id", "Thumbs.db"}
EXCLUDE_SUFFIXES = (".pem", ".ppk", ".tar.gz", ".pyc")
REQUIRED = [
    "notebooks/INF2006_G12_stage_corpus.ipynb",
    "notebooks/INF2006_G12_task1.ipynb",
    "notebooks/INF2006_G12_task3_volatility_labels.ipynb",
    "notebooks/INF2006_G12_task3_direction.ipynb",
    "notebooks/INF2006_G12_task3_volatility.ipynb",
    "notebooks/INF2006_G12_task4_analytics.ipynb",
    "task2_driver/INF2006_G12_task2_driver.ipynb",
    "task2_driver/train_task3.py",
    "task3_saved_models",
    "sql", "task2/code", "config/emr-configurations.json", "config/instance-groups.json",
    "evidence/task1", "evidence/task2", "evidence/task3", "evidence/task4",
]
SECRET_PATTERNS = [
    (r"AKIA[0-9A-Z]{16}", "AWS access key ID"),
    (r"ASIA[0-9A-Z]{16}", "AWS temporary access key ID"),
    (r"aws_secret_access_key\s*=", "AWS secret key line"),
    (r"aws_session_token\s*=", "AWS session token line"),
    (r"-----BEGIN [A-Z ]*PRIVATE KEY-----", "private key"),
    (r"ghp_[A-Za-z0-9]{36}", "GitHub token"),
]
MACHINE_PATH = re.compile(r"(/Users/[A-Za-z0-9._-]+/|[A-Za-z]:\\\\?Users\\\\?[A-Za-z0-9._-]+|/home/(?!hadoop\b|jovyan\b)[a-z][a-z0-9_-]+/)")
TEXT_SUFFIXES = (".py", ".sh", ".ps1", ".sql", ".md", ".txt", ".json", ".ipynb", ".csv", ".log", ".yml", ".yaml", ".dot")


def walk(rel):
    full = os.path.join(ROOT, rel)
    if os.path.isfile(full):
        yield rel
        return
    for d, dirs, files in os.walk(full):
        dirs[:] = sorted(x for x in dirs if x not in EXCLUDE_DIRS)
        for f in sorted(files):
            r = os.path.relpath(os.path.join(d, f), ROOT).replace(os.sep, "/")
            if f in EXCLUDE_FILES or (f.endswith(EXCLUDE_SUFFIXES) and r not in KEEP_ARCHIVES):
                continue
            yield r


def report_text(args):
    if args.report_text:
        return open(args.report_text, encoding="utf-8", errors="replace").read()
    if shutil.which("pdftotext"):
        out = subprocess.run(["pdftotext", args.report, "-"], capture_output=True, text=True)
        if out.returncode == 0:
            return out.stdout
    return None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--report", required=True, help="the exported report PDF")
    ap.add_argument("--out", default="INF2006_P2_G12.zip")
    ap.add_argument("--report-text", help="plain-text export of the report, for the cited-path check")
    ap.add_argument("--max-mb", type=float, default=50)
    args = ap.parse_args()

    results = []
    def check(name, ok, detail=""):
        results.append((name, ok, detail))

    files = []
    for rel in INCLUDE:
        if os.path.exists(os.path.join(ROOT, rel)):
            files.extend(walk(rel))
    files = sorted(set(files))

    check("report.pdf exists", os.path.isfile(args.report), args.report)
    missing = [r for r in REQUIRED if not os.path.exists(os.path.join(ROOT, r))]
    check("required files present", not missing, ", ".join(missing))
    check("Section 6 evidence present (evidence/task2_runs/aggregate_report.md)",
          os.path.isfile(os.path.join(ROOT, "evidence/task2_runs/aggregate_report.md")),
          "sync s3://<bucket>/evidence/task2_runs/ into evidence/task2_runs/ after the timed runs")

    empty = []
    for f in files:
        if f.endswith(".ipynb") and not f.startswith("runbooks/") and f not in NO_OUTPUT_OK:
            nb = json.load(open(os.path.join(ROOT, f), encoding="utf-8-sig"))
            code = [c for c in nb.get("cells", []) if c.get("cell_type") == "code"]
            if code and not any(c.get("outputs") for c in code):
                # The driver is a clean template that run_matrix.sh executes with nbconvert;
                # its executed copies (with outputs) live in evidence/task2_runs/executed_notebooks/.
                if f == DRIVER_TEMPLATE and executed_driver_copies(files):
                    continue
                empty.append(f)
    check("exported notebooks have saved outputs", not empty,
          ", ".join(empty) + (" (driver: ship the executed copies from ~/task2_runs/executed_notebooks)" if empty else ""))

    secrets, paths, big = [], [], []
    for f in files:
        full = os.path.join(ROOT, f)
        size = os.path.getsize(full)
        if size > args.max_mb * 1024 * 1024:
            big.append("%s (%.0f MB)" % (f, size / 1048576))
        if not f.endswith(TEXT_SUFFIXES) or size > 20 * 1024 * 1024:
            continue
        text = open(full, encoding="utf-8", errors="replace").read()
        for pat, label in SECRET_PATTERNS:
            if re.search(pat, text):
                secrets.append("%s: %s" % (f, label))
        m = MACHINE_PATH.search(text)
        if m and not f.endswith((".log",)):
            paths.append("%s: %s" % (f, m.group(0)))
    check("no credentials or private keys", not secrets, "; ".join(secrets))
    check("no machine-specific paths", not paths, "; ".join(paths[:10]))
    check("no file over %g MB" % args.max_mb, not big, "; ".join(big))

    text = report_text(args)
    if text is None:
        check("report cites only files that exist", True, "skipped: pass --report-text or install pdftotext")
    else:
        # Skip S3/HDFS paths (preceded by "/" or a bucket), and accept a path cut short by a PDF line break
        # when it is the start of a file that is in the ZIP.
        cited = set(re.findall(r"(?<![/\w<>.-])((?:evidence|notebooks|task2_driver|task2|task3_saved_models|sql|scripts|docs|config|runbooks)/[A-Za-z0-9_./*<>{}-]+)", text))
        bad = []
        for c in sorted(cited):
            c = c.rstrip(".,;:)")
            if any(ch in c for ch in "<>{}*"):
                c = re.split(r"[<>{}*]", c)[0].rstrip("/")
            if not c or os.path.exists(os.path.join(ROOT, c)):
                continue
            if c.endswith(("_", "-")) or any(("/" + c.rstrip("/")) in ("/" + f) for f in files):
                continue
            bad.append(c)
        check("report cites only files that exist", not bad, ", ".join(bad))

    with zipfile.ZipFile(args.out, "w", zipfile.ZIP_DEFLATED) as z:
        if os.path.isfile(args.report):
            z.write(args.report, "report.pdf")
        for f in files:
            z.write(os.path.join(ROOT, f), f)
    total = os.path.getsize(args.out) / 1048576

    print("\nSubmission checks")
    for name, ok, detail in results:
        print("  %-4s %s%s" % ("PASS" if ok else "FAIL", name, (": " + detail) if detail and not ok else ""))
    print("\nWrote %s: %d files, %.1f MB" % (args.out, len(files) + 1, total))
    print("Not included on purpose: raw/ (raw data), .git, *.pem, other *.tar.gz, README (Task 2).md (old handover note)")
    sys.exit(0 if all(ok for _, ok, _ in results) else 1)


if __name__ == "__main__":
    main()
