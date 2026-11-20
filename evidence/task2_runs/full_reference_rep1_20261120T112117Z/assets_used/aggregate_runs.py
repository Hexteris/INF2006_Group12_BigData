#!/usr/bin/env python3
"""Aggregate repeated runs into the Section 6 tables.

python3 aggregate_runs.py <runs_root>      (default ~/task2_runs)
For every (profile, config): all repetitions, median and range of each stage and of the total,
resource peaks and failures. Also compares the OUTPUT FINGERPRINT (reconciliation counts and
split row counts) across all runs of the same profile, so identical inputs give identical outputs
whatever the configuration.
Writes aggregate_runs.csv and aggregate_report.md in <runs_root>.
"""
import csv
import glob
import json
import os
import statistics as st
import sys
from collections import defaultdict

root = os.path.expanduser(sys.argv[1] if len(sys.argv) > 1 else "~/task2_runs")
runs = []
for p in sorted(glob.glob(os.path.join(root, "*", "run_summary.json"))):
    try:
        runs.append(json.load(open(p)))
    except Exception as e:
        print("skip", p, e)
if not runs:
    sys.exit("no run_summary.json files under " + root)

groups = defaultdict(list)
for r in runs:
    groups[(r["profile"], r["config"])].append(r)

lines, rows = [], []
for (profile, config), rs in sorted(groups.items()):
    ok = [r for r in rs if r["status"] == "OK"]
    lines.append("### %s / %s  (%d runs, %d OK)" % (profile, config, len(rs), len(ok)))
    lines.append("")
    lines.append("| rep | status | total stage s | wall s | peak primary MB | peak YARN MB | output GiB (x2) |")
    lines.append("|---|---|---|---|---|---|---|")
    for r in sorted(rs, key=lambda x: x["rep"] or 0):
        lines.append("| %s | %s | %s | %s | %s | %s | %.2f |" % (
            r["rep"], r["status"], r["sum_stage_seconds"],
            "%.0f" % r["wall_clock_seconds"] if r["wall_clock_seconds"] else "n/a",
            r["resources"]["peak_primary_mem_used_mb"], r["resources"]["peak_yarn_allocated_mb"],
            r["run_output_bytes_with_replicas"] / 1024 ** 3))
    for x in rs:
        if x["status"] != "OK":
            lines.append("- FAILURE rep %s: stages=%s checks=%s reconciliations=%s" % (
                x["rep"], x["failed_stages"], x["failed_checks"], x["failed_reconciliations"]))
    lines.append("")
    stages = []
    for r in ok:
        for s in r["stage_seconds"]:
            if s not in stages:
                stages.append(s)
    lines.append("| stage | " + " | ".join("rep%s" % r["rep"] for r in ok) + " | median | min | max |")
    lines.append("|---|" + "---|" * (len(ok) + 3))
    for s in stages + ["__total__"]:
        vals = [(r["sum_stage_seconds"] if s == "__total__" else r["stage_seconds"].get(s)) for r in ok]
        nums = [v for v in vals if v is not None]
        if not nums:
            continue
        med, lo, hi = st.median(nums), min(nums), max(nums)
        label = "TOTAL (sum of stages)" if s == "__total__" else s
        lines.append("| %s | %s | %.1f | %.1f | %.1f |" % (
            label, " | ".join("%.1f" % v if v is not None else "-" for v in vals), med, lo, hi))
        rows.append({"profile": profile, "config": config, "stage": label, "runs": len(nums),
                     "median_s": round(med, 1), "min_s": lo, "max_s": hi,
                     "values_s": ";".join("%.1f" % v for v in nums)})
    lines.append("")

# output equivalence across all runs of a profile (any configuration, any repetition)
lines.append("### Output fingerprint check (identical inputs must give identical outputs)")
lines.append("")
by_profile = defaultdict(list)
for r in runs:
    if r["status"] == "OK":
        fp = {k: v["actual"] for k, v in r["reconciliation"].items()}
        fp.update({"kept_" + k: v.get("kept") for k, v in r["split_report"].items()})
        fp.update({"model_ready_rows_" + k: (v or {}).get("rows") for k, v in r["model_ready"].items()})
        by_profile[r["profile"]].append((r["run_name"], fp))
for profile, items in sorted(by_profile.items()):
    base_name, base = items[0]
    diffs = [(n, k, base.get(k), fp.get(k)) for n, fp in items[1:] for k in base if fp.get(k) != base.get(k)]
    lines.append("- %s: %d runs compared with %s -> %s" % (
        profile, len(items), base_name, "IDENTICAL" if not diffs else "DIFFERENCES: %s" % diffs[:10]))
lines.append("")

open(os.path.join(root, "aggregate_report.md"), "w").write("\n".join(lines))
with open(os.path.join(root, "aggregate_runs.csv"), "w", newline="") as fh:
    w = csv.DictWriter(fh, fieldnames=["profile", "config", "stage", "runs", "median_s", "min_s", "max_s", "values_s"])
    w.writeheader()
    w.writerows(rows)
print("\n".join(lines))
print("wrote", os.path.join(root, "aggregate_report.md"), "and aggregate_runs.csv")
