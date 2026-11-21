#!/usr/bin/env python3
"""Summarise one pipeline run: stage timings, resource peaks, row/byte counts and checks.

python3 summarize_run.py <run_dir>      (plain python3; no pandas needed)
Reads  : timings.csv, resources_raw.log, disk.csv, reconcile.tsv, inputs.txt,
         features_summary.json, validation.json, run_manifest.json
Writes : run_summary.json and prints a readable table.
"""
import csv
import datetime as dt
import json
import os
import re
import sys

run = sys.argv[1]
P = lambda n: os.path.join(run, n)


def load_json(name):
    try:
        return json.load(open(P(name)))
    except Exception:
        return {}


def iso(s):
    return dt.datetime.strptime(s, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=dt.timezone.utc)


# ---- stage timings
stages = []
if os.path.exists(P("timings.csv")):
    stages = list(csv.DictReader(open(P("timings.csv"))))
timed = [s for s in stages if s["exit_code"] not in ("SKIPPED",)]
failed = [s["stage"] for s in timed if s["exit_code"] != "0"]
skipped = [s["stage"] for s in stages if s["exit_code"] == "SKIPPED"]
secs = {s["stage"]: float(s["seconds"]) for s in timed}
wall = None
if timed:
    wall = (iso(timed[-1]["end_utc"]) - iso(timed[0]["start_utc"])).total_seconds()

# ---- reconciliation and inputs
recon = {}
if os.path.exists(P("reconcile.tsv")):
    for line in open(P("reconcile.tsv")):
        f = line.rstrip("\n").split("\t")
        if len(f) >= 4:
            recon[f[0]] = {"expected": f[1], "actual": f[2], "status": f[3]}
inputs = {}
if os.path.exists(P("inputs.txt")):
    for line in open(P("inputs.txt")):
        if "=" in line:
            k, v = line.strip().split("=", 1)
            inputs[k] = v

# ---- resources (best effort parsing of the sampler log)
res = {"samples": 0, "peak_primary_mem_used_mb": None, "peak_yarn_allocated_mb": None, "datanodes": {}}
if os.path.exists(P("resources_raw.log")):
    blocks = open(P("resources_raw.log"), errors="replace").read().split("### epoch=")[1:]
    res["samples"] = len(blocks)
    for b in blocks:
        m = re.search(r"MEM used_mb=(\d+)", b)
        if m:
            v = int(m.group(1))
            res["peak_primary_mem_used_mb"] = max(res["peak_primary_mem_used_mb"] or 0, v)
        alloc = [int(x) for x in re.findall(r"Allocated Resources\s*:\s*<memory:(\d+)", b)]
        if alloc:
            res["peak_yarn_allocated_mb"] = max(res["peak_yarn_allocated_mb"] or 0, sum(alloc))
        node = None
        for line in b.splitlines():
            if line.startswith("Name:"):
                node = line.split()[1]
                res["datanodes"].setdefault(node, {"peak_dfs_used_gb": 0, "peak_non_dfs_used_gb": 0,
                                                   "min_dfs_remaining_gb": None})
            elif node:
                for key, tag in (("DFS Used:", "peak_dfs_used_gb"), ("Non DFS Used:", "peak_non_dfs_used_gb"),
                                 ("DFS Remaining:", "min_dfs_remaining_gb")):
                    if line.startswith(key):
                        gb = int(line.split()[len(key.split())]) / 1024 ** 3
                        d = res["datanodes"][node]
                        if tag.startswith("min"):
                            d[tag] = round(gb if d[tag] is None else min(d[tag], gb), 2)
                        else:
                            d[tag] = round(max(d[tag], gb), 2)

# ---- disk: HDFS bytes written by this run
out_bytes = out_repl = 0
if os.path.exists(P("disk.csv")):
    for row in csv.DictReader(open(P("disk.csv"))):
        try:
            out_bytes = max(out_bytes, int(row["run_dir_bytes"]))
            out_repl = max(out_repl, int(row["run_dir_bytes_with_replicas"]))
        except Exception:
            pass

feat, val, man = load_json("features_summary.json"), load_json("validation.json"), load_json("run_manifest.json")
summary = {
    "run_name": man.get("run_name") or os.path.basename(run.rstrip("/")),
    "profile": man.get("profile"), "config": man.get("config"), "rep": man.get("rep"),
    "status": "FAILED" if failed or val.get("failed") or any(
        v["status"] == "FAIL" for v in recon.values()) else ("INCOMPLETE" if skipped else "OK"),
    "failed_stages": failed, "skipped_stages": skipped,
    "failed_checks": val.get("failed", []),
    "failed_reconciliations": [k for k, v in recon.items() if v["status"] == "FAIL"],
    "stage_seconds": secs, "sum_stage_seconds": round(sum(secs.values()), 1),
    "wall_clock_seconds": wall,
    "inputs": inputs, "resource_settings": man.get("resource_settings", {}),
    "reconciliation": recon,
    "split_report": feat.get("split_report", {}),
    "model_ready": {k: v.get("model_ready") for k, v in feat.get("splits", {}).items()},
    "resources": res, "run_output_bytes": out_bytes, "run_output_bytes_with_replicas": out_repl,
    "app_ids": {s["stage"]: s["app_ids"] for s in timed},
}
json.dump(summary, open(P("run_summary.json"), "w"), indent=2, default=str)

print("RUN", summary["run_name"], "->", summary["status"])
print("%-22s %-10s %10s %6s  %s" % ("stage", "engine", "seconds", "exit", "ids"))
for s in stages:
    print("%-22s %-10s %10s %6s  %s" % (s["stage"], s["engine"], s["seconds"], s["exit_code"], s["app_ids"]))
print("sum of stage seconds: %.1f   wall clock first start to last end: %s" % (
    summary["sum_stage_seconds"], "%.1f" % wall if wall else "n/a"))
print("peak primary memory used (MB):", res["peak_primary_mem_used_mb"],
      "| peak YARN allocated (MB):", res["peak_yarn_allocated_mb"])
for n, d in res["datanodes"].items():
    print("  datanode", n, d)
print("run output on HDFS: %.2f GiB (%.2f GiB with replicas)" % (out_bytes / 1024 ** 3, out_repl / 1024 ** 3))
for k, v in recon.items():
    print("  %-50s expected=%-12s actual=%-12s %s" % (k, v["expected"], v["actual"], v["status"]))
if summary["failed_stages"] or summary["failed_checks"] or summary["failed_reconciliations"]:
    print("FAILURES:", summary["failed_stages"], summary["failed_checks"], summary["failed_reconciliations"])
