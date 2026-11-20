#!/usr/bin/env python3
"""Task 2 / stage spark_validate: re-check the prepared tables from Hive (read through Spark).

spark-submit validate_features.py --db <hive db> --summary-json features_summary.json \
                                  --local-json validation.json [--use-2018-labels 0]

Checks (each reported PASS/FAIL; exit code 1 if any FAIL):
  split dates stay inside their period and the periods do not overlap (no shuffling across periods)
  target windows end inside the period (boundary rule) for training and validation, and testing
  no duplicate (ticker, trade_date) rows
  model-ready tables contain no nulls / NaN / infinities in features or targets, labels are 0 or 1
  leakage columns (future_close, future_trade_date) are absent from the model-ready tables
  row accounting: Hive split rows = kept rows + excluded rows for every split
"""
import argparse
import json
import sys

from pyspark.sql import SparkSession, functions as F

ap = argparse.ArgumentParser()
ap.add_argument("--db", required=True)
ap.add_argument("--summary-json", required=True)
ap.add_argument("--local-json", default="validation.json")
ap.add_argument("--use-2018-labels", type=int, default=0)
a = ap.parse_args()

PERIODS = {"training": ("2013-01-01", "2015-12-31"),
           "validation": ("2016-01-01", "2016-12-31"),
           "testing": ("2017-01-01", "2017-12-31")}
LEAK = ["future_close", "future_trade_date"]

spark = SparkSession.builder.appName("task2_validate_" + a.db).enableHiveSupport().getOrCreate()
spark.sparkContext.setLogLevel("WARN")
summ = json.load(open(a.summary_json))
core, gd, targets = summ["core_features"], summ["gdelt_features"], summ["targets"]
checks = []


def add(name, ok, detail):
    checks.append({"check": name, "status": "PASS" if ok else "FAIL", "detail": detail})
    print("%-52s %s  %s" % (name, "PASS" if ok else "FAIL", detail))


ranges = {}
for name, (lo, hi) in PERIODS.items():
    feat = spark.table("%s.features_%s" % (a.db, name))
    ready = spark.table("%s.model_ready_%s" % (a.db, name))
    r = feat.agg(F.min("trade_date"), F.max("trade_date"), F.max("future_trade_date"),
                 F.count("*")).first()
    ranges[name] = (str(r[0]), str(r[1]))
    add("%s dates inside period" % name, str(r[0]) >= lo and str(r[1]) <= hi,
        "%s..%s expected within %s..%s" % (r[0], r[1], lo, hi))
    limit = "2099-12-31" if (name == "testing" and a.use_2018_labels) else hi
    add("%s target window ends inside period" % name, r[2] is None or str(r[2]) <= limit,
        "max future_trade_date=%s limit=%s" % (r[2], limit))
    dup = feat.groupBy("ticker", "trade_date").count().filter("count > 1").count()
    add("%s no duplicate ticker/date" % name, dup == 0, "duplicate keys=%d" % dup)
    bad = ready.agg(*[F.sum((F.col(c).isNull() | F.isnan(F.col(c).cast("double"))
                             | F.col(c).cast("double").isin(float("inf"), float("-inf"))).cast("int")).alias(c)
                      for c in core + targets]).first().asDict()
    total_bad = sum(v or 0 for v in bad.values())
    add("%s no null/NaN/inf in features and targets" % name, total_bad == 0,
        "bad cells=%d" % total_bad)
    lab = ready.filter(~F.col("label").isin(0, 1)).count()
    add("%s label is 0/1" % name, lab == 0, "invalid labels=%d" % lab)
    leaked = [c for c in LEAK if c in ready.columns]
    add("%s no leakage columns in model-ready" % name, not leaked, "found=%s" % leaked)
    hive_rows = spark.table("%s.%s_prices" % (a.db, name)).count()
    rep = summ["split_report"].get(name, {})
    acc = (rep.get("kept", 0) + rep.get("excluded_no_complete_target", 0)
           + rep.get("excluded_target_crosses_period_end", 0))
    add("%s Hive split rows = kept + excluded" % name, acc == hive_rows,
        "hive=%d kept=%d no_target=%d crosses_end=%d" % (
            hive_rows, rep.get("kept", 0), rep.get("excluded_no_complete_target", 0),
            rep.get("excluded_target_crosses_period_end", 0)))

order = [ranges[n] for n in ("training", "validation", "testing")]
add("periods are chronological and disjoint", order[0][1] < order[1][0] and order[1][1] < order[2][0],
    str(order))

res = {"db": a.db, "checks": checks, "failed": [c["check"] for c in checks if c["status"] == "FAIL"]}
with open(a.local_json, "w") as fh:
    json.dump(res, fh, indent=2)
print("FAILED CHECKS:", res["failed"] or "none")
spark.stop()
sys.exit(1 if res["failed"] else 0)
