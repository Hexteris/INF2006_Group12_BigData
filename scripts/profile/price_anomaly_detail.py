#!/usr/bin/env python3
"""List the individual price rows behind the Task 1 profile counts (read-only, stdlib).

The EMR notebook (notebooks/INF2006_G12_task1.ipynb) produced the counts; this lists
which rows they are, so the data dictionary and Task 2 rules can name them.
Prints the input sha256 so the output can be tied to the staged raw file.

Usage: python3 price_anomaly_detail.py <all_stocks_5yr.csv> [--end 2017-12-29]
"""
import argparse
import csv
import hashlib
from collections import defaultdict


def main():
    p = argparse.ArgumentParser()
    p.add_argument("csv")
    p.add_argument("--end", default="2017-12-29", help="last date of the accepted price scope")
    a = p.parse_args()

    h = hashlib.sha256(open(a.csv, "rb").read()).hexdigest()
    rows = list(csv.DictReader(open(a.csv, newline="", encoding="utf-8")))
    print(f"input={a.csv} sha256={h} rows={len(rows)}")

    num = lambda x: float(x) if x not in ("", None) else None
    flagged = {}
    print("\n# Rows with a null open/high/low")
    for r in rows:
        if any(r[k] == "" for k in ("open", "high", "low", "close", "volume")):
            print(f"  {r['Name']:<6} {r['date']}  open={r['open']!r} high={r['high']!r} low={r['low']!r} "
                  f"close={r['close']} volume={r['volume']}")
            flagged[(r["Name"], r["date"])] = "null_field"
    print("\n# Rows violating low <= open/close <= high")
    for r in rows:
        o, hi, lo, c = (num(r[k]) for k in ("open", "high", "low", "close"))
        if None in (o, hi, lo, c):
            continue
        why = ("high_lt_low" if hi < lo else "open_outside_range" if not lo <= o <= hi
               else "close_outside_range" if not lo <= c <= hi else "")
        if why:
            print(f"  {r['Name']:<6} {r['date']}  {why:<20} o={o} h={hi} l={lo} c={c}")
            flagged.setdefault((r["Name"], r["date"]), why)
    print("\n# Zero-volume rows")
    for r in rows:
        if r["volume"] == "0":
            print(f"  {r['Name']:<6} {r['date']}")
            flagged.setdefault((r["Name"], r["date"]), "zero_volume")
    print("\n# Day-over-day close moves > 50% (split, spin-off, data error or genuine)")
    by = defaultdict(list)
    for r in rows:
        by[r["Name"]].append(r)
    for t, v in sorted(by.items()):
        v.sort(key=lambda r: r["date"])
        for x, y in zip(v, v[1:]):
            if x["close"] and y["close"]:
                ratio = float(y["close"]) / float(x["close"])
                if abs(ratio - 1) > 0.5:
                    print(f"  {t:<6} {x['date']} -> {y['date']}  close {x['close']} -> {y['close']}  ratio={ratio:.3f}")

    inw = [r for r in rows if r["date"] <= a.end]
    bad_inw = [k for k in flagged if k[1] <= a.end]
    print(f"\n# Reconciliation to the spec's accepted-bar count")
    print(f"  rows_on_or_before_{a.end}={len(inw)}")
    print(f"  of which flagged (null field, OHLC inconsistency or zero volume)={len(bad_inw)}")
    print(f"  remaining={len(inw) - len(bad_inw)}   (spec: 605,887 accepted bars)")


if __name__ == "__main__":
    main()
