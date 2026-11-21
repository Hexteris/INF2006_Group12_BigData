#!/usr/bin/env python3
"""Hadoop Streaming mapper for stock-price CSV files.

Input: CSV lines (with a header in each source file).
Output: key<TAB>JSON. Valid rows are keyed by symbol+date so duplicate
observations for the same stock/date reach the same reducer.
"""
import csv
import json
import os
import sys
from datetime import datetime

# Adjust these names if your CSV uses different column headings.
ALIASES = {
    "date": ["date", "trading_date", "timestamp"],
    "symbol": ["symbol", "ticker", "stock", "tic", "name"],
    "open": ["open"],
    "high": ["high"],
    "low": ["low"],
    "close": ["close"],
    "adj_close": ["adj close", "adj_close", "adjusted close", "adjusted_close"],
    "volume": ["volume", "vol"],
}
REQUIRED = ["date", "symbol", "open", "high", "low", "close", "volume"]

def norm(s):
    return " ".join(str(s).strip().lower().replace("_", " ").split())

def parse_date(value):
    value = str(value).strip()
    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%m/%d/%Y", "%Y%m%d"):
        try:
            return datetime.strptime(value, fmt).strftime("%Y-%m-%d")
        except ValueError:
            pass
    raise ValueError("unrecognised date: " + value)

def number(value, field):
    value = str(value).strip().replace(",", "")
    if value == "" or value.lower() in {"null", "none", "na", "n/a"}:
        raise ValueError("missing " + field)
    result = float(value)
    if result != result or result in (float("inf"), float("-inf")):
        raise ValueError("non-finite " + field)
    return result

def emit(key, payload):
    print(key + "\t" + json.dumps(payload, separators=(",", ":"), allow_nan=False))

reader = csv.reader(sys.stdin)

# Known column positions for all_stocks_5yr.csv.
# Hadoop splits the input across mapper tasks, so not every task
# receives the original CSV header.
header_map = {
    "date": 0,
    "open": 1,
    "high": 2,
    "low": 3,
    "close": 4,
    "volume": 5,
    "symbol": 6,
}

for fields in reader:
    if not fields or all(not x.strip() for x in fields):
        continue

    # Detect a header in each file, not just the first file.
    candidate = {norm(name): i for i, name in enumerate(fields)}
    if any(alias in candidate for alias in ALIASES["symbol"]) and any(
        alias in candidate for alias in ALIASES["date"]
    ):
        header_map = {}
        for canonical, aliases in ALIASES.items():
            for alias in aliases:
                if alias in candidate:
                    header_map[canonical] = candidate[alias]
                    break
        missing = [f for f in REQUIRED if f not in header_map]
        if missing:
            emit("__REJECT__", {"status": "REJECT", "reason": "missing_columns:" + ",".join(missing),
                                "raw": fields})
            header_map = None
        continue

    try:
        if header_map is None:
            raise ValueError("header_not_seen_or_unrecognised")
        row = {}
        for field in REQUIRED:
            idx = header_map[field]
            if idx >= len(fields):
                raise ValueError("short_row")
            row[field] = fields[idx].strip()

        row["date"] = parse_date(row["date"])
        row["symbol"] = row["symbol"].strip().upper()
        if not row["symbol"]:
            raise ValueError("missing symbol")

        for field in ("open", "high", "low", "close", "volume"):
            row[field] = number(row[field], field)
        if "adj_close" in header_map and header_map["adj_close"] < len(fields):
            raw_adj = fields[header_map["adj_close"]].strip()
            row["adj_close"] = number(raw_adj, "adj_close") if raw_adj else None
        else:
            row["adj_close"] = None

        if min(row["open"], row["high"], row["low"], row["close"]) <= 0:
            raise ValueError("non_positive_price")
        if row["volume"] < 0:
            raise ValueError("negative_volume")
        if row["high"] < row["low"]:
            raise ValueError("high_below_low")
        tolerance = 1e-8
        if row["high"] + tolerance < max(row["open"], row["close"]):
            raise ValueError("high_below_open_or_close")
        if row["low"] - tolerance > min(row["open"], row["close"]):
            raise ValueError("low_above_open_or_close")
        if row["adj_close"] is not None and row["adj_close"] <= 0:
            raise ValueError("non_positive_adj_close")

        payload = {"status": "VALID", "record": row}
        emit(row["symbol"] + "|" + row["date"], payload)
    except Exception as exc:
        emit("__REJECT__", {"status": "REJECT", "reason": str(exc), "raw": fields})
