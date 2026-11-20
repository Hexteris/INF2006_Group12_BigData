#!/usr/bin/env python3
"""Streaming reducer for GDELT records and per-key audit summaries."""

import json
import sys

current_key = None
record_count = 0
warning_count = 0
status_counts = {"VALID": 0, "REJECT": 0}

def flush():
    if current_key is None:
        return

    print(json.dumps({
        "status": "AUDIT",
        "group_key": current_key,
        "records": record_count,
        "valid_records": status_counts["VALID"],
        "rejected_records": status_counts["REJECT"],
        "date_order_warnings": warning_count
    }, separators=(",", ":")))

for line in sys.stdin:
    line = line.rstrip("\n")
    if not line:
        continue

    try:
        key, value = line.split("\t", 1)
        item = json.loads(value)
    except (ValueError, json.JSONDecodeError):
        print(json.dumps({
            "status": "REJECT",
            "reason": "malformed_stream_line",
            "raw_line": line
        }))
        continue

    if current_key is not None and key != current_key:
        flush()
        record_count = 0
        warning_count = 0
        status_counts = {"VALID": 0, "REJECT": 0}

    current_key = key

    status = item.get("status", "UNKNOWN")
    if status in status_counts:
        status_counts[status] += 1

    record_count += 1

    if item.get("date_order_warning"):
        warning_count += 1

    # Output each original record immediately.
    print(json.dumps(item, separators=(",", ":")))

flush()
