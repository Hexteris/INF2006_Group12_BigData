#!/usr/bin/env python3
"""Reducer for stock records, duplicate detection and rejected records."""

import json
import sys

current_key = None
group = []

def flush(key, rows):
    if key is None or not rows:
        return

    parsed = [json.loads(value) for value in rows]
    valid = [x for x in parsed if x.get("status") == "VALID"]

    signatures = {
        json.dumps(x["record"], sort_keys=True, separators=(",", ":"))
        for x in valid
    }

    duplicate_group = len(valid) > 1
    conflicting = len(signatures) > 1

    for item in valid:
        item["duplicate_group"] = duplicate_group
        item["conflicting_duplicate_group"] = conflicting
        item["group_record_count"] = len(valid)
        print(json.dumps(item, separators=(",", ":")))

for line in sys.stdin:
    line = line.rstrip("\n")
    if not line:
        continue

    try:
        key, value = line.split("\t", 1)
    except ValueError:
        print(json.dumps({
            "status": "REJECT",
            "reason": "malformed_stream_line",
            "raw_line": line
        }))
        continue

    if key == "__REJECT__":
        if current_key is not None:
            flush(current_key, group)
            current_key = None
            group = []

        # Output rejected records immediately.
        try:
            print(json.dumps(json.loads(value), separators=(",", ":")))
        except json.JSONDecodeError:
            print(json.dumps({
                "status": "REJECT",
                "reason": "invalid_json",
                "raw_line": value
            }))
        continue

    if current_key is not None and key != current_key:
        flush(current_key, group)
        group = []

    current_key = key
    group.append(value)

flush(current_key, group)
