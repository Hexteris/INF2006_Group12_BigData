#!/usr/bin/env python3
"""Hive-compatible GDELT reducer.

Valid records are emitted as their original 58-column TSV rows.
Rejected records and audit summaries are emitted as JSON to stderr
for separate Hadoop Streaming output handling.
"""

import json
import sys

current_key = None
record_count = 0
warning_count = 0
valid_count = 0
rejected_count = 0


def flush():
    if current_key is None:
        return

    audit = {
        "status": "AUDIT",
        "group_key": current_key,
        "records": record_count,
        "valid_records": valid_count,
        "rejected_records": rejected_count,
        "date_order_warnings": warning_count
    }

    print(json.dumps(audit, separators=(",", ":")), file=sys.stderr)


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
        }), file=sys.stderr)
        continue

    if current_key is not None and key != current_key:
        flush()
        record_count = 0
        warning_count = 0
        valid_count = 0
        rejected_count = 0

    current_key = key
    record_count += 1

    if item.get("status") == "VALID":
        valid_count += 1

        if item.get("date_order_warning"):
            warning_count += 1

        print(item["raw_tsv"])

    elif item.get("status") == "REJECT":
        rejected_count += 1

        print(json.dumps(item, separators=(",", ":")),
              file=sys.stderr)

    else:
        print(json.dumps({
            "status": "REJECT",
            "reason": "unknown_status",
            "raw_record": item
        }), file=sys.stderr)

flush()
