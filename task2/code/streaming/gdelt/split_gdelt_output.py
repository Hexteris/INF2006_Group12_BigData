#!/usr/bin/env python3

import json
import sys
from collections import Counter

valid_count = 0
rejected_count = 0
audit_count = 0
invalid_count = 0
rejection_reasons = Counter()

with open("valid.tsv", "w", encoding="utf-8") as valid_file, \
     open("rejected.jsonl", "w", encoding="utf-8") as rejected_file, \
     open("audit.jsonl", "w", encoding="utf-8") as audit_file:

    for line in sys.stdin:
        line = line.rstrip("\n")

        if not line:
            continue

        try:
            item = json.loads(line)
        except json.JSONDecodeError:
            invalid_count += 1
            continue

        status = item.get("status")

        if status == "VALID":
            raw_tsv = item.get("raw_tsv")

            if raw_tsv and len(raw_tsv.split("\t")) == 58:
                valid_file.write(raw_tsv + "\n")
                valid_count += 1
            else:
                item["reason"] = "invalid_raw_tsv_column_count"
                rejected_file.write(json.dumps(item) + "\n")
                rejected_count += 1
                rejection_reasons["invalid_raw_tsv_column_count"] += 1

        elif status == "REJECT":
            rejected_file.write(json.dumps(item) + "\n")
            rejected_count += 1
            rejection_reasons[item.get("reason", "unspecified")] += 1

        elif status == "AUDIT":
            audit_file.write(json.dumps(item) + "\n")
            audit_count += 1

        else:
            invalid_count += 1

summary = {
    "valid_records": valid_count,
    "rejected_records": rejected_count,
    "audit_summaries": audit_count,
    "invalid_output_lines": invalid_count,
    "rejection_reasons": dict(rejection_reasons)
}

print(json.dumps(summary, indent=2))
