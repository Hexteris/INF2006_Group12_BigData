#!/usr/bin/env python3
"""Hadoop Streaming mapper for extracted GDELT TSV records.

Default assumptions for the 58-column input:
  column 1 (index 1) = SQLDATE/event date (YYYYMMDD)
  column 57 (index 56) = DATEADDED (YYYYMMDD)

Confirm these indexes against your exact GDELT file/schema before running.
Override with EVENT_DATE_INDEX and DATEADDED_INDEX environment variables.
"""
import json
import os
import sys
from datetime import datetime

EXPECTED_COLUMNS = int(os.environ.get("GDELT_EXPECTED_COLUMNS", "58"))
EVENT_DATE_INDEX = int(os.environ.get("EVENT_DATE_INDEX", "1"))
DATEADDED_INDEX = int(os.environ.get("DATEADDED_INDEX", "56"))

def parse_yyyymmdd(value, label):
    value = value.strip()
    datetime.strptime(value, "%Y%m%d")
    return value

for line in sys.stdin:
    raw = line.rstrip("\r\n")
    if not raw:
        continue
    fields = raw.split("\t")
    try:
        if len(fields) != EXPECTED_COLUMNS:
            raise ValueError("wrong_column_count:" + str(len(fields)))
        if EVENT_DATE_INDEX >= len(fields) or DATEADDED_INDEX >= len(fields):
            raise ValueError("configured_date_index_out_of_range")
        event_date = parse_yyyymmdd(fields[EVENT_DATE_INDEX], "event_date")
        date_added = parse_yyyymmdd(fields[DATEADDED_INDEX], "DATEADDED")
        if date_added < event_date:
            # Not automatically rejected: DATEADDED and event date can differ.
            # Keep the record and flag it for investigation.
            date_order_warning = True
        else:
            date_order_warning = False

        payload = {
            "status": "VALID",
            "event_date": event_date,
            "date_added": date_added,
            "date_order_warning": date_order_warning,
            "raw_tsv": raw,
        }
        key = "VALID|" + date_added
    except Exception as exc:
        payload = {"status": "REJECT", "reason": str(exc), "raw_tsv": raw}
        key = "REJECT|UNKNOWN"

    print(key + "\t" + json.dumps(payload, separators=(",", ":")))
