#!/usr/bin/env python3

import csv
import math
import os
import sys
from datetime import date

LAST_IN_SCOPE_DATE = date(2017, 12, 29)

EXPECTED_HEADER = [
    "date", "open", "high", "low", "close", "volume", "name"
]

SOURCE_FILE = os.path.basename(
    os.environ.get("mapreduce_map_input_file")
    or os.environ.get("map_input_file")
    or "stdin"
)


def emit(key):
    print(f"{key}\t1")


def is_header(row):
    return (
        len(row) == len(EXPECTED_HEADER)
        and [value.strip().lower().lstrip("\ufeff") for value in row]
        == EXPECTED_HEADER
    )


def process_row(row):
    emit(f"RAW|{SOURCE_FILE}")

    if len(row) != 7:
        emit(f"MALFORMED|{SOURCE_FILE}|wrong_column_count")
        return

    raw_date, open_, high, low, close, volume, ticker = row

    try:
        trade_date = date.fromisoformat(raw_date.strip())
    except (ValueError, TypeError):
        emit(f"MALFORMED|{SOURCE_FILE}|invalid_date")
        return

    if trade_date > LAST_IN_SCOPE_DATE:
        emit(f"OUT_OF_SCOPE|{SOURCE_FILE}")
        return

    year = trade_date.year
    ticker = ticker.strip()
    emit(f"ROWS|{SOURCE_FILE}|{ticker}|{year}")

    fields = [raw_date, open_, high, low, close, volume, ticker]
    if any(value.strip() == "" for value in fields):
        emit(f"REJECT|{SOURCE_FILE}|null_field")
        emit(f"STATUS|{SOURCE_FILE}|{year}|REJECTED")
        return

    try:
        o = float(open_)
        h = float(high)
        l = float(low)
        c = float(close)
        v = int(volume)

        if not all(math.isfinite(x) for x in (o, h, l, c)):
            raise ValueError("non-finite price")
    except (ValueError, TypeError):
        emit(f"MALFORMED|{SOURCE_FILE}|invalid_numeric_value")
        return

    reason = None
    if h < l:
        reason = "high_lt_low"
    elif not (l <= o <= h):
        reason = "open_outside_range"
    elif not (l <= c <= h):
        reason = "close_outside_range"
    elif v == 0:
        reason = "zero_volume"

    if reason:
        emit(f"REJECT|{SOURCE_FILE}|{reason}")
        emit(f"STATUS|{SOURCE_FILE}|{year}|REJECTED")
    else:
        emit(f"STATUS|{SOURCE_FILE}|{year}|ACCEPTED_CANDIDATE")


def main():
    reader = csv.reader(sys.stdin)

    try:
        first_row = next(reader, None)

        if first_row is None:
            return

        if not is_header(first_row):
            process_row(first_row)

        for row in reader:
            process_row(row)

    except csv.Error:
        emit(f"MALFORMED|{SOURCE_FILE}|csv_error")


if __name__ == "__main__":
    main()