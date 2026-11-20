# Task 2 – Hadoop Streaming Python scripts

This starter package contains the Python programs for the Stock Prices and
GDELT Hadoop Streaming stages.

## Contents

- `stock_prices/mapper.py` – parse and validate stock CSV records.
- `stock_prices/reducer.py` – group by stock symbol/date and flag duplicate
  or conflicting duplicate groups.
- `gdelt/extract_gdelt.py` – extract text/TSV members from every ZIP archive
  and create an extraction manifest.
- `gdelt/mapper.py` – validate GDELT record width and date fields.
- `gdelt/reducer.py` – retain rows and emit audit summaries.

## Important checks before running

1. Confirm your stock CSV header names. The stock mapper supports common
   headings such as `Date`, `Symbol`, `Open`, `High`, `Low`, `Close`,
   `Adj Close`, and `Volume`. Edit `ALIASES` if your files differ.
2. Confirm the exact GDELT 58-column schema. The GDELT mapper defaults to
   `SQLDATE` at index 1 and `DATEADDED` at index 57 (zero-based indexes).
   Do not assume these are correct until checked against your source header
   or the schema supplied with your dataset.
3. The mappers/reducers emit JSON Lines. Valid, rejected, and audit records
   are distinguished by the `status` field (`VALID`, `REJECT`, `AUDIT`).
   Downstream Spark code should filter these statuses into separate outputs.
4. The stock reducer flags duplicate groups but deliberately does not choose
   a winner. Investigate conflicting duplicates before deduplicating.
5. GDELT `DATEADDED` is retained separately from event date. For time-aware
   features, use `DATEADDED` as the availability date to prevent future
   information leakage.
6. These scripts do not yet launch Hadoop jobs or write directly to S3/HDFS.
   Hadoop Streaming commands and paths should be added after confirming your
   EMR Hadoop Streaming JAR, S3 paths, and HDFS output locations.

## Local syntax checks

```bash
python3 -m py_compile stock_prices/mapper.py stock_prices/reducer.py
python3 -m py_compile gdelt/extract_gdelt.py gdelt/mapper.py gdelt/reducer.py
```

## GDELT extraction example

```bash
python3 gdelt/extract_gdelt.py \
  --input-dir /data/gdelt_zip \
  --output-dir /data/gdelt_extracted
```

This creates `extracted_gdelt.tsv` and `extract_manifest.json`.
