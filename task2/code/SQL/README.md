# Hive Queries (Owner: B)

These scripts implement Role B's Hive SQL processing for Task 2.

The scope covers:
- Stock price data
- GDELT event data

The scripts register raw datasets, perform data cleaning and validation, separate rejected and out-of-scope records, generate chronological datasets and support data quality checks.

## Execution order

| Script | Purpose | Main output |
|---|---|---|
| `01_raw_tables.sql` | Register raw stock price data as a Hive external table | `g12.raw_prices` |
| `02_clean_prices.sql` | Clean and validate stock prices; separate rejected and out-of-scope records | `g12.cleaned_prices`, `g12.rejected_prices`, `g12.out_of_scope_prices` |
| `03_price_splits.sql` | Create chronological training, validation and testing datasets | `g12.training_prices`, `g12.validation_prices`, `g12.testing_prices` |
| `04_gdelt_tables.sql` | Register processed GDELT event records as a Hive external table | `g12.raw_gdelt` |
| `05_clean_gdelt.sql` | Clean GDELT records, separate rejected records and generate daily analytical features | `g12.cleaned_gdelt_events`, `g12.rejected_gdelt`, `g12.daily_gdelt_features` |
| `06_reconcile_prices.sql` | Verify stock price row counts, rejected records and data reconciliation | PASS/FAIL reconciliation results |

## Environment and input requirements

### S3 and HDFS structure

Follow the Task 1 setup notebook for the required S3 bucket structure, data staging procedures and HDFS corpus directory structure.

Each team may use its own AWS Learner Lab S3 bucket. The directory structure and HDFS paths should remain consistent across environments. Only the S3 bucket name needs to be changed to match the respective environment.

### Stock price input

The raw stock price dataset must be staged in HDFS at:

`/corpus/raw/prices/v1_2013-2017/acq_2026-11-04`

This is the input location used by `01_raw_tables.sql`.

### GDELT input

The GDELT dataset contains daily ZIP archives with 58 tab-separated fields per event record.

The ZIP archives must first be processed and decompressed using Hadoop Streaming.

The resulting tab-separated event records must be available in HDFS at:

`/corpus/processed/gdelt/events/`

This is the input location used by `04_gdelt_tables.sql`.

## Example execution

Run the commands from the project root after staging the required input files in HDFS.

### Stock prices

```bash
hive -f sql/01_raw_tables.sql

hive -f sql/02_clean_prices.sql

hive -f sql/03_price_splits.sql

hive -f sql/06_reconcile_prices.sql
```

### GDELT

GDELT processing must be completed by Hadoop Streaming before executing the Hive scripts.

```bash
hive -f sql/04_gdelt_tables.sql

hive -f sql/05_clean_gdelt.sql
```

Ensure that the GDELT daily feature aggregation uses the record availability date (`DATEADDED`) to prevent future-information leakage.

## Stock price chronological splits

The cleaned stock price dataset is divided chronologically:

| Dataset | Date range |
|---|---|
| Training | 2013–2015 |
| Validation | 2016 |
| Testing | 2017 |

No random shuffling is applied.

## Data validation and reconciliation

The stock price cleaning process separates records into three categories:

- Cleaned records: valid
