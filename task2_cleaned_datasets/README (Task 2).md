# INF2006 Task 2 – Big Data Pipeline Handover

## 1. Project Overview

This repository contains the code, SQL scripts and selected processed datasets for the INF2006 Task 2 big data pipeline.

The pipeline processes two datasets:

- **Stock prices:** Historical daily stock prices from the Kaggle `all_stocks_5yr.csv` dataset.
- **GDELT events:** Global news/event records from GDELT 1.0, sampled from selected dates in 2015, 2016 and 2017. The full GDELT dataset was not used.

The project demonstrates the use of:

- **Hadoop Streaming** for distributed data validation and processing.
- **HDFS** for storing raw, processed, cleaned and feature datasets.
- **Hive** for SQL-based cleaning, validation, aggregation and data splitting.
- **PySpark** for stock feature engineering and model-ready dataset preparation.

The main modelling task is to classify whether a stock's closing price will be higher after the next five trading sessions.

## 2. GitHub Repository Structure

The GitHub repository contains the following selected files and datasets:

```text
INF2006_Group12_BigData/
├── README (Task 2).md
└── task2/
    ├── cleaned/
    │   ├── prices/
    │   │   ├── year=2013/
    │   │   ├── year=2014/
    │   │   ├── year=2015/
    │   │   ├── year=2016/
    │   │   └── year=2017/
    │   └── gdelt/
    │       └── daily_features/
    ├── code/
    │   ├── SQL/
    │   └── task2_complete_code_backup_20261109.tar.gz
    └── model_ready_v1/
        ├── training/
        ├── validation/
        └── testing/
```

### Included in this repository

| Directory/file | Description |
|---|---|
| `task2/model_ready_v1/` | Final training, validation and testing datasets for stock direction modelling |
| `task2/cleaned/prices/` | Cleaned historical stock price data |
| `task2/cleaned/gdelt/daily_features/` | Daily aggregated GDELT news activity features |
| `task2/code/SQL/` | Hive SQL scripts for data processing and validation |
| `task2/code/task2_complete_code_backup_20261109.tar.gz` | Complete code archive containing Hadoop Streaming, Hive, PySpark and supporting scripts |

The datasets are stored as Parquet files. The `_SUCCESS` files indicate successful Spark output operations.

### Not included in this GitHub repository

To keep the repository manageable, the following are not uploaded:

- Original raw stock and GDELT datasets.
- Processed GDELT event-level records.
- Rejected GDELT records.
- Intermediate Hadoop Streaming outputs and full audit directories.
- The complete HDFS `/corpus` backup.

The complete HDFS backup is stored separately in the S3 location documented in Section 11.

**Important:** The GitHub repository contains the selected datasets needed for the main modelling workflow. It is not a complete replacement for the original HDFS backup.

## 3. Processing Workflow

The pipeline was completed in the following stages.

### Stage 1 – Raw Data

- Stock CSV was loaded into HDFS.
- GDELT ZIP archives were selected, extracted and prepared for Hadoop Streaming.
- Raw stock and GDELT data were retained for traceability in the full HDFS backup.

### Stage 2 – Hadoop Streaming

**Stock data**
- Validated stock records using a mapper and reducer.
- Separated valid and rejected records.
- Produced audit summaries.

**GDELT data**
- Processed 150 selected ZIP archives.
- Validated the expected 58-column GDELT TSV structure.
- Used `DATEADDED` as the news availability date.
- Separated valid and rejected records and produced batch audit summaries.

### Stage 3 – Hive SQL

Hive was used to:

- Create external raw-data tables.
- Clean and type-cast stock and GDELT records.
- Separate rejected and out-of-scope stock records.
- Create daily GDELT aggregate features.
- Create chronological stock data splits.
- Run reconciliation and data-quality checks.

### Stage 4 – PySpark

PySpark was used to:

- Combine cleaned stock data with 2018 context records for calculating late-2017 labels.
- Calculate stock-level features.
- Generate the five-trading-session direction label.
- Create training, validation and testing datasets.
- Remove rows with missing model features to produce model-ready data.
- Validate missing and non-finite feature values.

## 4. Main Modelling Dataset

**Recommended input for model training:** `task2/model_ready_v1/`

The three datasets are included in this repository:

```text
task2/model_ready_v1/training/
task2/model_ready_v1/validation/
task2/model_ready_v1/testing/
```

### Dataset statistics

| Split | Rows | Date period |
|---|---:|---|
| Training | 349,590 | 2013–2015 |
| Validation | 122,801 | 2016 |
| Testing | 126,001 | 2017 |
| **Total** | **598,392** | |

### Label distribution

| Split | Label 0 | Label 1 |
|---|---:|---:|
| Training | 158,911 | 190,679 |
| Validation | 55,553 | 67,248 |
| Testing | 55,010 | 70,991 |

### Label definition

- `label = 1`: Closing price after five trading sessions is higher than the current closing price.
- `label = 0`: Closing price after five trading sessions is equal to or lower than the current closing price.

### Available columns

| Column | Description |
|---|---|
| `ticker` | Stock symbol |
| `trade_date` | Current trading date |
| `open_price` | Opening price |
| `high_price` | Highest price |
| `low_price` | Lowest price |
| `close_price` | Closing price |
| `volume` | Trading volume |
| `daily_return` | One-day close-to-close return |
| `return_5d` | Five-trading-session historical return |
| `volatility_20d` | Rolling 20-session volatility measure |
| `volume_change_1d` | One-day volume change |
| `intraday_range` | High-low range relative to price |
| `label` | Five-session direction target |

The model-ready dataset was validated with no missing or non-finite values in its model feature columns.

The original `stock_direction_v1` feature output is retained in the full HDFS backup but is not included in this GitHub repository.

Use `model_ready_v1` for the main modelling workflow because it has already removed rows with missing model features.

## 5. Chronological Split and Leakage Controls

The data was split chronologically, not randomly:

- **Training:** 2013–2015.
- **Validation:** 2016.
- **Testing:** 2017.

The five-session future target must not cross the training or validation period boundary. Therefore, observations near the end of those periods without a complete five-session outcome within the same period were excluded.

2018 stock records were retained as context only to calculate five-session outcomes for late-2017 test observations. They are not included as prediction rows in the training, validation or testing datasets.

For modelling:

- Do not randomly reshuffle observations across the chronological splits.
- Fit preprocessing and model parameters on training data only.
- Use validation data for model selection and tuning.
- Keep testing data for final evaluation.
- Do not use future prices or target-derived fields as input features.

## 6. GDELT Data and Limitations

The following GDELT daily aggregate dataset is included in this repository:

```text
task2/cleaned/gdelt/daily_features/
```

The event-level cleaned and rejected GDELT datasets are retained in the separate full HDFS backup, not uploaded to GitHub.

### GDELT processing summary

| Item | Count |
|---|---:|
| ZIP archives processed | 150 |
| Input event records | 29,042,290 |
| Cleaned event records | 29,042,043 |
| Rejected event records | 247 |
| Daily feature dates | 150 |

### Sampled date coverage

The pipeline processed these selected date ranges:

- 2015-07-01 to 2015-09-28: 90 dates.
- 2016-01-01 to 2016-01-30: 30 dates.
- 2017-01-01 to 2017-01-30: 30 dates.

### Important limitations

1. This is a sample of 150 dates from the original 731 available archives, not complete year-round coverage.
2. Daily GDELT aggregates represent market-wide/global news activity. They are not company-specific or ticker-specific sentiment/news features.
3. `DATEADDED` is used as the news availability date. Do not substitute `SQLDATE` when aligning news with stock observations for leakage-sensitive modelling.
4. A date absent from the sampled GDELT data means it was not collected in this sample. It must not automatically be interpreted as a date with zero news.
5. If joining daily GDELT features to stock data, preserve the distinction between collected dates and uncollected dates. Consider an explicit collection-coverage indicator.

The daily feature table contains aggregates such as:

- Event count.
- Average Goldstein scale.
- Average tone.
- Total mentions.
- Total sources.
- Total articles.
- Distinct event codes.

## 7. Data Quality and Reconciliation

### Stock data

| Category | Rows |
|---|---:|
| Raw stock records | 619,040 |
| Cleaned stock records | 605,887 |
| Rejected stock records | 23 |
| Out-of-scope stock records | 13,130 |

Reconciliation:

`605,887 + 23 + 13,130 = 619,040`

The out-of-scope records are the 2018 observations retained separately as context for late-2017 target calculation.

### GDELT data

| Category | Rows |
|---|---:|
| Raw GDELT records | 29,042,290 |
| Cleaned GDELT records | 29,042,043 |
| Rejected GDELT records | 247 |

Reconciliation:

`29,042,043 + 247 = 29,042,290`

The Hive validation and reconciliation checks passed on the original pipeline run and were rechecked after restoring the data and Hive table definitions on the replacement EMR cluster.

## 8. Using the GitHub Datasets

For the main modelling workflow, the teammate can download or clone this repository and use the Parquet files under:

```text
task2/model_ready_v1/
```

The training, validation and testing directories should be read separately to preserve the chronological split.

The cleaned stock price data is available under:

```text
task2/cleaned/prices/
```

The daily GDELT features are available under:

```text
task2/cleaned/gdelt/daily_features/
```

These datasets can be used for further analysis or feature engineering.

**Note:** The selected GitHub datasets are sufficient to continue the main stock modelling workflow. They are not sufficient by themselves to reconstruct every stage of the original pipeline, because raw data, event-level GDELT records and intermediate outputs are not included.

## 9. Restoring the Full Backup to Another EMR Cluster

The full HDFS backup is stored separately from the GitHub repository.

The backup was created from an EMR cluster using:

- Hadoop 3.4.2-amzn-2
- Spark 3.5.8-amzn-0
- Hive 3.1.3-amzn-24

For a compatible replacement cluster:

1. Ensure the EMR EC2 role and S3 permissions allow access to the backup location, if restoring directly from S3.
2. Copy the contents of the backed-up `corpus/` directory into HDFS under `/corpus`, preserving the directory structure.
3. Extract the code archive.
4. Run the Hive metadata restoration SQL to recreate the external table definitions.
5. Verify table counts and HDFS paths before continuing.

### Restore Hive metadata

The code archive includes `restore_hive_metadata.sql`.

It recreates the Hive table definitions for the backed-up datasets. The data itself is stored in HDFS; the Hive metastore definitions are cluster-specific and must be recreated on a new cluster.

After extracting the code archive, locate the SQL file and run it in Hive, for example:

```bash
hive -f restore_hive_metadata.sql
```

Then verify the tables and counts.

| Hive table | Expected rows |
|---|---:|
| `raw_prices` | 619,040 |
| `cleaned_prices` | 605,887 |
| `rejected_prices` | 23 |
| `out_of_scope_prices` | 13,130 |
| `raw_gdelt` | 29,042,290 |
| `cleaned_gdelt_events` | 29,042,043 |
| `rejected_gdelt` | 247 |
| `daily_gdelt_features` | 150 |
| `training_prices` | 354,540 |
| `validation_prices` | 125,316 |
| `testing_prices` | 126,031 |

The price split table counts above are the Hive split counts before PySpark feature engineering and model-feature missing-value filtering. They are therefore different from the final `model_ready_v1` counts.

**Important:** Copying HDFS files does not automatically restore Hive tables. HDFS data and Hive metastore metadata are separate.

## 10. Code Archive Contents

The archive `task2/code/task2_complete_code_backup_20261109.tar.gz` contains the project scripts and supporting materials, including:

- Hadoop Streaming stock scripts.
- Hadoop Streaming GDELT scripts.
- GDELT batch preparation and extraction scripts.
- Hive SQL scripts for raw tables, cleaning, splits and reconciliation.
- Hive metadata restoration SQL.
- PySpark scripts for input inspection, feature engineering, validation, model-ready preparation and non-finite checks.
- Available batch logs, SQL execution logs and validation logs.

Extract on the EMR primary node:

```bash
mkdir -p ~/task2_handover_code
tar -xzf task2_complete_code_backup_20261109.tar.gz -C ~/task2_handover_code
```

Inspect the extracted folders before running scripts. Some scripts assume the original HDFS paths under `/corpus` and the original project directory layout.

The individual Hive SQL scripts are also available directly in `task2/code/SQL/`.

## 11. Full Backup Information

- **Backup date:** 9 November 2026.
- **Code archive:** `task2_complete_code_backup_20261109.tar.gz`.
- **Original S3 backup prefix:**

```text
s3://inf2006-big-data-project/backups/task2_emr_2026-11-09/
```

- **Original local backup folder:**

```text
task2_emr_2026-11-09/
```

The full local download was verified as 228 files with a combined size of 5,856,865,199 bytes.

The complete HDFS backup is separate from the selected datasets committed to GitHub. Teammates who need to restore the entire pipeline will require access to the S3 backup and the appropriate AWS permissions.

## 12. Important Notes

- Use `task2/model_ready_v1/` as the main dataset for model training.
- Keep the train/validation/test split chronological.
- Treat 2018 data as label-generation context only.
- GDELT features are global aggregates, not ticker-specific news features.
- GDELT coverage is limited to 150 sampled dates.
- Do not interpret missing GDELT collection dates as zero-news dates.
- Use `DATEADDED` as the news availability date.
- Hive metadata must be recreated when using a different EMR cluster.
- The GitHub repository contains selected processed datasets and code, not the complete HDFS corpus.
- The separate S3 backup is required to restore the full pipeline from raw data.

---

**End of handover README**