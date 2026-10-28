# Stage contracts (PROPOSED: team to confirm)

Agreed names let each stage be written before the previous one is finished. Change a name here first,
then in code. `<ACQ>` is the acquisition date (`YYYY-MM-DD`); `<RUN>` is a UTC run ID (`YYYYMMDDTHHMMSSZ`).
Hive database: **`g12`**. All HDFS writes use replication 2.

## HDFS layout

| Layer | Path | Written by | Notes |
|---|---|---|---|
| Raw (Task 1) | `/corpus/raw/<source>/<version>/acq_<ACQ>/` | staging script | Immutable |
| Raw manifests | `/corpus/raw/_manifests/...` | Task 1 | |
| Streaming audit | `/corpus/audit/<job>/run_<RUN>/` | B | Tab-separated reducer output |
| SEC parsed | `/corpus/staged/sec_parsed/run_<RUN>/` | B (Streaming) | One row per filing (and per XBRL fact) |
| Cleaned | `/corpus/cleaned/prices/` | B (Hive) | Parquet, partitioned by `year` |
| Rejected | `/corpus/rejected/prices/` | B (Hive) | Original strings + reason |
| Out of scope | `/corpus/out_of_scope/prices/` | B (Hive) | Rows after 2017-12-29 |
| Features | `/corpus/features/weekly/` | C | Parquet |
| Splits | `/corpus/splits/{train,val,test}/` | C | Parquet |
| Models | `/corpus/models/<model_name>/` | D | Saved `PipelineModel` |
| Predictions | `/corpus/predictions/<model_name>/` | D | Parquet |
| Analytics | `/corpus/analytics/<question>/` | E | Small aggregates for plotting |

## Hive tables

| Table | Type | Over | Columns (types) |
|---|---|---|---|
| `g12.raw_prices` | EXTERNAL, text | raw prices folder | `date, open, high, low, close, volume, name` all `STRING` (header skipped) |
| `g12.cleaned_prices` | managed/external Parquet, partitioned by `year INT` | `/corpus/cleaned/prices/` | `ticker STRING, trade_date DATE, open DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE, volume BIGINT` |
| `g12.rejected_prices` | Parquet | `/corpus/rejected/prices/` | the 7 raw `STRING` columns + `reject_reason STRING` |
| `g12.out_of_scope_prices` | Parquet | `/corpus/out_of_scope/prices/` | the 7 raw `STRING` columns |
| `g12.sec_filings` | EXTERNAL, text (TSV) | `/corpus/staged/sec_parsed/...` | `cik STRING, accession STRING, form STRING, filed_date DATE, acceptance_et TIMESTAMP, period_of_report DATE, has_xbrl BOOLEAN, main_doc_chars BIGINT, ...` (B to finalise) |
| `g12.sec_price_link` | Parquet | | `cik STRING, accession STRING, ticker STRING, available_from DATE` |

## Reconciliation targets (from Task 1)

| Check | Expected |
|---|---|
| `raw_prices` rows | 619,040 |
| `out_of_scope_prices` | 13,130 (dates 2018-01-02 to 2018-02-07) |
| `rejected_prices` | 23 (listed in `evidence/local_checks/price_anomaly_detail.txt`) |
| `cleaned_prices` | **605,887** (spec's accepted-bar count) |
| raw = cleaned + rejected + out_of_scope | 619,040 = 605,887 + 23 + 13,130 |
| `sec_filings` | 311 rows, 303 CIKs |
| `sec_price_link` | 11 rows (10 inside price dates) |

## Rules each stage must respect

- **Rejection reasons** (one per row, first match wins): `null_field`, `high_lt_low`, `open_outside_range`,
  `close_outside_range`, `zero_volume`.
- **No filling from the future.** Gaps stay gaps.
- **Filing availability:** from `acceptance_et`; before 09:30 ET → that trading day's open, otherwise the
  next trading day. Never `filed_date`.
- **Splits:** train = targets fully inside 2013–2015, val = 2016, test = 2017. A row is kept only if its
  target's outcome falls inside the same period. Fit imputers/scalers on train only.
- Predictive features exclude `ticker`, dates and anything derived from the target window.
