# Task 2 execution evidence: single-pass run, 8–9 Nov 2026

These logs come from the Task 2 run that produced the 9 Nov outputs (`task2/cleaned/`, `task2/model_ready_v1/`)
used by Tasks 3 and 4. They were extracted from `task2/code/task2_complete_code_backup_20261109.tar.gz`;
the scripts that wrote them are in `task2/code/`.

| Stage | Engine | YARN application ID(s) | Log | Outcome in the log |
|---|---|---|---|---|
| GDELT batches (first launch) | Hadoop Streaming | `application_1794125803062_0004` to `_0011` | `gdelt_batch_logs/process_all_20261108_094932.log` | 8 jobs "completed successfully" |
| GDELT batches (remaining) | Hadoop Streaming | `application_1794125803062_0014` to `_0017` | `gdelt_batch_logs/remaining_20261108_104332.log` | 4 jobs "completed successfully" |
| Clean GDELT | Hive | `application_1794125803062_0027` | `hive_logs/hive_clean_gdelt.log` | queries returned |
| Clean prices | Hive | `application_1794125803062_0035` | `hive_logs/hive_clean_prices.log` | queries returned |
| Price splits | Hive | `application_1794125803062_0038` | `hive_logs/hive_price_splits.log` | queries returned |
| Price reconciliation | Hive | `application_1794125803062_0039` | `hive_logs/hive_reconcile_prices.log` | queries returned |
| Inspect inputs | PySpark | `application_1794204729986_0012` | `pyspark_logs/01_inspect_inputs.log` | completed |
| Build features | PySpark | `application_1794204729986_0013` | `pyspark_logs/02_build_features.log` | completed |
| Validate features | PySpark | `application_1794204729986_0014` | `pyspark_logs/03_validate_features.log` | completed |
| Prepare model data | PySpark | `application_1794204729986_0015` | `pyspark_logs/04_prepare_model_data.log` | completed |
| Non-finite check | PySpark | `application_1794204729986_0016` | `pyspark_logs/05_check_nonfinite.log` | completed |

`hive_logs/restore_hive_*` record the metadata restore and count checks run after a cluster restart.

## Limits of this run (stated in report Section 3.5)

- **Not the reference release.** The logs show `hadoop-streaming-3.4.2-amzn-2` and two cluster IDs
  (`1794125803062`, `1794204729986`), not emr-7.10.0 (Hadoop 3.4.1). The timed reruns in Section 6 use
  `task2_driver/` on emr-7.10.0.
- **150 of 731 GDELT archives.** The batch scripts unzipped archives on the primary node (`/mnt1`) before
  Streaming, and stopped after the sample (July–September 2015, January 2016, January 2017).
- **Spark application names** in the PySpark logs still say "FlyWise" (copied from an earlier project); they do
  not affect the outputs.
