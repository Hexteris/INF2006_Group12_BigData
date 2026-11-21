# Task 2 code: the 8–9 Nov single-pass run

This folder holds the code that produced the 9 Nov Task 2 outputs used by Tasks 3 and 4. The timed reruns for
Section 6 use `task2_driver/` instead (see `runbooks/RUN_GUIDE_G12.ipynb`, Step 3).

| Folder | Contents |
|---|---|
| `streaming/gdelt/` | GDELT extraction (`extract_gdelt.py`, `prepare_gdelt_batch.py`), Streaming mapper and reducers, output splitter |
| `streaming/stock_prices/` | Price audit mapper and reducer (also in `scripts/streaming/`) |
| `batch/` | `process_all_gdelt_batches.sh`, `process_remaining_gdelt_batches.sh`: unzip on the primary node, put to HDFS, run Streaming in batches of 10 archives |
| `pyspark/` | `01_inspect_inputs.py` to `05_check_nonfinite.py`: features, labels, splits and checks that wrote `task2/model_ready_v1/` |
| `task2_complete_code_backup_20261109.tar.gz` | The original archive these files were extracted from |

Run order: `batch/process_all_gdelt_batches.sh` (Streaming), then the repo-level `sql/` scripts in numeric order
(`05_clean_gdelt_final.sql` for GDELT, the version that produced 247 rejected GDELT rows), then `pyspark/01…05` with `spark-submit`. The logs and YARN application IDs for each stage are in
`evidence/task2/run_20261108-09/`.

`sql/` holds the Hive scripts as run on 8–9 Nov, with `IF NOT EXISTS` / `OR REPLACE` added so they can be rerun. The
exact files as run are in the archive above.
