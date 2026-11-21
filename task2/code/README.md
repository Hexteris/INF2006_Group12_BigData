# Task 2 code: the 8–9 Nov single-pass run

This folder is the record of the code that produced the 9 Nov Task 2 outputs used by Tasks 3 and 4
(`task2/cleaned/`, `task2/model_ready_v1/`, both in the ZIP). It is not a turnkey rerun: the batch scripts have the
settings of that cluster (Hadoop 3.4.2 jar, home-folder `SCRIPT_DIR`, restart at archive 10, 731-archive check).
**To rerun Task 2, use `task2_driver/run_matrix.sh`** (report Appendix A; `runbooks/RUN_GUIDE_G12.ipynb`, Step 3),
which runs Streaming, Hive and PySpark end to end on emr-7.10.0. To rerun only Tasks 3 and 4, upload the two Task 2
output folders from the ZIP to `s3://<bucket>/task2/`.

| Folder | Contents |
|---|---|
| `streaming/gdelt/` | GDELT extraction (`extract_gdelt.py`, `prepare_gdelt_batch.py`), Streaming mapper and reducers, output splitter |
| `streaming/stock_prices/` | Price audit mapper and reducer (also in `scripts/streaming/`) |
| `batch/` | `process_all_gdelt_batches.sh`, `process_remaining_gdelt_batches.sh`: unzip on the primary node, put to HDFS, run Streaming in batches of 10 archives |
| `pyspark/` | `01_inspect_inputs.py` to `05_check_nonfinite.py`: features, labels, splits and checks that wrote `task2/model_ready_v1/` |
| `task2_complete_code_backup_20261109.tar.gz` | The original archive these files were extracted from (code and logs only, 60 KB) |

Run order on 8–9 Nov: `batch/process_all_gdelt_batches.sh` (Streaming), then the repo-level `sql/` scripts in numeric order
(`05_clean_gdelt_final.sql` for GDELT, the version that produced 247 rejected GDELT rows), then `pyspark/01…05` with `spark-submit`. The logs and YARN application IDs for each stage are in
`evidence/task2/run_20261108-09/`.

`sql/` holds the Hive scripts as run on 8–9 Nov, with `IF NOT EXISTS` / `OR REPLACE` added so they can be rerun. The
exact files as run are in the archive above.
