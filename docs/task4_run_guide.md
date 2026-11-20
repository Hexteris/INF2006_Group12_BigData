# Task 4 (analytics): how to run INF2006_G12_task4_analytics.ipynb on EMR

Time: about 6 minutes for the notebook (345 s on our run). Cluster launch is about 15 minutes.

## Inputs needed in your S3 bucket (`<BUCKET>`)

| What | Path | Check with |
|---|---|---|
| Task 2 cleaned prices | `s3://<BUCKET>/task2/cleaned/prices` (Task 3's README writes `task2/cleaned/cleaned/prices`; use whichever exists) | `aws s3 ls s3://<BUCKET>/task2/cleaned/` |
| Task 2 GDELT daily features | `s3://<BUCKET>/task2/cleaned/gdelt/daily_features` | `aws s3 ls` on that path |
| Task 2 splits | `s3://<BUCKET>/task2/model_ready_v1/{training,testing}` | `aws s3 ls` |
| Task 3 final predictions | `s3://<BUCKET>/task3final/direction/20261118T150036Z/predictions` and `.../volatility/20261118T151322Z/predictions` | `aws s3 ls` |

The notebook cannot read another account's bucket (Access Denied). Copy the data into your own bucket first.

## Steps

1. **Launch the cluster** as in report Appendix A part 1 (`project-up.ps1` on Windows, `project-up.sh` on Mac). Open JupyterHub at `https://localhost:9443` (user `jovyan`, password `jupyter`).
2. **Upload** `INF2006_G12_task4_analytics.ipynb` and open it with the **PySpark** kernel.
3. **Cell 1** (`%%configure -f` plus the pip installs) must finish before anything else. Wait for it.
4. **Parameters cell: set these three things**
   - `PRICES_PATH = "s3://<BUCKET>/task2/cleaned/prices"`
   - `GDELT_PATH  = "s3://<BUCKET>/task2/cleaned/gdelt/daily_features"`
   - Do not leave the default `/corpus/...` paths: HDFS is empty on a new cluster and you will get `PATH_NOT_FOUND`.
5. **Q3 inputs cell: set**
   - `T3_BASE = "s3://<BUCKET>"`
   - the two run IDs in the `pred` paths to `20261118T150036Z` (direction) and `20261118T151322Z` (volatility).
6. **Last cell: set `EVIDENCE_BUCKET` to your own bucket.** It is hard-coded to one team member's bucket and will fail or write to the wrong place otherwise.
7. **Run all cells in order** (Cell, then Run All).
8. **Download the executed notebook** (File, Download as, Notebook) so the outputs are saved in it.
9. **Download the evidence** on your laptop:
   `aws s3 cp --recursive s3://<BUCKET>/evidence/task4_<RUN_ID> ./evidence/task4`
10. **Shut down**: `project-down`, then End Lab. The driver's local disk and HDFS are lost when the cluster terminates, so do step 9 first.

## Expected results (a correct rerun)

| Check | Expected |
|---|---|
| Load cell | `price rows : 605887 PASS`, `gdelt rows : 150 PASS` |
| Q1 | 595,789 scored ticker-days; 6 returns above 50% set to null |
| Q2 | 100 joined days across 3 blocks |
| Q3 load cell | volatility 123,476 rows and direction 126,001 rows, 0 duplicates, 0 unmatched, both `PASS` |
| Q3 metrics (`q3_overall_metrics.csv`) | volatility AUC 0.745, balanced accuracy 0.640; direction AUC 0.513, balanced accuracy 0.511 |
| Output files in `evidence/task4/` | 13 CSV, 7 PNG, `run_manifest.json` |

Q1 and Q2 outputs should match the committed files in `evidence/task4/` (differences below 0.00001).

## If something fails

| Error | Cause and fix |
|---|---|
| `PATH_NOT_FOUND ... hdfs://.../corpus/...` | Parameters cell still has the HDFS defaults. Use the S3 paths and rerun that cell. |
| `AccessDeniedException ... s3a://...` (403) | The path points to another account's bucket. Copy the data to your own bucket and change `T3_BASE`. |
| Q3 `AssertionError: prediction file does not match` | Wrong run ID, or the predictions folder is incomplete. Check the row counts against the table above. |
| `NameError` or missing files after a kernel restart | Livy session restarted and lost state and the driver's disk. Run all cells again from the top, then the upload cell. |
| Nothing under `evidence/task4_<RUN_ID>` in S3 | The last cell did not run or `EVIDENCE_BUCKET` is wrong. Fix it and run that cell again before shutting down. |
| Notebook stuck on "Starting Spark" | Another notebook holds the cluster's memory. Kernel, Shutdown on the other notebook. |
