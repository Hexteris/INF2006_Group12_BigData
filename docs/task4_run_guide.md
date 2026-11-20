# Task 4 (analytics): how to run INF2006_G12_task4_analytics.ipynb on EMR

Time: about 1 minute for the notebook (64.5 s on our final run `20261120T075457Z`; the first run took 345 s). Cluster launch is about 15 minutes.

## Inputs needed in your S3 bucket (`<BUCKET>`)

Every path is derived from `BUCKET` in code cell 3; each can be overridden with the environment variable shown.

| What | Default path | Override |
|---|---|---|
| Task 2 cleaned prices | `<BUCKET>/task2/cleaned/prices` | `G12_PRICES` |
| Task 2 GDELT daily features | `<BUCKET>/task2/cleaned/gdelt/daily_features` | `G12_GDELT` |
| Task 2 splits (Q3) | `<BUCKET>/task3/task2/model_ready_v1` | `G12_MODEL_READY` (or `G12_T3_BASE` for the `task3` prefix) |
| Task 3 predictions (Q3) | `<BUCKET>/task3/task3final/direction/20261118T150036Z/predictions` and `.../volatility/20261118T151322Z/predictions` | `G12_PRED_DIR`, `G12_PRED_VOL` |
| Evidence upload (last cell) | `<BUCKET>/evidence/task4_<RUN_ID>/` | |

Task 3's notebooks read prices from `task2/cleaned/cleaned/prices`; check which prefix exists with
`aws s3 ls s3://<BUCKET>/task2/cleaned/`. The notebook cannot read another account's bucket (Access Denied), so copy
the inputs into your own bucket first.

## Steps

1. **Launch the cluster** as in report Appendix A part 1 (`project-up.ps1` on Windows, `project-up.sh -b <BUCKET>` on macOS). Open JupyterHub at `https://localhost:9443` (user `jovyan`, password `jupyter`).
2. **Upload** `notebooks/INF2006_G12_task4_analytics.ipynb` and open it with the **PySpark** kernel.
3. **Cell 1** (`%%configure -f` plus the pip installs) must finish before anything else. Wait for it.
4. **Code cell 3: set `BUCKET = "s3://<BUCKET>"`** (no trailing slash). Nothing else needs editing.
5. **Run all cells in order** (Cell, then Run All).
6. **Download the executed notebook** (File, Download as, Notebook) so the outputs are saved in it.
7. **Download the evidence** on your laptop:
   `aws s3 cp --recursive s3://<BUCKET>/evidence/task4_<RUN_ID> ./evidence/task4`
8. **Shut down**: `project-down`, then End Lab. The driver's local disk and HDFS are lost when the cluster terminates, so do step 7 first.

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
| `PATH_NOT_FOUND` | `BUCKET` in code cell 3 is still `s3://<YOUR-BUCKET>`, or a prefix differs in your bucket. Fix it and rerun from cell 3. |
| `AccessDeniedException ... s3a://...` (403) | The path points to another account's bucket. Copy the data to your own bucket and set `BUCKET`. |
| Q3 `AssertionError: prediction file does not match` | Wrong run ID, or the predictions folder is incomplete. Check the row counts against the table above. |
| `NameError` or missing files after a kernel restart | Livy session restarted and lost state and the driver's disk. Run all cells again from the top, then the upload cell. |
| Nothing under `evidence/task4_<RUN_ID>` in S3 | The last cell did not run or `BUCKET` is wrong. Fix it and run that cell again before shutting down. |
| Notebook stuck on "Starting Spark" | Another notebook holds the cluster's memory. Kernel, Shutdown on the other notebook. |
