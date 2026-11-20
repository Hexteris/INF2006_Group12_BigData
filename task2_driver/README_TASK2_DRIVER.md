# Task 2 driver: how to run it on EMR

`INF2006_G12_task2_driver.ipynb` runs the whole Task 2 pipeline for **one profile, one configuration and one
repetition**, on a fresh Hive database and fresh HDFS folders, and records the evidence the performance section needs.
It is a **Bash-kernel notebook** (like the Task 1 staging notebook) that runs on the primary node.

## Files

| File | Purpose |
|---|---|
| `INF2006_G12_task2_driver.ipynb` | The driver. It writes every helper file it runs, so it reproduces the pipeline on its own |
| `run_matrix.sh` | Runs the 9-run matrix (dev/reference ×3, full/reference ×3, full/alt ×3) with `nbconvert`, then aggregates |
| `assets/gdelt_zip_mapper.py` | Hadoop Streaming map-only job: zip → validated, projected, tagged gzip text |
| `assets/sql/01…05_*.sql` | Hive: raw tables, price cleaning, price splits, GDELT cleaning, reconciliation |
| `assets/build_features.py`, `validate_features.py` | PySpark: features, targets, splits, boundary rules, validation |
| `assets/summarize_run.py`, `aggregate_runs.py` | Per-run summary; median/range across repetitions and output-equivalence check |

The notebook embeds copies of everything in `assets/`; keep both in the ZIP.

## One-time set-up on the primary node (not timed)
```bash
pip install --user notebook nbconvert bash_kernel
python3 -m bash_kernel.install --user
mkdir -p ~/task2_driver && cp INF2006_G12_task2_driver.ipynb run_matrix.sh ~/task2_driver/
```
Check that `hdfs`, `hive`, `hadoop`, `spark-submit` and `/usr/lib/hadoop/hadoop-streaming*.jar` exist, and that
`/corpus/raw/prices/...csv` and `/corpus/raw/gdelt/...zip` are staged (Task 1 notebook).

## Run
First once, interactively, in a notebook session with the **Bash** kernel: *Run All* (profile `dev`, `reference`).
Fix anything that fails, then run the matrix unattended:
```bash
tmux new -s task2
cd ~/task2_driver && bash run_matrix.sh 2>&1 | tee run_matrix.log
```
A single run: `PROFILE=full CONFIG=alt REP=2 jupyter nbconvert --to notebook --execute INF2006_G12_task2_driver.ipynb --ExecutePreprocessor.timeout=-1 --output-dir ~/task2_runs/executed_notebooks --output run_full_alt_2`

### Parameters (environment variables; defaults in cell 1)
| Variable | Default | Meaning |
|---|---|---|
| `PROFILE` | `dev` | `dev` = first 50 tickers + first 14 archives; `full` = everything staged |
| `CONFIG` | `reference` | `reference` = Spark default shuffle partitions (200), 25 archives per map; `alt` = 16 shuffle partitions, 12 archives per map |
| `REP` | `1` | Repetition number |
| `CORPUS` | `/corpus` | HDFS root from the staging notebook |
| `RAW_PRICES_FILE` | auto | Set if several price CSVs are staged |
| `USE_2018_LABELS` | `0` | `1` labels late-2017 rows from 2018 prices (outside the brief's window) |
| `KEEP_BULK` | `0` | `1` keeps the large intermediate HDFS folders |
| `TRAIN_CMD` | empty | Command that trains the Task 3 model; timed as stage `train` |
| `BUCKET` | empty | Optional S3 bucket for evidence upload (use the instance role; no keys) |

Both configurations use the brief's reference hardware and Spark allocation (2 executors × 2 cores × 6 GiB + 2 GiB
overhead, driver 2 GiB, dynamic allocation off). Only the partitioning settings differ. Edit the `case` block in cell 1 if
you want a different alternative; keep hardware, release and replication fixed.

## What each run leaves behind (`~/task2_runs/<run name>/`)
`timings.csv` (per-stage times, exit codes, YARN/job IDs) · `run_summary.txt/json` · `reconcile.tsv` ·
`features_summary.json` (split report, null counts, label balance) · `validation.json` · `inputs.txt` (profile bytes and counts) ·
`resources_raw.log` (memory, YARN allocation, per-DataNode disk) · `disk.csv` · `run_manifest.json` (settings, versions) ·
`assets_sha256.txt`, `assets_used/` · `logs/<stage>.log` · `gdelt_paths.txt`.

Executed notebooks (with outputs) are in `~/task2_runs/executed_notebooks/`. Copy `~/task2_runs` to S3 before terminating
the cluster (set `BUCKET` to do it automatically).

`INF2006_G12_task2_driver.ipynb` in this folder is the clean template that `run_matrix.sh` executes, so it has no saved
outputs. The executed EMR copies of the six assessed runs (20 Nov 2026) are in
`evidence/task2_runs/executed_notebooks/`.

## Rules the driver follows (from the brief)
* Timing starts at the first stage and covers Streaming, Hive, feature preparation, validation and, if set, training. Not timed: staging, set-up, writing helper files, building the dev inputs, summaries, clean-up.
* Fresh Hive database, HDFS output folder and Spark/Hive processes every run; no `cache()`; prepared tables are never reused.
* Raw, cleaned and rejected data live in different folders and tables; row counts are reconciled (`PASS`/`FAIL`) and a failed reconciliation fails the run.
* Targets look 5 sessions ahead; a row is kept only if its target window ends inside its period; incomplete targets are excluded and counted, never filled; target and future-date columns are kept out of the model-ready tables; GDELT enters by `DATEADDED` + 1 day.
* A failed stage skips the later stages and the run is marked `FAILED`; failures are reported, not hidden.

## Design choices to confirm with your team / TA
1. **GDELT handling** replaces the batch scripts (local extraction on the primary node) with a map-only Streaming job that reads the zips from HDFS. Your original scripts remain valid evidence of the first run.
2. **Test boundary:** the last five trading days of 2017 are dropped by default. Use `USE_2018_LABELS=1` only if you justify using 2018 prices.
3. **Feature changes vs. the first version:** volatility needs 20 observations; added volatility target, volume ratio, market-wide features and lagged GDELT columns.
4. **Dev profile:** first 50 tickers and first 14 archives; change `DEV_TICKERS` / `DEV_GDELT_ARCHIVES` and keep them frozen once you compare configurations.
5. **Storage:** each full run writes tens of GB before clean-up (not measured yet). Clean-up deletes the Streaming output and the cleaned GDELT events with `-skipTrash`. Check HDFS free space before each full run.

## Tested here, not tested on EMR
The pipeline was executed end to end with `nbconvert` and the Bash kernel using local stand-ins (Hive via `spark-sql`,
Streaming via a simulator, Spark in local mode), including the failure paths (pre-flight failure, failed training stage)
and the aggregator. It has **not** run on a real EMR cluster. Expect small adjustments on the first run, for example
Hive/Streaming option names or the best-effort parsing of `yarn node -list -showDetails`.
