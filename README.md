# INF2006 Group 12: Big Data Project

End-to-end pipeline on AWS EMR: five raw sources staged in an HDFS corpus (Task 1); daily prices and GDELT → Hadoop Streaming / Hive / PySpark pipeline (Task 2) → Spark ML model (Task 3) → analytics (Task 4). Data flow: `docs/figures/data_flow.png`. Full run guide: `runbooks/RUN_GUIDE_G12.ipynb`.

| Task | Status | Owner |
|---|---|---|
| 1. Corpus | **Done** (acquisition 2026-10-28/29) | Daniel |
| 2. Pipeline | **Done** (single-pass run 2026-11-08/09, 150 GDELT dates; code in `task2/code/`, logs in `evidence/task2/run_20261108-09/`) | Jasper (Hive), Jinghong (Streaming + PySpark) |
| 3. Model | **Done** (EMR runs 2026-11-18/19; see "Task 3: how to run") | Xin Huei |
| 4. Analytics | **Done** (final EMR run `20261120T075457Z`; see "Task 4: how to run") | Damien |
| Section 6 timed runs | **To do**: `task2_driver/`, run guide Step 3 (dev ×3, full ×3, full alternative ×3) | team |

**Read first:** `docs/corpus_inventory.md`, `docs/data_dictionary.md`, `docs/decisions.md`, and `docs/contracts.md` (agreed paths and table names).

## Repo layout

| Path | What | Owner |
|---|---|---|
| `scripts/acquire/` | Download prices, SEC filings, GDELT, Wikinews with manifests | Task 1 |
| `scripts/stage/stage_raw_to_hdfs.sh` | Copy a source into `/corpus/raw/...` and verify sha256 | Task 1 |
| `scripts/profile/` | Read-only quality checks | Task 1 |
| `scripts/cluster/verify_cluster.sh` | Check a running cluster matches the reference config | Task 1 |
| `scripts/streaming/` | Hadoop Streaming mappers and reducers | B |
| `sql/` | Hive `.sql` files, run in numeric order (cleaned-up copy of `task2/code/SQL/`) | Task 2 |
| `task2/code/` | Code of the 8–9 Nov Task 2 run: Streaming, batch scripts, Hive SQL as executed, PySpark scripts | Task 2 |
| `task2_driver/` | Bash-kernel driver for the timed Task 2 runs (Section 6), `run_matrix.sh`, `train_task3.py` | Task 2 / Section 6 |
| `runbooks/RUN_GUIDE_G12.ipynb` | Step-by-step rerun guide for Tasks 1–4 and the timed runs | everyone |
| `docs/figures/` | Report figures (data-flow diagram and its Graphviz source) | everyone |
| `scripts/package/` | `build_submission_zip.sh` and the submission checklist | everyone |
| `notebooks/INF2006_G12_task1.ipynb` | Task 1 notebook exported from EMR (full evidence run) | Task 1 |
| `notebooks/INF2006_G12_stage_corpus.ipynb` | Copy-paste SSH cells: stage all 5 sources S3 → HDFS `/corpus/raw` with sha256 checks | Task 1 |
| `notebooks/INF2006_G12_task3_volatility_labels.ipynb`, `..._task3_direction.ipynb`, `..._task3_volatility.ipynb` | Task 3 notebooks exported from EMR (labels, direction model, volatility model) | Task 3 |
| `notebooks/INF2006_G12_task4_analytics.ipynb` | Task 4 analytics notebook exported from EMR (Q1 volume, Q2 news, Q3 model reliability); outputs in `evidence/task4/` | Task 4 |
| `task3_saved_models/<task>/<RUN_ID>/model` | Saved Spark ML `PipelineModel`s for the two final Task 3 models | Task 3 |
| `config/` | EMR configuration used by `project-up.ps1` | Task 1 |
| `evidence/` | Dated outputs from EMR runs (manifests, job IDs, listings), one subfolder per task: `task1/`, `task2/` (job IDs and logs), `task2_screenshots/`, `task3/`, `task4/`, and `task2_runs/` once the timed runs are done | everyone |
| `project-up.ps1`, `project-down.ps1` (`.sh` on macOS/Linux) | Launch / terminate the reference cluster (adds the `install-numpy.sh` bootstrap) | everyone |

## Working rules

- Code goes through GitHub `main`. Before every push: `git pull --rebase`. Small commits.
- Only edit files in your own folders. Never edit a `.ipynb` together; write your part as `notebooks/sections/<task>_<topic>.py` with `# %%` cells (they are joined into one notebook for the final run).
- **Never commit data, keys or credentials.** Put downloaded data under `raw/` (git-ignored). SEC filings are `.txt`, so outside `raw/` they *would* be committed.
- Read raw data as strings. Never modify anything under `/corpus/raw` on HDFS.
- Put every HDFS path and the acquisition date at the top of your script/section as a parameter.
- Mappers and reducers: Python 3 standard library only (they run on EMR's Python).
- Before running Hadoop Streaming or Hive on EMR, shut down any notebook Spark session. A notebook holds most of the cluster's YARN memory and other jobs will wait forever.

## 1. Laptop setup (once)

Install Git, Python 3 and the AWS CLI. For local PySpark also `pip install pyspark` (needs Java 17).

```powershell
git clone https://github.com/Hexteris/INF2006_Group12_BigData.git
cd INF2006_Group12_BigData
```

Download `SEC_Filing_Inventory.csv` from the course page into the repo folder (it is git-ignored).

## 2. Local data for development

```powershell
python scripts\acquire\acquire_url.py --github-pin --csv-stats --key-col Name --date-col date `
  --url https://raw.githubusercontent.com/plotly/datasets/master/all_stocks_5yr.csv `
  --source prices --version v1_2013-2017 --out-dir raw\prices --manifest raw\_manifests\acq.csv
$env:SEC_USER_AGENT="Your Name your@email"
python scripts\acquire\acquire_sec_filings.py --inventory SEC_Filing_Inventory.csv --out raw\sec_edgar --limit 20
```

Expected: price sha256 `6aea253c…`, 619,040 rows, 505 tickers (same bytes as the assessed corpus). Drop `--limit 20` for all 311 filings (about 2.8 GB, a few minutes).

## 3. Developing each part locally

**Hadoop Streaming (B).** Test the mapper/reducer as a pipe in **Git Bash** (PowerShell's `sort` differs from Hadoop's):

```bash
cat raw/prices/all_stocks_5yr.csv | python scripts/streaming/<name>_mapper.py \
  | LC_ALL=C sort | python scripts/streaming/<name>_reducer.py
```

**Hive (B).** Write `sql/NN_<name>.sql`. Hive does not run easily on Windows; test on EMR with `hive -f`.

**PySpark (C, D, E).** Run your section in local mode on the laptop copy, then on EMR. Read Hive tables with `spark.table("g12.<table>")` on EMR; locally, read the CSV directly with every column as string.

## 4. Testing on your own EMR cluster

Each member uses their own Learner Lab account. The data is reproducible, so your HDFS ends up byte-identical to everyone else's.

1. Start Learner Lab; paste **AWS Details → AWS CLI: Show** into `%USERPROFILE%\.aws\credentials`.
2. Download the key from AWS Details as `labsuser.pem` into the repo folder, then:
   ```powershell
   icacls labsuser.pem /inheritance:r /grant:r "${env:USERNAME}:(R)"
   ```
3. Launch (about 15 min). Use your own student ID in the bucket name:
   ```powershell
   .\project-up.ps1 -Bucket inf2006-g12-<studentID> -KeyName vockey -PemFile labsuser.pem
   ```
   If SSH times out later (your IP changed): `.\project-up.ps1 -ClusterId (Get-Content .project-cluster-id) -KeyName vockey -PemFile labsuser.pem`.
4. On the primary node:
   ```bash
   sudo yum install -y git tmux && git clone https://github.com/Hexteris/INF2006_Group12_BigData.git g12 && cd g12
   tmux new -s g12
   export B=inf2006-g12-<studentID> ACQ=$(date -u +%F)
   export SEC_USER_AGENT="Your Name your@email"
   sudo mkdir -p /mnt/g12 && sudo chown hadoop:hadoop /mnt/g12
   R=/mnt/g12/raw
   python3 scripts/acquire/acquire_url.py --github-pin --csv-stats --key-col Name --date-col date \
     --url https://raw.githubusercontent.com/plotly/datasets/master/all_stocks_5yr.csv \
     --source prices --version v1_2013-2017 --out-dir $R/prices --manifest $R/_manifests/acquisition_manifest.csv
   # upload SEC_Filing_Inventory.csv first: from the laptop, aws s3 cp SEC_Filing_Inventory.csv s3://$B/inventory/
   aws s3 cp s3://$B/inventory/SEC_Filing_Inventory.csv $R/sec_inventory/
   python3 scripts/acquire/acquire_sec_filings.py --inventory $R/sec_inventory/SEC_Filing_Inventory.csv \
     --out $R/sec_edgar --limit 20          # drop --limit for all 311
   bash scripts/stage/stage_raw_to_hdfs.sh prices    v1_2013-2017 $ACQ $R/prices           $R/_manifests
   bash scripts/stage/stage_raw_to_hdfs.sh sec_edgar v1           $ACQ $R/sec_edgar/filings $R/sec_edgar/_manifests
   ```
5. Run your jobs (examples):
   ```bash
   hadoop jar /usr/lib/hadoop/hadoop-streaming.jar \
     -files scripts/streaming/<name>_mapper.py,scripts/streaming/<name>_reducer.py \
     -mapper "python3 <name>_mapper.py" -reducer "python3 <name>_reducer.py" \
     -input /corpus/raw/prices/v1_2013-2017/acq_$ACQ \
     -output /corpus/audit/<name>/run_$(date -u +%Y%m%dT%H%M%SZ)
   hive --hivevar ACQ=$ACQ -f sql/01_raw_tables.sql
   ```
   JupyterHub: `https://localhost:9443` (user `jovyan`, password `jupyter`).
6. Save evidence **before** shutting down (job IDs, counts, logs), e.g. `aws s3 cp --recursive <dir> s3://$B/evidence/<task>/`, then on the laptop `aws s3 sync s3://<bucket>/evidence/ evidence/` and commit. HDFS and the primary node are deleted with the cluster.
7. `.\project-down.ps1`, then **End Lab**.

## Task 3: how to run

Full instructions and expected results are in report Appendix A (Part 4, Task 3 step; Part 5, reload check).

1. Launch: `.\project-up.ps1 -Bucket <bucket>`. The script uploads `install-numpy.sh` to `s3://<bucket>/bootstrap/` and adds it as a bootstrap action (Spark ML needs NumPy on every node).
2. Open JupyterHub (`https://localhost:9443`, PySpark kernel) and upload the three Task 3 notebooks from `notebooks/`. Set `BUCKET` in the first code cell of each. Inputs are the Task 2 outputs `task2/model_ready_v1` and `task2/cleaned/cleaned/prices` in that bucket.
3. Run them **one at a time** (Kernel → Shutdown between notebooks, because each Spark session holds most of the cluster's YARN memory):

   | Order | Notebook | Expected result |
   |---|---|---|
   | 1 | `INF2006_G12_task3_volatility_labels.ipynb` | Writes `task3/volatility_v1`; 349,590 / 122,801 / 123,476 rows; threshold about 0.01593 |
   | 2 | `INF2006_G12_task3_direction.ipynb` | Winner `logistic_full_r01`; 2017 test AUC 0.513 |
   | 3 | `INF2006_G12_task3_volatility.ipynb` | Winner `logistic_extra`; 2017 test AUC 0.745 |

   Each model notebook writes `model/`, `predictions/`, `metrics/`, `settings/` and `test_features/` to `s3://<bucket>/task3final/<task>/<RUN_ID>/`.
4. **Fresh-session reload check:** after Run All, do Kernel → Restart and run **only the last cell**. It loads the saved `PipelineModel` without retraining, re-scores every 2017 test row and compares it with the saved predictions (tolerance 1e-9). Expected: `RELOAD CHECK PASSED`.
5. Download each notebook (File → Download as → Notebook) so the outputs are saved in it, then `.\project-down.ps1`.

**Our runs (evidence in `evidence/task3/`):**

| Task | RUN_ID | 2017 AUC / balanced accuracy | Reload check |
|---|---|---|---|
| Direction | `20261118T150036Z` | 0.513 / 0.511 (baseline 0.500 / 0.500) | PASSED, 126,001 rows, max difference 0.0 |
| Volatility | `20261118T151322Z` | 0.745 / 0.640 (rule 0.703 / 0.620) | PASSED, 123,476 rows, max difference 0.0 |

Saved models: `task3_saved_models/<task>/<RUN_ID>/model`. Metrics, settings and reload records: `evidence/task3/<task>/<RUN_ID>/`. Cluster records: `evidence/task3/cluster/`.

## Task 4: how to run

Full guide: `docs/task4_run_guide.md` (inputs, steps, expected results, troubleshooting). Summary:

1. Launch the cluster (`project-up`), open JupyterHub (`https://localhost:9443`, PySpark kernel) and upload `notebooks/INF2006_G12_task4_analytics.ipynb`.
2. In code cell 3 set `BUCKET = "s3://<bucket>"`. The price, GDELT, Task 2 split and Task 3 prediction paths and the evidence upload all follow from it (HDFS is empty on a new cluster).
3. Run all cells in order (about 1 minute). Expected: 605,887 price rows and 150 GDELT dates (PASS), 595,789 scored ticker-days, 100 joined news days, and Q3 prediction counts 123,476 (volatility) and 126,001 (direction).
4. The last cell copies the outputs to `s3://<bucket>/evidence/task4_<RUN_ID>/`. Download them into `evidence/task4/` before `project-down`.

Our final run: `evidence/task4/run_manifest.json` (run `20261120T075457Z`, 64.5 s, 13 CSV and 7 PNG files). The first run (`20261116T164719Z`, 345 s) is in `evidence/task4/previous_runs/`.

## 5. The assessed run

The timed runs for Section 6 (`task2_driver/`, run guide Step 3) are produced once, in one account, from the merged `main` branch. Members join by screen share or SSH. Development runs in your own account are supporting evidence only.
