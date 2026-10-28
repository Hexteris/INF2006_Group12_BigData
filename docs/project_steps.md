# INF2006 Group 12: step-by-step project plan

Steps marked **Why** involve a design choice that the report has to defend. Steps without a **Why** are routine.
"Evidence" says what to keep for Appendix B. Screenshots alone do not count, so the evidence is always a file, an ID or a log.

---

## Phase 0: Setup (do once)

**Step 0.1. Get the source URLs and confirm the open points with the teaching team.**
You need the price CSV URL and the SEC inventory (.xlsx) URL from the course page. Ask three questions:
- Is the provisional cluster config final?
- What does "converted into the required output weights" in Task 3.1 mean?
- Which appendix holds the contributions table? Page 8 says Appendix D and page 11 says Appendix C.

**Step 0.2. Agree team scope: prices + SEC EDGAR. Skip GDELT and Wikinews.**
**Why:** The SEC inventory is a fixed list with accession numbers, so provenance is easy to prove. It is 2.78 GB, well under the 200 GiB HDFS ceiling. GDELT is 54.71 GB expanded, which adds storage and timeout risk without helping the core price model. Wikinews is not linked to tickers.

**Step 0.3. Create a git repo and a `.gitignore` that excludes `*.pem`, `.aws/`, `.lab-cluster-id` and any raw data.**
**Why:** The ZIP must contain no credentials or raw datasets. If you exclude them from day one, they cannot leak into the submission.

**Step 0.4. Write `project-up.ps1` that launches the exact reference cluster:** emr-7.10.0, 1 primary + 2 core m5.xlarge, 200 GiB gp3 (3000 IOPS, 125 MiB/s) per core node, `dfs.replication=2`, 2 executors × 2 cores × 6 GiB, 2 GiB overhead, 2 GiB driver, dynamic allocation off. Applications: Hadoop, Hive, Spark, JupyterHub (or Livy for EMR Studio). Set `--log-uri s3://<bucket>/emr-logs/`.
**Why:**
- `lab-up.ps1` does not set this config.
- A script, plus a `configurations.json` stored in the ZIP, is reproducible evidence for Appendix A. Console clicks are not.
- The submission needs an exported **.ipynb**. Zeppelin exports `.zpln`, so the notebook has to run in JupyterHub or EMR Studio.
- The log URI keeps YARN and step logs in S3 after the cluster is terminated.

**Step 0.5. Create one S3 bucket for the project:** `s3://inf2006-g12-<suffix>/` with the prefixes `raw/`, `manifests/`, `code/`, `outputs/`, `models/`, `emr-logs/`.
**Why:** Learner Lab clusters are terminated every session, and HDFS is lost with them. S3 is the only storage that lasts between sessions. Raw data is acquired once into S3, and every new cluster copies the same bytes into HDFS. This is what makes "the same source inputs can be identified precisely" true across reruns.

---

## Phase 1: Task 1, build the corpus

**Step 1.1. Launch a small dev cluster and SSH into the primary node.**

**Step 1.2. Download the price CSV and the SEC inventory .xlsx on the primary node** with `scripts/acquire/acquire_url.py`. The script records the URL, final URL, HTTP status, bytes, sha256 and UTC retrieval time in `acquisition_manifest.csv`.
**Why on the cluster rather than the laptop:** The download stays inside AWS, so it is fast and does not depend on home bandwidth. The manifest records the host, which proves where the data was acquired.

**Step 1.3. Download the 311 SEC filings** with `scripts/acquire/acquire_sec_filings.py`, using `SEC_USER_AGENT="Daniel Tay <email>"` at 4 requests/s.
**Why:**
- SEC access guidance requires a declared User-Agent and fewer than 10 requests/s.
- The script reads the .xlsx without pandas and keeps CIK and accession as text. It zero-pads numeric CIKs to 10 digits and logs every change it makes, which satisfies "preserve identifiers and leading zeros".
- Failed URLs go to `sec_unavailable.csv` and are never replaced, as the spec requires.
- Run `--limit 20` first as the dev sample.

**Step 1.4. Upload the originals and manifests to S3 unchanged:** `aws s3 cp --recursive ~/raw s3://<bucket>/raw/...` using the same `source/version/acq_date` layout.

**Step 1.5. Stage from S3 into HDFS** with `s3-dist-cp` or `hdfs dfs -cp`, then verify sha256 per file against the manifest. Use `scripts/stage/stage_raw_to_hdfs.sh` for the verification step and the staging record.
Layout:
```
/corpus/raw/prices/v1_2013-2017/acq_YYYY-MM-DD/
/corpus/raw/sec_edgar/v1/acq_YYYY-MM-DD/
/corpus/raw/_manifests/<source>/<version>/acq_YYYY-MM-DD/
```
**Why:**
- Separating paths by source, version and acquisition date is the spec's requirement to "distinguish sources, versions and acquisition dates".
- Keeping files in their original formats (CSV, HTML/XBRL) keeps raw truly raw. Conversion to Parquet belongs to Task 2.
- Re-checking the checksum after the copy proves that HDFS holds the same bytes that were acquired.
- The spec times work from "raw staged in HDFS" and reports S3-to-HDFS copy time separately, so record the copy time here.

**Step 1.6. Run the read-only profiling notebook** (`notebooks/task1_profile_raw.py`). It measures:
- physical lines vs parsed rows and malformed rows
- nulls per column
- exact and key duplicates, plus conflicting duplicates
- date coverage
- invalid OHLCV (high < low, close outside the range, zero or negative volume)
- extreme moves
- per-series gaps against the observed trading calendar
- SEC manifest vs HDFS reconciliation and error pages served with HTTP 200

**Why:** Task 1 asks you to find problems, not fix them. Every result is written to `_manifests/.../profile/<run_id>/`, so the report can cite a path instead of a screenshot.

**Step 1.7. Write the data dictionary and the corpus inventory.** For each source, record columns, types, units (USD, shares), time semantics (trading date, exchange local time; SEC filing date, time of day unknown), native IDs (ticker; CIK, accession) and relationships.
**Why:** The inventory gives a CIK, not a ticker. The CIK→ticker link has to be built and documented, for example from SEC `company_tickers.json` plus manual checks. That file reflects current tickers, not historical ones, so it is a stated limitation, and unmatched filings are reported.

**Step 1.8. Freeze the dev and full input profiles.** Dev = a fixed list of about 50 tickers and their matched filings. Full = all 505 tickers and all available filings. Save both lists as manifests.
**Why:** The spec says to develop on a sample until the workload is confirmed. The performance section needs "genuine input growth" between two frozen profiles.

---

## Phase 2: Task 2, the pipeline

**Step 2.1. Hadoop Streaming audit job on the raw price CSV.** A Python mapper and reducer count rows per file, per ticker and per year, and count malformed lines by reason.
**Why:** The spec assigns Hadoop Streaming to "source processing and audit summaries". The job works independently of Spark, so its counts cross-check the Spark row reconciliation. Keep the job ID and the `-D` config.

**Step 2.2. Hadoop Streaming job on SEC filings.** It parses each filing into (CIK, accession, form, filing date, section/paragraph, simple counts) and extracts XBRL facts where present, writing tab-separated output.
**Why:** This is meaningful source processing. HTML filings are document-at-a-time work that maps cleanly onto streaming.

**Step 2.3. Hive: external tables over the raw files, all columns STRING.** Then use HiveQL `INSERT OVERWRITE` to build `cleaned_prices` (typed, Parquet, partitioned by year) and `rejected_prices` (original row + `reject_reason`). Run them with `hive -f` or `beeline -f` from `.sql` files.
**Why:**
- Spark SQL does not count as Hive.
- External STRING tables leave raw untouched.
- Separate cleaned and rejected tables keep raw, cleaned and rejected distinct.
- Because the tables are registered in the shared metastore, Spark reads them through `enableHiveSupport()`.
- `.sql` files and Hive query IDs are the execution evidence.

**Step 2.4. Write down the cleaning rules.** Exact duplicates: keep one. Conflicting duplicates: reject both. Invalid OHLC: reject. Zero volume: flag and keep. Missing bars: leave as gaps and never fill from later data.
**Why:** The spec forbids filling with future observations and asks you to keep genuine gaps.

**Step 2.5. PySpark feature preparation.** Read the Hive tables. Build weekly rows anchored on the last trading day of each week (Friday, or Thursday when Friday is a holiday). Features use only bars up to the close before the anchor open: lagged returns, rolling volatility, volume z-score vs the series' own trailing window, and day-of-week effects. SEC features include a "days since last filing" flag, with availability = **first trading day after the filing date**.
**Why:**
- Window functions keep the work distributed, with no pandas.
- The filing time of day is unknown, so the next trading day is the earliest date the filing is safely known.

**Step 2.6. Target = next-week source-price ratio** = open at the next anchor / open at this anchor. Also derive direction (up/down).
**Why:** The spec's price row names "source-price-ratio prediction", and Task 3 suggests Friday-open to Friday-open.

**Step 2.7. Chronological splits.** Train = anchor and outcome both in 2013–2015. Val = 2016. Test = 2017. Drop rows whose target outcome falls in the next split. Report row counts, date coverage and exclusions per split.
**Why:** "Include a target only when its full outcome is known within that period." Features at the start of 2016 may use 2015 history, because that information was in the past.

**Step 2.8. Reconcile counts:** raw rows = cleaned + rejected (+ exact duplicates removed). Compare against the Streaming audit counts.

---

## Phase 3: Task 3, the model

**Step 3.1. Build one Spark ML `Pipeline`** with Imputer → VectorAssembler → StandardScaler → model, fitted on train only.
**Why:** Fitting the imputer and scaler inside the pipeline guarantees they learn from train only, and they are saved with the model.

**Step 3.2. Baselines on the same rows:** predict ratio = 1 (no change), and predict the train-mean ratio. For direction, use the majority class.

**Step 3.3. Compare a small set of choices on val:** Linear/Ridge vs GBT, and price-only vs price + SEC features. Metrics: RMSE, MAE and directional accuracy. Fix the seeds.
**Why:** The rubric rewards "a manageable set" of justified experiments. Price-only vs +SEC directly tests whether the auxiliary source adds value.

**Step 3.4. Freeze the choice, then score test once.** Report how rows with missing targets are handled.

**Step 3.5. Save the `PipelineModel`** to HDFS, then copy it to S3 and into the ZIP. In a new Spark session, reload it, re-predict on test and compare with the saved predictions at tolerance |Δ| ≤ 1e-9.

---

## Phase 4: Task 4, analytics

**Step 4.1. Q1, unusual activity:** volume z-score vs each series' own history, and how price moves relate to it. Use Hive grouped summaries and a chart.
**Step 4.2. Q2, filing events:** compare volatility and next-week change in filing weeks vs other weeks. Show group sizes and state that this is an association, not causation.
**Step 4.3. Q3, model reliability:** test error vs baseline by month and by volatility decile, with sample sizes.
**Why these three:** They use only the selected sources and cover the three spec examples. Q3 reuses the Task 3 outputs.

Aggregate in Hive or Spark first, then plot only the small results with matplotlib.

---

## Phase 5: Performance (report section 6)

**Step 5.1. Write a timing driver** that runs the full pipeline from staged HDFS raw data to verified outputs. It uses fresh output paths each run and logs stage timings, YARN app IDs, bytes, rows, `hdfs dfsadmin -report` disk use and peak executor memory.

**Step 5.2. Run 3 repetitions each** for: dev profile on the reference config, full profile on the reference config, and full profile on one alternative. Report the median and range.
**Why the alternative is `spark.sql.shuffle.partitions` = 200 → 16 (or AQE coalescing):** The data has about 0.6 M rows, so 200 shuffle partitions mostly add scheduling overhead. This is a software change, so hardware, replication and storage stay fixed, as the spec requires. If there is no speedup, report that, because the spec counts it as a valid finding.

---

## Phase 6: Reproducibility and submission

**Step 6.1.** On a fresh cluster, follow your own Appendix A from scratch. Run the notebook top to bottom and reload the model. Keep the dated outputs.
**Step 6.2.** Before terminating any cluster, copy outputs, logs, the notebook and the model to S3.
**Step 6.3.** Write the report: headings 1–7 + References, ≤3000 words, and the word count on the cover. Appendices A–D. Appendix B lists the check, the command, the expected and actual results, and the file path.
**Step 6.4.** Build the ZIP: `report.pdf`, the `.ipynb`, `models/`, `scripts/`, `sql/`, `config/`, and the small `manifests/` and profile outputs. Leave out the pem, credentials, raw data and absolute machine paths. Check that every referenced path opens from the ZIP root.
