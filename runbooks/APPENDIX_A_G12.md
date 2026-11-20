# Appendix A — Required EMR setup and rerun guide

> Paste this into the report as **Appendix A**. Fill every `⟨…⟩` from the run you actually did.
> The numbered headings follow the project instructions, so the teaching team can follow them on a fresh EMR cluster.

## A.1 Prerequisites and cluster configuration

- **EMR release** emr-7.10.0 (`⟨verify with aws emr describe-cluster → ReleaseLabel⟩`).
- **Applications**: Hadoop, Hive, Spark, Livy, JupyterHub. Versions: `⟨hadoop` / `hive` / `spark` / `python3 --version⟩`.
- **Instances**: 1 primary + 2 core m5.xlarge; no task nodes; no autoscaling.
- **Worker storage**: one gp3 data volume per core node, 3,000 IOPS, 125 MiB/s, `⟨size` GiB: 100 in `config/instance-groups.json;
  AWS Academy caps gp3 volume size⟩`.
- **HDFS**: `dfs.replication=2` (emr-configurations.json + forced on every write).
- **Spark**: 2 executors × 2 cores × 6 GiB heap, 2 GiB overhead, 2 GiB driver, dynamic allocation off.
- **AWS permissions**: Learner Lab role sufficient for EMR + S3; bucket `s3://⟨BUCKET⟩/` with prefixes `raw/`, `inventory/`,
  `evidence/`, `models/`, `emr-logs/`. **No credentials are included**; the instance role is used.
- **Configurable names/paths**: `⟨BUCKET⟩`, `⟨ACQ⟩`, `⟨CLUSTER⟩`, HDFS root `/corpus`.

## A.2 Prepare the environment

1. Start Learner Lab; paste the AWS CLI profile; download `vockey2.pem`; `icacls vockey2.pem /inheritance:r /grant:r …`.
2. `project-up.ps1 -Bucket ⟨BUCKET⟩ -KeyName vockey2 -PemFile vockey2.pem` (~10–15 min; writes
   `evidence/cluster_⟨CLUSTER⟩_⟨stamp⟩.json`).
3. On the primary node: `sudo yum install -y git tmux`; clone the repo to `~/g12`; `tmux new -s g12`.
4. Dependencies: `pip install --user notebook nbconvert bash_kernel`; `python3 -m bash_kernel.install --user`.
   Hadoop/Hive/Spark come from EMR; `spark-submit` uses YARN.
5. Verify the environment: `bash scripts/cluster/verify_cluster.sh ⟨BUCKET⟩` → all checks PASS
   (`evidence/cluster_verify_⟨RUN⟩.txt`).

## A.3 Data sources and staging

**The data is already in S3 and is not re-downloaded for the assessed runs.** `s3://⟨BUCKET⟩/raw/` holds the exact bytes
acquired earlier, with the manifests that prove where they came from. Each run cluster copies them into HDFS unchanged.

| Source | S3 prefix under `s3://⟨BUCKET⟩/raw/` | Version | Files | Bytes | Acquired (UTC) | Verification |
|---|---|---|---|---|---|---|
| Daily prices (`all_stocks_5yr.csv`, commit-pinned) | `prices/` | `v1_2013-2017` | 1 | `⟨28 MB⟩` | `⟨ACQ⟩` | sha256 `6aea253c…` |
| SEC filing inventory | `sec_inventory/` | `v1` | 1 | `⟨51 KB⟩` | `⟨ACQ⟩` | sha256 in `staging_record.csv` |
| SEC EDGAR filings | `sec_edgar/` | `v1` | 311 | `⟨2.8 GB⟩` | `⟨ACQ⟩` | per-file sha256 |
| Wikinews dumps | `wikinews/` | `20261001_meta-history` | 2 | `⟨662 MB⟩` | `⟨ACQ⟩` | publisher sha1 |
| GDELT 1.0 events | `gdelt/` | `v1_events_2015-2017` | 731 | `⟨9–10 GB⟩` | `⟨ACQ⟩` | publisher MD5 + sha256 |

How the S3 copy was produced (already done; not repeated): `scripts/acquire/acquire_url.py` for the commit-pinned price
file, `acquire_sec_filings.py` for the filings (`SEC_USER_AGENT` declared, 4 req/s, unavailable URLs logged and never
substituted), `acquire_gdelt_wikinews.py` for the archives and dumps. The acquisition manifests are stored beside the data
under `s3://⟨BUCKET⟩/raw/_manifests/`.

To stage and verify: run `notebooks/INF2006_G12_stage_corpus.ipynb` cells 1–5 (bash). Each source is downloaded from S3,
hashed locally, written to HDFS with `-D dfs.replication=2`, read back and re-hashed, and recorded in `staging_record.csv`
under `/corpus/raw/_manifests/…`. Expected: **1,046 files**, every record `match=yes`, `replication_not_2=0`. The staging
times (`download_s`, `put_s`, `readback_s`) are recorded and **excluded from the timed comparison**.

Note the GDELT coverage: the corpus has all **731** archives, but the recorded 2026-11-09 pipeline run processed a
**150-date sample** (90 dates Jul–Sep 2015, 30 Jan 2016, 30 Jan 2017). Say which coverage a reported number refers to, and
do not treat an uncollected date as a zero-news date.

Hive: `hive -f sql/01_raw_tables.sql` creates the external STRING table over the staged CSV; Spark reads the same tables via
`enableHiveSupport()` and `spark.table("⟨db⟩.cleaned_prices")`.

## A.4 Upload and execute the project

Order (all paths relative to the ZIP root; run from `~/g12`):

| # | File | Purpose | Expected output | Approx. runtime |
|---|---|---|---|---|
| 1 | `notebooks/INF2006_G12_stage_corpus.ipynb` | copy the existing S3 data into HDFS and verify | `/corpus/raw/…`, manifests | `⟨⟩` min (not timed) |
| 2 | `notebooks/INF2006_G12_task1.ipynb` (`FULL_RUN=True`, PySpark) | corpus profile | profile JSON + S3 evidence | `⟨⟩` min |
| 3 | `task2_driver/run_matrix.sh` (`MATRIX="dev:reference full:reference" REPS="1 2 3"`) | 6 timed runs (3 dev + 3 full), incl. `train` | `aggregate_report.md`, `aggregate_runs.csv` | `⟨⟩` h total |
| 4 | `INF2006_G12_task3.ipynb` (JupyterHub, `%%configure driverMemory 2g`) | train + save | `/corpus/models/…`, predictions | `⟨⟩` min |
| 5 | `evidence/task4/INF2006_G12_task4_analytics new.ipynb` (PySpark) | Q1, Q2 | `evidence/task4/*.csv`, `*.png` | ~2 min |
| 6 | fresh-session reload cell (run guide, Task 3) | reproduce predictions | max |Δ| ≤ 1e-9 | `⟨⟩` min |

Parameters set at the top of each notebook/section; helper scripts: `scripts/acquire/*`, `scripts/stage/…`,
`scripts/streaming/*`, `task2_driver/assets/*`, `sql/*`.

## A.5 Reload the saved model and verify results

- **Checkpoint**: `/corpus/models/⟨direction_v2⟩` (copy at `s3a://⟨BUCKET⟩/models/⟨…⟩`).
- Load in a fresh kernel with `PipelineModel.load(...)`; transform the test split; join to saved predictions on
  `(ticker, trade_date)`; tolerance **1e-9**; observed max |Δ| `⟨⟩`.
- Checks that confirm reproduction: row counts per split, label balance, reconcile PASS, prediction fingerprint equality,
  metric equality (AUC/balanced accuracy to `⟨4⟩` dp).
- **Dated evidence of our own fresh-session rerun**: `⟨date/time UTC, notebook name, file paths⟩`.

## A.6 Troubleshooting and clean-up

| Symptom | Diagnosis | Fix |
|---|---|---|
| `create-cluster` `VALIDATION_ERROR` (Service role … insufficient EC2 permissions) | AWS Academy caps gp3 volume size (observed at 200 GiB) | use the 100 GiB config; record the deviation |
| SSH times out after launch | laptop public IP changed | `project-up.ps1 -ClusterId ⟨CLUSTER⟩ …` re-opens the rule |
| A Streaming/Hive job hangs | a notebook Spark session holds the YARN memory | stop the notebook session, rerun the job |
| `hive` table empty | `LOCATION` date does not match the staged folder | edit `LOCATION` or pass `--hivevar` (driver SQL) |
| Pre-flight FAIL in the driver | missing command, multiple price CSVs, output folder already exists | install / set `RAW_PRICES_FILE` / use a new `RUN_ID` |
| Notebook cannot write `_manifests` | permissions | `hdfs dfs -chmod -R 777 /corpus/raw/_manifests` |
| `gdelt_streaming` map timeouts | large archives | `-D mapreduce.task.timeout=3600000` (already set) |

**Preserve, then clean up**: copy runs, evidence, notebooks and models to S3 (see Shut down and ship in the run guide);
terminate with `project-down.ps1`; click **End Lab**. HDFS and node disks are deleted with the cluster.
