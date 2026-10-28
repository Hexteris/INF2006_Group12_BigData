# INF2006 Group 12: Big Data Project

End-to-end pipeline on AWS EMR: daily stock prices + SEC filings → HDFS corpus → Hadoop Streaming / Hive /
PySpark pipeline → Spark ML model → analytics.

| Task | Status | Owner |
|---|---|---|
| 1. Corpus | Done (acquisition 2026-10-28) | Daniel |
| 2. Pipeline | Not started | B (Streaming + Hive), C (PySpark) |
| 3. Model | Not started | D |
| 4. Analytics | Not started | E |

**Read first:** `docs/data_dictionary.md`, `docs/corpus_inventory.md` §7 (limitations L1–L12 that Task 2
must handle), `docs/decisions.md`, and `docs/contracts.md` (agreed paths and table names).

## Repo layout

| Path | What | Owner |
|---|---|---|
| `scripts/acquire/` | Download prices, SEC filings and the ticker map, with manifests | Task 1 |
| `scripts/stage/stage_raw_to_hdfs.sh` | Copy a source into `/corpus/raw/...` and verify sha256 | Task 1 |
| `scripts/profile/` | Read-only quality checks | Task 1 |
| `scripts/cluster/verify_cluster.sh` | Check a running cluster matches the reference config | Task 1 |
| `scripts/streaming/` | Hadoop Streaming mappers and reducers | B |
| `sql/` | Hive `.sql` files, run in numeric order | B |
| `notebooks/sections/` | PySpark notebook sections as `# %%` `.py` files, one per task/topic | C, D, E |
| `notebooks/INF2006_G12_task1.ipynb` | Task 1 notebook exported from EMR | Task 1 |
| `config/` | EMR configuration used by `project-up.ps1` | Task 1 |
| `evidence/` | Dated outputs from EMR runs (manifests, job IDs, listings) | everyone |
| `project-up.ps1`, `project-down.ps1` | Launch / terminate the reference cluster | everyone |

## Working rules

- Code goes through GitHub `main`. Before every push: `git pull --rebase`. Small commits.
- Only edit files in your own folders. Never edit a `.ipynb` together; write your part as
  `notebooks/sections/<task>_<topic>.py` with `# %%` cells (they are joined into one notebook for the final run).
- **Never commit data, keys or credentials.** Put downloaded data under `raw/` (git-ignored). SEC filings
  are `.txt`, so outside `raw/` they *would* be committed.
- Read raw data as strings. Never modify anything under `/corpus/raw` on HDFS.
- Put every HDFS path and the acquisition date at the top of your script/section as a parameter.
- Mappers and reducers: Python 3 standard library only (they run on EMR's Python).
- Before running Hadoop Streaming or Hive on EMR, shut down any notebook Spark session. A notebook holds
  most of the cluster's YARN memory and other jobs will wait forever.

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

Expected: price sha256 `6aea253c…`, 619,040 rows, 505 tickers (same bytes as the assessed corpus).
Drop `--limit 20` for all 311 filings (about 2.8 GB, a few minutes).

## 3. Developing each part locally

**Hadoop Streaming (B).** Test the mapper/reducer as a pipe in **Git Bash** (PowerShell's `sort` differs from Hadoop's):

```bash
cat raw/prices/all_stocks_5yr.csv | python scripts/streaming/<name>_mapper.py \
  | LC_ALL=C sort | python scripts/streaming/<name>_reducer.py
```

**Hive (B).** Write `sql/NN_<name>.sql`. Hive does not run easily on Windows; test on EMR with `hive -f`.

**PySpark (C, D, E).** Run your section in local mode on the laptop copy, then on EMR. Read Hive tables with
`spark.table("g12.<table>")` on EMR; locally, read the CSV directly with every column as string.

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
6. Save evidence **before** shutting down (job IDs, counts, logs), e.g.
   `aws s3 cp --recursive <dir> s3://$B/evidence/<task>/`, then on the laptop
   `aws s3 sync s3://<bucket>/evidence/ evidence/` and commit. HDFS and the primary node are deleted with the cluster.
7. `.\project-down.ps1`, then **End Lab**.

## 5. The assessed run

The final timed run, exported notebook and saved model are produced once, in Daniel's account, from the
merged `main` branch. Members join by screen share or SSH. Development runs in your own account are
supporting evidence only.
