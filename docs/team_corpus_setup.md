# Team corpus setup: details and troubleshooting

The steps are in `README.md`. This page explains what the scripts do and how to fix problems.

## The bronze corpus (Task 1)

Raw data, stored exactly as served by each source. Unzipping, parsing and filtering are Task 2 (silver).
`evidence/expected_corpus.csv` lists the size and sha256 of every file; every copy is checked against it.

| Drive (`INF2006_bigdata_project/`) | Files | Size |
|---|---|---|
| `PRICE/` | `all_stocks_5yr.csv` | 30 MB |
| `SEC EDGAR/` | 311 `.txt` filings | 2.78 GB |
| `SEC_Filing_Inventory.csv` (top level) | 1 | 51 KB |
| `Wikinews/` | 2 `.bz2` history dumps | 662 MB |
| `GDELT/` | 731 daily `.zip` files, 2015-07-01 to 2017-06-30 | 9.61 GB |
| **Total** | **1,046** | **13.08 GB** |

Files are matched by name, so the folder layout after extracting the Drive zips does not matter.

## Where the data ends up

Laptop (`raw\`) and S3 (`s3://inf2006-g12-<studentID>/raw/`) use `<source>/<version>/acq_<date>/`.
HDFS is identical for everyone:

```
/corpus/raw/prices/v1_2013-2017/acq_2026-10-28/
/corpus/raw/sec_edgar/v1/acq_2026-10-28/
/corpus/raw/sec_inventory/v1/acq_2026-10-28/
/corpus/raw/wikinews/20261001_meta-history/acq_2026-10-29/
/corpus/raw/gdelt/v1_events_2015-2017/acq_2026-10-29/
```

## What the scripts do

| Script | Where | Does |
|---|---|---|
| `setup.ps1` | laptop | Checks Python, disk and AWS credentials; runs `setup_corpus.py`; creates the bucket; uploads `raw\` |
| `scripts/stage/setup_corpus.py` | both | Checks every file's size and sha256; arranges files into the `raw/` layout |
| `project-up.ps1` | laptop | Launches the reference cluster, opens SSH from your IP, connects |
| `scripts/stage/cluster_setup.sh` | cluster | Copies S3 → `/mnt/g12/raw`, checks again, stages into HDFS, saves records to S3 |
| `scripts/stage/stage_all.sh` | cluster | Runs `stage_raw_to_hdfs.sh` once per source (skips sources already in HDFS) |
| `scripts/stage/stage_raw_to_hdfs.sh` | cluster | Copies one source into HDFS (replication 2) and compares sha256 after reading back |

## Laptop sample for development

For testing mappers or PySpark locally, a fixed 30-file subset (prices, inventory, 20 SEC filings,
GDELT 2015-07-01 to 07-07, the smaller Wikinews file):

```powershell
py -3 scripts\stage\setup_corpus.py --from C:\Dev\drive_dl --to raw_sample --sample
```

Expect `OK 30/30`. All assessed runs happen on EMR on the full corpus.

## If something fails

| Message | Fix |
|---|---|
| `PROBLEM ...: missing` | That file was not in the download. Check every Drive zip part was downloaded and extracted into the same folder, then rerun |
| `PROBLEM ...: wrong size/sha256` | Corrupt or partial file. Delete it, re-download the zip part that contains it, extract, rerun |
| `running scripts is disabled on this system` | Start the command with `powershell -ExecutionPolicy Bypass -File` as shown in the README |
| `python` opens the Microsoft Store | Install Python from python.org with *Add python.exe to PATH*; `setup.ps1` also tries `py -3` |
| `AWS credentials missing or expired`, `ExpiredToken` | Learner Lab → AWS Details → AWS CLI: Show; paste into `%USERPROFILE%\.aws\credentials`; rerun |
| `only N GB free` / `No space left` | Free about 14 GB, or pass `-To D:\g12raw` to use another drive |
| `Key file not found` | AWS Details → Download PEM; save as `labsuser.pem` in the repo folder |
| SSH times out later (your IP changed) | `powershell -ExecutionPolicy Bypass -File .\project-up.ps1 -ClusterId (Get-Content .project-cluster-id)` |
| `STOP: ... already exists` | That source is already in HDFS on this cluster; nothing to do |
| `cannot read s3://.../raw/` | Wrong bucket name, or step 3 (`setup.ps1`) has not finished |
