# Team corpus setup (from Google Drive)

Goal: everyone gets the same 1,047 files (13.08 GB) in their own S3 bucket and HDFS, byte for byte.
`evidence/expected_corpus.csv` lists every file's size and sha256; `setup_corpus.py` checks against it.

| Drive folder | Files | Size |
|---|---|---|
| `PRICE` | `all_stocks_5yr.csv` | 30 MB |
| `SEC EDGAR` | 311 `.txt` filings | 2.78 GB |
| `REFERENCE` | `SEC_Filing_Inventory.csv`, `company_tickers.json` | 0.9 MB |
| `WIKINEWS` | 2 `.bz2` | 662 MB |
| `GDELT` | 731 `.zip` | 9.61 GB |
| `BUNDLES` (Route B only) | one `.tar` per source | 13.1 GB |

Develop on the laptop with the sample (`--sample`, 31 files). Run everything assessed on EMR.

---

## Route A: Drive → laptop → S3 (simplest)

1. **Get the repo:** `git pull` (or clone, see README step 1).
2. **Download from Drive.** Easiest: install [Google Drive for desktop](https://www.google.com/drive/download/),
   then right-click `INF2006_bigdata_project` → *Available offline*. Or download the folders in the browser
   and unzip all parts into one folder. Do not unzip the GDELT `.zip` or Wikinews `.bz2` files themselves.
3. **Verify and arrange** (PowerShell, repo folder):
   ```powershell
   python scripts\stage\setup_corpus.py --from "G:\My Drive\INF2006_bigdata_project" --to raw
   ```
   Expect `OK 1047/1047`. Only the sample for laptop work: add `--sample` (expect `OK 31/31`).
   Add `--move` if you downloaded to a normal folder and don't want two copies.
4. **Upload to your bucket.** Paste Learner Lab *AWS Details → AWS CLI* into `%USERPROFILE%\.aws\credentials`:
   ```powershell
   aws s3 sync raw s3://inf2006-g12-<studentID>/raw/ --exclude "*.part"
   aws s3 ls s3://inf2006-g12-<studentID>/raw/ --recursive --summarize | Select-Object -Last 2
   ```
   Expect `Total Objects: 1048` (1,047 files + `setup_report.txt`). Credentials expire after the lab
   session; if the sync stops, paste fresh ones and rerun the same command (it resumes).
5. Continue with **On the cluster** below.

## Route B: Drive → cluster directly (faster, needs SSH)

Daniel shares the `BUNDLES` files as *Anyone with the link* and puts their file IDs in
`config/drive_bundles.txt`. On the EMR primary node (README steps 3–4 for launch + clone):

```bash
sudo mkdir -p /mnt/g12 && sudo chown hadoop:hadoop /mnt/g12 && cd ~/g12
pip3 install --user gdown
mkdir -p /mnt/g12/incoming && cd /mnt/g12/incoming
grep -v '^#' ~/g12/config/drive_bundles.txt | while read -r name id; do ~/.local/bin/gdown "$id" -O "$name"; done
for t in *.tar; do tar -xf "$t" && rm "$t"; done
cd ~/g12
python3 scripts/stage/setup_corpus.py --from /mnt/g12/incoming --to /mnt/g12/raw --move   # expect OK 1047/1047
aws s3 sync /mnt/g12/raw s3://$B/raw/       # keep a durable copy; the cluster disk is deleted
```
If `gdown` reports a quota or permission error, use Route A.

## On the cluster (both routes)

```bash
export B=inf2006-g12-<studentID>
aws s3 sync s3://$B/raw/ /mnt/g12/raw/                     # Route A only
bash scripts/stage/stage_all.sh /mnt/g12/raw               # verify + stage all 6 sources into HDFS
aws s3 cp --recursive /mnt/g12/staging s3://$B/evidence/staging/
```
Expect `OK 1047/1047`, then `mismatches=0` per source and `ALL SOURCES STAGED AND VERIFIED`.
HDFS paths (identical for everyone):

```
/corpus/raw/prices/v1_2013-2017/acq_2026-10-28/
/corpus/raw/sec_edgar/v1/acq_2026-10-28/
/corpus/raw/sec_inventory/v1/acq_2026-10-28/
/corpus/raw/sec_company_tickers/current/acq_2026-10-28/
/corpus/raw/wikinews/20261001_meta-history/acq_2026-10-29/
/corpus/raw/gdelt/v1_events_2015-2017/acq_2026-10-29/
```
Next session (new cluster): only the *On the cluster* block, since S3 keeps the data.

## If something fails

| Message | Fix |
|---|---|
| `missing` | File not downloaded; check the Drive folder finished syncing, rerun |
| `wrong size/sha256` | Corrupt/partial download; delete that file, download it again, rerun |
| `ExpiredToken` / `InvalidAccessKeyId` | Paste fresh Learner Lab credentials, rerun |
| `STOP: ... already exists` | Already staged on this cluster; nothing to do |
| `No space left` (laptop) | Use `--move`, or `--sample` for laptop-only work |
