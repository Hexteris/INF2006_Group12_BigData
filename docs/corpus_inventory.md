# Raw-corpus inventory and reproducibility record (Task 1)

Acquisition date `2026-10-28`, all on EMR cluster `j-05901481MKGA6MF7D2EE`:
emr-7.10.0, 1 primary + 2 core m5.xlarge, `dfs.replication=2`.
Column meanings are in `docs/data_dictionary.md`. Evidence paths are relative to the repo root.
`M/` means `evidence/manifests_2026-10-28/`, a copy of HDFS `/corpus/raw/_manifests/`.

## 1. Inventory

| Source | HDFS path (`/corpus/raw/…`) | Files | Bytes | Rows / items | sha256 | Retrieved (UTC) | Origin |
|---|---|---|---|---|---|---|---|
| S1 prices | `prices/v1_2013-2017/acq_2026-10-28/all_stocks_5yr.csv` | 1 | 29,580,549 | 619,040 rows, 505 tickers, 2013-02-08 to 2018-02-07 | `6aea253cd19de60b568143991aaf1fa482456565c389205658d236e595e716cf` | 2026-10-28T06:05:40Z | `https://raw.githubusercontent.com/plotly/datasets/53cd4f5c9e682f59a9b86b6229aa304e570e09dc/all_stocks_5yr.csv` (commit dated 2020-10-05; `master` served identical bytes) |
| S2 SEC filings | `sec_edgar/v1/acq_2026-10-28/*.txt` | 311 | 2,777,408,405 | 311 filings (156 10-K, 155 10-Q), 303 issuers, filed 2013-01-14 to 2017-12-20 | per file in `M/sec_edgar/v1/acq_2026-10-28/sec_download_manifest.csv`. Corpus digest `5af6cd49ab21588c791863b823e0845c156d45fa839c339d435569e8726a3b2c`. | 2026-10-28T06:06:40Z – 06:09:48Z | The 311 URLs listed in S3, from `www.sec.gov/Archives` |
| S3 SEC inventory | `sec_inventory/v1/acq_2026-10-28/SEC_Filing_Inventory.csv` | 1 | 51,751 | 311 rows | `3227f5464490a97eb37c97255181adf9f950739e40c661ba4cf1f62ca5f2df9a` | Course download; uploaded to S3 2026-10-28 | Course page (CSV twin of the official .xlsx, sha256 `cb640a32…f19926`; 0 differences) |
| S4 SEC tickers | `sec_company_tickers/current/acq_2026-10-28/company_tickers.json` | 1 | 798,155 | 8,024 CIKs | `6f9cc4500a98fb4c048328d7df392ed48c092613ec0c3e93cd6c7a3502886074` | 2026-10-28T06:06:12Z | `https://www.sec.gov/files/company_tickers.json` (changes daily) |
| **Total** | | **314** | **2,807,838,860** | | | | |

**Corpus digest** = sha256 of the sorted lines `"<local_file>  <sha256>\n"` for the 311 filings.
One value identifies the whole SEC set. The method is reproducible from the manifest.

**HDFS footprint.** 2.81 GB logical, 5.62 GB with replication 2. That is about 3% of the 188.87 GB configured
HDFS capacity (`evidence/cluster_verify_20261028T060200Z.txt`). The manifests add about 0.4 MB.

## 2. Provenance chain: every file verified at every step

| Step | Check | Result | Evidence |
|---|---|---|---|
| Download | HTTP status, bytes, sha256 recorded per file | 311/311 SEC at HTTP 200 on the first attempt, 0 unavailable. S1 and S4 at HTTP 200. | `M/sec_edgar/.../sec_download_manifest.csv`, `M/prices/.../acquisition_manifest.csv`, `M/sec_edgar/.../sec_unavailable.csv` (header only) |
| SEC access guidance | Declared User-Agent, rate limit | `user_agent_declared=yes rate_limit_rps=4.0` (SEC limit: 10/s) | `M/sec_edgar/.../sec_acquisition_summary.txt` |
| Inventory integrity | Formats, uniqueness, URL vs CIK/accession; CSV vs .xlsx | 0 check failures; 0 CSV–xlsx differences | `M/sec_edgar/.../sec_inventory_checks.csv`, `evidence/inventory_crosscheck/` |
| Inventory: laptop → S3 → cluster | sha256 on the laptop vs on the cluster | `OK` | `M/sec_inventory/.../inventory_sha256_laptop.txt` |
| S3 → HDFS | Local vs HDFS read-back bytes and sha256 per file | 314/314 match | `M/<source>/.../staging_record.csv`, `staging_summary.txt` |
| Download → HDFS | Download-time sha256 vs HDFS sha256 | `acquired=314 staged=314 hdfs_matches_acquisition=314` | `M/acq_vs_hdfs.txt` |
| Content | Each filing's own header vs the inventory | accession, form, filing date and CIK each match 311/311; 311 complete | `M/task1_extra/content/sec_content_summary.txt` |
| **Re-acquisition** | Second independent download, sha256 compared | **prices 1/1 and SEC 311/311 identical** (07:27:47Z – 07:29:49Z) | `M/task1_extra/reacquisition_compare.txt`, `reacq_*.csv` |
| Replication | `hdfs dfs -stat %r` | 2 for every source | `staging_summary.txt` |
| HDFS state | Full recursive listing | 395 lines | `evidence/hdfs_listing_2026-10-28.txt` |

## 3. Configuration and code

| Item | Value | Evidence |
|---|---|---|
| Cluster | `j-05901481MKGA6MF7D2EE`, emr-7.10.0, Spark 3.5.5-amzn-1 | `evidence/cluster_j-05901481MKGA6MF7D2EE_20261028-134828.json`, notebook cell 1 |
| Config checks | replication 2, 2 live datanodes, 2 executors × 2 cores × 6g, 2g overhead, 2g driver, dynamic allocation off: all PASS | `evidence/cluster_verify_20261028T060200Z.txt` |
| Storage deviation | 100 GiB gp3 per core node instead of 200 GiB (Learner Lab limit) | `docs/decisions.md` D2 |
| Launch config | `config/emr-configurations.json`, `config/instance-groups.json`, `project-up.ps1` | repo |
| Code on cluster | repo commit `9526a19`; `scripts/acquire/*` unchanged since `627941d` | `M/task1_extra/cluster_repo_commit.txt`, `git log -- scripts/acquire` |
| Commands run | Full shell history of the session | `evidence/shell_history_2026-10-28.txt` |
| Profiling run | YARN `application_1793166394753_0001`, run ID `20261028T064058Z` | `notebooks/INF2006_G12_task1.ipynb`, `M/<source>/.../profile/20261028T064058Z/` |

## 4. How to acquire the same inputs again

The scripts are stdlib-only Python 3 and run on any EMR primary node.
They refuse to overwrite existing raw files.

```bash
export SEC_USER_AGENT="<Full Name> <email>"            # SEC requires a contact
R=/mnt/g12/raw; M=$R/_manifests/acquisition_manifest.csv
# S1: pinned commit, so the bytes cannot drift
python3 scripts/acquire/acquire_url.py --github-pin --csv-stats --key-col Name --date-col date \
  --url https://raw.githubusercontent.com/plotly/datasets/master/all_stocks_5yr.csv \
  --source prices --version v1_2013-2017 --out-dir $R/prices --manifest $M
# S3: from the course page (or s3://<bucket>/raw/acq_2026-10-28/sec_inventory/)
# S2: every URL in the inventory, 4 requests/s, resumable
python3 scripts/acquire/acquire_sec_filings.py --inventory $R/sec_inventory/SEC_Filing_Inventory.csv --out $R/sec_edgar
# S4: today's snapshot (will differ from ours; use the stored copy to reproduce our match)
python3 scripts/acquire/acquire_url.py --url https://www.sec.gov/files/company_tickers.json \
  --source sec_company_tickers --version current --out-dir $R/sec_ref --manifest $M
```

To check a new acquisition, compare its sha256 values with section 1 and the manifests.

| Source | Expected result | Why |
|---|---|---|
| S1 | Identical | The download is pinned to a commit |
| S2 | Identical while SEC keeps the archive unchanged | Shown by the second download |
| S3 | Identical | Fixed course file |
| S4 | **Will differ** | SEC republishes it daily, and our copy was already 78 bytes different from a laptop copy taken the same morning. Our exact copy is kept in S3 and HDFS. |

## 5. Storage format and organisation

- **Original bytes, original formats.** CSV, EDGAR `.txt` and JSON are stored exactly as served:
  - No decompression or re-encoding (`Accept-Encoding: identity`).
  - No typing. Every value in the price CSV stays a string.

  This is the only way the download-time sha256 still proves the HDFS bytes are the source's bytes.
  Parquet and Hive tables are Task 2 outputs built from this layer.
- **Layout `/<source>/<version>/acq_<YYYY-MM-DD>/`** keeps sources, versions and acquisition dates apart,
  as Task 1.2 requires. A later re-acquisition or a new source version lands in a new folder and never
  overwrites the old one. The staging step refuses to write into an existing raw folder.
- **Manifests in a separate tree.** `/_manifests/<source>/<version>/acq_<date>/` holds provenance, checks
  and profile outputs. Raw folders then contain only source bytes, so a glob over a raw folder reads data only.
- **One file per filing, no container format.** At 311 files the NameNode load is trivial, so there is
  no small-files problem to solve. Every filing fits in one 128 MiB HDFS block (the largest is 122 MiB),
  so each filing is a natural unit of work for one mapper. A SequenceFile or HAR would hide the per-file
  checksums and the accession-based file names.
- **Uncompressed.** 2.8 GB fits easily (about 3% of HDFS), so compression would save nothing that matters.
  Uncompressed text is readable directly by Hadoop Streaming, Hive and Spark, and the price CSV stays
  splittable. Gzip would add decompression to every read and make the CSV a single unsplittable task.
- **Replication 2**, set explicitly on every write, as the spec requires.
- **S3 as the durable copy** (`s3://inf2006-g12-2501690/raw/acq_2026-10-28/`). Learner Lab clusters are
  temporary, so every new cluster restages from S3 and repeats the checksum check.

**Timings** (reported separately from pipeline runtime, as the spec requires):

| Step | Time |
|---|---|
| S1 download | 0.49 s |
| S4 download | 0.08 s |
| S2 download, 311 files at 4 requests/s | 20 sample files at 06:06, remaining 291 files 06:07:31–06:09:48 (about 2.3 min) |
| S2 second download | 2.0 min |
| S3 → HDFS `put` | 24 s in total (prices 3, tickers 3, inventory 2, SEC 16) |
| HDFS read-back for checksums | 16 s |

The local→S3 and S3→local copies were not timed.

## 6. Raw-data quality summary

Counts come from the EMR profiling run (`20261028T064058Z`) and the EMR SEC content check.
The individual rows are listed in `evidence/local_checks/price_anomaly_detail.txt`, produced by
`scripts/profile/price_anomaly_detail.py` on a byte-identical copy (same sha256).

**S1 prices**

| Check | Result |
|---|---|
| Parsing | 619,041 lines = 1 header + 619,040 rows; 0 malformed, 0 blank; 0 unparsable dates or numbers |
| Duplicates | 0 exact; 0 duplicate `(Name, date)`; 0 conflicting |
| Missing values | `open` 11, `high` 8, `low` 8 (11 rows). Several share dates: 2015-07-17 (DHR, ES, O) and 2016-01-12 (DHR, O). |
| Placeholder bars | 4 rows with volume 0 and only a close (DHR and O 2016-01-12, FTV 2016-07-01, UA 2016-04-07) |
| OHLC inconsistency | EMR counts: 1 high < low, 4 open outside [low, high], 9 close outside [low, high]. These overlap (AOS 2014-05-19 fails all three), so **12 distinct rows**. Examples: CHD 2014-05-19 open 18.77 vs low 33.70; LNT 2016-05-19 close 17.87 vs low 35.09. |
| Extreme moves | 7 closes moving more than 50% in a day; 102 more than 20%. Split or spin-off pattern: DISCA, DISCK, EBAY, NI, LNT. Plausibly genuine: AMD 2016-04-22, VRTX 2013-04-19. |
| Coverage | 505 tickers; 29 start late (listings, spin-offs, renames); 0 end early; 6 tickers have 51 missing bars between their own first and last dates |
| Out of scope | 13,130 rows after 2017-12-29 (2018-01-02 to 2018-02-07) |
| **Reconciliation** | 605,910 rows on or before 2017-12-29, minus 23 rows with a null field, OHLC inconsistency or zero volume, leaves **605,887**. That is exactly the spec's "605,887 accepted bars". |

**S2 SEC filings**

| Check | Result |
|---|---|
| Availability and integrity | 311/311 downloaded, 0 unavailable, 311 distinct sha256 (no duplicate files), 0 error pages served as HTTP 200 |
| Completeness | 311/311 start with `<SEC-DOCUMENT>`, end with `</SEC-DOCUMENT>`, and have a closed header |
| Consistency with the inventory | accession, form, filing date and CIK each 311/311 |
| XBRL | 282 have XBRL (281 instance + 1 inline); **29 have none** (2013–2017, both forms) |
| Embedded binaries | 292 files contain uuencoded attachments (images, PDFs, ZIP, Excel) |
| Document count | 166 files have more `<DOCUMENT>` blocks than `PUBLIC DOCUMENT COUNT`, and none has fewer. In the file inspected (13 public documents, 128 blocks), 109 of the extra blocks are EDGAR-generated `R*.htm` rendering pages. Read as EDGAR additions, **not corruption**; the Task 2 parser should confirm by `<TYPE>`. |
| Multiple filers | 12 headers list more than one CIK |
| Timing | 161 accepted at or after 16:00 ET; 37 have an acceptance date different from the filing date (see the data dictionary) |
| Link to prices | 11/311 filings (11 issuers) map to a price ticker; 10 fall inside the price dates (`docs/decisions.md` D1) |

**S3 inventory:** 311 rows, 0 check failures, 0 differences from the official .xlsx.
**S4 tickers:** a current snapshot with no history (see the limitations below).

## 7. Limitations the Task 2 pipeline must handle

| # | Limitation | What Task 2 should do |
|---|---|---|
| L1 | 13,130 rows after 2017-12-29 | Exclude them from the project scope; count them as out of scope, not as rejected |
| L2 | 11 rows with null OHLC values, 4 of them zero-volume placeholders | Route them to the rejected table with a reason. Never fill them from later bars. |
| L3 | 12 rows violating `low ≤ open, close ≤ high` | Reject them with a reason; reconcile the cleaned count to 605,887 |
| L4 | Unknown and inconsistent price adjustment (DISCA, DISCK, LNT, EBAY, NI) | Flag large ratio jumps. Avoid cross-event returns in features or targets, or document how they are excluded. Do not "unadjust" without a corporate-actions source. |
| L5 | Late-starting series and 51 interior missing bars | Keep the gaps; require enough history before computing window features; exclude unavailable targets |
| L6 | Survivorship: the universe reflects index membership around 2018 | State it as a limitation of every result |
| L7 | Only 11 filings link to prices | SEC is not used as model features (D1). Streaming parses all 311; Task 4 uses only the linked filings. |
| L8 | The CIK → ticker map is current, not historical | Report unmatched filings; manual aliases need evidence |
| L9 | Filing availability | Use `ACCEPTANCE-DATETIME` (ET): before 09:30 it informs that day's open, otherwise the next trading day. Never use `filed_at` (late by 1 day in 1 case) or `DATE AS OF CHANGE`. |
| L10 | Container format, 292 files with uuencoded binaries, EDGAR rendering pages | The Streaming parser splits on `<DOCUMENT>`, keeps the main document, exhibits and XBRL by `<TYPE>`, and skips binaries and `R*.htm` |
| L11 | 29 filings without XBRL | Mark financial facts as missing for those; text features remain possible |
| L12 | Multiple filers in 12 headers | Attribute the filing to the inventory CIK |
