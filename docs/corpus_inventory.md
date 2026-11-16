# Raw-corpus inventory and reproducibility record (Task 1)

Acquisition dates `2026-10-28` (prices, SEC), `2026-10-29` (GDELT, Wikinews), on EMR cluster `j-00703753JKXXC2HG7Y9X`:
emr-7.10.0, 1 primary + 2 core m5.xlarge, `dfs.replication=2`.
Column meanings are in `docs/data_dictionary.md`. Evidence paths are relative to the repo root.

## 1. Inventory

| Source | HDFS path (`/corpus/raw/…`) | Files | Bytes | Rows / items | sha256 | Retrieved (UTC) | Origin |
|---|---|---|---|---|---|---|---|
| S1 prices | `prices/v1_2013-2017/acq_2026-10-28/all_stocks_5yr.csv` | 1 | 29,580,549 | 619,040 rows, 505 tickers, 2013-02-08 to 2018-02-07 | `6aea253cd19de60b568143991aaf1fa482456565c389205658d236e595e716cf` | 2026-10-28T06:05:40Z | `https://raw.githubusercontent.com/plotly/datasets/53cd4f5c9e682f59a9b86b6229aa304e570e09dc/all_stocks_5yr.csv` (commit dated 2020-10-05; `master` served identical bytes) |
| S2 SEC filings | `sec_edgar/v1/acq_2026-10-28/*.txt` | 311 | 2,777,408,405 | 311 filings (156 10-K, 155 10-Q), 303 issuers, filed 2013-01-14 to 2017-12-20 | per file in `evidence/task1_20261101T112713Z/_run/01_manifests_copied/part-*.json`. Corpus digest `5af6cd49ab21588c791863b823e0845c156d45fa839c339d435569e8726a3b2c`. | 2026-10-28T06:06:40Z – 06:09:48Z | The 311 URLs listed in S3, from `www.sec.gov/Archives` |
| S3 SEC inventory | `sec_inventory/v1/acq_2026-10-28/SEC_Filing_Inventory.csv` | 1 | 51,751 | 311 rows | `3227f5464490a97eb37c97255181adf9f950739e40c661ba4cf1f62ca5f2df9a` | Course download; uploaded to S3 2026-10-28 | Course page (CSV twin of the official .xlsx, sha256 `cb640a32…f19926`; 0 differences) |
| S4 GDELT | `gdelt/v1_events_2015-2017/acq_2026-10-29/*.zip` | 731 | 9,611,684,528 | 144,207,180 lines, 731 daily archives 2015-07-01 to 2017-06-30 | per file in `evidence/task1_20261101T112713Z/gdelt/20261101T112713Z/01_files_and_bytes/part-*.json` | 2026-10-29T08:29:33Z – 09:04:35Z | GDELT Event Database daily exports, 2015-07-01 to 2017-06-30 |
| S5 Wikinews | `wikinews/20261001_meta-history/acq_2026-10-29/*.bz2` | 2 | 662,002,512 | 1,954,230 revisions, 265,900 pages, 2004-11-08T08:11:42Z to 2026-09-30T13:50:02Z | per file in `evidence/task1_20261101T112713Z/wikinews/20261101T112713Z/01_per_file/part-*.json` | 2026-10-29T08:34:08Z – 08:36:23Z | Wikimedia meta-history dump; host `dumps.wikimedia.org` returned 404, mirror `mirror.accum.se` served the files |
| **Total** | | **1,046** | **13,080,727,745** | | | | |

**HDFS footprint.** 13.08 GB logical, 26.16 GB with replication 2. That is about 3% of the 188.87 GB configured HDFS capacity. The manifests add about 0.4 MB.

## 2. Provenance chain: every file verified at every step

| Step | Check | Result | Evidence |
|---|---|---|---|
| Download | HTTP status, bytes, sha256 recorded per file | 311/311 SEC at HTTP 200 on the first attempt, 0 unavailable. Prices, GDELT and Wikinews at HTTP 200. | `evidence/task1_20261101T112713Z/_run/01_manifests_copied/part-*.json` |
| Publisher checksums | Published hash vs downloaded file | GDELT md5 731/731, Wikinews sha1 2/2 matched. Prices pinned commit. | `evidence/task1_20261101T112713Z/_run/07_publisher_checksums/part-*.json` |
| Wikinews mirror fallback | Primary host unavailable | `dumps.wikimedia.org` returned 404 four times (2026-10-29 08:34:08–08:34:16Z), then `mirror.accum.se` served both files (08:35:47–08:36:23Z) | Same download manifest |
| S3 → HDFS | Local vs HDFS read-back bytes and sha256 per file | 1,046/1,046 match | `evidence/task1_20261101T112713Z/_run/03_staging_self_check/part-*.json` |
| Download → HDFS | Download-time sha256 vs HDFS sha256 | `acquired=1046 staged=1046 hdfs_matches_acquisition=1046` | `evidence/task1_20261101T112713Z/_run/06_provenance_chain/part-*.json` |
| Content checks | Each filing's own header vs the inventory; GDELT/Wikinews parsing | accession, form, filing date and CIK each match 311/311; 0 problem files; GDELT 0 zip errors; Wikinews 0 truncated | `evidence/task1_20261101T112713Z/sec_edgar/20261101T112713Z/01_content_summary/part-*.json`, `evidence/task1_20261101T112713Z/gdelt/20261101T112713Z/01_files_and_bytes/part-*.json` |
| Re-acquisition | Second independent download, sha256 compared | **prices 1/1 and SEC 311/311 identical** | Previous run evidence |
| Replication | `hdfs dfs -stat %r` | 2 for every source | Staging summaries |

## 3. Configuration and code

| Item | Value | Evidence |
|---|---|---|
| Cluster | `j-00703753JKXXC2HG7Y9X`, emr-7.10.0, Spark 3.5.5-amzn-1 | `evidence/cluster_j-00703753JKXXC2HG7Y9X_20261101-190507.json`, notebook cell 1 |
| Config checks | replication 2, 2 live datanodes, 2 executors × 2 cores × 6g, 2g overhead, **1000M driver**, dynamic allocation off: all PASS | `evidence/task1_20261101T112713Z/_run/00_cluster_config/part-*.json` |
| Storage | 100 GiB gp3 per core node (spec: 100 GiB) | `config/instance-groups.json` |
| Launch config | `config/emr-configurations.json`, `config/instance-groups.json`, `project-up.ps1` | repo |
| Profiling run | YARN `application_1793529609230_0007`, run ID `20261101T112713Z` | `notebooks/INF2006_G12_task1.ipynb`, `evidence/task1_20261101T112713Z/` |

## 4. How to acquire the same inputs again

| Source | Command | Expected result |
|---|---|---|
| Prices | `python scripts/acquire/acquire_url.py --github-pin --csv-stats --key-col Name --date-col date --url https://raw.githubusercontent.com/plotly/datasets/master/all_stocks_5yr.csv --source prices --version v1_2013-2017 --out-dir raw/prices --manifest raw/_manifests/acquisition_manifest.csv` | Identical (pinned commit) |
| SEC inventory | Download `SEC_Filing_Inventory.csv` from course page, upload to S3 | Identical (fixed file) |
| SEC filings | `python scripts/acquire/acquire_sec_filings.py --inventory SEC_Filing_Inventory.csv --out raw/sec_edgar` | Identical while SEC keeps archive unchanged |
| GDELT | `python scripts/acquire/acquire_gdelt_wikinews.py --source gdelt --start 2015-07-01 --end 2017-06-30 --out raw/gdelt` | 731 files, md5 matches publisher |
| Wikinews | `python scripts/acquire/acquire_gdelt_wikinews.py --source wikinews --dump-date 2026-10-01 --out raw/wikinews` | 2 files, sha1 matches publisher |
| Staging | Run `notebooks/INF2006_G12_stage_corpus.ipynb` on EMR | Copies S3 → HDFS with sha256 verification |

## 5. Storage format and organisation justification

- **Original bytes, original formats.** CSV, EDGAR `.txt`, ZIP and BZ2 are stored exactly as served (`Accept-Encoding: identity`). No typing, no decompression. This preserves download-time sha256 as proof that HDFS bytes match source bytes.
- **Layout `/<source>/<version>/acq_<YYYY-MM-DD>/`** separates sources, versions and acquisition dates. A later re-acquisition lands in a new folder; staging refuses to overwrite existing raw folders.
- **Manifests in a separate tree.** `/_manifests/<source>/<version>/acq_<date>/` holds provenance and profile outputs. Raw folders contain only source bytes.
- **GDELT `.zip` files are not splittable.** Spark reads them with `binaryFiles` (one task per file) and unzips in memory. Each ZIP contains exactly one CSV.
- **Wikinews `.bz2` is read directly by Hadoop** via `newAPIHadoopFile` with `TextInputFormat`. BZ2 is splittable, but one split per file is used to keep revisions in order.
- **Compressed vs expanded sizes are not interchangeable.** GDELT 9.61 GB zipped vs 54.71 GB expanded. Wikinews 662 MB compressed vs ~3 GB expanded. The raw layer stores the compressed bytes.
- **Replication 2**, set explicitly on every write as the spec requires.
- **S3 as durable copy** (`s3://inf2006-bigdata/evidence/task1_20261101T112713Z/`). Learner Lab clusters are temporary; every new cluster restages from S3 and repeats checksum checks.

## 6. Timings

Reported separately from pipeline runtime, as the spec requires.

| Step | Time (s) | Evidence |
|---|---|---|
| Prices download | 1 | `evidence/task1_20261101T112713Z/_run/10_timings/part-*.json` |
| SEC download | 21 | |
| GDELT download | 145 | |
| Wikinews download | 4 | |
| S3 → HDFS `put` (all) | 90 | |
| HDFS read-back for checksums | 1,140 | |
| SEC content check | 18.4 | |
| GDELT scan | 349.8 | |
| Wikinews scan | 648.6 | |

## 7. Quality summary per source

**S1 prices**
- 619,040 rows = 1 header + 619,040 data rows. 0 malformed, 0 blank, 0 duplicates.
- Nulls: open 11, high 8, low 8 (11 rows).
- Dates 2013-02-08 to 2018-02-07. 505 tickers, 1,259 trading dates.
- Validity: high<low 1; open outside range 4; close outside range 9; zero volume 4.
- 13,130 rows after 2017-12-29. 23 in-scope rows flagged: null_field 11, high_lt_low 1, open_outside_range 3, close_outside_range 8.
- **Accepted bars: 605,887** (matches spec).
- Moves over 50%: 7 (LNT +101%, NI -62.6%, VRTX +61.9%, EBAY -56.9%, AMD +52.3%, DISCK -51%, DISCA -50.6%). Over 20%: 102.
- Coverage: 29 series start late, 0 end early, 6 series have gaps (51 missing bars).

**S2 SEC filings**
- 311 files, all start/end with `<SEC-DOCUMENT>`, header closed, match inventory on accession, form, filing date and CIK.
- XBRL in 282 files, none in 29. Uuencoded binaries in 292.
- Document count equals blocks in 145 files; extra blocks are EDGAR rendering pages.
- Multiple filers: 12 headers.
- Acceptance time: before 09:30 ET 31; 09:30–16:00 ET 119; at or after 16:00 ET 161.
- 0 problem files, 0 not UTF-8, 0 error pages.

**S3 SEC inventory**
- 311 rows, 311 distinct accessions, 303 issuers. 156 10-K and 155 10-Q.
- Filed 2013-01-14 to 2017-12-20. All CIKs kept as 10-digit text.
- 0 nulls, 0 check failures.

**S4 GDELT**
- 731 files, one per day, 2015-07-01 to 2017-06-30, none missing.
- Zip 9,611,684,528 B (9.61 GB); expanded 54,706,262,609 B (54.71 GB) – matches spec.
- **144,207,180 lines**, all with 58 fields (spec 144.2M, +7,180 or +0.005%).
- 0 bad IDs, 0 duplicate IDs, 0 invalid SQLDATE, 0 non-UTF-8 lines.
- Empty fields: Actor1Code 13,164,100 (9.1%), Actor2Code 38,493,957 (26.7%), ActionGeo country 3,567,789 (2.5%).
- DATEADDED equals file date for every line.
- Event date vs file date: same day 139,620,872 (96.8%); 1 day 874,774; 2–7 days 1,367,732; 8–30 days 815,300; 31–365 days 1,501,519; over a year 26,983.

**S5 Wikinews**
- 2 files: p1p481753 (482,143,381 B): 142,719 pages, 1,500,001 revisions; p481754p3096188 (179,859,131 B): 123,181 pages, 454,229 revisions.
- 265,900 distinct pages, 1,954,230 distinct revision IDs.
- Timestamps 2004-11-08T08:11:42Z to 2026-09-30T13:50:02Z.
- Contributor deleted 543; text deleted 2,531; empty text 5,392.
- Revisions by namespace: ns0 623,795; ns10 437,843; ns3 303,133; ns4 158,560; ns1 122,841; ns2 95,870; ns14 62,580.
- 2013–2017: 426,482 revisions (21.8% of all). 90,053 are article (ns0) revisions covering 16,340 articles.

## 8. Limitations Task 2 must handle

| # | Limitation | What Task 2 should do |
|---|---|---|
| L1 | 13,130 rows after 2017-12-29 | Exclude from project scope; count as out of scope, not rejected |
| L2 | 11 rows with null OHLC values, 4 zero-volume placeholders | Route to rejected table with reason |
| L3 | 12 rows violating `low ≤ open, close ≤ high` | Reject with reason; reconcile cleaned count to 605,887 |
| L4 | Unknown/inconsistent price adjustment (split/spin-off jumps) | Flag large ratio jumps; avoid cross-event returns; document exclusion |
| L5 | Late-starting series and 51 interior missing bars | Keep gaps; require enough history before computing window features |
| L6 | Survivorship: universe reflects index membership around 2018 | State as limitation of every result |
| L7 | **No supported SEC to price-ticker link** | SEC used for document processing only, not as model features |
| L8 | **GDELT: empty actor/geo fields common** | Handle missing actor codes and coordinates |
| L9 | **GDELT: event date can lag file date by days** | Use SQLDATE for event date, DATEADDED for file date |
| L10 | **GDELT: 2 files have overlapping event-ID ranges** | Cross-file duplicates possible |
| L11 | **Wikinews: only 21.8% of revisions in 2013–2017** | Filter to project scope by timestamp |
| L12 | **Wikinews: deleted contributors and texts** | Handle missing contributor info and text |
| L13 | Filing availability (SEC) | Use `ACCEPTANCE-DATETIME` (ET): before 09:30 → that day's open, else next trading day |
| L14 | Container format, uuencoded binaries, EDGAR rendering | Parser splits on `<DOCUMENT>`, keeps main doc, exhibits, XBRL; skips binaries and `R*.htm` |
| L15 | 29 filings without XBRL | Mark financial facts as missing; text features remain possible |
| L16 | Multiple filers in 12 headers | Attribute filing to inventory CIK |
