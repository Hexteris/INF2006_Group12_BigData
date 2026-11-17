# Project decisions log

Each entry: what was decided, the evidence behind it, and where that evidence is.

---

## D1. Corpus scope: 5 sources, no ticker map (2026-11-01)

**Decision.** The raw corpus comprises **5 sources**: daily prices (S1), SEC EDGAR filings (S2), SEC filing inventory (S3), GDELT events (S4), and Wikinews revisions (S5). The SEC company ticker map, which was part of an earlier acquisition, is **not included** in the assessed corpus. SEC filings are used for document processing only (Task 1 corpus auditing); there is no supported SEC-to-price-ticker link. The scope for Tasks 2–4 is narrowed further in D6.

**Background.** An earlier acquisition (2026-10-28) included SEC's `company_tickers.json` and found that only 11 of 311 filings matched the price tickers, with 10 inside the price date range. With one filing for each of 10 companies, a model would have almost no SEC-informed rows to learn from. That finding is historical; the current corpus scope follows the latest project spec of 5 sources.

**Evidence**
- Inventory table: `docs/corpus_inventory.md` §1
- Relationships: `docs/data_dictionary.md` §Relationships ("SEC to prices: not supported")
- Earlier overlap check (2026-10-28): 11 of 311 filings matched a price ticker; the evidence file was retired with the 2026-10-28 manifests.
- Notebook: `notebooks/INF2006_G12_task1.ipynb` profiles 5 sources only

---

## D2. Core-node storage: 100 GiB gp3 matches spec (2026-10-28 → 2026-11-01)

**History.** The first launch with 200 GiB gp3 volumes failed with "Service role EMR_DefaultRole has insufficient EC2 permissions". AWS Academy organisation policy limits gp3 volumes to 101 GiB.

**Original decision (2026-10-28).** Use 100 GiB gp3 per core node, keeping 3,000 IOPS and 125 MiB/s. This was documented as a deviation from the spec's 200 GiB.

**Update (2026-11-01).** The latest project spec states **100 GiB per core node**, so this is no longer a deviation. The configuration matches the spec.

**Why this is acceptable**
- **Speed unchanged.** gp3 baseline performance (3,000 IOPS, 125 MiB/s) does not depend on volume size.
- **Capacity sufficient.** HDFS has 188.87 GB configured. Raw corpus uses 13.08 GB (26.16 GB at replication 2), about 14% of capacity.
- **Rejected alternative: two 100 GiB volumes per node.** Would double disk speed, changing the reference configuration more than necessary.

**Evidence**
- Failed launch `j-06143052K1KL7TE7PUW2`: state `TERMINATED_WITH_ERRORS`, code `VALIDATION_ERROR`
- Working cluster: `evidence/task1/cluster/cluster_j-00703753JKXXC2HG7Y9X_20261101-190507.json`
- Config: `config/instance-groups.json`

---

## D3. Driver memory: 1000M instead of 2g (JupyterHub default) (2026-11-01)

**Finding.** The notebook session ran with `spark.driver.memory = 1000M` (the JupyterHub/Livy default) rather than the spec's reference 2g. The executors had the reference 6g + 2g overhead.

**Impact.** The driver hosts the SparkContext and holds metadata (accumulators, broadcast variables). For this profiling workload (read-only scans, no shuffle), 1000M was sufficient: no out-of-memory errors, all tasks completed.

**Decision.** Accept the default for Task 1. **Tasks 2–4 must set `%%configure -f {"driverMemory": "2g"}` as their first cell** to match the spec's reference configuration for pipeline runs.

**Evidence**
- Configuration: `evidence/task1/run_20261101T112713Z/_run/00_cluster_config/part-*.json` (`driverMemory: 1000M`)
- Successful run: all profiling scans completed (SEC 18.4 s, GDELT 349.8 s, Wikinews 648.6 s)

---

## D4. Wikinews mirror fallback (2026-10-29)

**Finding.** The primary Wikimedia dump host `dumps.wikimedia.org` returned HTTP 404 for the two Wikinews files four times (2026-10-29 08:34:08–08:34:16Z). The mirror `mirror.accum.se` served both files successfully (08:35:47–08:36:23Z).

**Publisher checksum verification.** Both files matched the published sha1 values from the Wikimedia dump index.

**Decision.** Use the mirror as a fallback when the primary host is unavailable. The bytes are identical (sha1 match), so provenance is maintained.

**Evidence**
- Download manifest: `evidence/task1/run_20261101T112713Z/_run/01_manifests_copied/part-*.json`
- Publisher checksums: `evidence/task1/run_20261101T112713Z/_run/07_publisher_checksums/part-*.json`

---

## D5. GDELT "accepted event" = every well-formed 58-field line (2026-11-01)

**Finding.** The spec states "144.2 million accepted events". Our scan counted **144,207,180 lines**, all with 58 fields (+7,180 or +0.005%).

**Alternative count.** Counting only lines with `SQLDATE` inside July 2015 – June 2017 gives 143,327,058, which does not match the spec.

**Decision.** Define "accepted event" as **every line that parses with 58 fields**, regardless of `SQLDATE`. This matches the spec's count within 0.005% and is reproducible: it requires only field-count validation, not date-range filtering.

**Quality notes.**
- 0 bad event IDs, 0 within-file duplicate IDs, 0 invalid SQLDATE, 0 non-UTF-8 lines.
- Empty fields are common: `Actor1Code` 9.1%, `Actor2Code` 26.7%, `ActionGeo_CountryCode` 2.5%.
- `DATEADDED` equals file date for every line.

**Evidence**
- Reconciliation: `evidence/task1/run_20261101T112713Z/gdelt/07_reconciliation_to_spec/part-*.json`
- Quality counts: `evidence/task1/run_20261101T112713Z/gdelt/03_quality_counts/part-*.json`
- Date lag: `evidence/task1/run_20261101T112713Z/gdelt/04_event_date_lag/part-*.json`

---

## D6. Scope for Tasks 2–4: prices + GDELT; SEC EDGAR not used after Task 1 (2026-11-11)

**Decision.** Supersedes Step 0.2 in `docs/project_steps.md` ("Agree team scope: prices + SEC EDGAR. Skip GDELT and Wikinews."). The team's final scope for Tasks 2–4 is **prices + GDELT only**. SEC EDGAR filings remain in the Task 1 raw corpus and Hadoop Streaming audit, but are not used in Task 2 cleaning, Task 3 model features, or Task 4 analysis. Wikinews remains in the Task 1 corpus only.

**Reason.** D1 found only 11 of 311 SEC filings match a price ticker (10 inside the price date range), which is too sparse to support a sentiment or news-event question. GDELT's date-aligned daily event data was adopted instead for Task 4's news-vs-price-behaviour question.

**Status.** `docs/project_steps.md` Step 0.2 should point to this entry.

**Evidence.** D1 above; `README (Task 2).md`; `sql/README.md`; INF2006 Big Data Project_P2_Group12 (Google Doc), §1–3.
