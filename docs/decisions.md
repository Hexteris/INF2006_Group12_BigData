# Project decisions log

Each entry: what was decided, the evidence behind it, and where that evidence is.

---

## D1. SEC filings: only 11 of 311 match the price data (2026-10-28)

**Finding.** Only **11 of the 311** SEC filings belong to a company in our price data (505 tickers).
10 of those 11 were filed inside the price date range (2013-02-08 to 2017-12-29).
Each matched company has exactly one filing.

| Result | Filings |
|---|---|
| Company not in SEC's ticker map (no current ticker: bought out, renamed or delisted) | 200 |
| Company has a current ticker, but not one of our 505 price tickers | 100 |
| **Matched to a price ticker** | **11** |
| ...of which filed inside the price date range | 10 |

**The 11 matched filings**

| Company | CIK | Form | Filed | Ticker | Inside price dates? |
|---|---|---|---|---|---|
| VISA INC. | 0001403161 | 10-Q | 2013-02-06 | V | no (2 days before prices start) |
| TransDigm Group INC | 0001260221 | 10-Q | 2014-08-06 | TDG | yes |
| Prologis, Inc. | 0001045609 | 10-Q | 2014-11-05 | PLD | yes |
| HALF ROBERT INTERNATIONAL INC /DE/ | 0000315213 | 10-K | 2015-02-13 | RHI | yes |
| CITIGROUP INC | 0000831001 | 10-K | 2015-02-25 | C | yes |
| CONSOLIDATED EDISON INC | 0001047862 | 10-Q | 2015-08-06 | ED | yes |
| APPLE INC | 0000320193 | 10-K | 2015-10-28 | AAPL | yes |
| AIR PRODUCTS & CHEMICALS INC /DE/ | 0000002969 | 10-K | 2015-11-24 | APD | yes |
| MOODYS CORP /DE/ | 0001059556 | 10-Q | 2016-07-28 | MCO | yes |
| EQUINIX INC | 0001101239 | 10-K | 2017-02-27 | EQIX | yes |
| INTEL CORP | 0000050863 | 10-Q | 2017-10-26 | INTC | yes |

**How the match was made.** The SEC filings identify companies by CIK, and the price data uses tickers.
The inventory has no ticker column, so we added SEC's public CIK-to-ticker file
(`company_tickers.json`, retrieved 2026-10-28) and linked
inventory CIK → current ticker → price ticker (`scripts/acquire/check_sec_price_overlap.py`).

**Limitation.** `company_tickers.json` lists today's tickers only. A company that changed ticker after 2017
can be missed. Manual aliases could add a few matches at most; the finding would not change.
The file also changes daily: a laptop test run on the same day gave 201 / 99 / 11, because SEC had
updated the map in between. The EMR run above is the official result.

**Decision.** Under the agreed rule (≥30 matched companies → model features; 5–29 → Streaming and
analytics only; <5 → drop), 10 companies falls in the 5–29 band. SEC filings are used for
**Hadoop Streaming document processing and Task 4 analysis, not as model features**. With one filing for
each of 10 companies, a model would have almost no SEC-informed rows to learn from.

**Evidence**
- Summary: `evidence/manifests_2026-10-28/prices/v1_2013-2017/acq_2026-10-28/overlap/sec_price_overlap_summary.txt`
- Per-filing result (all 311 rows, with match status): `evidence/manifests_2026-10-28/prices/v1_2013-2017/acq_2026-10-28/overlap/sec_price_overlap.csv`
- Inputs, all pinned by sha256 in the summary file:
  - inventory `SEC_Filing_Inventory.csv` (`3227f546…`)
  - prices `all_stocks_5yr.csv` (`6aea253c…`)
  - `company_tickers.json` (`6f9cc450…`)
- Run on EMR cluster `j-05901481MKGA6MF7D2EE` at 2026-10-28T06:10:28Z.

---

## D2. Core-node storage: 100 GiB gp3 instead of 200 GiB (2026-10-28)

**Problem.** The reference configuration asks for one 200 GiB gp3 data volume per core node.
The first launch with it, `j-06143052K1KL7TE7PUW2`, failed within a minute with
"Service role EMR_DefaultRole has insufficient EC2 permissions".

**Cause, found in CloudTrail.** EMR's `RunInstances` calls were denied on `volume/*`
"with an explicit deny in a service control policy", which is an AWS Academy organisation rule that no
role or script setting can override. `ec2 run-instances --dry-run` tests from the lab role showed:
- gp3 volumes up to **101 GiB** are allowed; 102 GiB and above are denied.
- gp3 vs gp2 and the IOPS/throughput settings make no difference.
- Two 100 GiB volumes on one instance are allowed.

The dry runs used the `voclabs` role rather than `EMR_DefaultRole`. The successful relaunch with
100 GiB volumes confirms the limit applies to EMR.

**Decision.** One **100 GiB** gp3 volume per core node, keeping the reference 3,000 IOPS and 125 MiB/s.
Everything else in the reference configuration is unchanged (`config/instance-groups.json`).

**Why this is acceptable**
- **Speed is unchanged.** gp3 baseline performance (3,000 IOPS, 125 MiB/s) does not depend on volume
  size, so timing comparisons stay valid. Only capacity changes.
- **Capacity is sufficient.** HDFS has 188.87 GB configured (`evidence/cluster_verify_20261028T060200Z.txt`).
  The raw corpus uses 5.62 GB at replication 2, about 3%. The estimated peak with prepared tables, shuffle
  space and 3 timing repetitions × 2 configurations is about 20–25 GB. Measured peak use goes in report section 6.
- **Rejected alternative: two 100 GiB volumes per node.** This restores 200 GiB of capacity, but HDFS
  spreads data across both disks, giving each node about 6,000 IOPS and 250 MiB/s. That doubles the
  reference disk speed and changes the reference configuration more than this decision does.
- **Rejected alternative: a separate paid AWS account** with exactly 200 GiB, as the prof's Q&A allows.
  This is kept as a fallback if the teaching team asks for exact 200 GiB volumes.

**Status.** Pending teaching-team confirmation (draft email below).

**Evidence**
- Failed launch: `aws emr describe-cluster --cluster-id j-06143052K1KL7TE7PUW2` (state `TERMINATED_WITH_ERRORS`,
  code `VALIDATION_ERROR`). **To do in the next lab session:** save it as
  `evidence/failed_launch_j-06143052K1KL7TE7PUW2.json`. EMR keeps terminated-cluster records for about
  2 months. The lab credentials had already expired on 2026-10-28, so it could not be saved then.
- Working cluster: `evidence/cluster_j-05901481MKGA6MF7D2EE_20261028-134828.json`, `evidence/cluster_verify_20261028T060200Z.txt`.

**Draft email to the teaching team**

> Subject: INF2006 Group 12 – AWS Academy limit on 200 GiB worker volumes
>
> Hi Prof,
>
> When we launched the reference configuration in AWS Academy (emr-7.10.0, 1 primary + 2 core
> m5.xlarge, one 200 GiB gp3 volume per core node), the cluster failed with "Service role
> EMR_DefaultRole has insufficient EC2 permissions". CloudTrail shows RunInstances denied on
> volume/* by an AWS Academy service control policy. Dry-run tests in our account showed that gp3
> volumes up to 101 GiB are allowed and 102 GiB and above are denied.
>
> We are using one 100 GiB gp3 volume per core node, at the reference 3,000 IOPS and 125 MiB/s.
> gp3 performance does not depend on size, so only capacity changes. HDFS shows 188.87 GB
> configured. Our raw corpus uses 5.6 GB at replication 2, and we estimate a peak of about
> 20–25 GB. We will report measured peak use.
>
> Is this acceptable for the assessed runs, or should we use a separate AWS account to get exactly
> 200 GiB?
>
> Thank you,
> Daniel Tay (2501690), on behalf of Group 12
