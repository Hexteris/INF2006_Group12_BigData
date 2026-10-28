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
