# Data dictionary: Task 1 raw corpus (acquisition 2026-10-28)

Scope: **2 datasets** (daily prices, SEC filings) plus **2 reference inputs** that support the SEC
dataset (the course filing inventory, SEC's CIK-to-ticker map). Everything is described here **as stored
raw**. Cleaning and typing belong to Task 2.

All HDFS paths are under `/corpus/raw/`. All times are UTC unless marked ET (US Eastern, America/New_York).

| # | Source | Role | HDFS path | Version / pin | Native identifier |
|---|---|---|---|---|---|
| S1 | Daily price CSV | Dataset (core) | `prices/v1_2013-2017/acq_2026-10-28/all_stocks_5yr.csv` | GitHub `plotly/datasets` commit `53cd4f5c9e682f59a9b86b6229aa304e570e09dc` | `(Name, date)` |
| S2 | SEC EDGAR complete submissions | Dataset (auxiliary) | `sec_edgar/v1/acq_2026-10-28/<CIK10>_<ACCESSION>_<FORM>.txt` | Fixed course inventory v1 | `accession` |
| S3 | SEC filing inventory | Reference: defines S2 | `sec_inventory/v1/acq_2026-10-28/SEC_Filing_Inventory.csv` | Course file, sha256 `3227f546…` | `accession` |
| S4 | SEC company tickers | Reference: links S2 to S1 | `sec_company_tickers/current/acq_2026-10-28/company_tickers.json` | SEC snapshot of 2026-10-28 (changes daily) | `cik_str` |

---

## S1. Daily price CSV (`all_stocks_5yr.csv`)

- **Format:** UTF-8 CSV, comma-separated, one header line, LF line endings, no quoting. 29,580,549 bytes.
- **Grain:** one row per ticker per US trading session.
- **Coverage:** 619,040 rows, 505 tickers, 1,259 distinct trading dates, 2013-02-08 to 2018-02-07.
  - The project scope ends at **2017-12-29**: 605,910 rows fall on or before that date and 13,130 after.
  - The median ticker has 1,259 bars (the full span). The minimum is 44 (APTV, from 2017-12-05).

| Column | Raw type | Meaning | Unit | Nulls (raw) | Notes |
|---|---|---|---|---|---|
| `date` | string `YYYY-MM-DD` | Trading session date | US exchange local date (ET); no time of day | 0 | Weekdays only; exchange holidays absent |
| `open` | decimal string | First traded price of the session | USD per share | 11 | Up to 4 decimals |
| `high` | decimal string | Highest price in the session | USD per share | 8 | |
| `low` | decimal string | Lowest price in the session | USD per share | 8 | |
| `close` | decimal string | Last price of the session | USD per share | 0 | |
| `volume` | integer string | Shares traded in the session | shares | 0 | 4 rows are `0` |
| `Name` | string | Ticker symbol | none | 0 | Uses `.` for share classes, e.g. `BRK.B`; SEC uses `-` |

**Time semantics.** A bar's `open` is known at the session open of `date`; `high`, `low`, `close` and
`volume` are known only after that session closes. A feature for a prediction made at a session's
open may use only bars from earlier dates.

**Adjustment basis: not documented, and inconsistent.**
- AAPL's first open is 67.71 on 2013-02-08. That is consistent with adjustment for its June 2014
  7-for-1 split, which suggests split-adjusted prices.
- Several other events appear as raw jumps instead (see `evidence/local_checks/price_anomaly_detail.txt`):
  - DISCA and DISCK about ×0.49 on 2014-08-07.
  - EBAY ×0.43 on 2015-07-20 (PayPal spin-off).
  - NI ×0.37 on 2015-07-02 (Columbia Pipeline spin-off).
  - LNT ×2.01 on 2016-05-20, right after a row whose close is already halved.
- The dates match known splits and spin-offs, but that link has not been verified against a corporate-actions
  source. Treat prices as **source prices of unknown adjustment**, consistent with the spec's term
  "source-price ratio".

**Ticker universe.** The ticker set looks like index membership around early 2018. Several names start
late because they listed, spun off or were renamed (for example DWDP 2017-09-01, PYPL and KHC
2015-07-06), and no series ends early. Companies that left the index before 2018 are therefore absent,
which is survivorship bias in the universe.

---

## S2. SEC complete-submission files (`*.txt`)

- **Count and size:** 311 files, one per inventory row, 2,777,408,405 bytes in total.
  Sizes run from 77,364 bytes to 127,913,308 bytes, median 5,268,992.
- **File name:** `<CIK 10 digits>_<accession>_<form>.txt`, built from the inventory row.
- **Format:** EDGAR "complete submission" text. This is a plain-text SGML-like container holding one
  header and all of the filing's documents. All 311 files are valid UTF-8.

```
<SEC-DOCUMENT>{accession}.txt : {file assembly date YYYYMMDD}
<SEC-HEADER>{accession}.hdr.sgml : {date}
<ACCEPTANCE-DATETIME>{YYYYMMDDHHMMSS, ET}
ACCESSION NUMBER:            ...
CONFORMED SUBMISSION TYPE:   10-K | 10-Q
PUBLIC DOCUMENT COUNT:       n
CONFORMED PERIOD OF REPORT:  YYYYMMDD
FILED AS OF DATE:            YYYYMMDD
DATE AS OF CHANGE:           YYYYMMDD
FILER:  COMPANY DATA: COMPANY CONFORMED NAME / CENTRAL INDEX KEY / STANDARD INDUSTRIAL CLASSIFICATION /
        FISCAL YEAR END ...   (repeated per filer)
</SEC-HEADER>
<DOCUMENT> <TYPE>… <SEQUENCE>… <FILENAME>… <DESCRIPTION>… <TEXT> … </TEXT> </DOCUMENT>   (repeated)
</SEC-DOCUMENT>
```

| Header field | Meaning | Time semantics / use |
|---|---|---|
| `ACCEPTANCE-DATETIME` | When EDGAR accepted the submission | **ET wall-clock time; the earliest moment the filing was public.** Use it for information availability. |
| `FILED AS OF DATE` | Official filing date | ET business date. Matches the inventory's `filed_at` in 311/311 files. The acceptance date differs from it in 37 files: 36 were accepted after 17:00 ET and carry the next business day, and 1 (`0001185185-17-000309`) carries 2017-02-13 but was accepted 2017-02-14 08:10 ET. That one would leak a day if `filed_at` were used. |
| `CONFORMED PERIOD OF REPORT` | Fiscal period end the report covers | The figures describe this period, but they are not known until acceptance |
| `DATE AS OF CHANGE`, and the date on the `<SEC-DOCUMENT>` line | Later EDGAR record changes and file reassembly | **Can be later than filing**, e.g. 2016-04-22 for a filing made 2015-11-24. Never use these as availability dates. |
| `CENTRAL INDEX KEY` | Filer CIK | 12 files list more than one filer; the inventory CIK is among them in all 311 |
| `PUBLIC DOCUMENT COUNT` | Documents the filer submitted | Smaller than the number of `<DOCUMENT>` blocks in 166 files and never larger, consistent with EDGAR adding its own rendering pages (`R*.htm`). See `docs/corpus_inventory.md` §6. |

**Document blocks (`<TYPE>`).**
- The main `10-K` or `10-Q` document is HTML and present in 311/311 files.
- Exhibits are `EX-*`.
- XBRL is present in 282 files: 281 have an instance document (`EX-101.INS`) plus schema and linkbases (`EX-101.SCH/CAL/DEF/LAB/PRE`), and 1 has inline XBRL. **29 files have no XBRL.**
- EDGAR adds rendering pages `R*.htm`, `FilingSummary.xml` and `Financial_Report.xlsx`.
- Binary attachments (`GRAPHIC`, `ZIP`, `EXCEL`, PDF) are uuencoded inside the text and occur in 292 files.

**Units.** Text has none. XBRL facts carry their own `unitRef` (e.g. `usd`, `shares`, `usdPerShare`)
and `decimals`, which Task 2 must read per fact.

---

## S3. SEC filing inventory (`SEC_Filing_Inventory.csv`)

- **Format:** UTF-8 CSV, header on row 1, 311 data rows, 51,751 bytes.
  It is the CSV twin of the course's official `SEC_Filing_Inventory(SEC filings).xlsx`.
- **Equivalence with the .xlsx:** a cross-check found 0 differences across all 311 accessions and all
  fields (`evidence/inventory_crosscheck/`). The CSV was used because its header is on row 1, its dates
  are already ISO and its file name has no spaces.
- **Coverage:** 311 filings from 303 issuers. 156 are 10-K and 155 are 10-Q. Filing dates run from 2013-01-14 to 2017-12-20, with 63, 62, 62, 62 and 62 filings per year for 2013–2017.

| Column | Raw type | Meaning | Notes |
|---|---|---|---|
| `cik` | string, 10 digits | SEC Central Index Key of the issuer | **Keep as text; leading zeros are significant.** 303 distinct |
| `company_name` | string | Issuer name as SEC records it | Not a join key: spelling differs from other sources |
| `form` | string | `10-K` (annual) or `10-Q` (quarterly) | Original forms only, no amendments |
| `filed_at` | string `YYYY-MM-DD` | EDGAR filing date | ET business date (see S2 for the acceptance time) |
| `accession` | string `##########-YY-######` | Unique filing ID | Unique in all 311 rows. The first 10 digits are the submitting agent's CIK, which is not necessarily the issuer. |
| `document_kind` | string | Always `complete_submission` | |
| `url` | string | Download URL `https://www.sec.gov/Archives/edgar/data/<cik without zeros>/<accession>.txt` | URL CIK and accession agree with the row in 311/311 |

---

## S4. SEC company tickers (`company_tickers.json`)

- **Format:** JSON object whose keys are `"0"`, `"1"`, ...; each value is `{cik_str, ticker, title}`.
  798,155 bytes, 8,024 distinct CIKs.
- **Meaning:** SEC's **current** mapping from CIK to exchange ticker for companies listed today. It is
  not historical: companies acquired, renamed or delisted since 2017 are missing or carry today's ticker.
  SEC republishes it daily, so only the stored copy (sha256 `6f9cc450…`) reproduces our match.

| Field | Raw type | Meaning | Notes |
|---|---|---|---|
| `cik_str` | integer | CIK without leading zeros | Zero-pad to 10 characters before joining to S3 |
| `ticker` | string | Current ticker | Uses `-` for share classes (`BRK-B`). One CIK can have several tickers. |
| `title` | string | Company name | |

---

## Relationships between the sources

```
S3 inventory.accession ──1:1──  S2 file   (name part + header ACCESSION NUMBER; verified 311/311)
S3 inventory.cik       ──∈────  S2 header CENTRAL INDEX KEY(s)          (verified 311/311)
S3 inventory.cik  ──zero-pad──  S4 cik_str ──1:n──> S4 ticker (current)
S4 ticker ──'-'→'.'──  S1 Name                                           (11 filings match; 10 inside price dates)
S2 ACCEPTANCE-DATETIME (ET)  ──>  first S1 trading date the filing can inform
```

- **Supported link (S2/S3 → S1):** CIK → current ticker → price ticker. Only 11 of 311 filings
  (11 issuers) link to a price series. See `docs/decisions.md` D1 for the list and the limitation.
  Company names are not used as join keys.
- **Availability rule for Task 2:**
  - If the acceptance time is **before 09:30 ET**, the filing can inform that day's session open.
  - If it is **after the open**, it can only inform the next trading day.
  - Acceptance times: 31 before 09:30 ET, 119 during the session, 161 at or after 16:00 ET.

  Never use `filed_at`, `DATE AS OF CHANGE` or the file assembly date as the availability time.
- **Unrelated by design:** S1 has no CIK, and S4 has no history. No other joins are supported.
