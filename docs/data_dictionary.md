# Data dictionary: Task 1 raw corpus (acquisition 2026-10-28/29)

Scope: **5 datasets** (daily prices, SEC EDGAR filings, SEC filing inventory, GDELT events, Wikinews revisions). Everything is described here **as stored raw**. Cleaning and typing belong to Task 2.

All HDFS paths are under `/corpus/raw/`. All times are UTC unless marked ET (US Eastern, America/New_York).

| # | Source | Role | HDFS path | Version / pin | Native identifier |
|---|---|---|---|---|---|
| S1 | Daily price CSV | Dataset (core) | `prices/v1_2013-2017/acq_2026-10-28/all_stocks_5yr.csv` | GitHub `plotly/datasets` commit `53cd4f5c9e682f59a9b86b6229aa304e570e09dc` | `(Name, date)` |
| S2 | SEC EDGAR complete submissions | Dataset (auxiliary) | `sec_edgar/v1/acq_2026-10-28/<CIK10>_<ACCESSION>_<FORM>.txt` | Fixed course inventory v1 | `accession` |
| S3 | SEC filing inventory | Reference: defines S2 | `sec_inventory/v1/acq_2026-10-28/SEC_Filing_Inventory.csv` | Course file, sha256 `3227f546…` | `accession` |
| S4 | GDELT Event Database | Dataset (auxiliary) | `gdelt/v1_events_2015-2017/acq_2026-10-29/*.zip` | GDELT 1.0 codebook, 2015-07-01 to 2017-06-30 | `GLOBALEVENTID` |
| S5 | Wikinews meta-history | Dataset (auxiliary) | `wikinews/20261001_meta-history/acq_2026-10-29/*.bz2` | MediaWiki export 0.11, dump date 2026-10-01 | `page_id`, `rev_id` |

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
| `Name` | string | Ticker symbol | none | 0 | Uses `.` for share classes, e.g. `BRK.B` |

**Time semantics.** A bar's `open` is known at the session open of `date`; `high`, `low`, `close` and `volume` are known only after that session closes. A feature for a prediction made at a session's open may use only bars from earlier dates.

**Adjustment basis: not documented, and inconsistent.**
- AAPL's first open is 67.71 on 2013-02-08. That is consistent with adjustment for its June 2014 7-for-1 split.
- Several other events appear as raw jumps: DISCA and DISCK ×0.49 on 2014-08-07, EBAY ×0.43 on 2015-07-20, NI ×0.37 on 2015-07-02, LNT ×2.01 on 2016-05-20.
- The dates match known splits and spin-offs. Treat prices as **source prices of unknown adjustment**, consistent with the spec's term "source-price ratio".

**Ticker universe.** The ticker set looks like index membership around early 2018. Several names start late because they listed, spun off or were renamed (DWDP 2017-09-01, PYPL and KHC 2015-07-06). Companies that left the index before 2018 are absent (survivorship bias).

---

## S2. SEC complete-submission files (`*.txt`)

- **Count and size:** 311 files, one per inventory row, 2,777,408,405 bytes total. Sizes: 77,364 to 127,913,308 bytes, median 5,268,992.
- **File name:** `<CIK 10 digits>_<accession>_<form>.txt`, built from the inventory row.
- **Format:** EDGAR "complete submission" text. Plain-text SGML-like container holding one header and all filing documents. All 311 files are valid UTF-8.

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
| `ACCEPTANCE-DATETIME` | When EDGAR accepted the submission | **ET wall-clock time; earliest moment filing was public.** Use for information availability. |
| `FILED AS OF DATE` | Official filing date | ET business date. Matches inventory `filed_at` in 311/311 files. |
| `CONFORMED PERIOD OF REPORT` | Fiscal period end the report covers | Figures describe this period, not known until acceptance |
| `DATE AS OF CHANGE`, file assembly date | Later EDGAR record changes | **Can be later than filing.** Never use as availability dates. |
| `CENTRAL INDEX KEY` | Filer CIK | 12 files list multiple filers; inventory CIK is among them in all 311 |
| `PUBLIC DOCUMENT COUNT` | Documents filer submitted | Smaller than `<DOCUMENT>` blocks in 166 files (EDGAR adds rendering pages) |

**Document blocks (`<TYPE>`).**
- Main `10-K` or `10-Q` document is HTML and present in 311/311 files.
- Exhibits are `EX-*`.
- XBRL in 282 files: 281 have instance document (`EX-101.INS`), 1 has inline XBRL. **29 files have no XBRL.**
- EDGAR adds rendering pages `R*.htm`, `FilingSummary.xml`, `Financial_Report.xlsx`.
- Binary attachments (`GRAPHIC`, `ZIP`, `EXCEL`, PDF) are uuencoded inside text in 292 files.

**Units.** Text has none. XBRL facts carry their own `unitRef` (e.g. `usd`, `shares`, `usdPerShare`).

---

## S3. SEC filing inventory (`SEC_Filing_Inventory.csv`)

- **Format:** UTF-8 CSV, header on row 1, 311 data rows, 51,751 bytes. CSV twin of official `.xlsx`.
- **Coverage:** 311 filings from 303 issuers. 156 are 10-K, 155 are 10-Q. Filed 2013-01-14 to 2017-12-20.

| Column | Raw type | Meaning | Notes |
|---|---|---|---|
| `cik` | string, 10 digits | SEC Central Index Key of issuer | **Keep as text; leading zeros significant.** 303 distinct |
| `company_name` | string | Issuer name as SEC records it | Not a join key: spelling differs |
| `form` | string | `10-K` (annual) or `10-Q` (quarterly) | Original forms only, no amendments |
| `filed_at` | string `YYYY-MM-DD` | EDGAR filing date | ET business date |
| `accession` | string `##########-YY-######` | Unique filing ID | Unique in 311 rows |
| `document_kind` | string | Always `complete_submission` | |
| `url` | string | Download URL `https://www.sec.gov/Archives/edgar/data/<cik without zeros>/<accession>.txt` | URL matches row in 311/311 |

---

## S4. GDELT Event Database (`.zip` files)

- **Count and size:** 731 files, one per day, 2015-07-01 to 2017-06-30, none missing. 9,611,684,528 bytes zipped, 54,706,262,609 bytes expanded.
- **Format:** ZIP archives, each containing exactly one CSV with no header line, tab-separated, UTF-8, 58 columns.
- **Column names and order** from GDELT 1.0 codebook (not present in files):

| Col | Name | Col | Name | Col | Name |
|---|---|---|---|---|---|
| 0 | `GLOBALEVENTID` | 20 | `Actor2Type3Code` | 40 | `Actor2Geo_ADM1Code` |
| 1 | `SQLDATE` | 21 | `Actor2Group1Code` | 41 | `Actor2Geo_Lat` |
| 2 | `MonthYear` | 22 | `Actor2Group2Code` | 42 | `Actor2Geo_Long` |
| 3 | `Year` | 23 | `Actor2Group3Code` | 43 | `Actor2Geo_FeatureID` |
| 4 | `FractionDate` | 24 | `Actor2EthnicCode` | 44 | `ActionGeo_Type` |
| 5 | `Actor1Code` | 25 | `IsRootEvent` | 45 | `ActionGeo_FullName` |
| 6 | `Actor1Name` | 26 | `EventCode` | 46 | `ActionGeo_CountryCode` |
| 7 | `Actor1CountryCode` | 27 | `EventBaseCode` | 47 | `ActionGeo_ADM1Code` |
| 8 | `Actor1KnownGroupCode` | 28 | `EventRootCode` | 48 | `ActionGeo_Lat` |
| 9 | `Actor1EthnicCode` | 29 | `QuadClass` | 49 | `ActionGeo_Long` |
| 10 | `Actor1Religion1Code` | 30 | `GoldsteinScale` | 50 | `ActionGeo_FeatureID` |
| 11 | `Actor1Religion2Code` | 31 | `NumMentions` | 51 | `ActionGeo_CountryCode` |
| 12 | `Actor1Type1Code` | 32 | `NumSources` | 52 | `DATEADDED` |
| 13 | `Actor1Type2Code` | 33 | `NumArticles` | 53 | `ActionGeo_Lat` |
| 14 | `Actor1Type3Code` | 34 | `AvgTone` | 54 | `ActionGeo_Long` |
| 15 | `Actor2Code` | 35 | `Actor1Geo_Type` | 55 | `SOURCEURL` |
| 16 | `Actor2Name` | 36 | `Actor1Geo_FullName` | 56 | `ActionGeo_Lat` |
| 17 | `Actor2CountryCode` | 37 | `Actor1Geo_CountryCode` | 57 | `ActionGeo_Long` |
| 18 | `Actor2KnownGroupCode` | 38 | `Actor1Geo_ADM1Code` | (Note: columns 53–57 are repeated in codebook) |
| 19 | `Actor2EthnicCode` | 39 | `Actor1Geo_Lat` | |

**Time semantics.**
- `SQLDATE`: event date (YYYYMMDD). 96.8% of events have SQLDATE equal to file date.
- `DATEADDED`: file date (YYYYMMDD). Verified equal to file date for all 144,207,180 lines.
- `Year`: equals SQLDATE year for every line.

**Quality notes.**
- All 144,207,180 lines have 58 fields.
- Empty fields: `Actor1Code` 13,164,100 (9.1%), `Actor2Code` 38,493,957 (26.7%), `ActionGeo_CountryCode` 3,567,789 (2.5%).
- `EventCode`, `SOURCEURL`, `AvgTone` never empty.
- 689 lines have non-numeric Goldstein/NumMentions/AvgTone.
- 17,651 lines have out-of-range ActionGeo coordinates.

---

## S5. Wikinews meta-history (`.bz2` files)

- **Count and size:** 2 files, 662,002,512 bytes compressed. File 1: 482,143,381 B (142,719 pages, 1,500,001 revisions). File 2: 179,859,131 B (123,181 pages, 454,229 revisions).
- **Format:** MediaWiki export 0.11 XML, BZ2-compressed. One `<page>` per article, containing `<revision>` elements.
- **Structure:**
```
<mediawiki xmlns="..." version="0.11">
  <siteinfo><dbname>enwikinews</dbname><generator>MediaWiki 1.47.0-wmf.22</generator></siteinfo>
  <page>
    <title>Article title</title>
    <ns>0</ns>                    <!-- namespace 0 = articles -->
    <id>12345</id>               <!-- page_id -->
    <revision>
      <id>67890</id>             <!-- rev_id -->
      <parentid>67889</parentid>
      <timestamp>2015-03-15T14:30:00Z</timestamp>
      <contributor>
        <username>User</username>
        <id>54321</id>
      </contributor>
      <comment>edit summary</comment>
      <text bytes="1234" sha1="...">Article text</text>
      <sha1>...</sha1>
    </revision>
    <!-- more revisions -->
  </page>
</mediawiki>
```

| Element | Meaning | Notes |
|---|---|---|
| `<title>` | Article title | Unicode, may contain colons for namespace prefixes |
| `<ns>` | Namespace integer | 0 = articles, 1 = talk, 2 = user, 3 = user talk, 4 = project, 10 = template, etc. |
| `<id>` (page) | Page identifier | 265,900 distinct pages |
| `<id>` (revision) | Revision identifier | 1,954,230 distinct revisions |
| `<timestamp>` | Revision timestamp | UTC, format `YYYY-MM-DDTHH:MM:SSZ` |
| `<contributor>` | Editor | Can be `<username>`+`<id>`, `<ip>`, or `deleted="deleted"` |
| `<text>` | Article text | Attributes: `bytes` (size), `sha1` (hash), can be `deleted="deleted"` |

**Coverage.**
- Timestamps: 2004-11-08T08:11:42Z to 2026-09-30T13:50:02Z.
- 2013–2017: 426,482 revisions (21.8% of all). 90,053 are article (ns0) revisions covering 16,340 articles.
- Contributor deleted: 543 revisions. Text deleted: 2,531 revisions. Empty text: 5,392 revisions.

---

## Relationships between the sources

```
S3 inventory.accession ──1:1──  S2 file   (verified 311/311)
S3 inventory.cik       ──∈────  S2 header CENTRAL INDEX KEY(s)  (verified 311/311)

S2 ACCEPTANCE-DATETIME (ET)  ──>  first S1 trading date the filing can inform
S1 date ──by date only──  S4 SQLDATE     (505 trading days have a GDELT file)
S1 date ──by date only──  S5 timestamp   (revision timestamps vs trading dates)
```

**Supported links:**
- **SEC inventory to EDGAR files:** by accession and CIK (311/311).
- **SEC to prices:** not supported (no ticker map in corpus).
- **Prices to GDELT/Wikinews:** by date only (no ticker in GDELT/Wikinews).

**Availability rules:**
- **SEC filings:** Use `ACCEPTANCE-DATETIME` (ET). Before 09:30 ET → informs that day's open. After open → informs next trading day. 31 filings accepted before 09:30 ET, 119 during session, 161 at/after 16:00 ET.
- **GDELT:** Use `SQLDATE` as event date. `DATEADDED` equals file date.
- **Wikinews:** Use `<timestamp>` (UTC) as revision time.

**Cross-source coverage:**
- 505 trading days have a GDELT file (2013-02-08 to 2017-12-29).
- SEC filings span 2013-01-14 to 2017-12-20.
- GDELT spans 2015-07-01 to 2017-06-30.
- Wikinews revisions span 2004-11-08 to 2026-09-30.
