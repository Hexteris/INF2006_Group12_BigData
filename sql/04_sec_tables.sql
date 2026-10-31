
-- Create the SEC tables and link matched filings to stock prices.
-- The input paths and metadata columns should be checked against
-- the actual Hadoop Streaming output before running this script.

USE g12;

-- 1. Store the SEC filing information from the TSV output

CREATE EXTERNAL TABLE IF NOT EXISTS g12.sec_filings (
    cik           STRING,
    accession     STRING,
    form          STRING,
    filing_date   STRING,
    acceptance_et STRING,
    hdr_period    STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '${hivevar:SEC_PARSED_PATH}';


-- 2. Load the SEC filing and stock ticker matching results

CREATE EXTERNAL TABLE IF NOT EXISTS g12.sec_price_overlap (
    source_row            STRING,
    cik                   STRING,
    issuer                STRING,
    form                  STRING,
    filing_date           STRING,
    accession             STRING,
    sec_current_tickers   STRING,
    price_ticker          STRING,
    all_price_matches     STRING,
    match_method          STRING,
    price_first_date      STRING,
    price_last_date       STRING,
    filing_in_price_range STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    'separatorChar' = ',',
    'quoteChar' = '"',
    'escapeChar' = '\\'
)
STORED AS TEXTFILE
LOCATION '${hivevar:SEC_OVERLAP_PATH}'
TBLPROPERTIES (
    'skip.header.line.count' = '1'
);


-- 3. Create a table to store the SEC filing and price links

CREATE TABLE IF NOT EXISTS g12.sec_price_link (
    cik            STRING,
    accession      STRING,
    ticker         STRING,
    available_from DATE
)
STORED AS PARQUET;


-- 4. Work out when each SEC filing becomes available for price analysis.
-- Filings accepted before 09:30 ET use the same trading day.
-- Filings accepted at or after 09:30 ET use the next trading day.
--
-- The Visa filing was accepted on 6 February 2013 at 16:17:56 ET.
-- Its next trading day was 7 February 2013, which is before the
-- price dataset starts on 8 February 2013. This date is handled
-- separately because it is not in the available trading calendar.

WITH matched_filings AS (
    SELECT
        f.cik,
        f.accession,
        o.price_ticker AS ticker,
        CAST(
            REPLACE(f.acceptance_et, 'T', ' ')
            AS TIMESTAMP
        ) AS acceptance_ts,
        TO_DATE(
            CAST(
                REPLACE(f.acceptance_et, 'T', ' ')
                AS TIMESTAMP
            )
        ) AS acceptance_date
    FROM g12.sec_filings f
    INNER JOIN g12.sec_price_overlap o
        ON f.accession = o.accession
    WHERE o.match_method = 'company_tickers'
      AND COALESCE(o.price_ticker, '') <> ''
),
trading_calendar AS (
    SELECT DISTINCT trade_date
    FROM g12.cleaned_prices
),
availability AS (
    SELECT
        m.cik,
        m.accession,
        m.ticker,
        CASE
            WHEN m.acceptance_ts IS NULL THEN NULL

            WHEN DATE_FORMAT(m.acceptance_ts, 'HH:mm:ss') < '09:30:00'
                THEN MIN(
                    CASE
                        WHEN c.trade_date >= m.acceptance_date
                        THEN c.trade_date
                    END
                )

            ELSE COALESCE(
                MIN(
                    CASE
                        WHEN c.trade_date > m.acceptance_date
                        THEN c.trade_date
                    END
                ),
                CASE
                    WHEN m.accession = '0001384108-13-000004'
                    THEN CAST('2013-02-07' AS DATE)
                END
            )
        END AS available_from
    FROM matched_filings m
    LEFT JOIN trading_calendar c
        ON c.trade_date >= m.acceptance_date
    GROUP BY
        m.cik,
        m.accession,
        m.ticker,
        m.acceptance_ts,
        m.acceptance_date
)

INSERT OVERWRITE TABLE g12.sec_price_link
SELECT
    cik,
    accession,
    ticker,
    available_from
FROM availability
WHERE available_from IS NOT NULL;


-- 5. Check the number of SEC filings and generated price links

SELECT COUNT(*) AS sec_filing_count
FROM g12.sec_filings;

SELECT COUNT(*) AS sec_price_link_count
FROM g12.sec_price_link;

SELECT
    COUNT(*) AS links_within_price_period
FROM g12.sec_price_link
WHERE available_from BETWEEN
    (SELECT MIN(trade_date) FROM g12.cleaned_prices)
    AND
    (SELECT MAX(trade_date) FROM g12.cleaned_prices);
