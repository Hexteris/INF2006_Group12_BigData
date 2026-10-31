-- ============================================================
-- Output locations:
-- /corpus/cleaned/prices/
-- /corpus/rejected/prices/
-- /corpus/out_of_scope/prices/
--
-- Raw input is read-only. Original values are preserved in
-- rejected_prices and out_of_scope_prices.
--
-- IMPORTANT:
-- First execution requires fresh output locations.
-- These are managed Hive tables; dropping them can delete data.
-- ============================================================

USE g12;

-- ------------------------------------------------------------
-- 1. Parse raw strings without modifying the source table
-- ------------------------------------------------------------

CREATE TEMPORARY VIEW price_validation AS
SELECT
    `date`,
    `open`,
    `high`,
    `low`,
    `close`,
    `volume`,
    `name`,

    trade_date,
    open_value,
    high_value,
    low_value,
    close_value,
    volume_value,

    YEAR(trade_date) AS year,

    CASE
        WHEN COALESCE(TRIM(`date`), '') = ''
          OR COALESCE(TRIM(`open`), '') = ''
          OR COALESCE(TRIM(`high`), '') = ''
          OR COALESCE(TRIM(`low`), '') = ''
          OR COALESCE(TRIM(`close`), '') = ''
          OR COALESCE(TRIM(`volume`), '') = ''
          OR COALESCE(TRIM(`name`), '') = ''
            THEN 'null_field'

        WHEN high_value < low_value
            THEN 'high_lt_low'

        WHEN NOT (open_value BETWEEN low_value AND high_value)
            THEN 'open_outside_range'

        WHEN NOT (close_value BETWEEN low_value AND high_value)
            THEN 'close_outside_range'

        WHEN volume_value = 0
            THEN 'zero_volume'

        ELSE NULL
    END AS reject_reason

FROM (
    SELECT
        `date`,
        `open`,
        `high`,
        `low`,
        `close`,
        `volume`,
        `name`,

        CAST(TRIM(`date`) AS DATE) AS trade_date,
        CAST(TRIM(`open`) AS DOUBLE) AS open_value,
        CAST(TRIM(`high`) AS DOUBLE) AS high_value,
        CAST(TRIM(`low`) AS DOUBLE) AS low_value,
        CAST(TRIM(`close`) AS DOUBLE) AS close_value,
        CAST(TRIM(`volume`) AS BIGINT) AS volume_value

    FROM raw_prices
) parsed;


-- ------------------------------------------------------------
-- 1A. Stop if non-empty source values cannot be parsed.
-- These are unexpected source-data issues, not new reject reasons.

SELECT assert_true(COUNT(*) = 0)
FROM price_validation
WHERE
    (COALESCE(TRIM(`date`), '') <> '' AND trade_date IS NULL)
    OR (COALESCE(TRIM(`open`), '') <> '' AND open_value IS NULL)
    OR (COALESCE(TRIM(`high`), '') <> '' AND high_value IS NULL)
    OR (COALESCE(TRIM(`low`), '') <> '' AND low_value IS NULL)
    OR (COALESCE(TRIM(`close`), '') <> '' AND close_value IS NULL)
    OR (COALESCE(TRIM(`volume`), '') <> '' AND volume_value IS NULL);


-- 2. Create processed output tables.
-- ------------------------------------------------------------

CREATE TABLE IF NOT EXISTS g12.cleaned_prices (
    ticker STRING,
    trade_date DATE,
    open DOUBLE,
    high DOUBLE,
    low DOUBLE,
    close DOUBLE,
    volume BIGINT
)
PARTITIONED BY (year INT)
STORED AS PARQUET
LOCATION '/corpus/cleaned/prices/';


CREATE TABLE IF NOT EXISTS g12.rejected_prices (
    `date` STRING,
    `open` STRING,
    `high` STRING,
    `low` STRING,
    `close` STRING,
    `volume` STRING,
    `name` STRING,
    reject_reason STRING
)
STORED AS PARQUET
LOCATION '/corpus/rejected/prices/';


CREATE TABLE IF NOT EXISTS g12.out_of_scope_prices (
    `date` STRING,
    `open` STRING,
    `high` STRING,
    `low` STRING,
    `close` STRING,
    `volume` STRING,
    `name` STRING
)
STORED AS PARQUET
LOCATION '/corpus/out_of_scope/prices/';


-- ------------------------------------------------------------
-- 3. Store records after 2017-12-29 separately.
-- ------------------------------------------------------------

INSERT OVERWRITE TABLE g12.out_of_scope_prices
SELECT
    `date`,
    `open`,
    `high`,
    `low`,
    `close`,
    `volume`,
    `name`
FROM price_validation
WHERE trade_date > CAST('2017-12-29' AS DATE);


-- ------------------------------------------------------------
-- 4. Store invalid in-scope records with their rejection reason
-- ------------------------------------------------------------

INSERT OVERWRITE TABLE g12.rejected_prices
SELECT
    `date`,
    `open`,
    `high`,
    `low`,
    `close`,
    `volume`,
    `name`,
    reject_reason
FROM price_validation
WHERE (
        trade_date <= CAST('2017-12-29' AS DATE)
        OR (
            trade_date IS NULL
            AND COALESCE(TRIM(`date`), '') = ''
        )
    )
  AND reject_reason IS NOT NULL;


-- ------------------------------------------------------------
-- 5. Store accepted records as Parquet n partitioned by year
-- ------------------------------------------------------------

SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;

INSERT OVERWRITE TABLE g12.cleaned_prices
PARTITION (year)
SELECT
    name,
    trade_date,
    open_value,
    high_value,
    low_value,
    close_value,
    volume_value,
    year
FROM price_validation
WHERE trade_date <= CAST('2017-12-29' AS DATE)
  AND reject_reason IS NULL;


-- ------------------------------------------------------------
-- 6. Display output counts for verify
-- ------------------------------------------------------------

SELECT 'cleaned_prices' AS table_name, COUNT(*) AS row_count
FROM g12.cleaned_prices;

SELECT 'rejected_prices' AS table_name, COUNT(*) AS row_count
FROM g12.rejected_prices;

SELECT 'out_of_scope_prices' AS table_name, COUNT(*) AS row_count
FROM g12.out_of_scope_prices;