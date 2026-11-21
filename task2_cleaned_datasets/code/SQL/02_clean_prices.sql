USE g12;

CREATE VIEW price_parsed AS
SELECT
    `date`, `open`, `high`, `low`, `close`, `volume`, `name`,
    CAST(TRIM(`date`) AS DATE) AS trade_date,
    CAST(TRIM(`open`) AS DOUBLE) AS open_value,
    CAST(TRIM(`high`) AS DOUBLE) AS high_value,
    CAST(TRIM(`low`) AS DOUBLE) AS low_value,
    CAST(TRIM(`close`) AS DOUBLE) AS close_value,
    CAST(TRIM(`volume`) AS BIGINT) AS volume_value
FROM raw_prices;

CREATE VIEW price_validation AS
SELECT *,
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
        WHEN trade_date IS NULL THEN 'invalid_date'
        WHEN open_value IS NULL THEN 'invalid_open'
        WHEN high_value IS NULL THEN 'invalid_high'
        WHEN low_value IS NULL THEN 'invalid_low'
        WHEN close_value IS NULL THEN 'invalid_close'
        WHEN volume_value IS NULL THEN 'invalid_volume'
        WHEN open_value <= 0 OR high_value <= 0
          OR low_value <= 0 OR close_value <= 0
            THEN 'non_positive_price'
        WHEN volume_value <= 0 THEN 'invalid_volume'
        WHEN high_value < low_value THEN 'high_lt_low'
        WHEN open_value < low_value OR open_value > high_value
            THEN 'open_outside_range'
        WHEN close_value < low_value OR close_value > high_value
            THEN 'close_outside_range'
        ELSE NULL
    END AS reject_reason
FROM price_parsed;

CREATE TABLE cleaned_prices (
    ticker STRING,
    trade_date DATE,
    open_price DOUBLE,
    high_price DOUBLE,
    low_price DOUBLE,
    close_price DOUBLE,
    volume BIGINT
)
PARTITIONED BY (year INT)
STORED AS PARQUET
LOCATION '/corpus/cleaned/prices/';

CREATE TABLE rejected_prices (
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

CREATE TABLE out_of_scope_prices (
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

INSERT OVERWRITE TABLE out_of_scope_prices
SELECT `date`, `open`, `high`, `low`, `close`, `volume`, `name`
FROM price_validation
WHERE trade_date > CAST('2017-12-31' AS DATE);

INSERT OVERWRITE TABLE rejected_prices
SELECT `date`, `open`, `high`, `low`, `close`, `volume`, `name`, reject_reason
FROM price_validation
WHERE reject_reason IS NOT NULL
  AND (trade_date IS NULL OR trade_date <= CAST('2017-12-31' AS DATE));

SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;

INSERT OVERWRITE TABLE cleaned_prices PARTITION (year)
SELECT
    name, trade_date, open_value, high_value,
    low_value, close_value, volume_value, year
FROM price_validation
WHERE trade_date BETWEEN CAST('2013-01-01' AS DATE)
                     AND CAST('2017-12-31' AS DATE)
  AND reject_reason IS NULL;

SELECT 'cleaned_prices' AS table_name, COUNT(*) AS row_count
FROM cleaned_prices;

SELECT 'rejected_prices' AS table_name, COUNT(*) AS row_count
FROM rejected_prices;

SELECT 'out_of_scope_prices' AS table_name, COUNT(*) AS row_count
FROM out_of_scope_prices;
