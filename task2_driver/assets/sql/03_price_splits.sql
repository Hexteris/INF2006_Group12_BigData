-- Task 2 / stage hive_price_splits   (chronological splits unchanged from the team's 03_price_splits.sql)
-- Variables: DB, OUT
USE ${hivevar:DB};

CREATE TABLE training_prices (
    ticker STRING, trade_date DATE, open_price DOUBLE, high_price DOUBLE,
    low_price DOUBLE, close_price DOUBLE, volume BIGINT
)
STORED AS PARQUET
LOCATION '${hivevar:OUT}/splits/prices/training';

CREATE TABLE validation_prices (
    ticker STRING, trade_date DATE, open_price DOUBLE, high_price DOUBLE,
    low_price DOUBLE, close_price DOUBLE, volume BIGINT
)
STORED AS PARQUET
LOCATION '${hivevar:OUT}/splits/prices/validation';

CREATE TABLE testing_prices (
    ticker STRING, trade_date DATE, open_price DOUBLE, high_price DOUBLE,
    low_price DOUBLE, close_price DOUBLE, volume BIGINT
)
STORED AS PARQUET
LOCATION '${hivevar:OUT}/splits/prices/testing';

INSERT OVERWRITE TABLE training_prices
SELECT ticker, trade_date, open_price, high_price, low_price, close_price, volume
FROM cleaned_prices
WHERE trade_date BETWEEN '2013-01-01' AND '2015-12-31';

INSERT OVERWRITE TABLE validation_prices
SELECT ticker, trade_date, open_price, high_price, low_price, close_price, volume
FROM cleaned_prices
WHERE trade_date BETWEEN '2016-01-01' AND '2016-12-31';

INSERT OVERWRITE TABLE testing_prices
SELECT ticker, trade_date, open_price, high_price, low_price, close_price, volume
FROM cleaned_prices
WHERE trade_date BETWEEN '2017-01-01' AND '2017-12-31';
