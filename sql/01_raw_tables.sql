CREATE DATABASE IF NOT EXISTS g12;
USE g12;

CREATE EXTERNAL TABLE IF NOT EXISTS raw_prices (
    `date` STRING,
    `open` STRING,
    `high` STRING,
    `low` STRING,
    `close` STRING,
    `volume` STRING,
    `name` STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/corpus/raw/prices/v1_2013-2017/acq_2026-11-04'
TBLPROPERTIES ('skip.header.line.count' = '1');
