-- Task 2 / stage hive_raw_tables
-- Variables (passed with --hivevar): DB, RAW_PRICES, STREAM_OUT
CREATE DATABASE IF NOT EXISTS ${hivevar:DB};
USE ${hivevar:DB};

-- Raw prices: original CSV text, every column kept as STRING so nothing is lost on read.
CREATE EXTERNAL TABLE raw_prices (
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
LOCATION '${hivevar:RAW_PRICES}'
TBLPROPERTIES ('skip.header.line.count' = '1');

-- Output of the Hadoop Streaming GDELT job: tagged lines (V = valid, R = rejected, A = audit).
CREATE EXTERNAL TABLE gdelt_stream (
    tag STRING,
    c01 STRING, c02 STRING, c03 STRING, c04 STRING, c05 STRING,
    c06 STRING, c07 STRING, c08 STRING, c09 STRING, c10 STRING,
    c11 STRING, c12 STRING, c13 STRING, c14 STRING, c15 STRING,
    c16 STRING, c17 STRING, c18 STRING, c19 STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '${hivevar:STREAM_OUT}';
