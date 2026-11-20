USE g12;

-- Cleaned stock prices (partitioned by year)
CREATE EXTERNAL TABLE IF NOT EXISTS cleaned_prices (
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

-- Register existing year partitions
MSCK REPAIR TABLE cleaned_prices;

-- Rejected stock prices
CREATE EXTERNAL TABLE IF NOT EXISTS rejected_prices (
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

-- Out-of-scope stock prices
CREATE EXTERNAL TABLE IF NOT EXISTS out_of_scope_prices (
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

-- Chronological stock datasets
CREATE EXTERNAL TABLE IF NOT EXISTS training_prices (
    ticker STRING,
    trade_date DATE,
    open_price DOUBLE,
    high_price DOUBLE,
    low_price DOUBLE,
    close_price DOUBLE,
    volume BIGINT
)
STORED AS PARQUET
LOCATION '/corpus/splits/prices/training';

CREATE EXTERNAL TABLE IF NOT EXISTS validation_prices (
    ticker STRING,
    trade_date DATE,
    open_price DOUBLE,
    high_price DOUBLE,
    low_price DOUBLE,
    close_price DOUBLE,
    volume BIGINT
)
STORED AS PARQUET
LOCATION '/corpus/splits/prices/validation';

CREATE EXTERNAL TABLE IF NOT EXISTS testing_prices (
    ticker STRING,
    trade_date DATE,
    open_price DOUBLE,
    high_price DOUBLE,
    low_price DOUBLE,
    close_price DOUBLE,
    volume BIGINT
)
STORED AS PARQUET
LOCATION '/corpus/splits/prices/testing';

-- Cleaned GDELT events
CREATE EXTERNAL TABLE IF NOT EXISTS cleaned_gdelt_events (
    event_id STRING,
    event_date DATE,
    available_date DATE,
    event_code STRING,
    event_base_code STRING,
    event_root_code STRING,
    quad_class INT,
    goldstein_scale DOUBLE,
    num_mentions INT,
    num_sources INT,
    num_articles INT,
    avg_tone DOUBLE,
    actor1_name STRING,
    actor1_country STRING,
    actor2_name STRING,
    actor2_country STRING,
    action_country STRING,
    action_location STRING,
    date_added STRING,
    source_url STRING
)
STORED AS PARQUET
LOCATION '/corpus/cleaned/gdelt/events/';

-- Rejected GDELT events
CREATE EXTERNAL TABLE IF NOT EXISTS rejected_gdelt (
    event_id STRING,
    event_date DATE,
    available_date DATE,
    event_code STRING,
    reject_reason STRING
)
STORED AS PARQUET
LOCATION '/corpus/cleaned/gdelt/rejected/';

-- Daily GDELT features
CREATE EXTERNAL TABLE IF NOT EXISTS daily_gdelt_features (
    available_date DATE,
    event_count BIGINT,
    avg_goldstein DOUBLE,
    avg_tone DOUBLE,
    total_mentions BIGINT,
    total_sources BIGINT,
    total_articles BIGINT,
    distinct_event_codes BIGINT
)
STORED AS PARQUET
LOCATION '/corpus/cleaned/gdelt/daily_features/';
