USE g12;

CREATE VIEW gdelt_parsed AS
SELECT
    TRIM(GLOBALEVENTID) AS event_id,
    CASE
        WHEN TRIM(SQLDATE) RLIKE '^[0-9]{8}$'
        THEN CAST(CONCAT(
            SUBSTR(TRIM(SQLDATE), 1, 4), '-',
            SUBSTR(TRIM(SQLDATE), 5, 2), '-',
            SUBSTR(TRIM(SQLDATE), 7, 2)
        ) AS DATE)
        ELSE NULL
    END AS event_date,
    TRIM(EventCode) AS event_code,
    TRIM(EventBaseCode) AS event_base_code,
    TRIM(EventRootCode) AS event_root_code,
    CAST(TRIM(QuadClass) AS INT) AS quad_class,
    CAST(TRIM(GoldsteinScale) AS DOUBLE) AS goldstein_scale,
    CAST(TRIM(NumMentions) AS INT) AS num_mentions,
    CAST(TRIM(NumSources) AS INT) AS num_sources,
    CAST(TRIM(NumArticles) AS INT) AS num_articles,
    CAST(TRIM(AvgTone) AS DOUBLE) AS avg_tone,
    Actor1Name AS actor1_name,
    Actor1CountryCode AS actor1_country,
    Actor2Name AS actor2_name,
    Actor2CountryCode AS actor2_country,
    ActionGeo_CountryCode AS action_country,
    ActionGeo_FullName AS action_location,
    DATEADDED AS date_added,
    SOURCEURL AS source_url
FROM raw_gdelt;

CREATE VIEW gdelt_ranked AS
SELECT *,
    ROW_NUMBER() OVER (
        PARTITION BY event_id
        ORDER BY date_added DESC
    ) AS duplicate_rank
FROM gdelt_parsed;

CREATE VIEW gdelt_classified AS
SELECT *,
    CASE
        WHEN event_id IS NULL OR event_id = ''
            THEN 'missing_event_id'
        WHEN event_id NOT RLIKE '^[0-9]+$'
            THEN 'invalid_event_id'
        WHEN event_date IS NULL
            THEN 'invalid_event_date'
        WHEN event_code IS NULL OR event_code = ''
            THEN 'missing_event_code'
        WHEN duplicate_rank > 1
            THEN 'duplicate_event_id'
        ELSE NULL
    END AS reject_reason
FROM gdelt_ranked;

CREATE TABLE rejected_gdelt (
    event_id STRING,
    event_date DATE,
    event_code STRING,
    reject_reason STRING
)
STORED AS PARQUET
LOCATION '/corpus/rejected/gdelt/';

CREATE TABLE cleaned_gdelt_events (
    event_id STRING,
    event_date DATE,
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

INSERT OVERWRITE TABLE rejected_gdelt
SELECT event_id, event_date, event_code, reject_reason
FROM gdelt_classified
WHERE reject_reason IS NOT NULL;

INSERT OVERWRITE TABLE cleaned_gdelt_events
SELECT
    event_id, event_date, event_code, event_base_code,
    event_root_code, quad_class, goldstein_scale,
    num_mentions, num_sources, num_articles, avg_tone,
    actor1_name, actor1_country, actor2_name,
    actor2_country, action_country, action_location,
    date_added, source_url
FROM gdelt_classified
WHERE reject_reason IS NULL;

CREATE TABLE daily_gdelt_features (
    event_date DATE,
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

INSERT OVERWRITE TABLE daily_gdelt_features
SELECT
    event_date,
    COUNT(*) AS event_count,
    AVG(goldstein_scale) AS avg_goldstein,
    AVG(avg_tone) AS avg_tone,
    SUM(num_mentions) AS total_mentions,
    SUM(num_sources) AS total_sources,
    SUM(num_articles) AS total_articles,
    COUNT(DISTINCT event_code) AS distinct_event_codes
FROM cleaned_gdelt_events
GROUP BY event_date;

SELECT 'cleaned_gdelt_events' AS table_name, COUNT(*) AS row_count
FROM cleaned_gdelt_events;

SELECT 'rejected_gdelt' AS table_name, COUNT(*) AS row_count
FROM rejected_gdelt;

SELECT 'daily_gdelt_features' AS table_name, COUNT(*) AS row_count
FROM daily_gdelt_features;
