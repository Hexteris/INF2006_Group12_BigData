-- Task 2 / stage hive_clean_gdelt   (value rules unchanged from the team's 05_clean_gdelt_final.sql)
-- Variables: DB, OUT
USE ${hivevar:DB};

-- 0. Split the tagged Streaming output into valid events, structural rejects and per-archive audit lines.
CREATE VIEW gdelt_valid_raw AS
SELECT c01 AS GLOBALEVENTID, c02 AS SQLDATE, c03 AS EventCode, c04 AS EventBaseCode,
       c05 AS EventRootCode, c06 AS QuadClass, c07 AS GoldsteinScale, c08 AS NumMentions,
       c09 AS NumSources, c10 AS NumArticles, c11 AS AvgTone, c12 AS Actor1Name,
       c13 AS Actor1CountryCode, c14 AS Actor2Name, c15 AS Actor2CountryCode,
       c16 AS ActionGeo_CountryCode, c17 AS ActionGeo_FullName, c18 AS DATEADDED,
       c19 AS SOURCEURL
FROM gdelt_stream WHERE tag = 'V';

CREATE VIEW gdelt_struct_rejects AS
SELECT c01 AS reject_reason, c02 AS source_archive, c03 AS raw_line
FROM gdelt_stream WHERE tag = 'R';

CREATE VIEW gdelt_audit AS
SELECT c01 AS source_archive,
       CAST(c02 AS BIGINT) AS rows_read,
       CAST(c03 AS BIGINT) AS valid_rows,
       CAST(c04 AS BIGINT) AS rejected_rows,
       CAST(c05 AS BIGINT) AS zip_bytes
FROM gdelt_stream WHERE tag = 'A';

-- 1. Parse fields into typed columns.
CREATE VIEW gdelt_parsed AS
SELECT
    TRIM(GLOBALEVENTID) AS event_id,
    CASE
        WHEN TRIM(SQLDATE) RLIKE '^[0-9]{8}$'
        THEN CAST(CONCAT(SUBSTR(TRIM(SQLDATE), 1, 4), '-',
                         SUBSTR(TRIM(SQLDATE), 5, 2), '-',
                         SUBSTR(TRIM(SQLDATE), 7, 2)) AS DATE)
        ELSE NULL
    END AS event_date,
    CASE
        WHEN TRIM(DATEADDED) RLIKE '^[0-9]{8}$'
        THEN CAST(CONCAT(SUBSTR(TRIM(DATEADDED), 1, 4), '-',
                         SUBSTR(TRIM(DATEADDED), 5, 2), '-',
                         SUBSTR(TRIM(DATEADDED), 7, 2)) AS DATE)
        ELSE NULL
    END AS available_date,
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
    TRIM(DATEADDED) AS date_added,
    SOURCEURL AS source_url,
    TRIM(QuadClass) AS raw_quad_class,
    TRIM(GoldsteinScale) AS raw_goldstein_scale,
    TRIM(NumMentions) AS raw_num_mentions,
    TRIM(NumSources) AS raw_num_sources,
    TRIM(NumArticles) AS raw_num_articles,
    TRIM(AvgTone) AS raw_avg_tone
FROM gdelt_valid_raw;

-- 2. Classify invalid records.
CREATE VIEW gdelt_classified AS
SELECT *,
    CASE
        WHEN event_id IS NULL OR event_id = '' THEN 'missing_event_id'
        WHEN event_id NOT RLIKE '^[0-9]+$' THEN 'invalid_event_id'
        WHEN event_date IS NULL THEN 'invalid_event_date'
        WHEN available_date IS NULL THEN 'invalid_available_date'
        WHEN event_code IS NULL OR event_code = '' THEN 'missing_event_code'
        WHEN quad_class IS NULL OR raw_quad_class NOT RLIKE '^[1-4]$' THEN 'invalid_quad_class'
        WHEN goldstein_scale IS NULL
          OR raw_goldstein_scale NOT RLIKE '^-?[0-9]+(\\.[0-9]+)?$' THEN 'invalid_goldstein_scale'
        WHEN num_mentions IS NULL OR raw_num_mentions NOT RLIKE '^[0-9]+$' THEN 'invalid_num_mentions'
        WHEN num_sources IS NULL OR raw_num_sources NOT RLIKE '^[0-9]+$' THEN 'invalid_num_sources'
        WHEN num_articles IS NULL OR raw_num_articles NOT RLIKE '^[0-9]+$' THEN 'invalid_num_articles'
        WHEN avg_tone IS NULL
          OR raw_avg_tone NOT RLIKE '^-?[0-9]+(\\.[0-9]+)?$' THEN 'invalid_avg_tone'
        ELSE NULL
    END AS reject_reason
FROM gdelt_parsed;

-- 3. Rejected records stay separate (value-level rejects, then structural rejects from Streaming).
CREATE TABLE rejected_gdelt (
    event_id STRING, event_date DATE, available_date DATE, event_code STRING, reject_reason STRING
)
STORED AS PARQUET
LOCATION '${hivevar:OUT}/rejected/gdelt';

CREATE TABLE rejected_gdelt_structural (
    reject_reason STRING, source_archive STRING, raw_line STRING
)
STORED AS PARQUET
LOCATION '${hivevar:OUT}/rejected/gdelt_structural';

-- 4. Cleaned events.
CREATE TABLE cleaned_gdelt_events (
    event_id STRING, event_date DATE, available_date DATE, event_code STRING,
    event_base_code STRING, event_root_code STRING, quad_class INT, goldstein_scale DOUBLE,
    num_mentions INT, num_sources INT, num_articles INT, avg_tone DOUBLE,
    actor1_name STRING, actor1_country STRING, actor2_name STRING, actor2_country STRING,
    action_country STRING, action_location STRING, date_added STRING, source_url STRING
)
STORED AS PARQUET
LOCATION '${hivevar:OUT}/cleaned/gdelt/events';

INSERT OVERWRITE TABLE rejected_gdelt
SELECT event_id, event_date, available_date, event_code, reject_reason
FROM gdelt_classified WHERE reject_reason IS NOT NULL;

INSERT OVERWRITE TABLE rejected_gdelt_structural
SELECT reject_reason, source_archive, raw_line FROM gdelt_struct_rejects;

INSERT OVERWRITE TABLE cleaned_gdelt_events
SELECT event_id, event_date, available_date, event_code, event_base_code, event_root_code,
       quad_class, goldstein_scale, num_mentions, num_sources, num_articles, avg_tone,
       actor1_name, actor1_country, actor2_name, actor2_country, action_country,
       action_location, date_added, source_url
FROM gdelt_classified WHERE reject_reason IS NULL;

-- 5. Daily news features by availability date (DATEADDED), never by event date.
CREATE TABLE daily_gdelt_features (
    available_date DATE, event_count BIGINT, avg_goldstein DOUBLE, avg_tone DOUBLE,
    total_mentions BIGINT, total_sources BIGINT, total_articles BIGINT, distinct_event_codes BIGINT
)
STORED AS PARQUET
LOCATION '${hivevar:OUT}/cleaned/gdelt/daily_features';

INSERT OVERWRITE TABLE daily_gdelt_features
SELECT available_date, COUNT(*), AVG(goldstein_scale), AVG(avg_tone),
       SUM(num_mentions), SUM(num_sources), SUM(num_articles), COUNT(DISTINCT event_code)
FROM cleaned_gdelt_events
GROUP BY available_date;
