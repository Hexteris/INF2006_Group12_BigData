-- Task 2 / stage hive_reconcile   (run with `hive -S`; prints check<TAB>expected<TAB>actual<TAB>status)
-- Variables: DB, EXPECTED_ARCHIVES
USE ${hivevar:DB};

SELECT 'prices_raw_rows' AS check_name, CAST(NULL AS BIGINT) AS expected, COUNT(*) AS actual, 'INFO' AS status
FROM raw_prices
UNION ALL
SELECT 'prices_cleaned_rows', CAST(NULL AS BIGINT), COUNT(*), 'INFO' FROM cleaned_prices
UNION ALL
SELECT 'prices_rejected_rows', CAST(NULL AS BIGINT), COUNT(*), 'INFO' FROM rejected_prices
UNION ALL
SELECT 'prices_out_of_scope_rows', CAST(NULL AS BIGINT), COUNT(*), 'INFO' FROM out_of_scope_prices
UNION ALL
SELECT 'prices_raw_eq_cleaned_rejected_out_of_scope', r.n, c.n + x.n + o.n,
       CASE WHEN r.n = c.n + x.n + o.n THEN 'PASS' ELSE 'FAIL' END
FROM (SELECT COUNT(*) AS n FROM raw_prices) r
CROSS JOIN (SELECT COUNT(*) AS n FROM cleaned_prices) c
CROSS JOIN (SELECT COUNT(*) AS n FROM rejected_prices) x
CROSS JOIN (SELECT COUNT(*) AS n FROM out_of_scope_prices) o
UNION ALL
SELECT 'prices_cleaned_eq_train_val_test', c.n, t.n + v.n + s.n,
       CASE WHEN c.n = t.n + v.n + s.n THEN 'PASS' ELSE 'FAIL' END
FROM (SELECT COUNT(*) AS n FROM cleaned_prices) c
CROSS JOIN (SELECT COUNT(*) AS n FROM training_prices) t
CROSS JOIN (SELECT COUNT(*) AS n FROM validation_prices) v
CROSS JOIN (SELECT COUNT(*) AS n FROM testing_prices) s
UNION ALL
SELECT 'gdelt_archives_audited', CAST(${hivevar:EXPECTED_ARCHIVES} AS BIGINT), COUNT(*),
       CASE WHEN COUNT(*) = ${hivevar:EXPECTED_ARCHIVES} THEN 'PASS' ELSE 'FAIL' END
FROM gdelt_audit
UNION ALL
SELECT 'gdelt_rows_read', CAST(NULL AS BIGINT), SUM(rows_read), 'INFO' FROM gdelt_audit
UNION ALL
SELECT 'gdelt_streaming_rows_eq_valid_plus_rejected', SUM(rows_read), SUM(valid_rows) + SUM(rejected_rows),
       CASE WHEN SUM(rows_read) = SUM(valid_rows) + SUM(rejected_rows) THEN 'PASS' ELSE 'FAIL' END
FROM gdelt_audit
UNION ALL
SELECT 'gdelt_valid_eq_cleaned_plus_value_rejected', a.n, c.n + r.n,
       CASE WHEN a.n = c.n + r.n THEN 'PASS' ELSE 'FAIL' END
FROM (SELECT COALESCE(SUM(valid_rows), 0) AS n FROM gdelt_audit) a
CROSS JOIN (SELECT COUNT(*) AS n FROM cleaned_gdelt_events) c
CROSS JOIN (SELECT COUNT(*) AS n FROM rejected_gdelt) r
UNION ALL
SELECT 'gdelt_structural_rejects_eq_audit', a.n, s.n,
       CASE WHEN a.n = s.n THEN 'PASS' ELSE 'FAIL' END
FROM (SELECT COALESCE(SUM(rejected_rows), 0) AS n FROM gdelt_audit) a
CROSS JOIN (SELECT COUNT(*) AS n FROM rejected_gdelt_structural) s
UNION ALL
SELECT 'gdelt_unreadable_archives', CAST(0 AS BIGINT), COUNT(*),
       CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END
FROM rejected_gdelt_structural WHERE reject_reason = 'unreadable_archive'
UNION ALL
SELECT 'gdelt_cleaned_rows', CAST(NULL AS BIGINT), COUNT(*), 'INFO' FROM cleaned_gdelt_events
UNION ALL
SELECT 'gdelt_daily_feature_days', CAST(NULL AS BIGINT), COUNT(*), 'INFO' FROM daily_gdelt_features;
