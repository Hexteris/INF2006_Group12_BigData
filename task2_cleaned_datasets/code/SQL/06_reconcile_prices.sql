
USE g12;

-- 1. Compare the actual row counts with the expected counts

SELECT
    'raw_prices' AS table_name,
    619040 AS expected_count,
    COUNT(*) AS actual_count,
    CASE WHEN COUNT(*) = 619040 THEN 'PASS' ELSE 'FAIL' END AS status
FROM g12.raw_prices

UNION ALL

SELECT
    'cleaned_prices',
    605887,
    COUNT(*),
    CASE WHEN COUNT(*) = 605887 THEN 'PASS' ELSE 'FAIL' END
FROM g12.cleaned_prices

UNION ALL

SELECT
    'rejected_prices',
    23,
    COUNT(*),
    CASE WHEN COUNT(*) = 23 THEN 'PASS' ELSE 'FAIL' END
FROM g12.rejected_prices

UNION ALL

SELECT
    'out_of_scope_prices',
    13130,
    COUNT(*),
    CASE WHEN COUNT(*) = 13130 THEN 'PASS' ELSE 'FAIL' END
FROM g12.out_of_scope_prices;


-- 2. Check that all raw records are accounted for

SELECT
    raw_count,
    cleaned_count,
    rejected_count,
    out_of_scope_count,

    cleaned_count + rejected_count + out_of_scope_count
        AS processed_total,

    raw_count -
        (cleaned_count + rejected_count + out_of_scope_count)
        AS unaccounted_rows,

    CASE
        WHEN raw_count =
             cleaned_count + rejected_count + out_of_scope_count
        THEN 'PASS'
        ELSE 'FAIL'
    END AS reconciliation_status

FROM
    (SELECT COUNT(*) AS raw_count
     FROM g12.raw_prices) r

CROSS JOIN
    (SELECT COUNT(*) AS cleaned_count
     FROM g12.cleaned_prices) c

CROSS JOIN
    (SELECT COUNT(*) AS rejected_count
     FROM g12.rejected_prices) x

CROSS JOIN
    (SELECT COUNT(*) AS out_of_scope_count
     FROM g12.out_of_scope_prices) o;


-- 3. Count rejected records for each reason

SELECT
    reject_reason,
    COUNT(*) AS row_count
FROM g12.rejected_prices
GROUP BY reject_reason
ORDER BY reject_reason;


-- 4. Check the date range of the cleaned price records

SELECT
    MIN(trade_date) AS earliest_trade_date,
    MAX(trade_date) AS latest_trade_date,
    COUNT(*) AS cleaned_row_count
FROM g12.cleaned_prices;


-- 5. Check the date range of records outside the project scope

SELECT
    MIN(CAST(`date` AS DATE)) AS earliest_out_of_scope_date,
    MAX(CAST(`date` AS DATE)) AS latest_out_of_scope_date,
    COUNT(*) AS out_of_scope_row_count
FROM g12.out_of_scope_prices;
