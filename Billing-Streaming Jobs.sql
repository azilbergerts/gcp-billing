-- STREAMING JOBS - GA4 Intraday tables
SELECT
  -- start_timestamp,
  -- error_code,
EXTRACT(MONTH from DATETIME(start_timestamp)) month, 
EXTRACT(YEAR from DATETIME(start_timestamp)) year, 
dataset_id,
table_id,
SUM(total_rows) total_rows,
SUM(total_input_bytes) total_input_bytes
  -- SUM(total_requests) AS num_failed_requests
 
FROM
  `region-us`.INFORMATION_SCHEMA.STREAMING_TIMELINE_BY_PROJECT
WHERE date(start_timestamp) >='2024-10-03'
  -- and error_code IS NOT NULL
  -- AND start_timestamp > TIMESTAMP_SUB(CURRENT_TIMESTAMP, INTERVAL 30 MINUTE)
GROUP BY ALL
--   start_timestamp,
--   error_code
ORDER BY month, year
