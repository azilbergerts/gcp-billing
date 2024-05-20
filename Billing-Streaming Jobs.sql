SELECT
  -- start_timestamp,
  -- error_code,
  -- SUM(total_requests) AS num_failed_requests
  *
FROM
  `region-us`.INFORMATION_SCHEMA.STREAMING_TIMELINE_BY_PROJECT
WHERE date(start_timestamp) ='2023-10-03'
  and error_code IS NOT NULL
  -- AND start_timestamp > TIMESTAMP_SUB(CURRENT_TIMESTAMP, INTERVAL 30 MINUTE)
-- GROUP BY
--   start_timestamp,
--   error_code
ORDER BY
  start_timestamp DESC;
