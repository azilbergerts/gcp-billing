
-- ETL Query Cost calculation Per Month (GA JOBS EXLUDED)
-- Free Tier - 1TB of queries is free
-- 1 TB = 1e+12 KB

# Monitor Query costs in BigQuery; standard-sql; 2020-06-21
# @see http://www.pascallandau.com/bigquery-snippets/monitor-query-costs/

DECLARE timezone STRING DEFAULT  'America/New_York'; --"Europe/Berlin";
DECLARE gb_divisor INT64 DEFAULT 1024*1024*1024;
DECLARE tb_divisor INT64 DEFAULT gb_divisor*1024;
DECLARE cost_per_tb_in_dollar INT64 DEFAULT 5;
DECLARE cost_factor FLOAT64 DEFAULT cost_per_tb_in_dollar / tb_divisor;

-- SELECT 5/1099511627776 --(1024*1024*1024*1024)
with costs as (
select * from (
SELECT creation_time,
EXTRACT(MONTH from DATETIME(creation_time, timezone)) month, 
    project_id, destination_table.dataset_id, destination_table.table_id, user_email, query,
    sum(total_bytes_processed)/1e+12 processed_TB, sum(IFNULL(total_bytes_billed,0))/1e+12  billed_TB, 
    IF(SUM(IFNULL(total_bytes_billed,0))/1e+12 < 1, 0, (SUM(IFNULL(total_bytes_billed,0))/1e+12-1)  *.02) total_cost,

    IF(cache_hit != true, SUM(ROUND(total_bytes_processed * cost_factor,4)), 0) as cost_in_dollar,
 FROM
  `region-us.INFORMATION_SCHEMA.JOBS` --, unnest(labels) labels. --JOBS, JOBS_BY_USER, JOBS_BY_ORGANIZATION
WHERE
  user_email NOT IN ('analytics-processing-dev@system.gserviceaccount.com', 
                  'firebase-measurement@system.gserviceaccount.com') --these are GA and GA4
  AND EXTRACT(date FROM creation_time) >='2024-05-17' 
--  and REGEXP_CONTAINS(job_id,  'script_job_|bqts_|scheduled_query_') -- tried to exclude window queries - ?
GROUP by 1,2,3 ,4 ,5,6,7,cache_hit--,error_result.message
)
where cost_in_dollar >0
ORDER  by creation_time desc --,creation_time desc

)

select *
-- creation_time, -- month, EXTRACT(DAY from DATETIME(creation_time, timezone)), user_email,dataset_id, 
-- sum(cost_in_dollar)
from costs
where
-- user_email like 'alina%' and
 query like   '%ga_sessions_us%' --'%CREATE OR REPLACE%'
group by all
