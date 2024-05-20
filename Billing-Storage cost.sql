
DECLARE timezone STRING DEFAULT  'America/New_York'; --"Europe/Berlin";
DECLARE gb_divisor INT64 DEFAULT 1024*1024*1024;

DECLARE active_logical_storage_cost_per_gb  FLOAT64 DEFAULT 0.02;
DECLARE longterm_logical_storage_cost_per_gb  FLOAT64 DEFAULT 0.01;
DECLARE active_physical_storage_cost_per_gb  FLOAT64 DEFAULT 0.04;
DECLARE longterm_physical_storage_cost_per_gb  FLOAT64 DEFAULT 0.02;


SELECT table_schema dataset,
--    table_name, 
  -- count(table_name) tables,

  -- Active logical size
  SUM(active_logical_bytes)/gb_divisor active_logical_Gb,
  SUM(active_logical_bytes)/gb_divisor-10  active_logical_Gb_billed,
  CAST(active_logical_storage_cost_per_gb AS STRING FORMAT '$999,999.00') active_logical_unit_price,
  (SUM(active_logical_bytes)/gb_divisor-10) * active_logical_storage_cost_per_gb active_logical_storage_cost,
  -- Active physical size
  SUM(active_physical_bytes)/gb_divisor active_physical_Gb,
  SUM(active_physical_bytes)/gb_divisor-10  active_physical_Gb_billed,
  CAST(active_physical_storage_cost_per_gb AS STRING FORMAT '$999,999.00') active_physical_unit_price,
  (SUM(active_physical_bytes)/gb_divisor-10) * active_physical_storage_cost_per_gb active_physcial_storage_cost,
  
  CAST((SUM(active_logical_bytes)/gb_divisor-10) * active_logical_storage_cost_per_gb
        - 
        (SUM(active_physical_bytes)/gb_divisor-10) * active_physical_storage_cost_per_gb 
  AS STRING FORMAT '$999,999.00') total_cost
  
--   Long term size - not applicabe for now (after 90 day, appprox October 1 2023?)
  ,SUM(long_term_logical_bytes)/gb_divisor long_term_logcal_GB,
  SUM(long_term_logical_bytes)/gb_divisor - 10 long_term_logcal_GB_billed,
--   CAST( 0.01 AS STRING FORMAT '$999,999.00') long_term_unit_price,
  CAST((SUM(long_term_logical_bytes)/ power(1024, 3) -10) *0.01 AS STRING FORMAT '$999,999.00') long_term_storage_cost,
  -- SUM(total_logical_bytes) / power(1024, 3) AS total_logical_GB,
  -- CAST( (SUM(active_logical_bytes)/ power(1024, 3)*0.02 + SUM(long_term_logical_bytes)/ power(1024, 3)*0.01) AS STRING FORMAT '$999,999.00') total_cost
-- SELECT table_schema, SUBSTRING(table_name, 8,6) YEAR_MONTH,
--   SUM(active_logical_bytes)/(1024*1024*1024) active_logical_Gb,
--   CAST(0.02 AS STRING FORMAT '$999,999.00') active_logical_unit_price,
--   CAST(SUM(active_logical_bytes)/(1024*1024*1024) * 0.02  AS STRING FORMAT '$999,999.00') active_logical_storage_cost,

FROM `region-us`.INFORMATION_SCHEMA.TABLE_STORAGE
where table_schema like 'analytics_%'  --and table_name like 'events_202307%'
GROUP by 1 --,2
-- ORDER BY YEAR_MONTH
