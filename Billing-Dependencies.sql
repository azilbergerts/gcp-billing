
SELECT user_email, job_id, job_type, referenced_tables,query,
FROM `region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT --, UNNEST(referenced_tables) referenced_tables
-- WHERE REGEXP_CONTAINS(referenced_tables.table_id, 'hmn_dashboard')
-- and job_id like 'script_%'
where cache_hit != true
order by creation_time desc
