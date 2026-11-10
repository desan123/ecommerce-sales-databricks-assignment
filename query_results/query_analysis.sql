%sql
SELECT COUNT(*) as open_cnt
FROM bronze_lhdev.jobs
WHERE lower(status) = 'open'


%sql
SELECT
    j.department,
    COUNT(*) AS total_applications
FROM bronze_lhdev.applications a
JOIN bronze_lhdev.jobs j
ON a.job_id = j.job_id
WHERE j.department IS NOT NULL
GROUP BY j.department
ORDER BY total_applications DESC
LIMIT 5


%sql
SELECT
    c.candidate_id,
    c.first_name,
    c.last_name,
    COUNT(a.job_id) AS applications
FROM bronze_lhdev.candidates c
JOIN bronze_lhdev.applications a
ON c.candidate_id = a.candidate_id
GROUP BY
    c.candidate_id,
    c.first_name,
    c.last_name
HAVING COUNT(a.job_id) > 3
ORDER BY applications desc


# Query used for debugging
%sql
with applied_ts as(
  select application_id, min(event_timestamp) as applied_date from base_workflow where upper(new_status) = "APPLIED" group by application_id
),
hired_ts as (
  select application_id, min(event_timestamp) as hired_date from base_workflow where upper(new_status) = "HIRED" group by application_id
),joined_data as (
select * from base_workflow as wf 
left join applied_ts on wf.application_id = applied_ts.application_id 
left join hired_ts on wf.application_id = hired_ts.application_id)

select * from joined_data where hired_date < applied_date


