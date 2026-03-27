-- Delete records older than December 1, 2025 from various tables in the 'realtime' schema
-- Note: Adjust the date as needed for different cleanup operations
-- Ensure these queries are executed in order to maintain referential integrity if necessary

delete from realtime.indexltp where indexts < '2025-12-01'

-- Validation query to check which job_iteration_ids will be affected


select i.id from realtime.job_iteration_details jd
inner join
indexltp i
on jd.id = i.job_iteration_id
where iteration_start_time < '2025-12-01'


delete from realtime.ltptracker where indexts < '2025-12-01'

select i.id from realtime.job_iteration_details jd
inner join
ltptracker i
on jd.id = i.job_iteration_id
where iteration_start_time < '2025-12-01'

delete from realtime.trade_decisions where trade_decisionts < '2025-12-01'

select i.id from realtime.job_iteration_details jd
inner join
trade_decisions i
on jd.id = i.job_iteration_id
where iteration_start_time < '2025-12-01'


delete from realtime.mini_delta where delta_instant < '2025-12-01'

select i.id from realtime.job_iteration_details jd
inner join
mini_delta i
on jd.id = i.job_iteration_id
where iteration_start_time < '2025-12-01'

delete from realtime.job_iteration_details where iteration_start_time < '2025-12-01'