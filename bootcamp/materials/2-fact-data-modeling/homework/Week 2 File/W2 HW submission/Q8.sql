
-- task 8: incremental query for the monthly reduced fact table host_activity_reduced 

insert into host_activity_reduced
with 
	events_deduped as (
	select 	
			row_number() over (partition by host, event_time ) as row_num,
			*
	from events
),
	daily_aggregate as (
	select
		host,
		date(event_time) as date,
		count(1) as num_site_hits,
		count(distinct user_id) as unique_visitor
	from events_deduped
	where date(event_time) = date('2023-01-01')
	and user_id is not null
	and row_num = 1
	group by host, date(event_time)
),
	yesterday_array as (
	select * 
	from host_activity_reduced
	where month_start = date('2023-01-01')	
)

select 
	coalesce(da.host, ya.host) as host,
	coalesce(ya.month_start, date_trunc('month', da.date)) as month_start,
	case when ya.hit_array is not null then
		ya.hit_array || array[coalesce(da.num_site_hits, 0)] 
		when ya.hit_array is null then 
			array_fill(0, array[coalesce(date - date(date_trunc('month',date)),0)]) || array[coalesce(da.num_site_hits, 0)] 
	end as hit_array,
	case when ya.unique_visitor_array is not null then
		ya.unique_visitor_array || array[coalesce(da.unique_visitor, 0)] 
		when ya.unique_visitor_array is null then 
			array_fill(0, array[coalesce(date - date(date_trunc('month',date)),0)]) || array[coalesce(da.unique_visitor, 0)] 
	end as unique_visitor_array
	from daily_aggregate da 
	full outer join yesterday_array ya 
	on da.host = ya.host
	ON CONFLICT (host, month_start)
	DO 
	    UPDATE SET 
			hit_array = EXCLUDED.hit_array,
			unique_visitor_array = EXCLUDED.unique_visitor_array; 
