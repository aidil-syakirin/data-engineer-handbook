-- task 1: deduped version of game_details

-- with deduped as (
-- 	select *,
-- 	row_number() over(partition by team_id,player_id,game_id) as row_num
-- 	from game_details
-- )

-- select * from deduped where row_num = 1

-- task 2: create ddl for user_devices_cumulated

-- create table user_devices_cumulated(
-- 	user_id numeric,
-- 	browser_type text,
-- 	dates_active date[],
-- 	date date,
-- 	primary key(user_id, browser_type, date)
-- )

-- task 3: cumulative query for user_devices_cumulated

-- insert into user_devices_cumulated
-- -- first 4 CTE are for removing duplicates from devices and events
-- with 
-- 	devices_deduped as (
-- 	select 	
-- 			row_number() over (partition by device_id) as row_num,
-- 			*
-- 	from devices
-- 	where 
-- 	device_id is not null	
-- ),
-- 	devices as(
-- 	select 	
-- 			*
-- 	from devices_deduped
-- 	where row_num = 1
-- ),	
-- 	today_deduped as (
-- 	select 	
-- 			row_number() over (partition by user_id, device_id) as row_num,
-- 			*
-- 	from events
-- 	where 
-- 	date(cast(event_time as timestamp)) = date('2023-01-07')
-- 	and 
-- 	user_id is not null
-- 	and 
-- 	device_id is not null
-- ), today as (
-- 	select 	
-- 			user_id,
-- 			device_id,
-- 			date(cast(event_time as timestamp)) as event_time
-- 	from today_deduped
-- 	where row_num = 1
-- ),
-- 	yesterday as (
-- 	select * 
-- 	from user_devices_cumulated
-- 	where date = date('2023-01-06')
-- ),
-- -- these 2 CTEs are for removing similar browser type with different device_id of each user 
-- 	device_today as (
-- 	select 
-- 		row_number() over (partition by user_id, browser_type ) as row_num,
-- 		t.user_id,
-- 		d.browser_type,
-- 		t.event_time
-- 	from today t 
-- 	left join devices d 
-- 	on t.device_id = d.device_id	
-- ),
-- 	device_today_deduped as (
-- 	select 
-- 		user_id,
-- 		browser_type,
-- 		event_time
-- 	from device_today
-- 	where row_num = 1	
-- ),
-- 	coa_users as (	
-- 	select 
-- 	coalesce(t.user_id, y.user_id) as user_id,
-- 	coalesce(t.browser_type, y.browser_type) as browser_type,
-- 	case 
-- 		when y.dates_active is null
-- 		then array[t.event_time]
-- 		when t.event_time is null then y.dates_active
-- 		else array[t.event_time] || y.dates_active 
-- 		end as dates_active, 
-- 	date(coalesce(t.event_time, y.date + interval '1 day')) 
-- 	as date
-- 	from device_today_deduped t full outer join yesterday y
-- 	on t.user_id = y.user_id 
-- 	and 
-- 	t.browser_type = y.browser_type
-- )

-- select * from coa_users

-- task 4: datelist generation query
	
-- with user_devices as (
-- 	select 
--  	* 
-- 	from user_devices_cumulated 
-- 	where date = '2023-01-31' 
-- ), 
-- 	series_date as (
-- 	select
-- 	* 
-- 	from generate_series(date('2023-01-01'),date('2023-01-31'), interval '1 day') as series_date
-- ),
-- 	user_series as (
-- 	select ud.*
-- 	, sd.series_date
-- 	from user_devices ud 
-- 	cross join series_date sd 
-- ), 
-- 	place_holder_ints as (

-- 	select 
-- 	-- must change the series date column into an array before comparing with the dates_active array (an array rule)
-- 	case when dates_active @> array[date(series_date)] 
-- 	then cast(pow(2, 32 - (date - date(series_date))) as bigint)
-- 	else 0 
-- 	end as placeholder_int_value
-- 	, *
-- 	from user_series
-- )

-- 	select 
-- 		user_id,
-- 		browser_type, 
-- 	cast(cast(sum(placeholder_int_value) as bigint) as bit(32)),
	
-- 	bit_count(cast(cast(sum(placeholder_int_value) as bigint) as bit(32))) > 0 
-- 	as dim_monthly_active,
	
-- 	--use AND gate to nullify bit except the first 7 days(bits) to track the user weekly activity
-- 	bit_count(cast('11111110000000000000000000000000' as bit(32)) & 
-- 	cast(cast(sum(placeholder_int_value) as bigint) as bit(32))) > 0
-- 	as dim_weekly_active,
	
-- 	--use AND gate to nullify bit except the first day(bit) to track the user latest activity
-- 	bit_count(cast('10000000000000000000000000000000' as bit(32)) & 
-- 	cast(cast(sum(placeholder_int_value) as bigint) as bit(32))) > 0 
-- 	as dim_daily_active
	
-- 	from place_holder_ints
-- 	group by user_id, browser_type

-- task 5: DDL for host_cumulated table

-- create table host_cumulated(
-- 	host text,
-- 	dates_active date[],
-- 	date date,
-- 	primary key(host, date)
-- )

-- task 6: incremental query to generate host_activity_datelist

-- insert into host_cumulated
-- with 
-- 	today_deduped as (
-- 	select 	
-- 			row_number() over (partition by host, event_time ) as row_num,
-- 			*
-- 	from events
-- 	where 
-- 	date(cast(event_time as timestamp)) = date('2023-01-03')
-- ), 
-- 	today as (
-- 	select
-- 		host,
-- 		date(cast(event_time as timestamp)) as event_time
-- 	from today_deduped
-- 	where user_id is not null
-- 	and row_num = 1
-- 	group by host, date(cast(event_time as timestamp))
-- ),
-- 	yesterday as (
-- 	select * 
-- 	from host_cumulated
-- 	where date = date('2023-01-02')
-- ),
-- 	c_hosts as (	
-- 	select 
-- 	coalesce(t.host, y.host) as host,
-- 	case 
-- 		when y.dates_active is null
-- 		then array[t.event_time]
-- 		when t.event_time is null then y.dates_active
-- 		else array[t.event_time] || y.dates_active 
-- 		end as dates_active, 
-- 	date(coalesce(t.event_time, y.date + interval '1 day')) 
-- 	as date
-- 	from today t full outer join yesterday y
-- 	on t.host = y.host 

-- )

-- select * from c_hosts 

-- task 7: DDL for monthly reduced fact table 
-- create table host_activity_reduced (
-- 	host text,
-- 	month_start date,
-- 	hit_array real[],
-- 	unique_visitor_array integer[],
-- 	primary key(host, month_start)
-- )


-- task 8: incremental query for the monthly reduced fact table host_activity_reduced 

-- insert into host_activity_reduced
-- with 
-- 	events_deduped as (
-- 	select 	
-- 			row_number() over (partition by host, event_time ) as row_num,
-- 			*
-- 	from events
-- ),
-- 	daily_aggregate as (
-- 	select
-- 		host,
-- 		date(event_time) as date,
-- 		count(1) as num_site_hits,
-- 		count(distinct user_id) as unique_visitor
-- 	from events_deduped
-- 	where date(event_time) = date('2023-01-01')
-- 	and user_id is not null
-- 	and row_num = 1
-- 	group by host, date(event_time)
-- ),
-- 	yesterday_array as (
-- 	select * 
-- 	from host_activity_reduced
-- 	where month_start = date('2023-01-01')	
-- )

-- select 
-- 	coalesce(da.host, ya.host) as host,
-- 	coalesce(ya.month_start, date_trunc('month', da.date)) as month_start,
-- 	case when ya.hit_array is not null then
-- 		ya.hit_array || array[coalesce(da.num_site_hits, 0)] 
-- 		when ya.hit_array is null then 
-- 			array_fill(0, array[coalesce(date - date(date_trunc('month',date)),0)]) || array[coalesce(da.num_site_hits, 0)] 
-- 	end as hit_array,
-- 	case when ya.unique_visitor_array is not null then
-- 		ya.unique_visitor_array || array[coalesce(da.unique_visitor, 0)] 
-- 		when ya.unique_visitor_array is null then 
-- 			array_fill(0, array[coalesce(date - date(date_trunc('month',date)),0)]) || array[coalesce(da.unique_visitor, 0)] 
-- 	end as unique_visitor_array
-- 	from daily_aggregate da 
-- 	full outer join yesterday_array ya 
-- 	on da.host = ya.host
-- 	ON CONFLICT (host, month_start)
-- 	DO 
-- 	    UPDATE SET 
-- 			hit_array = EXCLUDED.hit_array,
-- 			unique_visitor_array = EXCLUDED.unique_visitor_array; 

