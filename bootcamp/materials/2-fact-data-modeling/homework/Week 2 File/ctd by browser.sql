-- insert into user_devices_cumulated
with 
	devices_deduped as (
	select 	
			row_number() over (partition by device_id) as row_num,
			*
	from devices
	where 
	device_id is not null	
),
	devices as(
	select 	
			*
	from devices_deduped
	where row_num = 1
),
	yesterday as (
	select * 
	from user_devices_cumulated
	where date = date('2023-01-06')
),	
	today_deduped as (
	select 	
			row_number() over (partition by user_id, device_id) as row_num,
			*
	from events
	where 
	date(cast(event_time as timestamp)) = date('2023-01-07')
	and 
	user_id is not null
	and 
	device_id is not null
), today as (
	select 	
			user_id,
			device_id,
			date(cast(event_time as timestamp)) as event_time
	from today_deduped
	where row_num = 1
),
	device_today as (
	select 
		row_number() over (partition by user_id, browser_type ) as row_num,
		t.user_id,
		d.browser_type,
		t.event_time
	from today t 
	left join devices d 
	on t.device_id = d.device_id	
),
	device_today_deduped as (
	select 
		user_id,
		browser_type,
		event_time
	from device_today
	where row_num = 1	
),
	coa_users as (	
	select 
	coalesce(t.user_id, y.user_id) as user_id,
	coalesce(t.browser_type, y.browser_type) as browser_type,
	case 
		when y.dates_active is null
		then array[t.event_time]
		when t.event_time is null then y.dates_active
		else array[t.event_time] || y.dates_active 
		end as dates_active, 
	date(coalesce(t.event_time, y.date + interval '1 day')) 
	as date
	from device_today_deduped t full outer join yesterday y
	on t.user_id = y.user_id 
	and 
	t.browser_type = y.browser_type
)

select * from coa_users

	-- select 
	-- user_id,
	-- device_id,
	-- date(cast(event_time as timestamp)) as event_time
	-- from events 
	-- where ( date(cast(event_time as timestamp)) = date('2023-01-26')
	-- or date(cast(event_time as timestamp)) = date('2023-01-27'))
	-- and 
	-- user_id = '439578290726747300'
	-- ORDER BY user_id

	
	-- select 
	-- 	co.user_id,
	-- 	co.device_id,
	-- 	d.browser_type,
	-- 	co.dates_active,
	-- 	co.date
	-- from coa_users co 
	-- left join devices d 
	-- on co.device_id = d.device_id
	-- order by user_id

-- select * from user_devices_cumulated where date = date('2023-01-05') order by user_id

