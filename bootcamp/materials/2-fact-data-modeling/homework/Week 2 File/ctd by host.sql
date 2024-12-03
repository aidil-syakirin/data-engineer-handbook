-- create table host_cumulated(
-- 	host text,
-- 	dates_active date[],
-- 	date date,
-- 	primary key(host, date)
-- )

-- insert into host_cumulated

with 
	today_deduped as (
	select 	
			row_number() over (partition by host, event_time ) as row_num,
			*
	from events
	where 
	date(cast(event_time as timestamp)) = date('2023-01-03')

), today as (
	select
		host,
		date(cast(event_time as timestamp)) as event_time
	-- ,count(1) as num_site_hits
	from today_deduped
	where user_id is not null
	and row_num = 1
	group by host, date(cast(event_time as timestamp))
),
	yesterday as (
	select * 
	from host_cumulated
	where date = date('2023-01-02')
),
	c_hosts as (	
	select 
	coalesce(t.host, y.host) as host,
	case 
		when y.dates_active is null
		then array[t.event_time]
		when t.event_time is null then y.dates_active
		else array[t.event_time] || y.dates_active 
		end as dates_active, 
	date(coalesce(t.event_time, y.date + interval '1 day')) 
	as date
	from today t full outer join yesterday y
	on t.host = y.host 

)

select * from c_hosts 

-- select * from host_cumulated