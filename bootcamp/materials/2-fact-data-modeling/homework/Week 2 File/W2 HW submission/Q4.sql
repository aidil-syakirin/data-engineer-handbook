
-- task 4: datelist generation query
	
with user_devices as (
	select 
 	* 
	from user_devices_cumulated 
	where date = '2023-01-31' 
), 
	series_date as (
	select
	* 
	from generate_series(date('2023-01-01'),date('2023-01-31'), interval '1 day') as series_date
),
	user_series as (
	select ud.*
	, sd.series_date
	from user_devices ud 
	cross join series_date sd 
), 
	place_holder_ints as (

	select 
	-- must change the series date column into an array before comparing with the dates_active array (an array rule)
	case when dates_active @> array[date(series_date)] 
	then cast(pow(2, 32 - (date - date(series_date))) as bigint)
	else 0 
	end as placeholder_int_value
	, *
	from user_series
)

	select 
		user_id,
		browser_type, 
	cast(cast(sum(placeholder_int_value) as bigint) as bit(32)),
	
	bit_count(cast(cast(sum(placeholder_int_value) as bigint) as bit(32))) > 0 
	as dim_monthly_active,
	
	--use AND gate to nullify bit except the first 7 days(bits) to track the user weekly activity
	bit_count(cast('11111110000000000000000000000000' as bit(32)) & 
	cast(cast(sum(placeholder_int_value) as bigint) as bit(32))) > 0
	as dim_weekly_active,
	
	--use AND gate to nullify bit except the first day(bit) to track the user latest activity
	bit_count(cast('10000000000000000000000000000000' as bit(32)) & 
	cast(cast(sum(placeholder_int_value) as bigint) as bit(32))) > 0 
	as dim_daily_active
	
	from place_holder_ints
	group by user_id, browser_type
