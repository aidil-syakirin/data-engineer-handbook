-- create table if not exists actors (
-- 	actor text,
-- 	actorid text,
-- 	films films[],
-- 	quality_class quality_class,
-- 	is_active boolean,
-- 	current_year integer
-- );

-- create type scd_type as (
-- 	quality_class quality_class,
-- 	is_active boolean,
-- 	start_year integer,
-- 	end_year integer	
-- )

-- -- drop table actors_scd
-- create table if not exists actors_scd(
-- 	actor text,
-- 	quality_class quality_class,
-- 	is_active boolean,
-- 	start_year integer, 
-- 	end_year integer,
-- 	current_year integer,
-- 	primary key (actor, start_year)	
-- );

-- insert into actors_scd
	
with with_previous as (
	select actor, 
		current_year,
		quality_class, 
		is_active,
		lag(quality_class, 1) over (partition by actor order by current_year) as previous_quality_class,
		lag(is_active, 1) over (partition by actor order by current_year) as previous_is_active
		from bf_actors 
		where current_year <= 2021
),
	with_indicators as (
	select *, 
		case 	when quality_class <> previous_quality_class then 1
				when is_active <> previous_is_active then 1 else 0 end as change_indicator
	from with_previous		
), 
	with_streaks as (
	select *,
		sum(change_indicator) over (partition by actor order by current_year) as streak_identifier
		from with_indicators
), 
	last_year_scd as (
	select * from actors_scd
	where current_year = 2020
	and end_year = 2020
),
	historical_scd as (
	select actor, quality_class, is_active, start_year, end_year 
	from actors_scd
	where current_year = 2021
	and end_year < 2021
),
	this_year_data as (
	select * 
	from bf_actors
	where current_year = 2021
),
	unchanged_records as (
	select 	ty.actor,
		ty.quality_class, ty.is_active,
		ly.start_year, ty.current_year as end_season
	from this_year_data ty 
	join last_year_scd ly
	on ty.actor = ly.actor
	where ty.quality_class = ly.quality_class and ty.is_active = ly.is_active
),
	changed_records as (
	select 	ty.actor,
		unnest(
		array[
			row(ly.quality_class,
				ly.is_active,
				ly.start_year,
				ly.end_year)::scd_type,
			row(ty.quality_class,
				ty.is_active,
				ty.current_year,
				ty.current_year)::scd_type
		]) as records
	from this_year_data ty 
	left join last_year_scd ly 
	on ty.actor = ly.actor
	where (ty.quality_class <> ly.quality_class or ty.is_active <> ly.is_active)
), 
	unnested_changed_records as (
	select actor, 
	(records::scd_type).quality_class,
	(records::scd_type).is_active,
	(records::scd_type).start_year,
	(records::scd_type).end_year
	from changed_records
	
),
	new_records as (
	select ty.actor,
			ty.quality_class,
			ty.is_active,
			ty.current_year as start_year,
			ty.current_year as end_year
	from this_year_data ty left join last_year_scd ly
	on ty.actor = ly.actor where ly.actor is null
	
)


select * from historical_scd
union all	
select * from unchanged_records
union all	
select * from unnested_changed_records
union all
select * from new_records
	
