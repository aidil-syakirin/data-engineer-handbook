-- drop table actors_scd

create table if not exists actors_scd(
	actor text,
	actorid text,
	quality_class quality_class,
	is_active boolean,
	start_year integer, 
	end_year integer,
	current_year integer,
	primary key (actor, actorid, start_year)	
);

-- insert into actors_scd
	
with with_previous as (
	select actor, 
		actorid,
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
)
select actor,
	actorid,
	quality_class,
	is_active, 
	min(current_year) as start_year,
	max(current_year) as end_year,
	2021 as current_year
	from with_streaks
	group by actor, actorid, streak_identifier, is_active, quality_class
	order by actor, streak_identifier

select * from actors_scd where actor = 'Charlie Murphy';
