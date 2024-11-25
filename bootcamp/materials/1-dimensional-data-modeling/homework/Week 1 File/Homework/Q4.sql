insert into actors_history_scd

with
	years as (
	select * from generate_series(1970,2021) as year
),
	p as (
	select actor, actorid, min(year) as start_year
	from actor_films
	group by actor, actorid
),
	actor_career as(
	select * from p 
	join years y
	on p.start_year <= y.year	
),
	ia_actors as (
	select c.actor, 
			c.actorid,
			array_remove(
			array_agg( 
				case when a.year is not null then cast(row(  
					a.film,
					a.votes, 
					a.rating,
					a.filmid
						) as films)	
				end) over (partition by c.actorid order by coalesce(a.year,c.year)),null) as films,
			c.year,
			case when a.film is not null then true else false end as is_active
	from actor_career c  
	left join 
	actor_films a
	on 
	c.actor = a.actor 
	and c.year = a.year
	group by c.actor, c.actorid, c.year, a.year, a.film, a.votes, a.rating, a.filmid
),
	tcom_actor as (
		select 
			distinct(fg.actorid, fg.year),
			fg.actor,
			fg.actorid, 
			fg.films,
			case 	when (films[CARDINALITY(films)]::films).rating > 8 then 'star'
				when (films[CARDINALITY(films)]::films).rating > 7 then 'good'
				when (films[CARDINALITY(films)]::films).rating > 6 then 'average'
				else 'bad'
			end::quality_class as quality_class,
			is_active,
			fg.year as current_year
		from ia_actors fg

), 	
	final_actor as ( 
	select 
	actor, actorid, films, quality_class, is_active, current_year
	from tcom_actor 
),	
	with_previous as (
	select actor, 
		actorid,
		current_year,
		quality_class, 
		is_active,
		lag(quality_class, 1) over (partition by actor order by current_year) as previous_quality_class,
		lag(is_active, 1) over (partition by actor order by current_year) as previous_is_active
		from final_actor
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