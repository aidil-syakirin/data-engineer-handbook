-- drop table bf_actors
create table if not exists bf_actors (
	actor text,
	actorid text,
	films films[],
	quality_class quality_class,
	is_active boolean,
	current_year integer
);


insert into bf_actors
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
	ar_actor as (
	select af.actor, actorid,
	af.year,
	avg(af.rating) as avg_rating
	from 
	actor_films af
	group by af.actorid, actor, af.year
	order by actor, year
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
	group by c.actor, c.actorid, c.year
	,a.year,a.film, a.votes, a.rating, a.filmid
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
	-- left join ar_actor ar
	-- on fg.actorid = ar.actorid and fg.year = ar.year	

)
-- 	,
-- 	tactor_dedupe as (
-- 	select 
-- 	distinct (actorid, year), actor,
-- 	actorid, films, avg_rating, is_active, year
-- 	from tcom_actor
-- )

	-- case when ar.avg_rating is null then 
	-- 			coalesce(ar.avg_rating,	lag(ar.avg_rating) over (partition by fg.actorid order by fg.year ))
	-- 			else ar.avg_rating end as avg_rating,
	
-- select 
-- 	actor,
-- 	actorid,
-- 	films as films,
-- 		case 	when avg_rating > 8 then 'star'
-- 				when avg_rating > 7 then 'good'
-- 				when avg_rating > 6 then 'average'
-- 				else 'bad'
-- 		end::quality_class as quality_class,
-- 	is_active,
-- 	year as current_year
-- 	from tactor_dedupe
-- 	order by actor, current_year

select 
	actor, actorid, films, quality_class, is_active, current_year
	from tcom_actor 
	-- limit 20

select * from bf_actors where actor = '50 Cent'

