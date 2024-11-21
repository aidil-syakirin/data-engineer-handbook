-- select * from actor_films where year = 1970;

-- select min(year) from actor_films;

-- create type films as (
-- 	film text,
-- 	votes integer,
-- 	rating real,
-- 	filmid text
-- );

-- create type quality_class as enum ('star','good','average','bad');
-- drop table actors;
-- create table if not exists actors (
-- 	actor text,
-- 	actorid text,
-- 	films films[],
-- 	quality_class quality_class,
-- 	current_year integer
-- );

-- insert into actors

with last_year as (
	select * from actors
	where current_year = 1969
),
	next_year as (
	select * from actor_films
	where year = 1970
),
	films_quality as(
	select actorid, 
	sum(rating) over()::numeric / count(rating) over () as avg_rating 
	from next_year
	
),
	films_agg as(
	select c.actor, 
			c.actorid,
			array_agg(row(  
					c.film,
					c.votes,
					c.rating,
					c.filmid
						)::films)				
			as films,
			c.year
	from next_year c 
	group by c.actor, c.actorid, c.year
),
	films_details as(
	select c.actor, 
			c.actorid,
			c.films,
			c.year,
			q.avg_rating
	from films_agg c 
	inner join films_quality q 
	on c.actorid = q.actorid
)

	-- select * from films_agg order by actor
select 
	coalesce(c.actor, l.actor) as actor,
	coalesce(c.actorid, l.actorid) as actorid,
	case when l.films is null
		then c.films
		when c.films is not null then l.films || c.films
		else l.films
		end as films,
	case when avg_rating is not null then
		case 	when avg_rating > 8 then 'star'
				when avg_rating > 7 then 'good'
				when avg_rating > 6 then 'average'
				else 'bad'
		end::quality_class
		else l.quality_class end as quality_class,
	coalesce(c.year, l.current_year + 1) as current_year

	from films_details c 
	full outer join last_year l 
	on c.actor = l.actor



-- select * from actors order by actor;