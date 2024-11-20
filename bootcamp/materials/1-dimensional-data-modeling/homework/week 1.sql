-- select * from actor_films;

-- select min(year) from actor_films;

-- create type films as (
-- 	film text,
-- 	votes integer,
-- 	rating real,
-- 	filmid text
-- );

-- create type quality_class as enum ('star','good','average','bad');

-- create table actors (
-- 	actor text,
-- 	actorid text,
-- 	films films[],
-- 	quality_class quality_class,
-- 	current_year integer,
-- 	primary key (actorid,current_year)
-- );

with last_year as (
	select * from actors
	where current_year = 1969
),
	next_year as (
	select * from actor_films
	where year = 1970
)

select 
	coalesce(c.actor, l.actor) as actor,
	coalesce(c.actorid, l.actorid) as actorid,
	case when l.films is null
		then array[row( 
					c.film,
					c.votes,
					c.rating,
					c.filmid
						)::films]
		when c.film is not null then l.films || array[row( 
					c.film,
					c.votes,
					c.rating,
					c.filmid
						)::films]
		else l.films
		end as films,
	case when c.film is not null then
		case 	when c.rating > 8 then 'star'
				when c.rating > 7 then 'good'
				when c.rating > 6 then 'average'
				else 'bad'
		end::quality_class
		else l.quality_class end as quality_clas,
	
	coalesce(c.year, l.current_year + 1) as current_year

	from next_year c full outer join last_year l
	on c.actorid = l.actorid


