
insert into actors

with last_year as (
	select * from actors
	where current_year = 1976
),
	next_year as (
	select * from actor_films
	where year = 1977
),
	films_quality as(
	select actorid, 
	avg(rating) as avg_rating
	from next_year
	group by actorid
	
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
	left join films_quality q
	on c.actorid = q.actorid
)

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
	case when c.films is not null then true else false end as is_active,
	coalesce(c.year, l.current_year + 1) as current_year

	from films_details c 
	full outer join last_year l 
	on c.actor = l.actor
	order by actor