create type films as (
	film text,
	votes integer,
	rating real,
	filmid text
);

create type quality_class as enum ('star','good','average','bad');

create table if not exists actors (
	actor text,
	actorid text,
	films films[],
	quality_class quality_class,
	is_active boolean,
	current_year integer
);