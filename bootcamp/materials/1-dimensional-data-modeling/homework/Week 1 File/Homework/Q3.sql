create table if not exists actors_history_scd(
	actor text,
	actorid text,
	quality_class quality_class,
	is_active boolean,
	start_year integer, 
	end_year integer,
	current_year integer,
	primary key (actor, actorid, start_year)	
);
