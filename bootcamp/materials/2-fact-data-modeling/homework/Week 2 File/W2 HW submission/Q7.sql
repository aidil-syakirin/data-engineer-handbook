
-- task 7: DDL for monthly reduced fact table 
create table host_activity_reduced (
	host text,
	month_start date,
	hit_array real[],
	unique_visitor_array integer[],
	primary key(host, month_start)
)
