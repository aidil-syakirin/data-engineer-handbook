
-- task 5: DDL for host_cumulated table

create table host_cumulated(
	host text,
	dates_active date[],
	date date,
	primary key(host, date)
)
