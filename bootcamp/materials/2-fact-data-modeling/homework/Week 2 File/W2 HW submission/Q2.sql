
-- task 2: create ddl for user_devices_cumulated

create table user_devices_cumulated(
	user_id numeric,
	browser_type text,
	dates_active date[],
	date date,
	primary key(user_id, browser_type, date)
)
