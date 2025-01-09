-- state change tracking query for players table
WITH players_table AS(
	SELECT 
		-- these are non changing details of our players 
		player_name,
		college, 
		country,
		--these 2 parameters (is_active and current_season) are crucial for tracking the player state change 
		is_active,
		current_season
		FROM players 
),
	self_joined AS(
	SELECT 
		coalesce(l.player_name,n.player_name) as player_name,
		coalesce(l.college,n.college) as college,
		coalesce(l.country,n.country) as country,
		case
		-- handling case when player is new to the league by giving it false value instead of null
			when l.is_active is null then false 
				else l.is_active end
					as was_active,
		case
		-- handling case when player is new to the league by giving it '0000' value instead of null
			when l.current_season is null then '0'
				else l.current_season end
					as last_season,
		n.is_active,
		n.current_season
	FROM players_table l right join players_table n
	ON l.player_name = n.player_name
	-- to ensure we self join the table with gap of 1 year of data
	AND n.current_season - l.current_season = 1
),	
	state_change as (
	SELECT 
		player_name,
		college,
		country,
		last_season,
		current_season,
		case 
			when was_active = false and last_season = 0 then 'New'
			when was_active = true and is_active = false then 'Retired'
			when was_active = true and is_active = true then 'Continue Playing'
			when was_active = false and is_active = true then 'Returned from Retirement'
			when was_active = false and is_active = false then 'Stayed Retired'
		end as state_change
		FROM self_joined 
)	

select * from state_change where player_name = 'Michael Jordan'