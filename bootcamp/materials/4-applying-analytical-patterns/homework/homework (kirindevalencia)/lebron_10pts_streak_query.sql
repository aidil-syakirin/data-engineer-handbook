-- select * from games limit 10;
with 
	-- these first 2 CTEs are for removing duplicates if any
	deduped_gd as(
	select *, 
	row_number() over (partition by game_id, team_id, player_id) as row_num
	from game_details
	where player_name = 'LeBron James'
),
	games_acc as (
	select 
		gd.game_id, 
		gd.player_name,
		gd.pts,
		g.game_date_est,
		g.season
		from deduped_gd gd
		join games g
		on gd.game_id = g.game_id
		where 
		gd.row_num = 1
),
	streaks as (
	select *,
	-- flag to identify if the pts > 10 condition is met
	case when pts > 10 then 1 else 0 end as pts_flag,
	-- create a grouping key based on consecutive pts > 10
	row_number() over (order by game_date_est) -
	row_number() over 
	(partition by 
		case when pts > 10 then 1 else 0
		end order by game_date_est) as group_key
	from games_acc
)
	
SELECT
    MIN(game_date_est) AS streak_start,
    MAX(game_date_est) AS streak_end,
    COUNT(*) AS games_in_streak
FROM
    streaks
WHERE
    pts_flag = 1
GROUP BY
    group_key
ORDER BY
    streak_start;
