with 
	-- these first 2 CTEs are for removing duplicates if any
	team_identifier as (
	select 
		abbreviation, 
		nickname,
		team_id,
		row_number() over(partition by abbreviation, team_id) as row_num
	from teams
),
	game_results as (
	select 	
		game_date_est as date,
		home_team_id as team_id,
		case 
			when pts_home > pts_away then 1 else 0
		end as win_flag
	from games
	
	union all

	select 	
		game_date_est as date,
		visitor_team_id as team_id,
		case 
			when pts_home > pts_away then 1 else 0
		end as win_flag
	from games
	
),
	cummulative_wins as (
	select 
		team_id,
        date,
        sum(win_flag) OVER 
			(PARTITION BY team_id ORDER BY date 
			ROWS BETWEEN 89 PRECEDING AND CURRENT ROW) 
			AS wins_90_game_stretch
    FROM
        game_results 
),
	most_wins as (
	select 
		team_id,
		max(wins_90_game_stretch) as max_wins
		from cummulative_wins
		group by team_id
	)

select 
	t.abbreviation as team_name,
	t.nickname,
	mw.max_wins
	from most_wins mw
	join team_identifier t
	on mw.team_id = t.team_id
	where t.row_num = 1
	order by mw.max_wins desc

