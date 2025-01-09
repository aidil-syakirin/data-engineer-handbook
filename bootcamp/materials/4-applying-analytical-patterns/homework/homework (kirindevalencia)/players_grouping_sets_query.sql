with 
	-- these first 2 CTEs are for removing duplicates if any
	deduped_gd as(
	select *, 
	row_number() over (partition by game_id, team_id, player_id) as row_num
	from game_details
),
	deduped_g as(
	select *, 
	row_number() over (partition by game_id, home_team_id, visitor_team_id) as row_num
	from games
),
	games_acc as (
	select 
	-- selecting all the required columns for aggregation later on
		gd.game_id, 
		gd.team_abbreviation,
		gd.team_id,
		gd.player_name,
	-- for this query, we will be focusing on the pts value of the player
		gd.pts,
		g.season,
	-- these last 4 columns are for deciding which team win in each game record 
		g.home_team_id,
		g.visitor_team_id,
		g.pts_home,
		g.pts_away
		from deduped_gd gd
		join deduped_g g
		on gd.game_id = g.game_id
		where 
		gd.pts is not null
		and 
		gd.row_num = 1
),
	home_away as(
	select 
			game_id,
			season,
			team_abbreviation,
			player_name,
			pts,
			case 
		-- for deciding the player for home or away team
				when team_id = home_team_id then 'HOME'
				else 'AWAY' end as home_or_away,
			case 
		-- for deciding which teams win in the match
				when pts_home > pts_away then 'HOME'
				when pts_home < pts_away then 'AWAY'
				else 'DRAW'	 end as winning_team	
			from games_acc
),
	pts_group_set as (
	select 
		-- to mark the aggregration level at season
		coalesce(season, '0000' ) as season,
		-- to mark the aggregration level at team
		coalesce(team_abbreviation,'(overall)') as team_abbreviation,
		-- to mark the aggregration level at player
		coalesce(player_name, '(overall)') as player_name,
		sum(pts) as total_pts,
		-- to count the total win of the teams
		sum(case when home_or_away = winning_team then 1 else 0 end) as total_win
		from home_away 
		group by grouping sets(
		(team_abbreviation, player_name),
		(season, player_name),
		(team_abbreviation)
		)
)

select *
	from pts_group_set
	where 
	-- where parameter for Q1: player with most points for one team
	-- season = 0 and player_name <> '(overall)'
	-- where parameter for Q2: player with most point in one season
	-- season > 0
	-- where parameter for Q3: team with most wins
	-- player_name = '(overall)'
