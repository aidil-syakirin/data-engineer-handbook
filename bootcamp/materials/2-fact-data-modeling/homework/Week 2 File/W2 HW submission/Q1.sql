-- task 1: deduped version of game_details

with deduped as (
	select *,
	row_number() over(partition by team_id,player_id,game_id) as row_num
	from game_details
)
select * from deduped where row_num = 1