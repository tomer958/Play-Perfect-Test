  {{
    config(
        materialized='table'
    )
 }}

 with room_tournament_details as (
 select
   room_id,
   tournament_id,
   entry_fee,
   players_capacity,
 timestamp_utc as joined_time,
from play-pefect-test.dbt_tomer.events
where event_name='tournamentJoined'
)
,
open_time as (
select
tournament_id,
    room_id,
   min(timestamp_utc) as room_open_time,
   sum(coalesce(entry_fee,0)) as total_coins_sink
from play-pefect-test.dbt_tomer.events
   where event_name='tournamentJoined'
group by 1,2
)
,
close_time as (
select
tournament_id,
    room_id,
   max(timestamp_utc) as room_close_time,
   sum(coalesce(reward,0)) as total_coins_rewards
from play-pefect-test.dbt_tomer.events
   where event_name='tournamentRoomClosed'
   group by 1,2
),
match_duration as (
 SELECT
tournament_id,
    room_id,
   players_active_in_toom,
   avg(coalesce(play_duration,0)) as total_matches_duration,
from play-pefect-test.dbt_tomer.events
 WHERE
event_name='tournamentFinished'
group by 1,2,3
)


select
 room_tournament_details.room_id,
 room_tournament_details.tournament_id,
 room_tournament_details.players_capacity,
 room_tournament_details.entry_fee,
 open_time.room_open_time,
 close_time.room_close_time,
 match_duration.players_active_in_toom as actual_players,
 open_time.total_coins_sink,
 close_time.total_coins_rewards,
 match_duration.total_matches_duration,
 timestamp_diff(close_time.room_close_time, open_time.room_open_time,minute) as room_open_duration,
 case
   when timestamp_diff(close_time.room_close_time, open_time.room_open_time,minute)>45
   or match_duration.players_active_in_toom = room_tournament_details.players_capacity
   then true
   else false
 end as is_closed,
 case
   when match_duration.players_active_in_toom = room_tournament_details.players_capacity then true
   else false
 end as is_full
from room_tournament_details
left join open_time
 on room_tournament_details.room_id = open_time.room_id
 and room_tournament_details.tournament_id = open_time.tournament_id
left join close_time
 on room_tournament_details.room_id = close_time.room_id
 and room_tournament_details.tournament_id = close_time.tournament_id
left join match_duration
 on room_tournament_details.room_id = match_duration.room_id
 and room_tournament_details.tournament_id = match_duration.tournament_id
