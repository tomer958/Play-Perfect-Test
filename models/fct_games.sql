  {{
    config(
        materialized='table'
    )
 }}

  with joined_time_table as (
 SELECT
   date_utc,
   player_id,
   room_id,
   tournament_id,
   balance_before,
   entry_fee,
   players_capacity,
 timestamp_utc as joined_time
from play-pefect-test.dbt_tomer.events
 WHERE
event_name='tournamentJoined'
)
,submit_time_table as (
   SELECT
   player_id,
   room_id,
   tournament_id,
   play_duration,
   players_submited,
   players_active_in_toom as actual_players_in_room,
   score,
 timestamp_utc  as submit_time
from play-pefect-test.dbt_tomer.events
 WHERE
event_name='tournamentFinished'
)
,room_close_time_table as (
   SELECT
   player_id,
   room_id,
   tournament_id,
   ifnull(reward,coins_claimed) as reward,
   balance_before + ifnull(reward,coins_claimed) as balance_after_claim,
   position,
   score,
 timestamp_utc  as room_close_time
from play-pefect-test.dbt_tomer.events
 WHERE
event_name='tournamentRoomClosed'
)


,room_reward_time_table as (
   SELECT
   player_id,
   room_id,
   tournament_id,
 timestamp_utc  as claim_time
from play-pefect-test.dbt_tomer.events
 WHERE
event_name='tournamentRewardClaimed'
)

select
joined_time_table.player_id,
joined_time_table.date_utc,
joined_time,
submit_time,
room_close_time,
play_duration,
balance_before,
balance_after_claim,
joined_time_table.tournament_id,
joined_time_table.room_id,
entry_fee,
   players_capacity,
   actual_players_in_room,
   ifnull(room_close_time_table.score,submit_time_table.score) as score,
   position,
   reward,
    case when claim_time is null then false Else true end as did_claim_reward,
   claim_time
from joined_time_table
left join submit_time_table
on    
joined_time_table.player_id=submit_time_table.player_id
   and joined_time_table.room_id=submit_time_table.room_id and joined_time_table.tournament_id=submit_time_table.tournament_id
   left join room_close_time_table
on    
joined_time_table.player_id=room_close_time_table.player_id
   and joined_time_table.room_id=room_close_time_table.room_id and joined_time_table.tournament_id=room_close_time_table.tournament_id
       left join room_reward_time_table
on    
joined_time_table.player_id=room_reward_time_table.player_id
   and joined_time_table.room_id=room_reward_time_table.room_id and joined_time_table.tournament_id=room_reward_time_table.tournament_id