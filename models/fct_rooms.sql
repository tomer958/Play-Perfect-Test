-- This table is refreshed hourly using a cron job: 0 * * * * (every hour)

{{ config(
    materialized='incremental',
    unique_key=['room_id', 'tournament_id'],
    partition_by={
        "field": "room_open_time",
        "data_type": "timestamp"
    },
    incremental_strategy='merge'
) }}
--gather basic details about each room and tournament when players join
with room_tournament_details as (
  select
    room_id,
    tournament_id,
    entry_fee,
    players_capacity
  from {{ ref('event_tournamentJoined') }}
  {{ incremental_filter('timestamp_utc', 'room_open_time') }}
)
,
--get the time when the room was opened and total coins spent (entry fees) by players
open_time as (
select
tournament_id,
    room_id,
   min(timestamp_utc) as room_open_time,
   sum(coalesce(entry_fee,0)) as total_coins_sink
from {{ ref('event_tournamentJoined') }}
group by 1,2
)
,

--get the time when the room was closed, total rewards distributed, and the number of players at close
close_time as (
select
tournament_id,
    room_id,
   max(timestamp_utc) as room_close_time,
   sum(coalesce(reward,0)) as total_coins_rewards
from {{ ref('event_tournamentRoomClosed') }}
   group by 1,2
),
--get average play duration and the number of active players during the match
match_duration as (
 SELECT
tournament_id,
    room_id,
   players_active_in_toom,
   avg(coalesce(play_duration,0)) as total_matches_duration,
from {{ ref('event_tournamentFinished') }}
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
