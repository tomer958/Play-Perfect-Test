{{ config(
    materialized='incremental',
    unique_key=['player_id', 'room_id', 'tournament_id'],
    partition_by={
        "field": "date_utc",
        "data_type": "date"
    },
    incremental_strategy='merge'
) }}

WITH joined_time_table AS (
  SELECT
    date_utc,
    player_id,
    room_id,
    tournament_id,
    balance_before,
    entry_fee,
    players_capacity,
    timestamp_utc AS joined_time
  FROM {{ source('dbt_tomer', 'events') }}
  WHERE event_name = 'tournamentJoined'
  {% if is_incremental() %}
    AND date_utc >= (SELECT MAX(date_utc) FROM {{ this }})
  {% endif %}
),
submit_time_table AS (
  SELECT
    player_id,
    room_id,
    tournament_id,
    play_duration,
    players_submited,
    players_active_in_toom AS actual_players_in_room,
    score,
    timestamp_utc AS submit_time
  FROM play-pefect-test.dbt_tomer.events
  WHERE event_name = 'tournamentFinished'
),
room_close_time_table AS (
  SELECT
    player_id,
    room_id,
    tournament_id,
    IFNULL(reward, coins_claimed) AS reward,
    balance_before + IFNULL(reward, coins_claimed) AS balance_after_claim,
    position,
    score,
    timestamp_utc AS room_close_time
  FROM play-pefect-test.dbt_tomer.events
  WHERE event_name = 'tournamentRoomClosed'
),
room_reward_time_table AS (
  SELECT
    player_id,
    room_id,
    tournament_id,
    timestamp_utc AS claim_time
  FROM play-pefect-test.dbt_tomer.events
  WHERE event_name = 'tournamentRewardClaimed'
)

SELECT
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
  IFNULL(room_close_time_table.score, submit_time_table.score) AS score,
  position,
  reward,
  CASE WHEN claim_time IS NULL THEN FALSE ELSE TRUE END AS did_claim_reward,
  claim_time
FROM joined_time_table
LEFT JOIN submit_time_table
  ON joined_time_table.player_id = submit_time_table.player_id
  AND joined_time_table.room_id = submit_time_table.room_id
  AND joined_time_table.tournament_id = submit_time_table.tournament_id
LEFT JOIN room_close_time_table
  ON joined_time_table.player_id = room_close_time_table.player_id
  AND joined_time_table.room_id = room_close_time_table.room_id
  AND joined_time_table.tournament_id = room_close_time_table.tournament_id
LEFT JOIN room_reward_time_table
  ON joined_time_table.player_id = room_reward_time_table.player_id
  AND joined_time_table.room_id = room_reward_time_table.room_id
  AND joined_time_table.tournament_id = room_reward_time_table.tournament_id
