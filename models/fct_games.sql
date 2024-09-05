  {{
    config(
        materialized='table'
    )
 }}

  with balance_start as (
 SELECT
   date_utc,
   player_id,
   balance_before,
   rank() over (partition by player_id,date_utc order by timestamp_utc asc) as rank,
from play-pefect-test.dbt_tomer.events
 WHERE
event_name='tournamentJoined'
qualify rank=1
)
,
balance_end as (
 SELECT
   date_utc,
   player_id,
   balance_before as balance_day_end,
   rank() over (partition by player_id,date_utc order by timestamp_utc desc) as rank
from play-pefect-test.dbt_tomer.events
 WHERE
event_name='tournamentFinished'
qualify rank=1
)


,
match_played as (
 SELECT
   date_utc,
   player_id,
   count(*) as matches_played
from play-pefect-test.dbt_tomer.events
 WHERE
event_name='tournamentJoined'
group by 1,2
)
,
match_duration as (
 SELECT
   date_utc,
   player_id,
   sum(coalesce(play_duration,0)) as total_matches_duration,
from play-pefect-test.dbt_tomer.events
 WHERE
event_name='tournamentFinished'
group by 1,2
)
,
match_summary as (
 SELECT
   date_utc,
   player_id,
   max(coalesce(score,0)) as max_score,
 avg(coalesce(score,0)) as avg_score,
 max(position) as max_position,
 avg(position) as avg_position
from play-pefect-test.dbt_tomer.events
 WHERE
event_name='tournamentRoomClosed'
group by 1,2
)
,
match_reward as (
 SELECT
   date_utc,
   player_id,
   count(case when reward is not null or coins_claimed is not null then 1 else 0 end) as matches_won_reward,
   count(*) as matches_claimed
from play-pefect-test.dbt_tomer.events
 WHERE
event_name='tournamentRewardClaimed'
group by 1,2
)
,
coins_summary AS (
 SELECT
 date_utc,
   player_id,
   sum(case when event_name = 'tournamentJoined' then entry_fee else 0 end) as coins_sink_tournaments,
   sum(case when event_name = 'tournamentRewardClaimed' then reward else 0 end) as coins_source_tournaments
from play-pefect-test.dbt_tomer.events
 group by 1,2
)
,
purchases as (
 select
 date_utc,
 player_id,
 sum(price_usd) as revenue,
 sum(coins_claimed) as coins_source_purchases
from play-pefect-test.dbt_tomer.events
where event_name='purchase'
group by 1,2
)
,
event_data as (
 select
   player_id,
   date_utc,
   case
     when position = 0 then 1
     else 0
   end as is_win,
   case
     when coalesce(reward, 0) = 0 then 1
     else 0
   end as is_loss,
   row_number() over (partition by  player_id, date_utc order by date_utc) as event_order,
   lag(case
         when  position = 0 THEN 1
         else 0
       end, 1, 0) over (partition by player_id, date_utc order by date_utc) as previous_is_win,
   lag(case
         when coalesce(reward, 0) = 0 then 1
         else 0
       end, 1, 0) over (partition by  player_id, date_utc order by date_utc) as previous_is_loss
from play-pefect-test.dbt_tomer.events
 where event_name = 'tournamentRoomClosed'
),
streak_grouping as (
 select
   player_id,
   date_utc,
   is_win,
   is_loss,
   sum(case when is_win != previous_is_win then 1 else 0 end)
     over (partition by  player_id, date_utc order by  date_utc) as win_streak_group,
   sum(case when  is_loss != previous_is_loss then 1 else 0 end)
     over (partition by  player_id, date_utc order by  date_utc) as loss_streak_group
 from event_data
)
,
streak_lengths as (
 select
   player_id,
   date_utc,
   win_streak_group,
   loss_streak_group,
   count(case when is_win = 1 then 1 end) as win_streak_length,
   count(case when is_loss = 1 then 1 end) as loss_streak_length
 from streak_grouping
 group by player_id, date_utc, win_streak_group, loss_streak_group
),
final_streaks as (
 select
   player_id,
   date_utc,
   max(win_streak_length) as max_reward_won_streak,
   max(loss_streak_length) as max_losing_streak
 from streak_lengths
 group by player_id, date_utc
)


select
 balance_start.player_id,
 balance_start.date_utc,
 balance_start.balance_before,
 balance_end.balance_day_end,
 match_played.matches_played,
 match_duration.total_matches_duration,
 match_summary.max_score,
 match_summary.avg_score,
 match_summary.max_position,
 match_summary.avg_position,
 match_reward.matches_won_reward,
 match_reward.matches_claimed,
 coins_summary.coins_sink_tournaments,
 coins_summary.coins_source_tournaments,
 purchases.revenue,
 purchases.coins_source_purchases,
 final_streaks.max_reward_won_streak,
 final_streaks.max_losing_streak
FROM balance_start
LEFT JOIN balance_end
 ON balance_start.player_id = balance_end.player_id
 AND balance_start.date_utc = balance_end.date_utc
LEFT JOIN match_played
 ON balance_start.player_id = match_played.player_id
 AND balance_start.date_utc = match_played.date_utc
LEFT JOIN match_duration
 ON balance_start.player_id = match_duration.player_id
 AND balance_start.date_utc = match_duration.date_utc
LEFT JOIN match_summary
 ON balance_start.player_id = match_summary.player_id
 AND balance_start.date_utc = match_summary.date_utc
LEFT JOIN match_reward
 ON balance_start.player_id = match_reward.player_id
 AND balance_start.date_utc = match_reward.date_utc
LEFT JOIN coins_summary
 ON balance_start.player_id = coins_summary.player_id
 AND balance_start.date_utc = coins_summary.date_utc
LEFT JOIN purchases
 ON balance_start.player_id = purchases.player_id
 AND balance_start.date_utc = purchases.date_utc
LEFT JOIN final_streaks
 ON balance_start.player_id = final_streaks.player_id
 AND balance_start.date_utc = final_streaks.date_utc