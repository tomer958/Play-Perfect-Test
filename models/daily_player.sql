  {{
    config(
        materialized='table'
    )
 }}

 WITH player_first_appearance AS (
  -- Get the first date a player appears in the dataset
  SELECT 
    player_id, 
    MIN(date_utc) AS first_appearance_date
  FROM play-pefect-test.dbt_tomer.events
  GROUP BY player_id
),
date_table AS (
  -- Find the minimum and maximum dates in the dataset for generating all dates
  SELECT 
    MIN(date_utc) AS min_date, 
    MAX(date_utc) AS max_date
  FROM play-pefect-test.dbt_tomer.events
),
all_dates AS (
  -- Create a table of all dates for each player starting from their first appearance
  SELECT 
    player_id, 
    date_utc
  FROM player_first_appearance, 
       UNNEST(GENERATE_DATE_ARRAY(first_appearance_date, (SELECT max_date FROM date_table))) AS date_utc
),
balance_start AS (
  -- Fetch the starting balance for each player on each date based on their first tournamentJoined event
  SELECT
    date_utc,
    player_id,
    balance_before,
    RANK() OVER (PARTITION BY player_id, date_utc ORDER BY timestamp_utc ASC) AS rank
  FROM play-pefect-test.dbt_tomer.events
  WHERE event_name='tournamentJoined'
  QUALIFY rank=1
),
balance_end AS (
  -- Fetch the end-of-day balance for each player on each date based on their last tournamentFinished event
  SELECT
    date_utc,
    player_id,
    balance_before AS balance_day_end,
    RANK() OVER (PARTITION BY player_id, date_utc ORDER BY timestamp_utc DESC) AS rank
  FROM play-pefect-test.dbt_tomer.events
  WHERE event_name='tournamentFinished'
  QUALIFY rank=1
),
match_played AS (
  -- Calculate the number of matches played by a player on each date
  SELECT
    date_utc,
    player_id,
    COUNT(*) AS matches_played
  FROM play-pefect-test.dbt_tomer.events
  WHERE event_name='tournamentJoined'
  GROUP BY 1, 2
),
match_duration AS (
  -- Calculate the total duration of matches played by a player on each date
  SELECT
    date_utc,
    player_id,
    SUM(COALESCE(play_duration, 0)) AS total_matches_duration
  FROM play-pefect-test.dbt_tomer.events
  WHERE event_name='tournamentFinished'
  GROUP BY 1, 2
),
match_summary AS (
  -- Calculate the max score, average score, max position, and average position of a player on each date
  SELECT
    date_utc,
    player_id,
    MAX(COALESCE(score, 0)) AS max_score,
    AVG(COALESCE(score, 0)) AS avg_score,
    MAX(position) AS max_position,
    AVG(position) AS avg_position
  FROM play-pefect-test.dbt_tomer.events
  WHERE event_name in('tournamentRoomClosed','tournamentRewardClaimed')
  GROUP BY 1, 2
),
match_reward AS (
  -- Calculate the number of matches won and claimed by a player on each date
  SELECT
    date_utc,
    player_id,
    COUNT(CASE WHEN reward IS NOT NULL OR coins_claimed IS NOT NULL THEN 1 ELSE 0 END) AS matches_won_reward,
    COUNT(*) AS matches_claimed
  FROM play-pefect-test.dbt_tomer.events
  WHERE event_name='tournamentRewardClaimed'
  GROUP BY 1, 2
),
coins_summary AS (
  -- Calculate the total coins spent (sink) and total rewards (source) from tournaments for each player on each date
  SELECT
    date_utc,
    player_id,
    SUM(CASE WHEN event_name = 'tournamentJoined' THEN entry_fee ELSE 0 END) AS coins_sink_tournaments,
    SUM(CASE WHEN event_name = 'tournamentRewardClaimed' THEN reward ELSE 0 END) AS coins_source_tournaments
  FROM play-pefect-test.dbt_tomer.events
  GROUP BY 1, 2
),
purchases AS (
  -- Calculate the total revenue and coins from purchases for each player on each date
  SELECT
    date_utc,
    player_id,
    SUM(price_usd) AS revenue,
    SUM(coins_claimed) AS coins_source_purchases
  FROM play-pefect-test.dbt_tomer.events
  WHERE event_name='purchase'
  GROUP BY 1, 2
),
event_data AS (
  -- Identify win/loss events for each player on each date
  SELECT
    player_id,
    date_utc,
    CASE
      WHEN position = 0 THEN 1
      ELSE 0
    END AS is_win,
    CASE
      WHEN COALESCE(reward, 0) = 0 THEN 1
      ELSE 0
    END AS is_loss,
    ROW_NUMBER() OVER (PARTITION BY player_id, date_utc ORDER BY date_utc) AS event_order,
    LAG(CASE WHEN position = 0 THEN 1 ELSE 0 END, 1, 0) OVER (PARTITION BY player_id, date_utc ORDER BY date_utc) AS previous_is_win,
    LAG(CASE WHEN COALESCE(reward, 0) = 0 THEN 1 ELSE 0 END, 1, 0) OVER (PARTITION BY player_id, date_utc ORDER BY date_utc) AS previous_is_loss
  FROM play-pefect-test.dbt_tomer.events
  WHERE event_name = 'tournamentRoomClosed'
),
streak_grouping AS (
  -- Group win/loss streaks for each player on each date
  SELECT
    player_id,
    date_utc,
    is_win,
    is_loss,
    SUM(CASE WHEN is_win != previous_is_win THEN 1 ELSE 0 END) 
      OVER (PARTITION BY player_id, date_utc ORDER BY date_utc) AS win_streak_group,
    SUM(CASE WHEN is_loss != previous_is_loss THEN 1 ELSE 0 END) 
      OVER (PARTITION BY player_id, date_utc ORDER BY date_utc) AS loss_streak_group
  FROM event_data
),
streak_lengths AS (
  -- Calculate the length of win/loss streaks for each player on each date
  SELECT
    player_id,
    date_utc,
    win_streak_group,
    loss_streak_group,
    COUNT(CASE WHEN is_win = 1 THEN 1 END) AS win_streak_length,
    COUNT(CASE WHEN is_loss = 1 THEN 1 END) AS loss_streak_length
  FROM streak_grouping
  GROUP BY player_id, date_utc, win_streak_group, loss_streak_group
),
final_streaks AS (
  -- Get the max win/loss streak for each player on each date
  SELECT
    player_id,
    date_utc,
    MAX(win_streak_length) AS max_reward_won_streak,
    MAX(loss_streak_length) AS max_losing_streak
  FROM streak_lengths
  GROUP BY player_id, date_utc
),
final_data AS (
  -- Join all calculated metrics to generate a comprehensive dataset for each player and date
  SELECT 
    all_dates.player_id, 
    all_dates.date_utc,
    balance_start.balance_before,
    balance_end.balance_day_end,
    match_played.matches_played,
    match_duration.total_matches_duration,
    match_summary.max_score,
    ROUND(match_summary.avg_score, 3) AS avg_score,  -- Rounding avg_score to 3 decimal places
    match_summary.max_position,
    ROUND(match_summary.avg_position, 3) AS avg_position,  -- Rounding avg_position to 3 decimal places
    COALESCE(match_reward.matches_won_reward, 0) AS matches_won_reward,  -- Replace NULL with 0
    COALESCE(match_reward.matches_claimed, 0) AS matches_claimed,  -- Replace NULL with 0
    coins_summary.coins_sink_tournaments,
    coins_summary.coins_source_tournaments,
    COALESCE(purchases.revenue, 0) AS revenue,  -- Replace NULL with 0
    COALESCE(purchases.coins_source_purchases, 0) AS coins_source_purchases,  -- Replace NULL with 0
    final_streaks.max_reward_won_streak,
    final_streaks.max_losing_streak
  FROM all_dates
  LEFT JOIN balance_start 
    ON all_dates.player_id = balance_start.player_id 
    AND all_dates.date_utc = balance_start.date_utc
  LEFT JOIN balance_end 
    ON all_dates.player_id = balance_end.player_id 
    AND all_dates.date_utc = balance_end.date_utc
  LEFT JOIN match_played 
    ON all_dates.player_id = match_played.player_id 
    AND all_dates.date_utc = match_played.date_utc
  LEFT JOIN match_duration 
    ON all_dates.player_id = match_duration.player_id 
    AND all_dates.date_utc = match_duration.date_utc
  LEFT JOIN match_summary 
    ON all_dates.player_id = match_summary.player_id 
    AND all_dates.date_utc = match_summary.date_utc
  LEFT JOIN match_reward 
    ON all_dates.player_id = match_reward.player_id 
    AND all_dates.date_utc = match_reward.date_utc
  LEFT JOIN coins_summary 
    ON all_dates.player_id = coins_summary.player_id 
    AND all_dates.date_utc = coins_summary.date_utc
  LEFT JOIN purchases 
    ON all_dates.player_id = purchases.player_id 
    AND all_dates.date_utc = purchases.date_utc
  LEFT JOIN final_streaks 
    ON all_dates.player_id = final_streaks.player_id 
    AND all_dates.date_utc = final_streaks.date_utc
)
    -- Carry forward values if a player does not have new events on a particular date
  SELECT 
    date_utc,
    player_id,
    LAST_VALUE(balance_before IGNORE NULLS) 
      OVER (PARTITION BY player_id ORDER BY date_utc ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS balance_day_start,
    LAST_VALUE(balance_day_end IGNORE NULLS) 
      OVER (PARTITION BY player_id ORDER BY date_utc ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS balance_day_end,
    LAST_VALUE(matches_played IGNORE NULLS) 
      OVER (PARTITION BY player_id ORDER BY date_utc ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS matches_played,
    LAST_VALUE(total_matches_duration IGNORE NULLS) 
      OVER (PARTITION BY player_id ORDER BY date_utc ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS total_matches_duration,
    LAST_VALUE(matches_won_reward IGNORE NULLS) 
      OVER (PARTITION BY player_id ORDER BY date_utc ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS matches_won_reward,
    LAST_VALUE(matches_claimed IGNORE NULLS) 
      OVER (PARTITION BY player_id ORDER BY date_utc ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS matches_claimed,
    LAST_VALUE(coins_sink_tournaments IGNORE NULLS) 
      OVER (PARTITION BY player_id ORDER BY date_utc ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS coins_sink_tournaments,
    LAST_VALUE(coins_source_tournaments IGNORE NULLS) 
      OVER (PARTITION BY player_id ORDER BY date_utc ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS coins_source_tournaments,
    LAST_VALUE(max_score IGNORE NULLS) 
      OVER (PARTITION BY player_id ORDER BY date_utc ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_score,
    LAST_VALUE(avg_score IGNORE NULLS) 
      OVER (PARTITION BY player_id ORDER BY date_utc ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS avg_score,
    LAST_VALUE(max_position IGNORE NULLS) 
      OVER (PARTITION BY player_id ORDER BY date_utc ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_position,
    LAST_VALUE(avg_position IGNORE NULLS) 
      OVER (PARTITION BY player_id ORDER BY date_utc ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS avg_position,
     LAST_VALUE(max_reward_won_streak IGNORE NULLS) 
      OVER (PARTITION BY player_id ORDER BY date_utc ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_reward_won_streak,
    LAST_VALUE(max_losing_streak IGNORE NULLS) 
      OVER (PARTITION BY player_id ORDER BY date_utc ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_losing_streak,
    LAST_VALUE(revenue IGNORE NULLS) 
      OVER (PARTITION BY player_id ORDER BY date_utc ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS revenue,
    LAST_VALUE(coins_source_purchases IGNORE NULLS) 
      OVER (PARTITION BY player_id ORDER BY date_utc ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS coins_source_purchases,
  FROM final_data