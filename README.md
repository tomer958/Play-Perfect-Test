# DBT Project: Game Analytics and Marketing ROAS Dashboard

## Project Overview
This project is designed to handle game event analytics and marketing ROAS (Return on Ad Spend) calculations for a game application. The project builds three major fact tables to summarize players' activity, room events, and marketing spend, allowing the analytics team to create dashboards for player behavior analysis and marketing effectiveness.

## Tables
This project builds the following tables:
1. **fct_games**: Summarizes the details of a player’s match in the game.
2. **daily_player**: Summarizes a player's daily activity.
3. **fct_rooms**: Summarizes the events related to rooms in the game.
4. **marketing_roas**: Calculates the ROAS based on player revenue and marketing spend, segmented by various cohort periods.

## Data Sources
The project uses the following source tables from the BigQuery dataset `dbt_tomer`:
- **events**: Contains all time-series game events for each player, partitioned by `date_utc` and clustered by `event_name`.
- **installs_attribution**: Contains data related to player installations, media source, and country.
- **marketing_spend**: Tracks the marketing spend by media source and country.


## Table Details
### 1. `fct_games`
- **Description**: This table summarizes each unique match of a unique player. It gathers all event data such as match join time, finish time, rewards, and room details for each player.
- **Refresh Frequency**: Hourly
  
### 2. `daily_player`
- **Description**: This table summarizes a player's daily activity, including revenue, matches played, rewards won, and more.
- **Refresh Frequency**: Daily
  
### 3. `fct_rooms`
- **Description**: This table summarizes the entire lifecycle of a room, from when it was opened to when it was closed, along with the total coins spent and rewards distributed.
- **Refresh Frequency**: Hourly

### 4. `marketing_roas`
- **Description**: This table calculates the Return on Ad Spend (ROAS) for different cohort periods (D7, D14, D30, D90) based on player revenue and marketing spend. It breaks down the data by `media_source`, `install_country`, and `install_date`.
- **Refresh Frequency**: Daily

