-- This table is refreshed daily after the daily_player model is completed using a cron job: 0 0 * * * (at midnight every day)

{{ config(
    materialized='incremental',
    unique_key=['media_source', 'install_country', 'install_date', 'cohort_period'],
    partition_by={
        "field": "install_date",
        "data_type": "date"
    },
    incremental_strategy='merge'
) }}

WITH max_date_utc AS (
  -- Retrieve the max date_utc from the marketing_roas table to avoid using subqueries in JOINs
  SELECT MAX(date_utc) AS max_date
  FROM {{ ref('daily_player') }}
),
player_revenue AS (
 -- Aggregating player revenue over time (cumulative)
 SELECT
   i.player_id,
   p.date_utc,
   i.install_date,
   i.media_source,
   i.install_country,
   COALESCE(SUM(p.revenue) OVER (PARTITION BY i.player_id ORDER BY p.date_utc ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), 0) AS cumulative_revenue
  from {{ source('dbt_tomer', 'installs_attribution') }} i
  left join {{ ref('daily_player') }} p
   ON i.player_id = p.player_id
  where p.date_utc >= (SELECT max_date FROM max_date_utc)  -- Now using the value from max_install_date CTE

),
cohort_revenue AS (
 -- Calculate cohort cumulative revenue for D7, D14, D30, D90 periods
 SELECT
   player_id,
   media_source,
   install_country,
   install_date,
   DATE_DIFF(p.date_utc, install_date, DAY) AS days_since_install,
   cumulative_revenue
 FROM player_revenue p
),
marketing_spend AS (
 -- Aggregate marketing spend per media source and country
 SELECT
   media_source,
   country AS install_country,
   SUM(spend) AS total_spend
 FROM {{ source('dbt_tomer', 'marketing_spend') }} 
 GROUP BY 1, 2
),
final_roas AS (
 -- Calculate the final ROAS by cohort period
 SELECT
   cr.media_source,
   cr.install_country,
   cr.install_date,
   CASE
     WHEN cr.days_since_install <= 7 THEN 'D7'
     WHEN cr.days_since_install <= 14 THEN 'D14'
     WHEN cr.days_since_install <= 30 THEN 'D30'
     WHEN cr.days_since_install <= 90 THEN 'D90'
     ELSE NULL
   END AS cohort_period,
   SUM(cr.cumulative_revenue) AS cohort_revenue,
   ms.total_spend
 FROM cohort_revenue cr
 LEFT JOIN marketing_spend ms
   ON cr.media_source = ms.media_source
   AND cr.install_country = ms.install_country
 WHERE cr.days_since_install <= 90 -- filter to 90 days
 GROUP BY cr.media_source, cr.install_country, cr.install_date, cohort_period, ms.total_spend
)
SELECT
 media_source,
 install_country,
 install_date,
 cohort_period,
 ROUND(cohort_revenue / NULLIF(total_spend, 0), 3) AS roas
FROM final_roas



