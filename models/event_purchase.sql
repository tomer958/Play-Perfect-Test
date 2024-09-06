select *
from {{ source('dbt_tomer', 'events') }}
where event_name='purchase'