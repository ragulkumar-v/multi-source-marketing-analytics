{{ config(materialized='view') }}

select
    record_value:date::date as session_date,
    record_value:source::varchar as source,
    record_value:medium::varchar as medium,
    record_value:campaign::varchar as campaign,
    record_value:sessions::int as sessions,
    record_value:users::int as users,
    record_value:new_users::int as new_users,
    record_value:pageviews::int as pageviews,
    record_value:bounce_rate::float as bounce_rate,
    record_value:avg_session_duration::float as avg_session_duration,
    record_value:goal_completions::int as goal_completions,
    loaded_at
from {{ source('raw', 'raw_ga_sessions') }}
