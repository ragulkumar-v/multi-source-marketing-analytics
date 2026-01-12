{{ config(materialized='table') }}

-- Marketing channel attribution using GA session data + order conversions
with sessions as (
    select * from {{ ref('stg_ga_sessions') }}
),
channel_daily as (
    select
        session_date,
        source,
        medium,
        campaign,
        -- Channel grouping
        case
            when medium = 'cpc' or medium = 'ppc' then 'paid_search'
            when medium = 'paid_social' or source in ('facebook', 'instagram', 'linkedin') then 'paid_social'
            when medium = 'email' then 'email'
            when medium = 'organic' then 'organic_search'
            when medium = 'social' then 'organic_social'
            when medium = 'referral' then 'referral'
            when source = '(direct)' then 'direct'
            else 'other'
        end as channel_group,
        sum(sessions) as sessions,
        sum(users) as users,
        sum(new_users) as new_users,
        sum(pageviews) as pageviews,
        avg(bounce_rate) as avg_bounce_rate,
        avg(avg_session_duration) as avg_session_duration,
        sum(goal_completions) as conversions
    from sessions
    group by 1, 2, 3, 4, 5
)
select
    *,
    case when sessions > 0 then round(conversions::float / sessions * 100, 4) else 0 end as conversion_rate,
    case when users > 0 then round(sessions::float / users, 2) else 0 end as sessions_per_user,
    case when sessions > 0 then round(pageviews::float / sessions, 2) else 0 end as pages_per_session
from channel_daily
