{{ config(materialized='view') }}

select
    record_value:date::date as ad_date,
    record_value:campaign_id::varchar as campaign_id,
    record_value:campaign_name::varchar as campaign_name,
    record_value:adset_id::varchar as adset_id,
    record_value:adset_name::varchar as adset_name,
    record_value:impressions::int as impressions,
    record_value:clicks::int as clicks,
    record_value:spend::number(12,2) as cost,
    record_value:actions::int as conversions,
    record_value:action_values::float as conversion_value,
    record_value:reach::int as reach,
    record_value:frequency::float as frequency,
    'facebook_ads' as platform,
    loaded_at
from {{ source('raw', 'raw_fb_ads') }}
