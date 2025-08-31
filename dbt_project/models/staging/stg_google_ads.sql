{{ config(materialized='view') }}

select
    record_value:date::date as ad_date,
    record_value:campaign_id::varchar as campaign_id,
    record_value:campaign_name::varchar as campaign_name,
    record_value:ad_group_id::varchar as ad_group_id,
    record_value:ad_group_name::varchar as ad_group_name,
    record_value:impressions::int as impressions,
    record_value:clicks::int as clicks,
    (record_value:cost_micros::bigint / 1000000.0)::number(12,2) as cost,
    record_value:conversions::float as conversions,
    record_value:conversion_value::float as conversion_value,
    'google_ads' as platform,
    loaded_at
from {{ source('raw', 'raw_google_ads') }}
