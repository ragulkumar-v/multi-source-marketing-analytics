{{ config(
    materialized='incremental',
    unique_key=['ad_date', 'platform', 'campaign_id', 'ad_set_id']
) }}

with unified_ads as (
    select * from {{ ref('int_unified_ad_performance') }}
    {% if is_incremental() %}
    where ad_date > (select max(ad_date) from {{ this }})
    {% endif %}
)
select
    ad_date,
    platform,
    campaign_id,
    campaign_name,
    ad_set_id,
    ad_set_name,
    sum(impressions) as impressions,
    sum(clicks) as clicks,
    sum(cost) as cost,
    sum(conversions) as conversions,
    sum(conversion_value) as conversion_value,
    sum(reach) as reach,
    -- Calculated metrics
    case when sum(impressions) > 0
        then round(sum(clicks)::float / sum(impressions) * 100, 4) else 0 end as ctr,
    case when sum(clicks) > 0
        then round(sum(cost) / sum(clicks), 2) else 0 end as cpc,
    case when sum(impressions) > 0
        then round(sum(cost) / sum(impressions) * 1000, 2) else 0 end as cpm,
    case when sum(conversions) > 0
        then round(sum(cost) / sum(conversions), 2) else 0 end as cpa,
    case when sum(cost) > 0
        then round(sum(conversion_value) / sum(cost), 2) else 0 end as roas
from unified_ads
group by 1, 2, 3, 4, 5, 6
