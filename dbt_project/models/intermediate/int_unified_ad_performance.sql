{{ config(materialized='ephemeral') }}

-- Unify Google Ads and Facebook Ads into a single ad performance model
with google as (
    select
        ad_date,
        campaign_id,
        campaign_name,
        ad_group_id as ad_set_id,
        ad_group_name as ad_set_name,
        impressions,
        clicks,
        cost,
        conversions,
        conversion_value,
        null::int as reach,
        null::float as frequency,
        platform
    from {{ ref('stg_google_ads') }}
),
facebook as (
    select
        ad_date,
        campaign_id,
        campaign_name,
        adset_id as ad_set_id,
        adset_name as ad_set_name,
        impressions,
        clicks,
        cost,
        conversions,
        conversion_value,
        reach,
        frequency,
        platform
    from {{ ref('stg_fb_ads') }}
),
unified as (
    select * from google
    union all
    select * from facebook
)
select
    *,
    case when impressions > 0 then round(clicks::float / impressions * 100, 4) else 0 end as ctr,
    case when clicks > 0 then round(cost / clicks, 2) else 0 end as cpc,
    case when impressions > 0 then round(cost / impressions * 1000, 2) else 0 end as cpm,
    case when conversions > 0 then round(cost / conversions, 2) else 0 end as cpa,
    case when cost > 0 then round(conversion_value / cost, 2) else 0 end as roas
from unified
