{{ config(materialized='table') }}

-- Campaign-level ROI combining ad spend with conversion value
with ad_spend as (
    select
        campaign_name,
        platform,
        sum(cost) as total_spend,
        sum(impressions) as total_impressions,
        sum(clicks) as total_clicks,
        sum(conversions) as total_conversions,
        sum(conversion_value) as total_conversion_value,
        min(ad_date) as campaign_start,
        max(ad_date) as campaign_end,
        datediff('day', min(ad_date), max(ad_date)) + 1 as campaign_days
    from {{ ref('fct_ad_performance_daily') }}
    group by campaign_name, platform
)
select
    campaign_name,
    platform,
    campaign_start,
    campaign_end,
    campaign_days,
    total_spend,
    total_impressions,
    total_clicks,
    total_conversions,
    total_conversion_value,
    -- ROI Metrics
    case when total_spend > 0
        then round((total_conversion_value - total_spend) / total_spend * 100, 2)
        else 0 end as roi_pct,
    case when total_spend > 0
        then round(total_conversion_value / total_spend, 2)
        else 0 end as roas,
    case when total_conversions > 0
        then round(total_spend / total_conversions, 2)
        else 0 end as cost_per_acquisition,
    case when total_clicks > 0
        then round(total_spend / total_clicks, 2)
        else 0 end as cost_per_click,
    round(total_spend / nullif(campaign_days, 0), 2) as daily_spend,
    -- Performance tier
    case
        when total_spend > 0 and total_conversion_value / total_spend >= 5 then 'excellent'
        when total_spend > 0 and total_conversion_value / total_spend >= 3 then 'good'
        when total_spend > 0 and total_conversion_value / total_spend >= 1 then 'break_even'
        else 'underperforming'
    end as performance_tier
from ad_spend
