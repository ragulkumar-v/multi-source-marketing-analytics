{{ config(materialized='table') }}

-- Customer 360 dimension: unified view across CRM + E-Commerce + Marketing
with crm_customers as (
    select * from {{ ref('stg_crm_customers') }}
),
order_summary as (
    select * from {{ ref('int_customer_order_summary') }}
),
crm_campaigns as (
    select
        record_value:customer_id::varchar as customer_id,
        count(*) as campaign_responses,
        count(distinct record_value:campaign_id::varchar) as campaigns_engaged,
        max(record_value:responded_at::timestamp_ntz) as last_campaign_response
    from {{ source('raw', 'raw_crm_campaign_responses') }}
    group by 1
)
select
    c.customer_id,
    c.email,
    c.first_name,
    c.last_name,
    concat(c.first_name, ' ', c.last_name) as full_name,
    c.company,
    c.industry,
    c.segment as crm_segment,
    c.lifecycle_stage,
    c.lead_source,
    c.created_at as customer_since,
    datediff('day', c.created_at, current_timestamp()) as tenure_days,

    -- E-Commerce metrics
    coalesce(o.total_orders, 0) as total_orders,
    coalesce(o.lifetime_revenue, 0) as lifetime_revenue,
    coalesce(o.avg_order_value, 0) as avg_order_value,
    o.first_order_date,
    o.last_order_date,
    o.customer_lifespan_days,
    o.total_items_purchased,
    o.purchase_channels,

    -- Campaign engagement
    coalesce(cr.campaign_responses, 0) as campaign_responses,
    coalesce(cr.campaigns_engaged, 0) as campaigns_engaged,
    cr.last_campaign_response,

    -- Derived segments
    case
        when o.lifetime_revenue >= 5000 then 'platinum'
        when o.lifetime_revenue >= 1000 then 'gold'
        when o.lifetime_revenue >= 250 then 'silver'
        when o.lifetime_revenue > 0 then 'bronze'
        else 'prospect'
    end as value_tier,

    case
        when o.last_order_date >= dateadd('day', -30, current_date()) then 'active'
        when o.last_order_date >= dateadd('day', -90, current_date()) then 'cooling'
        when o.last_order_date >= dateadd('day', -180, current_date()) then 'at_risk'
        when o.last_order_date is not null then 'churned'
        else 'never_purchased'
    end as engagement_status,

    case
        when cr.campaign_responses >= 5 then 'highly_engaged'
        when cr.campaign_responses >= 2 then 'engaged'
        when cr.campaign_responses >= 1 then 'responsive'
        else 'unresponsive'
    end as campaign_engagement_level,

    -- Composite score (0-100)
    least(100, (
        coalesce(o.total_orders, 0) * 5
        + least(coalesce(o.lifetime_revenue, 0) / 100, 30)
        + coalesce(cr.campaign_responses, 0) * 3
        + case when o.last_order_date >= dateadd('day', -30, current_date()) then 20 else 0 end
    ))::int as customer_health_score

from crm_customers c
left join order_summary o on lower(c.email) = lower(o.customer_email)
left join crm_campaigns cr on c.customer_id = cr.customer_id
