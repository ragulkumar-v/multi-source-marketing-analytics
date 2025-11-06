{{ config(materialized='ephemeral') }}

-- Aggregate e-commerce order data per customer email
with orders as (
    select * from {{ ref('stg_ecom_orders') }}
    where status = 'completed'
),
items as (
    select * from {{ ref('stg_ecom_order_items') }}
),
order_items_agg as (
    select
        order_id,
        sum(line_total) as items_total,
        count(*) as item_count,
        count(distinct product_id) as unique_products
    from items
    group by order_id
)
select
    o.customer_email,
    count(distinct o.order_id) as total_orders,
    sum(o.total_amount) as lifetime_revenue,
    avg(o.total_amount) as avg_order_value,
    min(o.order_date) as first_order_date,
    max(o.order_date) as last_order_date,
    datediff('day', min(o.order_date), max(o.order_date)) as customer_lifespan_days,
    sum(oi.item_count) as total_items_purchased,
    sum(oi.unique_products) as total_unique_products,
    listagg(distinct o.channel, ', ') within group (order by o.channel) as purchase_channels,
    listagg(distinct o.payment_method, ', ') within group (order by o.payment_method) as payment_methods
from orders o
left join order_items_agg oi on o.order_id = oi.order_id
group by o.customer_email
