{{ config(materialized='view') }}

with raw as (
    select
        record_value:order_item_id::varchar as order_item_id,
        record_value:order_id::varchar as order_id,
        record_value:product_id::varchar as product_id,
        record_value:quantity::int as quantity,
        record_value:unit_price::number(12,2) as unit_price,
        record_value:discount::number(6,4) as discount,
        loaded_at,
        row_number() over (
            partition by record_value:order_item_id::varchar
            order by loaded_at desc
        ) as rn
    from {{ source('raw', 'raw_ecom_order_items') }}
)
select
    *  exclude (rn),
    (unit_price * quantity * (1 - coalesce(discount, 0)))::number(12,2) as line_total
from raw
where rn = 1
