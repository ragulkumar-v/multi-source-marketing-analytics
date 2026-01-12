{{ config(materialized='view') }}

with raw as (
    select
        record_value:order_id::varchar as order_id,
        record_value:customer_email::varchar as customer_email,
        record_value:order_date::date as order_date,
        record_value:status::varchar as status,
        record_value:total_amount::number(12,2) as total_amount,
        record_value:discount_amount::number(12,2) as discount_amount,
        record_value:shipping_amount::number(12,2) as shipping_amount,
        record_value:payment_method::varchar as payment_method,
        record_value:channel::varchar as channel,
        loaded_at,
        row_number() over (
            partition by record_value:order_id::varchar
            order by loaded_at desc
        ) as rn
    from {{ source('raw', 'raw_ecom_orders') }}
)
select * exclude (rn)
from raw
where rn = 1
