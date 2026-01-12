{{ config(materialized='view') }}

with raw as (
    select
        record_value:customer_id::varchar as customer_id,
        record_value:email::varchar as email,
        record_value:first_name::varchar as first_name,
        record_value:last_name::varchar as last_name,
        record_value:company::varchar as company,
        record_value:industry::varchar as industry,
        record_value:segment::varchar as segment,
        record_value:lifecycle_stage::varchar as lifecycle_stage,
        record_value:lead_source::varchar as lead_source,
        record_value:created_at::timestamp_ntz as created_at,
        record_value:updated_at::timestamp_ntz as updated_at,
        loaded_at,
        row_number() over (
            partition by record_value:customer_id::varchar
            order by loaded_at desc
        ) as rn
    from {{ source('raw', 'raw_crm_customers') }}
)
select * exclude (rn)
from raw
where rn = 1
