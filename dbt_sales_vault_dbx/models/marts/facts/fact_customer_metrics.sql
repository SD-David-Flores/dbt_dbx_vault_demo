{{ config(materialized='table') }}

with source as (
    select
        customer_id,
        total_orders,
        total_revenue,
        avg_order_value,
        last_order_date,
        case
            when last_order_date is not null
            then year(last_order_date) * 10000 + month(last_order_date) * 100 + day(last_order_date)
        end as last_order_date_key,
        load_dts
    from {{ ref('biz_vault_customer_metrics') }}
)

select *
from source