{{ config(materialized='table') }}

with customer_hub as (
    select
        customer_hk,
        id as customer_id
    from {{ ref('hub_customer') }}
),

orders as (
    select
        lco.customer_hk,
        so.order_date,
        so.total_amount
    from {{ ref('link_order_customer') }} lco
    join {{ ref('sat_order') }} so
        on lco.order_hk = so.order_hk
),

aggregated as (
    select
        ch.customer_id,
        count(distinct o.order_date) as total_orders,
        sum(o.total_amount) as total_revenue,
        avg(o.total_amount) as avg_order_value,
        max(o.order_date) as last_order_date
    from customer_hub ch
    left join orders o
        on ch.customer_hk = o.customer_hk
    group by ch.customer_id
)

select
    customer_id,
    total_orders,
    total_revenue,
    avg_order_value,
    last_order_date,
    current_timestamp() as load_dts
from aggregated