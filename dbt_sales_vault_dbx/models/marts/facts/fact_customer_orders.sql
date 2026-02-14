{{ config(materialized='table') }}

with customer_hub as (
    select
        customer_hk,
        id as customer_id
    from {{ ref('hub_customer') }}
),

order_hub as (
    select
        order_hk,
        id as order_id
    from {{ ref('hub_order') }}
),

orders as (
    select
        lco.customer_hk,
        lco.order_hk,
        so.order_date,
        so.total_amount,
        so.status as order_status
    from {{ ref('link_order_customer') }} lco
    join {{ ref('sat_order') }} so
        on lco.order_hk = so.order_hk
),

fact as (
    select
        ch.customer_id,
        oh.order_id,
        cast(o.order_date as date) as order_date,
        year(o.order_date) * 10000 + month(o.order_date) * 100 + day(o.order_date) as date_key, -- surrogate date key
        o.total_amount,
        o.order_status
    from orders o
    join customer_hub ch
        on o.customer_hk = ch.customer_hk
    join order_hub oh
        on o.order_hk = oh.order_hk
)

select *
from fact