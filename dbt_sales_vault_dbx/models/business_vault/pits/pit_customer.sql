{{ config(
    materialized = "incremental",
    unique_key = "customer_hk",
    incremental_strategy = "merge"
) }}

with hub as (
    select
        customer_hk,
        load_dts as hub_load_dts
    from {{ ref('hub_customer') }}
),

latest_sat as (
    select
        customer_hk,
        max(load_dts) as latest_sat_customer_dts
    from {{ ref('sat_customer') }}
    where load_dts <= current_timestamp()
    group by customer_hk
)

select
    h.customer_hk,
    h.hub_load_dts,
    l.latest_sat_customer_dts as sat_customer_load_dts,
    l.latest_sat_customer_dts
from hub h
left join latest_sat l
    on h.customer_hk = l.customer_hk