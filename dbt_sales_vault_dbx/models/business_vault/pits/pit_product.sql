{{ config(
    materialized = "incremental",
    unique_key = "product_hk",
    incremental_strategy = "merge"
) }}

with hub as (
    select
        product_hk,
        load_dts as hub_load_dts
    from {{ ref('hub_product') }}
),

latest_sat as (
    select
        product_hk,
        max(load_dts) as latest_sat_product_dts
    from {{ ref('sat_product') }}
    where load_dts <= current_timestamp()
    group by product_hk
)

select
    h.product_hk,
    h.hub_load_dts,
    l.latest_sat_product_dts as sat_product_load_dts,
    l.latest_sat_product_dts
from hub h
left join latest_sat l
    on h.product_hk = l.product_hk