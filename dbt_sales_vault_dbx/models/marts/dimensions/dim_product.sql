{{ config(materialized='incremental', unique_key='product_hk') }}

select
    h.product_hk,
    h.load_dts as hub_load_dts,
    s.load_dts as sat_load_dts,
    s.name as product_name,
    s.category as product_category,
    s.price as product_price
from {{ ref('hub_product') }} h
left join {{ ref('sat_product') }} s
    on h.product_hk = s.product_hk

{% if is_incremental() %}
  where h.load_dts > (select max(hub_load_dts) from {{ this }})
{% endif %}