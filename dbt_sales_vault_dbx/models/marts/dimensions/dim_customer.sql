{{ config(materialized='incremental', unique_key='customer_hk') }}

select
    h.customer_hk,
    h.load_dts as hub_load_dts,
    s.load_dts as sat_customer_load_dts,
    max(s.load_dts) over (partition by h.customer_hk) as latest_sat_customer_dts,
    s.name,
    s.email,
    s.country
from {{ ref('hub_customer') }} h
left join {{ ref('sat_customer') }} s
    on h.customer_hk = s.customer_hk

{% if is_incremental() %}
  where s.load_dts > (select max(sat_customer_load_dts) from {{ this }})
{% endif %}