{%- set yaml_metadata -%}
source_model: 'orders'
derived_columns:
  record_source: '!seed_orders'
  load_dts: current_timestamp()
hashed_columns:
  order_hk: 'id'
  customer_hk: 'customer_id'
  link_order_customer_hk: 
    - 'id'
    - 'customer_id'
  hash_diff:
    - 'order_date'
    - 'total_amount'
    - 'status'
{%- endset -%}
{% set metadata_dict = fromyaml(yaml_metadata) %}
{% set source_model = metadata_dict['source_model'] %}
{% set derived_columns = metadata_dict['derived_columns'] %}
{% set hashed_columns = metadata_dict['hashed_columns'] %}
{{ automate_dv.stage(
    include_source_columns=true,
    source_model=source_model,
    derived_columns=derived_columns,
    hashed_columns=hashed_columns,
    ranked_columns=none
) }}