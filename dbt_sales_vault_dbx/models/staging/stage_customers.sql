{%- set yaml_metadata -%}
source_model: 'customers'
derived_columns:
  record_source: '!seed_customers'
  load_dts: current_timestamp()
hashed_columns:
  customer_hk: 'id'
  country_hk: 'country'
  link_country_customer_hk:
    - 'id'
    - 'country'
  hash_diff:
    - 'name'
    - 'email'
    - 'country'
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