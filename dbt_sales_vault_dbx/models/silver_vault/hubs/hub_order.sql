{{ config(materialized='incremental') }}

{{ automate_dv.hub(
    src_pk='order_hk',
    src_nk='id',
    src_ldts='load_dts',
    src_source='record_source',
    source_model='stage_orders'
) }}