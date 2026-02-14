{{ config(materialized='incremental') }}

{%- set source_model = "stage_orders" -%}
{%- set src_pk = "link_order_customer_hk" -%}
{%- set src_fk = ["order_hk", "customer_hk"] -%}
{%- set src_ldts = "load_dts" -%}
{%- set src_source = "record_source" -%}

{{ automate_dv.link(src_pk=src_pk, src_fk=src_fk, src_ldts=src_ldts,
                    src_source=src_source, source_model=source_model) }}