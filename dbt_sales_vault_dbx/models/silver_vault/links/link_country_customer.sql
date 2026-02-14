{{ config(materialized='incremental') }}

{%- set source_model = "stage_customers" -%}
{%- set src_pk = "link_country_customer_hk" -%}
{%- set src_fk = ["customer_hk", "country_hk"] -%}
{%- set src_ldts = "load_dts" -%}
{%- set src_source = "record_source" -%}

{{ automate_dv.link(src_pk=src_pk, src_fk=src_fk, src_ldts=src_ldts,
                    src_source=src_source, source_model=source_model) }}