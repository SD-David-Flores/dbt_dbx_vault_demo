{{ config(
    materialized = "incremental",
    unique_key = ["customer_hk", "as_of_date"]
) }}

-- 1. Generate calendar dynamically (safe for Databricks)
with date_range as (
    select 
        coalesce(
            cast(min(cast(load_dts as date)) as date),
            current_date()
        ) as min_date,
        current_date() as max_date
    from {{ ref('hub_customer') }}
),

calendar as (
    select explode(
        sequence(
            least(min_date, max_date),
            greatest(min_date, max_date),
            interval 1 day
        )
    ) as as_of_date
    from date_range
),

-- 2. Hub customers
hub_customer as (
    select customer_hk
    from {{ ref('hub_customer') }}
),

-- 3. Links
link_country as (
    select
        link_country_customer_hk as customer_bridge_hk,
        customer_hk,
        country_hk,
        load_dts as link_load_dts
    from {{ ref('link_country_customer') }}
),

link_order as (
    select
        link_order_customer_hk as customer_bridge_hk,
        customer_hk,
        order_hk,
        load_dts as link_load_dts
    from {{ ref('link_order_customer') }}
),

-- 4. Latest satellite load per customer
sat_customer_latest as (
    select
        s.customer_hk,
        s.name,
        s.email,
        s.country,
        s.load_dts as sat_customer_load_dts
    from {{ ref('sat_customer') }} s
),

-- 5. Cross join customers with calendar
customer_dates as (
    select
        hc.customer_hk,
        c.as_of_date
    from hub_customer hc
    cross join calendar c
),

-- 6. Latest links as of each date
links_as_of as (
    select
        cd.customer_hk,
        cd.as_of_date,
        lc.country_hk,
        lo.order_hk,
        greatest(
            coalesce(lc.link_load_dts, cast('1900-01-01' as timestamp)),
            coalesce(lo.link_load_dts, cast('1900-01-01' as timestamp))
        ) as latest_link_load_dts
    from customer_dates cd
    left join link_country lc
        on cd.customer_hk = lc.customer_hk
       and lc.link_load_dts <= cd.as_of_date
    left join link_order lo
        on cd.customer_hk = lo.customer_hk
       and lo.link_load_dts <= cd.as_of_date
),

-- 7. Join latest satellite attributes as of date
bridge_as_of as (
    select
        l.customer_hk,
        l.as_of_date,
        l.country_hk,
        l.order_hk,
        l.latest_link_load_dts,
        s.name as customer_name,
        s.email as customer_email,
        s.country as customer_country,
        s.sat_customer_load_dts,
        concat(l.customer_hk, '-', cast(l.as_of_date as string)) as customer_bridge_hk
    from links_as_of l
    left join sat_customer_latest s
        on l.customer_hk = s.customer_hk
       and s.sat_customer_load_dts <= l.as_of_date
)

-- Final select
select *
from bridge_as_of

{% if is_incremental() %}
  where as_of_date > (select max(as_of_date) from {{ this }})
{% endif %}