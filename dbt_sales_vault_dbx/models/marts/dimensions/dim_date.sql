{{ config(materialized='table') }}

with date_spine as (
    select explode(sequence(date('2025-01-01'), current_date(), interval 1 day)) as date_day
)

select
    date_day as date_key,
    year(date_day) as year,
    month(date_day) as month,
    day(date_day) as day,
    dayofweek(date_day) as day_of_week,
    quarter(date_day) as quarter
from date_spine