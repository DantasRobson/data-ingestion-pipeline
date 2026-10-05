{{
    config(
        materialized='table'
    )
}}

select
    calendar_date as date,
    day_of_month as day,
    month_number as month,
    month_name,
    quarter_number as quarter,
    year_number as year,
    day_of_week_num as day_of_week,
    day_of_week_name as day_name,
    week_of_year,
    is_weekend,
    is_business_day,
    is_holiday,
    holiday_name,
    holiday_type,
    days_to_holiday,
    days_after_holiday,
    is_pre_holiday_window,
    is_post_holiday_window
from {{ ref('int_calendario_comercial') }}
