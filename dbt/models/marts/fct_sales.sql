{{
    config(
        materialized='table'
    )
}}

with sales as (

    select * from {{ ref('stg_sales') }}

),

calendar as (

    select * from {{ ref('dim_calendario') }}

)

select
    s.sale_id,
    s.sale_date,
    s.product_id,
    s.product_category,
    s.quantity,
    s.unit_price,
    s.revenue,
    s.region,
    c.day_name,
    c.month_name,
    c.year,
    c.is_weekend,
    c.is_business_day,
    c.is_holiday,
    c.holiday_name,
    c.days_to_holiday,
    c.days_after_holiday,
    c.is_pre_holiday_window,
    c.is_post_holiday_window,
    case
        when c.is_holiday then 'DURANTE_FERIADO'
        when c.is_pre_holiday_window then 'PRE_FERIADO'
        when c.is_post_holiday_window then 'POS_FERIADO'
        else 'DIA_COMUM'
    end as sales_period_context
from sales s
inner join calendar c
    on s.sale_date = c.date
