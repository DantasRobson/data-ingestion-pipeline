{{
    config(
        materialized='table'
    )
}}

with date_spine as (

    -- Série temporal cobrindo 2024 a 2026
    select
        date_day as calendar_date
    from unnest(
        generate_date_array(date('2024-01-01'), date('2026-12-31'), interval 1 day)
    ) as date_day

),

feriados as (

    select
        holiday_id,
        holiday_date,
        holiday_name,
        holiday_type
    from {{ ref('stg_feriados') }}

),

calendar_with_holidays as (

    select
        d.calendar_date,
        extract(day from d.calendar_date) as day_of_month,
        extract(month from d.calendar_date) as month_number,
        format_date('%B', d.calendar_date) as month_name,
        extract(quarter from d.calendar_date) as quarter_number,
        extract(year from d.calendar_date) as year_number,
        extract(dayofweek from d.calendar_date) as day_of_week_num,
        format_date('%A', d.calendar_date) as day_of_week_name,
        extract(isoweek from d.calendar_date) as week_of_year,

        -- Classificação de fim de semana (1=Domingo, 7=Sábado no BigQuery)
        case 
            when extract(dayofweek from d.calendar_date) in (1, 7) then true 
            else false 
        end as is_weekend,

        -- Feriado
        case 
            when f.holiday_name is not null then true 
            else false 
        end as is_holiday,
        coalesce(f.holiday_name, 'Dia Comum') as holiday_name,
        coalesce(f.holiday_type, 'regular') as holiday_type,

        -- Dia útil (Nem fim de semana, nem feriado nacional)
        case 
            when extract(dayofweek from d.calendar_date) not in (1, 7) 
             and f.holiday_name is null then true 
            else false 
        end as is_business_day

    from date_spine d
    left join feriados f
        on d.calendar_date = f.holiday_date

),

holidays_proximity as (

    select
        c.calendar_date,
        min(case when h.holiday_date >= c.calendar_date then h.holiday_date end) as next_holiday_date,
        max(case when h.holiday_date <= c.calendar_date then h.holiday_date end) as prev_holiday_date
    from date_spine c
    cross join feriados h
    group by c.calendar_date

)

select
    c.calendar_date,
    c.day_of_month,
    c.month_number,
    c.month_name,
    c.quarter_number,
    c.year_number,
    c.day_of_week_num,
    c.day_of_week_name,
    c.week_of_year,
    c.is_weekend,
    c.is_holiday,
    c.holiday_name,
    c.holiday_type,
    c.is_business_day,
    p.next_holiday_date,
    date_diff(p.next_holiday_date, c.calendar_date, day) as days_to_holiday,
    p.prev_holiday_date,
    date_diff(c.calendar_date, p.prev_holiday_date, day) as days_after_holiday,
    -- Janelas estratégicas para campanhas e varejo
    case 
        when date_diff(p.next_holiday_date, c.calendar_date, day) between 1 and 7 then true 
        else false 
    end as is_pre_holiday_window,
    case 
        when date_diff(c.calendar_date, p.prev_holiday_date, day) between 1 and 3 then true 
        else false 
    end as is_post_holiday_window
from calendar_with_holidays c
left join holidays_proximity p
    on c.calendar_date = p.calendar_date
