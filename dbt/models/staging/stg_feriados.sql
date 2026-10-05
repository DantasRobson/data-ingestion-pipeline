{{
    config(
        materialized='view'
    )
}}

with source_data as (

    select
        holiday_id,
        cast(date as date) as holiday_date,
        trim(name) as holiday_name,
        lower(trim(type)) as holiday_type,
        source as data_source,
        cast(ingested_at as timestamp) as ingested_at
    from {{ source('raw_data', 'feriados') }}

),

deduplicated as (

    select
        holiday_id,
        holiday_date,
        holiday_name,
        holiday_type,
        data_source,
        ingested_at,
        row_number() over (
            partition by holiday_id 
            order by ingested_at desc
        ) as row_num
    from source_data

)

select
    holiday_id,
    holiday_date,
    holiday_name,
    holiday_type,
    data_source,
    ingested_at
from deduplicated
where row_num = 1
