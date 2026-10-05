{{
    config(
        materialized='view'
    )
}}

with source_sales as (

    select
        sale_id,
        cast(sale_date as date) as sale_date,
        trim(product_id) as product_id,
        trim(product_category) as product_category,
        cast(quantity as int64) as quantity,
        cast(unit_price as numeric) as unit_price,
        cast(revenue as numeric) as revenue,
        trim(region) as region
    from {{ ref('raw_sales') }}

)

select
    sale_id,
    sale_date,
    product_id,
    product_category,
    quantity,
    unit_price,
    revenue,
    region
from source_sales
