/*
- Granularity: one line per event
- Source: marketing_raw.products_raw
- Objective: to standardize customers attributes for analysis
*/

with 
    source_data as (
        select *
        from {{ source('marketing_raw', 'products_raw') }}
    )
    
    , cast_and_rename_columns as (
        select
            safe_cast(product_id as string) as product_id
            , safe_cast(category as string) as category_product
            , safe_cast(brand as string) as brand_product
            , safe_cast(base_price as float64) as base_price_product
            , safe_cast(launch_date as date) as launch_date
            , safe_cast(is_premium as int64) as is_premium
        from source_data
    )

select *
from cast_and_rename_columns
