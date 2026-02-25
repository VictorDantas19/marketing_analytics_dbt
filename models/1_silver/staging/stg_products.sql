with 
    source_data as (
        select *
        from {{ source('marketing_raw', 'products_raw') }}
    )
    
    , cast_and_rename_columns as (
        select
            safe_cast(product_id as string)                 as product_id
            , trim(lower(safe_cast(category as string)))    as product_category
            , trim(lower(safe_cast(brand as string)))       as product_brand
            , safe_cast(base_price as numeric)              as product_base_price
            , safe_cast(launch_date as date)                as product_launch_date
            , safe_cast(is_premium as boolean)              as is_premium
        from source_data
    )

select *
from cast_and_rename_columns
