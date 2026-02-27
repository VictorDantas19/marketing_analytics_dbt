with 
    stg_products as (
        select *
        from {{ ref('stg_products') }}
    )

    , rename_columns as (
        select
            product_id              as product_id
            , product_category      as category
            , product_brand         as brand
            , product_base_price    as base_price
            , product_launch_date   as launch_date
            , is_premium            as is_premium
        from stg_products
    )

select *
from rename_columns
