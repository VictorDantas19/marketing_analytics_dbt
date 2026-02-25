with 
    source_data as (
        select *
        from {{ source('marketing_raw', 'customers_raw') }}
    )
    
    , cast_and_rename_columns as (
        select
            safe_cast(customer_id as string)                        as customer_id
            , safe_cast(signup_date as date)                        as customer_signup_date
            , safe_cast(country as string)                          as customer_country
            , safe_cast(age as string)                              as customer_age
            , lower(trim(safe_cast(gender as string)))              as customer_gender
            , lower(trim(safe_cast(loyalty_tier as string)))        as customer_loyalty_tier
            , lower(trim(safe_cast(acquisition_channel as string))) as customer_acquisition_channel
        from source_data
    )

select *
from cast_and_rename_columns
