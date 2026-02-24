/*
- Granularity: one line per customer
- Source: marketing_raw.customers_raw
- Objective: to standardize customers attributes for analysis
*/

with 
    source_data as (
        select *
        from {{ source('marketing_raw', 'customers_raw') }}
    )
    
    , cast_and_rename_columns as (
        select
            safe_cast(customer_id as string)            as customer_id
            , safe_cast(signup_date as date)            as signup_date
            , safe_cast(country as string)              as country
            , safe_cast(age as int)                     as age
            , safe_cast(gender as string)               as gender
            , safe_cast(loyalty_tier as string)         as loyalty_tier
            , safe_cast(acquisition_channel as string)  as acquisition_channel
        from source_data
    )

select *
from cast_and_rename_columns
