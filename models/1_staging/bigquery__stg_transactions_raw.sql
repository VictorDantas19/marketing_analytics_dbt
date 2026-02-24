/*
- Granularity: one line per event
- Source: marketing_raw.transactions_raw
- Objective: to standardize customers attributes for analysis
*/

with 
    source_data as (
        select *
        from {{ source('marketing_raw', 'transactions_raw') }}
    )
    
    , cast_and_rename_columns as (
        select
            safe_cast(transaction_id as string) as transaction_id
            , safe_cast(timestamp as timestamp) as trasaction_date
            , safe_cast(customer_id as string) as customer_id
            , safe_cast(product_id as string) as product_id
            , safe_cast(quantity as int64) as quantity
            , safe_cast(discount_applied as float64) as discount_applied
            , safe_cast(gross_revenue as float64) as gross_revenue
            , safe_cast(campaign_id as string) as campaign_id
            , safe_cast(refund_flag as int64) as is_refund
        from source_data
    )

select *
from cast_and_rename_columns
