with 
    source_data as (
        select *
        from {{ source('marketing_raw', 'transactions_raw') }}
    )

    , cast_and_rename_columns as (
        select
            safe_cast(transaction_id as string)         as transaction_id
            , safe_cast(timestamp as timestamp)         as transaction_timestamp
            , safe_cast(customer_id as string)          as customer_id
            , safe_cast(product_id as string)           as product_id
            , safe_cast(quantity as string)             as transaction_quantity
            , safe_cast(discount_applied as numeric)    as transaction_discount_applied
            , safe_cast(gross_revenue as numeric)       as net_revenue
            , safe_cast(campaign_id as string)          as campaign_id
            , safe_cast(refund_flag as boolean)         as is_refund
        from source_data
    )

    , add_campaign_organic as (
        select
            *
            , case
                when campaign_id in (select campaign_id from {{ ref('stg_campaigns') }})
                    then campaign_id
                else '(organic)'
            end as campaign_id_clean
        from cast_and_rename_columns
    )

    , add_transaction_date as (
        select
            *
            , date(transaction_timestamp) as transaction_date
            , date_trunc(date(transaction_timestamp), month) as transaction_month
        from add_campaign_organic
    )

    , flag_valid_revenue as (
        select
            *
            , case
                when 
                    net_revenue > 0
                    and is_refund = false
                    then true
                else false
            end as is_valid_revenue
        from add_transaction_date
    )

select *
from flag_valid_revenue
