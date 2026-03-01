with 
    int_valid_transaction as (
        select
            transaction_id
            , customer_id
            , product_id
            , transaction_date
            , transaction_timestamp
            , transaction_quantity
            , net_revenue
            , is_refund
            , is_valid_revenue
        from {{ ref('int_valid_transaction') }}
    )

    , enriched as (
        select
            transaction_id
            , first_purchase_date
            , cohort_month
            , days_since_first_purchase
            , is_new_customer_transaction
        from {{ ref('int_transactions_enriched') }}
    )

    , attrib as (
        select
            transaction_id
            , campaign_id_30d as campaign_id
        from {{ ref('int_attribution_last_click_30d') }}
    )

    , joins_and_rename as (
        select
            vt.transaction_id
            , vt.customer_id
            , vt.product_id
            , vt.transaction_date as date
            , vt.transaction_timestamp as timestamp
            , vt.transaction_quantity as quantity
            , vt.net_revenue
            , e.first_purchase_date
            , e.cohort_month
            , e.days_since_first_purchase
            , e.is_new_customer_transaction
            , coalesce(a.campaign_id, '(organic)') as campaign_id,
        from int_valid_transaction as vt
        left join enriched as e using (transaction_id)
        left join attrib as a using (transaction_id)
    )

select *
from joins_and_rename
