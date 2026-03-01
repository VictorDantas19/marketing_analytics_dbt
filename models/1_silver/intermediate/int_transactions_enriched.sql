with 
    int_valid_transaction as (
        select
            transaction_id
            , customer_id
            , transaction_date
            , product_id
            , net_revenue
        from {{ ref('int_valid_transaction') }}
    )

    , int_customer_first_purchase as (
        select
            customer_id
            , first_purchase_date
            , cohort_month
        from {{ ref('int_customer_enriched') }}
    )

    , joins as (
        select
            vt.transaction_id
            , vt.customer_id
            , vt.transaction_date
            , vt.product_id
            , vt.net_revenue
            , icfp.first_purchase_date
            , icfp.cohort_month
        from int_valid_transaction as vt
        left join int_customer_first_purchase as icfp
            using (customer_id)
    )

    , rules as (
        select
            transaction_id
            , customer_id
            , transaction_date
            , product_id
            , net_revenue
            , first_purchase_date
            , cohort_month
            , date_diff(transaction_date, first_purchase_date, day) as days_since_first_purchase
            , case
                when transaction_date = first_purchase_date then true
                else false
            end as is_new_customer_transaction
        from joins
    )

select *
from rules
