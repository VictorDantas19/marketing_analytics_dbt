with 
    stg_transactions as (
        select
            customer_id
            , transaction_date
        from {{ ref('stg_transactions') }}
        where is_valid_revenue = true
    )

    , first_purchase as (
        select
            customer_id
            , min(transaction_date) as first_purchase_date
        from stg_transactions
        group by customer_id
    )

    , cohort_month as (
        select
            customer_id
            , first_purchase_date
            , date_trunc(first_purchase_date, month) as cohort_month
        from first_purchase
    )

select *
from cohort_month
