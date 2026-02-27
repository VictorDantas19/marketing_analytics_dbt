with 
    stg_transactions as (
        select
            transaction_id
            , customer_id
            , transaction_date
            , product_id
            , net_revenue
        from {{ ref('stg_transactions') }}
        where is_valid_revenue = true
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
            st.transaction_id
            , st.customer_id
            , st.transaction_date
            , st.product_id
            , st.net_revenue
            , icfp.first_purchase_date
            , icfp.cohort_month
        from stg_transactions as st
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
