with 
    fact_transactions as (
        select
            customer_id
            , transaction_id
            , date as transaction_date
            , net_revenue
            , first_purchase_date
            , cohort_month
        from {{ ref('fact_transactions') }}
    )

    , cohort_size as (
        select
            cohort_month
            , count(distinct customer_id) as cohort_customers
        from fact_transactions
        where transaction_date = first_purchase_date
        group by cohort_month
    )

    , monthly as (
        select
            cohort_month,
            date_diff(date_trunc(transaction_date, month), date_trunc(first_purchase_date, month), month) as months_since_first_purchase,
            count(distinct customer_id) as active_customers,
            count(distinct transaction_id) as transactions,
            sum(net_revenue) as revenue
        from fact_transactions
        group by 
            cohort_month
            , months_since_first_purchase
    )

    , final as (
        select
            m.cohort_month
            , m.months_since_first_purchase
            , cs.cohort_customers
            , m.active_customers
            , m.transactions
            , m.revenue
            , safe_divide(m.active_customers, cs.cohort_customers) as retention_rate
        from monthly m
        left join cohort_size cs
            using (cohort_month)
    )

select 
    cohort_month
    , months_since_first_purchase
    , retention_rate
from final
