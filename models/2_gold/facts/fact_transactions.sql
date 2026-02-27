with tx as (

    select
        transaction_id,
        customer_id,
        product_id,
        transaction_date,
        transaction_timestamp,
        transaction_quantity,
        net_revenue,
        is_refund,
        is_valid_revenue
    from {{ ref('stg_transactions') }}
    where is_valid_revenue = true

),

enriched as (

    select
        transaction_id,
        first_purchase_date,
        cohort_month,
        days_since_first_purchase,
        is_new_customer_transaction
    from {{ ref('int_transactions_enriched') }}

),

attrib as (

    select
        transaction_id,
        campaign_id_30d as campaign_id
    from {{ ref('int_attribution_last_click_30d') }}

)

select
    t.transaction_id,
    t.customer_id,
    t.product_id,
    t.transaction_date,
    t.transaction_timestamp,
    t.transaction_quantity,
    t.net_revenue,

    e.first_purchase_date,
    e.cohort_month,
    e.days_since_first_purchase,
    e.is_new_customer_transaction,

    coalesce(a.campaign_id, '(organic)') as campaign_id,

from tx t
left join enriched e using (transaction_id)
left join attrib a using (transaction_id)