with 
    int_valid_transaction as (
        select
            transaction_id
            , customer_id
            , transaction_date
            , transaction_timestamp
        from {{ ref('int_valid_transaction') }}
    )

    , first_purchase as (
        select
            customer_id
            , min(transaction_date) as first_purchase_date
        from int_valid_transaction
        group by customer_id
    )

    , acquisition as (
        select
            vt.customer_id
            , c30.campaign_id_30d as acquisition_campaign
            , row_number() over (
                partition by vt.customer_id
                order by vt.transaction_timestamp asc
            ) as rn_transaction
        from int_valid_transaction as vt
        left join {{ ref('int_attribution_last_click_30d') }} as c30
            using (transaction_id)
    )

    , join_and_cohort_month as (
        select
            fp.customer_id
            , fp.first_purchase_date
            , date_trunc(fp.first_purchase_date, month) as cohort_month
            , ac.acquisition_campaign
        from first_purchase as fp
        left join acquisition as ac
            on fp.customer_id = ac.customer_id
            and rn_transaction = 1
    )

select *
from join_and_cohort_month
