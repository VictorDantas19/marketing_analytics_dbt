with 
    stg_transactions as (
        select
            transaction_id
            , customer_id
            , transaction_date
            , transaction_timestamp
        from {{ ref('stg_transactions') }}
        where is_valid_revenue = true
    )

    , stg_events as (
        select
            customer_id
            , event_timestamp
            , campaign_id_clean
        from {{ ref('stg_events') }}
    )

    , join_and_rule_30d_events_x_transaction as (
        select
            st.transaction_id
            , st.customer_id
            , st.transaction_date
            , st.transaction_timestamp
            , se.campaign_id_clean
            , row_number() over (
                partition by st.transaction_id
                order by se.event_timestamp desc
            ) as rn_event_date
        from stg_transactions as st
        left join stg_events as se
            on st.customer_id = se.customer_id
            and date_diff(se.event_timestamp, st.transaction_timestamp, day) between 0 and 30
            and se.event_timestamp <= st.transaction_timestamp
    )

    , select_last_event as (
        select
            transaction_id
            , customer_id
            , transaction_date
            , transaction_timestamp
            , coalesce(campaign_id_clean, 'organic') as campaign_id_30d
        from join_and_rule_30d_events_x_transaction
        where rn_event_date = 1
    )

select *
from select_last_event
