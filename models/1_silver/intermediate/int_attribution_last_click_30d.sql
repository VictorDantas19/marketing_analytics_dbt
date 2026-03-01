with 
    int_valid_transaction as (
        select
            transaction_id
            , customer_id
            , transaction_date
            , transaction_timestamp
        from {{ ref('int_valid_transaction') }}
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
            vt.transaction_id
            , vt.customer_id
            , vt.transaction_date
            , vt.transaction_timestamp
            , se.campaign_id_clean
            , row_number() over (
                partition by vt.transaction_id
                order by se.event_timestamp desc
            ) as rn_event_date
        from int_valid_transaction as vt
        left join stg_events as se
            on vt.customer_id = se.customer_id
            and date_diff(se.event_timestamp, vt.transaction_timestamp, day) between 0 and 30
            and se.event_timestamp <= vt.transaction_timestamp
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
