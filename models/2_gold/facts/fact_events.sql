with
    events as (
        select
            event_id
            , event_timestamp as timestamp
            , event_date as date
            , customer_id
            , session_id
            , event_type as type
            , product_id
            , event_device_type as device_type
            , event_traffic_source as traffic_source
            , campaign_id_clean as campaign_id
            , is_paid_campaign
            , event_page_category as page_category
            , event_session_duration_sec as session_duration_sec
            , event_experiment_group as experiment_group
        from {{ ref('stg_events') }}
    )

select *
from events