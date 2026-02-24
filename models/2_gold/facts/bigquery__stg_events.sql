/*
- Granularity: one line per event
- Source: marketing_raw.events_raw
- Objective: to standardize customers attributes for analysis
*/

with 
    source_data as (
        select *
        from {{ source('marketing_raw', 'events_raw') }}
    )
    
    , cast_and_rename_columns as (
        select
            safe_cast(event_id as string)                   as event_id
            , safe_cast(timestamp as timestamp)             as event_date_time
            , safe_cast(customer_id as string)              as customer_id
            , safe_cast(session_id as string)               as session_id
            , safe_cast(event_type as string)               as event_type
            , safe_cast(product_id as string)               as product_id
            , safe_cast(device_type as string)              as device_type
            , safe_cast(traffic_source as string)           as traffic_source
            , safe_cast(campaign_id as string)              as campaign_id
            , safe_cast(page_category as string)            as page_category
            , safe_cast(session_duration_sec as numeric)    as session_duration_sec
            , safe_cast(experiment_group as string)         as experiment_group
        from source_data
    )
    
    , add_event_scope as (
        select
            event_id
            , event_date_time
            , customer_id
            , session_id
            , event_type
            , product_id
            , case
                when product_id is not null then 'product_level'
                when event_type in ('purchase', 'checkout_start') then 'session_level'
                else 'session_level'
            end as event_scope
            , device_type
            , traffic_source
            , campaign_id
            , page_category
            , session_duration_sec
            , experiment_group
        from cast_and_rename_columns
    )

select *
from add_event_scope
