with 
    source_data as (
        select *
        from {{ source('marketing_raw', 'events_raw') }}
    )
    
    , cast_and_rename_columns as (
        select
            safe_cast(event_id as string)                                           as event_id
            , safe_cast(timestamp as timestamp)                                     as event_timestamp
            , safe_cast(customer_id as string)                                      as customer_id
            , safe_cast(session_id as string)                                       as session_id
            , trim(lower(safe_cast(event_type as string)))                          as event_type
            , safe_cast(product_id as string)                                       as product_id
            , coalesce(trim(lower(safe_cast(device_type as string))), 'unknown')    as event_device_type
            , trim(lower(safe_cast(traffic_source as string)))                      as event_traffic_source
            , safe_cast(campaign_id as string)                                      as campaign_id
            , trim(lower(safe_cast(page_category as string)))                       as event_page_category
            , safe_cast(session_duration_sec as numeric)                            as event_session_duration_sec
            , trim(lower(safe_cast(experiment_group as string)))                    as event_experiment_group
        from source_data
    )

    , add_campaign_organic as (
        select
            *
            , case
                when campaign_id in (select campaign_id from {{ ref('stg_campaigns') }})
                    then campaign_id
                else '(organic)'
            end as campaign_id_clean
        from cast_and_rename_columns
    )

    , add_event_date as (
        select
            *
            , date(event_timestamp) as event_date
            , date_trunc(date(event_timestamp), month) as event_month
        from add_campaign_organic
    )

    , flag_paid_campaign as (
        select
            *
            , case
                when campaign_id_clean = '(organic)'
                    then false
                else true
            end as is_paid_campaign
        from add_event_date
    )

select *
from flag_paid_campaign
