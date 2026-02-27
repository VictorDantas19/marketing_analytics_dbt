with 
    source_data as (
        select *
        from {{ source('marketing_raw', 'campaigns_raw') }}
    )
    
    , cast_and_rename_columns as (
        select
            safe_cast(campaign_id as string)                    as campaign_id
            , lower(trim(safe_cast(channel as string)))         as campaign_channel
            , lower(trim(safe_cast(objective as string)))       as campaign_objective
            , safe_cast(start_date as date)                     as campaign_start_date
            , safe_cast(end_date as date)                       as campaign_end_date
            , lower(trim(safe_cast(target_segment as string)))  as campaign_target_segment
            , safe_cast(expected_uplift as numeric)             as campaign_expected_uplift
            , true                                              as campaign_is_marketing_campaign
        from source_data
    )

    , add_organic_campaign as (
        select *
        from cast_and_rename_columns
        union all
        select
            '(organic)'             as campaign_id
            , 'organic'             as campaign_channel
            , 'no_paid_campaign'    as campaign_objective
            , null                  as campaign_start_date
            , null                  as campaign_end_date
            , 'all'                 as campaign_target_segment
            , null                  as campaign_expected_uplift
            , false                 as campaign_is_marketing_campaign
    )

select *
from add_organic_campaign
