with 
    stg_campaigns as (
        select *
        from {{ ref('stg_campaigns') }}
    )

    , rename_columns as (
        select
            campaign_id                         as campaign_id
            , campaign_channel                  as channel
            , campaign_objective                as objective
            , campaign_start_date               as start_date
            , campaign_end_date                 as end_date
            , campaign_target_segment           as target_segment
            , campaign_expected_uplift          as expected_uplift
            , campaign_is_marketing_campaign    as is_marketing_campaign
        from stg_campaigns
    )

select *
from rename_columns
