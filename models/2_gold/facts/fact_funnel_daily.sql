with 
    base as (
        select
            event_date
            , campaign_id_clean as campaign_id
            , event_type
        from {{ ref('stg_events') }}
    )

    , funnel as (
        select
            event_date as date_day
            , campaign_id
            , countif(event_type = 'view') as views
            , countif(event_type = 'click') as clicks
            , countif(event_type = 'add_to_cart') as conversions
            , countif(event_type = 'purchase') as purchases
        from base
        group by 
            date_day
            , campaign_id
    )

select
    date_day
    , campaign_id
    , views
    , clicks
    , conversions
    , purchases
from funnel