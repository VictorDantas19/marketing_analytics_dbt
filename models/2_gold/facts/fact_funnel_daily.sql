with base as (

    select
        event_date,
        campaign_id_clean as campaign_id,
        event_type
    from {{ ref('stg_events') }}

),

agg as (

    select
        event_date as date_day,
        campaign_id,

        countif(event_type = 'click') as clicks,
        countif(event_type = 'lead') as leads,
        countif(event_type = 'conversion') as conversions,
        countif(event_type = 'purchase') as purchases

    from base
    group by 1, 2

)

select *
from agg