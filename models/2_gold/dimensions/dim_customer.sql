with 
    stg_customers as (
        select *
        from {{ ref('stg_customers') }}
    )

    , rename_columns as (
        select
            customer_id                     as customer_id
            , customer_signup_date          as signup_date
            , customer_country              as country
            , customer_age                  as age
            , customer_gender               as gender
            , customer_loyalty_tier         as loyalty_tier
            , customer_acquisition_channel  as acquisition_channel
        from stg_customers
    )

    , join_int as (
        select *
        from rename_columns
        left join {{ ref('int_customer_enriched') }}
            using (customer_id)
    )

select *
from join_int
