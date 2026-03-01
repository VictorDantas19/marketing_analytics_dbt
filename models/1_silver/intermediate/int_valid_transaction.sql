with 
    stg_transactions as (
        select *
        from {{ ref('stg_transactions') }}
        where is_valid_revenue = true
    )

select *
from stg_transactions
