with 
    date_interval as (
        select day as date_day
        from unnest(
            generate_date_array(
                date('2020-01-01')
                , date('2030-12-31')
                , interval 1 day
            )
        ) as day
    )

    , all_formats_date as (
        select
            date_day
            , safe_cast(format_date('%Y%m%d', date_day) as string)                                  as date_sk
            , extract(year from date_day)                                                           as year
            , extract(isoyear from date_day)                                                        as iso_year
            , extract(quarter from date_day)                                                        as quarter
            , concat('Q', cast(extract(quarter from date_day) as string))                           as quarter_name
            , extract(month from date_day)                                                          as month
            , format_date('%B', date_day)                                                           as month_name
            , format_date('%b', date_day)                                                           as month_name_short
            , format_date('%Y-%m', date_day)                                                        as year_month
            , date_trunc(date_day, month)                                                           as first_day_month
            , concat(format_date('%b', date_day), '/', cast(extract(year from date_day) as string)) as month_year_label
            , extract(week from date_day)                                                           as week_of_year
            , extract(isoweek from date_day)                                                        as iso_week
            , extract(day from date_day)                                                            as day_of_month
            , extract(dayofweek from date_day)                                                      as day_of_week
            , format_date('%A', date_day)                                                           as day_name
            , format_date('%a', date_day)                                                           as day_name_short
            , case 
                when extract(dayofweek from date_day) in (1,7) then true
                else false
            end as is_weekend
        from date_interval
    )

select
    date_day
    , date_sk
    , year
    , iso_year
    , quarter
    , quarter_name
    , month
    , month_name
    , month_name_short
    , year_month
    , first_day_month
    , month_year_label
    , week_of_year
    , iso_week
    , day_of_month
    , day_of_week
    , day_name
    , day_name_short
from all_formats_date