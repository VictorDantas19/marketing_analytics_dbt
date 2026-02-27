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

select
    safe_cast(format_date('%Y%m%d', date_day) as string) as date_sk
    , date_day

    , extract(year from date_day) as year
    , extract(isoyear from date_day) as iso_year

    , extract(quarter from date_day) as quarter
    , concat('Q', cast(extract(quarter from date_day) as string)) as quarter_name

    , extract(month from date_day) as month
    , format_date('%B', date_day) as month_name
    , format_date('%b', date_day) as month_name_short
    , format_date('%Y-%m', date_day) as year_month
    , date_trunc(date_day, month) as first_day_month
    , concat(format_date('%b', date_day), '/', cast(extract(year from date_day) as string)) as month_year_label

    , extract(week from date_day) as week_of_year
    , extract(isoweek from date_day) as iso_week

    , extract(day from date_day) as day_of_month
    , extract(dayofweek from date_day) as day_of_week
    , format_date('%A', date_day) as day_name
    , format_date('%a', date_day) as day_name_short

    , case 
        when extract(dayofweek from date_day) in (1,7) then true
        else false
    end as is_weekend

from date_interval