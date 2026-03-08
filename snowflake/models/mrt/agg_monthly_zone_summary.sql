{{
    config(
        materialized = 'incremental',
        unique_key   = ['pickup_year', 'pickup_month', 'pickup_zone'],
        incremental_strategy = 'merge'
    )
}}

with trips as (

    select * from {{ ref('fact_yellow_taxi_trips') }}

    {% if is_incremental() %}
    where pickup_date > (select max(date_from_parts(pickup_year, pickup_month, 1)) from {{ this }})
    {% endif %}

),

monthly_zone as (

    select
        pickup_year,
        pickup_month,
        pickup_borough,
        pickup_zone,
        pickup_service_zone,
        pickup_borough_group,
        is_airport_pickup,

        -- volume metrics
        count(*)                                                as trip_count,
        sum(passenger_count)                                    as total_passengers,
        count(distinct pickup_date)                             as active_days,
        round(count(*) / nullif(count(distinct pickup_date), 0), 2)
                                                                as avg_daily_trips,

        -- fare metrics
        round(sum(fare_amount), 2)                              as total_fare,
        round(sum(tip_amount), 2)                               as total_tips,
        round(sum(tolls_amount), 2)                             as total_tolls,
        round(sum(total_amount), 2)                             as total_revenue,
        round(avg(fare_amount), 2)                              as avg_fare,
        round(avg(tip_amount), 2)                               as avg_tip,
        round(avg(total_amount), 2)                             as avg_total,
        round(avg(tip_percentage), 2)                           as avg_tip_percentage,

        -- distance metrics
        round(sum(trip_distance_miles), 2)                      as total_distance_miles,
        round(avg(trip_distance_miles), 2)                      as avg_distance_miles,

        -- duration metrics
        round(avg(trip_duration_minutes), 2)                    as avg_duration_minutes,
        round(avg(avg_speed_mph), 2)                            as avg_speed_mph,

        -- payment mix
        sum(case when payment_type_id = 1 then 1 else 0 end)   as credit_card_trips,
        sum(case when payment_type_id = 2 then 1 else 0 end)   as cash_trips,
        round(
            sum(case when payment_type_id = 1 then 1 else 0 end) * 100.0
            / nullif(count(*), 0), 2
        )                                                       as credit_card_pct,

        -- time-of-day mix
        sum(case when is_rush_hour then 1 else 0 end)          as rush_hour_trips,
        sum(case when is_weekend then 1 else 0 end)            as weekend_trips,
        sum(case when is_late_night then 1 else 0 end)         as late_night_trips

    from trips
    group by 1, 2, 3, 4, 5, 6, 7

)

select * from monthly_zone
