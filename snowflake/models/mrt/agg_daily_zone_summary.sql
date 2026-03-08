{{
    config(
        materialized = 'incremental',
        unique_key   = ['pickup_date', 'pickup_zone'],
        incremental_strategy = 'merge'
    )
}}

with trips as (

    select * from {{ ref('fact_yellow_taxi_trips') }}

    {% if is_incremental() %}
    where pickup_date > (select max(pickup_date) from {{ this }})
    {% endif %}

),

daily_zone as (

    select
        pickup_date,
        pickup_year,
        pickup_month,
        pickup_day_of_week,
        is_weekend,
        pickup_borough,
        pickup_zone,
        pickup_service_zone,
        pickup_borough_group,
        is_airport_pickup,

        -- volume metrics
        count(*)                                                as trip_count,
        sum(passenger_count)                                    as total_passengers,

        -- fare metrics
        sum(fare_amount)                                        as total_fare,
        sum(tip_amount)                                         as total_tips,
        sum(tolls_amount)                                       as total_tolls,
        sum(total_amount)                                       as total_revenue,
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

        -- rush hour share
        sum(case when is_rush_hour then 1 else 0 end)          as rush_hour_trips

    from trips
    group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10

)

select * from daily_zone
