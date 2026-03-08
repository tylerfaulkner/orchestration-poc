{{
    config(
        materialized = 'incremental',
        unique_key   = ['pickup_date', 'pickup_hour', 'pickup_borough'],
        incremental_strategy = 'merge'
    )
}}

with trips as (

    select * from {{ ref('fact_yellow_taxi_trips') }}

    {% if is_incremental() %}
    where pickup_date > (select max(pickup_date) from {{ this }})
    {% endif %}

),

hourly as (

    select
        pickup_date,
        pickup_year,
        pickup_month,
        pickup_day_of_week,
        pickup_hour,
        is_weekend,
        is_rush_hour,
        is_late_night,
        pickup_borough,
        pickup_borough_group,

        -- volume
        count(*)                                                    as trip_count,
        sum(passenger_count)                                        as total_passengers,

        -- revenue
        round(sum(total_amount), 2)                                 as total_revenue,
        round(avg(total_amount), 2)                                 as avg_revenue_per_trip,

        -- trip characteristics
        round(avg(trip_distance_miles), 2)                          as avg_distance_miles,
        round(avg(trip_duration_minutes), 2)                        as avg_duration_minutes,
        round(avg(avg_speed_mph), 2)                                as avg_speed_mph,

        -- tipping
        round(avg(tip_percentage), 2)                               as avg_tip_percentage,

        -- airport trips
        sum(case when is_airport_trip then 1 else 0 end)            as airport_trip_count,
        sum(case when is_airport_pickup then 1 else 0 end)          as airport_pickup_count,

        -- cross-borough movement
        sum(case when trip_borough_type = 'Cross Borough'
                 then 1 else 0 end)                                 as cross_borough_trip_count

    from trips
    group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10

)

select * from hourly
