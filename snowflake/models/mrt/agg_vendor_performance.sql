{{
    config(
        materialized = 'incremental',
        unique_key   = ['pickup_year', 'pickup_month', 'vendor_id'],
        incremental_strategy = 'merge'
    )
}}

with trips as (

    select * from {{ ref('fact_yellow_taxi_trips') }}

    {% if is_incremental() %}
    where pickup_date > (select max(date_from_parts(pickup_year, pickup_month, 1)) from {{ this }})
    {% endif %}

),

vendor_monthly as (

    select
        pickup_year,
        pickup_month,
        vendor_id,
        vendor_name,

        -- volume
        count(*)                                                        as trip_count,
        sum(passenger_count)                                            as total_passengers,
        round(avg(passenger_count), 2)                                  as avg_passengers_per_trip,

        -- revenue
        round(sum(total_amount), 2)                                     as total_revenue,
        round(avg(total_amount), 2)                                     as avg_revenue_per_trip,
        round(avg(fare_per_mile), 2)                                    as avg_fare_per_mile,
        round(avg(fare_per_minute), 2)                                  as avg_fare_per_minute,

        -- trip profile
        round(avg(trip_distance_miles), 2)                              as avg_distance_miles,
        round(avg(trip_duration_minutes), 2)                            as avg_duration_minutes,
        round(avg(avg_speed_mph), 2)                                    as avg_speed_mph,

        -- tipping
        round(avg(tip_percentage), 2)                                   as avg_tip_percentage,
        round(sum(tip_amount), 2)                                       as total_tips,

        -- payment split
        round(
            sum(case when payment_type_id = 1 then 1 else 0 end) * 100.0
            / nullif(count(*), 0), 2
        )                                                               as credit_card_pct,
        round(
            sum(case when payment_type_id = 2 then 1 else 0 end) * 100.0
            / nullif(count(*), 0), 2
        )                                                               as cash_pct,

        -- store-and-forward (connectivity health indicator)
        round(
            sum(case when store_and_fwd_flag = 'Y' then 1 else 0 end) * 100.0
            / nullif(count(*), 0), 2
        )                                                               as store_and_fwd_pct,

        -- trip type mix
        round(
            sum(case when is_airport_trip then 1 else 0 end) * 100.0
            / nullif(count(*), 0), 2
        )                                                               as airport_trip_pct,
        round(
            sum(case when is_rush_hour then 1 else 0 end) * 100.0
            / nullif(count(*), 0), 2
        )                                                               as rush_hour_trip_pct

    from trips
    group by 1, 2, 3, 4

)

select * from vendor_monthly
