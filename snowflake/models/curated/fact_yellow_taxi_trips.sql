{{
    config(
        materialized = 'incremental',
        unique_key   = 'trip_id',
        incremental_strategy = 'merge'
    )
}}

with trips as (

    select * from {{ ref('stg_yellow_taxi_trips') }}

    {% if is_incremental() %}
    where pickup_datetime > (select max(pickup_datetime) from {{ this }})
    {% endif %}

),

vendors as (

    select * from {{ ref('vendor') }}

),

rate_codes as (

    select * from {{ ref('rate_code') }}

),

payment_types as (

    select * from {{ ref('payment_type') }}

),

store_fwd as (

    select * from {{ ref('store_and_fwd_flag') }}

),

pickup_loc as (

    select * from {{ ref('dim_location') }}

),

dropoff_loc as (

    select * from {{ ref('dim_location') }}

),

enriched as (

    select
        -- surrogate key
        t.trip_id,

        -- timestamps
        t.pickup_datetime,
        t.dropoff_datetime,
        date(t.pickup_datetime)                                 as pickup_date,
        dayname(t.pickup_datetime)                              as pickup_day_of_week,
        hour(t.pickup_datetime)                                 as pickup_hour,
        month(t.pickup_datetime)                                as pickup_month,
        year(t.pickup_datetime)                                 as pickup_year,

        -- trip duration
        datediff('second', t.pickup_datetime, t.dropoff_datetime)
                                                                as trip_duration_seconds,
        round(
            datediff('second', t.pickup_datetime, t.dropoff_datetime) / 60.0, 2
        )                                                       as trip_duration_minutes,

        -- trip details
        t.passenger_count,
        t.trip_distance_miles,

        -- average speed (guard against division by zero)
        case
            when datediff('second', t.pickup_datetime, t.dropoff_datetime) > 0
                 and t.trip_distance_miles > 0
            then round(
                t.trip_distance_miles
                / (datediff('second', t.pickup_datetime, t.dropoff_datetime) / 3600.0),
                2
            )
            else null
        end                                                     as avg_speed_mph,

        -- vendor
        t.vendor_id,
        v.vendor_name,

        -- rate code
        t.rate_code_id,
        rc.rate_code_name,

        -- payment
        t.payment_type_id,
        pt.payment_type_name,

        -- store and forward
        t.store_and_fwd_flag,
        sf.store_and_fwd_description,

        -- pickup location
        t.pickup_location_id,
        pul.borough                                             as pickup_borough,
        pul.zone_name                                           as pickup_zone,
        pul.service_zone                                        as pickup_service_zone,
        pul.is_airport                                          as is_airport_pickup,
        pul.borough_group                                       as pickup_borough_group,
        pul.manhattan_subregion                                  as pickup_manhattan_subregion,

        -- dropoff location
        t.dropoff_location_id,
        dol.borough                                             as dropoff_borough,
        dol.zone_name                                           as dropoff_zone,
        dol.service_zone                                        as dropoff_service_zone,
        dol.is_airport                                          as is_airport_dropoff,
        dol.borough_group                                       as dropoff_borough_group,
        dol.manhattan_subregion                                  as dropoff_manhattan_subregion,

        -- fare components
        t.fare_amount,
        t.extra_amount,
        t.mta_tax,
        t.tip_amount,
        t.tolls_amount,
        t.improvement_surcharge,
        t.congestion_surcharge,
        t.airport_fee,
        t.total_amount,

        -- calculated fare metrics
        case
            when t.fare_amount > 0
            then round(t.tip_amount / t.fare_amount * 100, 2)
            else null
        end                                                     as tip_percentage,

        case
            when t.trip_distance_miles > 0
            then round(t.fare_amount / t.trip_distance_miles, 2)
            else null
        end                                                     as fare_per_mile,

        case
            when datediff('second', t.pickup_datetime, t.dropoff_datetime) > 0
            then round(
                t.fare_amount
                / (datediff('second', t.pickup_datetime, t.dropoff_datetime) / 60.0),
                2
            )
            else null
        end                                                     as fare_per_minute,

        -- categorical flags
        case
            when hour(t.pickup_datetime) between 7 and 9
              or hour(t.pickup_datetime) between 16 and 19
            then true
            else false
        end                                                     as is_rush_hour,

        case
            when dayname(t.pickup_datetime) in ('Sat', 'Sun')
            then true
            else false
        end                                                     as is_weekend,

        case
            when hour(t.pickup_datetime) between 0 and 5
            then true
            else false
        end                                                     as is_late_night,

        case
            when pul.is_airport = true or dol.is_airport = true
            then true
            else false
        end                                                     as is_airport_trip,

        case
            when pul.borough = dol.borough then 'Same Borough'
            else 'Cross Borough'
        end                                                     as trip_borough_type

    from trips t
    left join vendors v          on t.vendor_id = v.vendor_id
    left join rate_codes rc      on t.rate_code_id = rc.rate_code_id
    left join payment_types pt   on t.payment_type_id = pt.payment_type_id
    left join store_fwd sf       on t.store_and_fwd_flag = sf.store_and_fwd_flag
    left join pickup_loc pul     on t.pickup_location_id = pul.location_id
    left join dropoff_loc dol    on t.dropoff_location_id = dol.location_id

)

select * from enriched
where trip_duration_seconds > 0
  and trip_duration_seconds < 86400       -- exclude trips longer than 24 hours
  and trip_distance_miles >= 0
  and trip_distance_miles < 500           -- exclude implausible distances
  and fare_amount >= 0                    -- exclude negative fares
  and total_amount >= 0
