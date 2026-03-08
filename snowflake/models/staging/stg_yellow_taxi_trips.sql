with source as (

    select * from {{ source('edl', 'YELLOW_TAXI_TRIPS') }}

),

renamed as (

    select
        -- identifiers
        cast("VENDORID" as integer)                     as vendor_id,
        cast("RATECODEID" as integer)                   as rate_code_id,
        cast("PULOCATIONID" as integer)                 as pickup_location_id,
        cast("DOLOCATIONID" as integer)                 as dropoff_location_id,
        cast("PAYMENT_TYPE" as integer)                 as payment_type_id,

        -- flags
        upper(trim("STORE_AND_FWD_FLAG"))               as store_and_fwd_flag,

        -- trip details
        cast("PASSENGER_COUNT" as integer)              as passenger_count,
        cast("TRIP_DISTANCE" as float)                  as trip_distance_miles,

        -- timestamps
        cast("TPEP_PICKUP_DATETIME" as timestamp_ntz)   as pickup_datetime,
        cast("TPEP_DROPOFF_DATETIME" as timestamp_ntz)  as dropoff_datetime,

        -- fare components
        cast("FARE_AMOUNT" as number(10,2))             as fare_amount,
        cast("EXTRA" as number(10,2))                   as extra_amount,
        cast("MTA_TAX" as number(10,2))                 as mta_tax,
        cast("TIP_AMOUNT" as number(10,2))              as tip_amount,
        cast("TOLLS_AMOUNT" as number(10,2))            as tolls_amount,
        cast("IMPROVEMENT_SURCHARGE" as number(10,2))   as improvement_surcharge,
        cast("CONGESTION_SURCHARGE" as number(10,2))    as congestion_surcharge,
        cast("AIRPORT_FEE" as number(10,2))             as airport_fee,
        cast("TOTAL_AMOUNT" as number(10,2))            as total_amount

    from source

),

deduplicated as (

    select
        *,
        row_number() over (
            partition by
                vendor_id,
                rate_code_id,
                pickup_location_id,
                dropoff_location_id,
                payment_type_id,
                store_and_fwd_flag,
                passenger_count,
                trip_distance_miles,
                pickup_datetime,
                dropoff_datetime,
                fare_amount,
                extra_amount,
                mta_tax,
                tip_amount,
                tolls_amount,
                improvement_surcharge,
                congestion_surcharge,
                airport_fee,
                total_amount
            order by pickup_datetime
        ) as row_num

    from renamed

),

final as (

    select
        -- surrogate key using row-level hash (now unique after dedup)
        {{ dbt_utils.generate_surrogate_key([
            'pickup_datetime',
            'dropoff_datetime',
            'pickup_location_id',
            'dropoff_location_id',
            'vendor_id',
            'rate_code_id',
            'payment_type_id',
            'passenger_count',
            'trip_distance_miles',
            'fare_amount',
            'tip_amount',
            'tolls_amount',
            'total_amount'
        ]) }}                   as trip_id,

        vendor_id,
        rate_code_id,
        pickup_location_id,
        dropoff_location_id,
        payment_type_id,
        store_and_fwd_flag,
        passenger_count,
        trip_distance_miles,
        pickup_datetime,
        dropoff_datetime,
        fare_amount,
        extra_amount,
        mta_tax,
        tip_amount,
        tolls_amount,
        improvement_surcharge,
        congestion_surcharge,
        airport_fee,
        total_amount

    from deduplicated
    where row_num = 1

)

select * from final
