with source as (

    select * from {{ source('edl', 'TAXI_ZONE_LOOKUP') }}

),

renamed as (

    select
        cast("LOCATIONID" as integer)   as location_id,
        trim("BOROUGH")                 as borough,
        trim("ZONE")                    as zone_name,
        trim("SERVICE_ZONE")            as service_zone

    from source

)

select * from renamed
