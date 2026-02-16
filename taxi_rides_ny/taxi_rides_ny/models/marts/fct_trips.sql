-- step1
--select *
--from {{ ref('int_trips_unioned') }}
-- step2
--with trips as (
--    select * from {{ ref('int_trips_unioned') }}
--)
--select * from trips

--step4
/*
with trips as (
    select * from {{ ref('int_trips_unioned') }}
),

dedup as (
    select *
    from (
        select
            *,
            row_number() over (
                partition by
                    vendor_id,
                    pickup_datetime,
                    dropoff_datetime,
                    pickup_location_id,
                    dropoff_location_id,
                    passenger_count,
                    trip_distance,
                    total_amount
                order by pickup_datetime
            ) as rn
        from trips
    )
    where rn = 1
)

select * from dedup
*/
--##5
/*
 with trips as (
    select * from {{ ref('int_trips_unioned') }}
),

dedup as (
    select *
    from (
        select
            *,
            row_number() over (
                partition by
                    vendor_id,
                    pickup_datetime,
                    dropoff_datetime,
                    pickup_location_id,
                    dropoff_location_id,
                    passenger_count,
                    trip_distance,
                    total_amount
                order by pickup_datetime
            ) as rn
        from trips
    )
    where rn = 1
)

select
    md5(
        cast(vendor_id as varchar) || '|' ||
        cast(pickup_datetime as varchar) || '|' ||
        cast(dropoff_datetime as varchar) || '|' ||
        cast(pickup_location_id as varchar) || '|' ||
        cast(dropoff_location_id as varchar) || '|' ||
        cast(passenger_count as varchar) || '|' ||
        cast(trip_distance as varchar) || '|' ||
        cast(total_amount as varchar)
    ) as trip_id,
    *
from dedup
*/

--#6
{{ config(materialized='view') }}

with trips as (
    select * from {{ ref('int_trips_unioned') }}
),

dedup as (
    select *
    from (
        select
            *,
            row_number() over (
                partition by
                    vendor_id,
                    pickup_datetime,
                    dropoff_datetime,
                    pickup_location_id,
                    dropoff_location_id,
                    passenger_count,
                    trip_distance,
                    total_amount
                order by pickup_datetime
            ) as rn
        from trips
    )
    where rn = 1
)

select
    md5(
        cast(vendor_id as varchar) || '|' ||
        cast(pickup_datetime as varchar) || '|' ||
        cast(dropoff_datetime as varchar) || '|' ||
        cast(pickup_location_id as varchar) || '|' ||
        cast(dropoff_location_id as varchar) || '|' ||
        cast(passenger_count as varchar) || '|' ||
        cast(trip_distance as varchar) || '|' ||
        cast(total_amount as varchar)
    ) as trip_id,

    case payment_type
        when 1 then 'Credit card'
        when 2 then 'Cash'
        when 3 then 'No charge'
        when 4 then 'Dispute'
        when 5 then 'Unknown'
        when 6 then 'Voided trip'
        else 'Other/Null'
    end as payment_type_desc,

    *
   
from dedup
