with trips_unioned as (
    select * from {{ ref('int_trips_unioned') }}
),
vendors as (
    select distinct vendor_id from trips_unioned
)
select * from vendors