{{
    config(
        materialized='table'
    )
}}

select trips_unioned.tripid, 
    trips_unioned.dispatching_base_num, 
    trips_unioned.pickup_locationid, 
    pickup_zone.borough as pickup_borough, 
    pickup_zone.zone as pickup_zone, 
    trips_unioned.dropoff_locationid,
    trips_unioned.pickup_datetime, 
    trips_unioned.dropoff_datetime,
    dropoff_zone.borough as dropoff_borough, 
    dropoff_zone.zone as dropoff_zone,   
    trips_unioned.store_and_fwd_flag,
    trips_unioned.affiliated_base_number,
from trips_unioned
inner join dim_zones as pickup_zone
on trips_unioned.pickup_locationid = pickup_zone.locationid
inner join dim_zones as dropoff_zone
on trips_unioned.dropoff_locationid = dropoff_zone.locationid