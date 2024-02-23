{{
    config(
        materialized='view'
    )
}}

select
    -- identifiers
    cast(Dispatching_base_num as varchar) as dispatching_base_num,
    {{ dbt.safe_cast("PUlocationID", api.Column.translate_type("integer")) }} as pickup_locationid,
    {{ dbt.safe_cast("DOlocationID", api.Column.translate_type("integer")) }} as dropoff_locationid,
    
    -- timestamps
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropOff_datetime as timestamp) as dropoff_datetime,
    
    -- trip info
    cast(SR_Flag as varchar) as store_and_fwd_flag,
    cast(Affiliated_base_number as varchar) as affiliated_base_number,

from {{ source('staging', 'fhv_tripdata')}}