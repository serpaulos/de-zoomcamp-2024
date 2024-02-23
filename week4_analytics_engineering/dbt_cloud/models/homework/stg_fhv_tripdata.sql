{{
    config(
        materialized='view'
    )
}}

select
    -- identifiers
    cast(Dispatching_base_num as string) as dispatching_base_num,
    {{ dbt.safe_cast("PUlocationID", api.Column.translate_type("integer")) }} as pickup_locationid,
    {{ dbt.safe_cast("DOlocationID", api.Column.translate_type("integer")) }} as dropoff_locationid,
    
    -- timestamps
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropOff_datetime as timestamp) as dropoff_datetime,
    
    -- trip info
    cast(SR_Flag as string) as store_and_fwd_flag,
    cast(Affiliated_base_number as string) as affiliated_base_number,

from {{ source('staging', 'fhv_tripdata')}}