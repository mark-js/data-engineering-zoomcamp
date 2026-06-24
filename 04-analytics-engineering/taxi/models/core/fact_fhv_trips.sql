{{ config(materialized='table') }}

with fhv_data as (
    select *,
        'fhv' as service_type
    from {{ ref('stg_fhv_tripdata') }}
),

dim_zones as (
    select * from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)

select
    fhv_data.trip_id,
    fhv_data.pickup_location_id,
    pickup_zone.borough as pickup_borough,
    pickup_zone.zone as pickup_zone,
    fhv_data.dropoff_location_id,
    dropoff_zone.borough as dropoff_borough,
    dropoff_zone.zone as dropoff_zone,
    fhv_data.pickup_datetime,
    fhv_data.dropoff_datetime,
    sr_flag,
    dispatching_base_num,
    affiliated_base_number
from 
    fhv_data
    inner join dim_zones as pickup_zone on fhv_data.pickup_location_id = pickup_zone.location_id
    inner join dim_zones as dropoff_zone on fhv_data.dropoff_location_id = dropoff_zone.location_id