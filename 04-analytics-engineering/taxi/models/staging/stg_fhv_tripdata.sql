{{ config(materialized='view') }}

select
    -- identifiers
    {{ dbt_utils.surrogate_key('dispatching_base_num', 'pickup_datetime') }} as trip_id,
    cast(pu_location_id as integer) as pickup_location_id,
    cast(do_location_id as integer) as dropoff_location_id,
    -- timestamps
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropoff_datetime as timestamp) as dropoff_datetime,
    -- trip info
    sr_flag,
    dispatching_base_num,
    affiliated_base_number
from {{ source('staging', 'fhv') }}
where date_part('year', pickup_datetime) = 2019
{% if var('is_test_run', default=true) %}
    limit 100
{% endif %}