{{ config(materialized="view") }}

with
    tripdata as (
        select
            *,
            row_number() over (partition by dispatching_base_num, pickup_datetime) as rn
        from {{ source("staging", "fhv_tripdata") }}
        where
            dispatching_base_num is not null
            and extract(year from pickup_datetime) = 2019
    )
select
    -- identifiers
    {{ dbt_utils.surrogate_key(["dispatching_base_num", "pickup_datetime"]) }}
    as tripid,
    affiliated_base_number,
    dispatching_base_num,
    cast(pulocation_id as integer) as pickup_locationid,
    cast(dolocation_id as integer) as dropoff_locationid,

    -- timestamps
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(drop_off_datetime as timestamp) as dropoff_datetime,

    -- trip info
    sr_flag
from tripdata
where rn = 1

{% if var("is_test_run", default=false) %} limit 100 {% endif %}
