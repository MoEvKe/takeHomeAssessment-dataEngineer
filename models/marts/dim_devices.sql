{{
    config(
        materialized='table'
    )
}}

{#
    Task 2: Data Modeling - Device Dimension

    Goal: Create a dimension table for devices

    Consider including:
        - device_id (natural key)
        - First seen / last seen timestamps
        - Associated user_id (owner)
        - Device metadata if available  
        - Any derived attributes

    Document in README:
        - Why you chose these columns
        - What business questions this supports
#}

with stg as (
    select *
    from {{ ref('stg_events')}}
    where device_id is not null
),

device_events as (
    select 
        device_id,
        user_id,
        event_ts_utc,
        device_metadata
    from stg
),

aggregated as (
    select 
        device_id,
        min(event_ts_utc) as first_seen_ts,
        max(event_ts_utc) as last_seen_ts,
        arg_max(user_id, event_ts_utc) as owner_user_id,
        arg_max(
            json_extract(device_metadata, '$.model')::varchar,
            event_ts_utc
        ) as device_model,
        arg_max(
            try_cast(json_extract(device_metadata, '$.install_date') as date),
            event_ts_utc
        ) as install_date,
        count(*) as total_events
    from device_events
    group by device_id
)

select *
from aggregated
