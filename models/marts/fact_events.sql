{{
    config(
        materialized='table'
    )
}}

{#
    Task 2: Data Modeling - Fact Table

    Goal: Create the central fact table for event analytics

    Requirements:
        - Reference staging model (stg_events)
        - Include foreign keys to dimension tables
        - Include relevant measures/metrics
        - Support time-series analysis

    Consider including:
        - event_id (degenerate dimension)
        - Foreign keys: device_id, user_id
        - event_timestamp
        - event_date (for partitioning/filtering)
        - event_type
        - Measures from payload (confidence scores, durations, etc.)

    Document in README:
        - Why you chose this structure
        - What business questions it supports
        - How it would scale as data grows
#}



with stg as (
    select *
    from {{ ref('stg_events')}}
),

typed as (
    select 
        event_id,
        -- foreign keys
        device_id,
        user_id,

        event_ts_utc,
        cast(event_ts_utc as date) as event_date,
        -- dimensions
        event_type,

        try_cast(json_extract(payload, '$.confidence') as double) as confidence_score,
        try_cast(json_extract(payload, '$.duration_seconds') as integer) as duration_seconds,
        json_extract(payload, '$.severity')::varchar as severity,
        json_extract(payload, '$.object_type')::varchar as object_type
    from stg
)

select * 
from typed


