{{
    config(
        materialized='table'
    )
}}

{#
    Task 2: Data Modeling - User Dimension (Optional)

    Goal: Create a dimension table for users

    Consider including:
        - user_id (natural key)
        - First activity timestamp
        - Last activity timestamp
        - Device count
        - Preferred platform (from login events)

    This dimension is optional but recommended.
#}

with stg as (
    select *
    from {{ ref('stg_events')}}
    where user_id is not null
),

user_activity as (
    select 
        user_id,
        device_id,
        event_type,
        event_ts_utc,
        payload
    from stg
),

aggregated as (
    select 
        user_id,

        min(event_ts_utc) as first_activity_ts,
        max(event_ts_utc) as last_activity_ts,
        count(*) as total_events,
        count(distinct device_id) as device_count,
        arg_max(
            json_extract(payload, '$.platform')::varchar,
            case when event_type = 'user_login' then event_ts_utc else null end
        ) as perferred_platform
    from user_activity
    group by user_id
)

select *
from aggregated
