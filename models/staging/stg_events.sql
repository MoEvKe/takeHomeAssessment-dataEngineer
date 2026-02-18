{{
    config(
        materialized='view'
    )
}}

{#
    ***Task 1 Complete. dbt test was successfull for all four columns***
    
    Task 1: Data Ingestion & Cleaning

    Goal: Transform raw events into a clean, standardized format

    Requirements:
        1. Normalize timestamps to a consistent format (UTC)
        2. Deduplicate events (hint: check event_id)
        3. Handle missing or invalid fields
        4. Standardize case (e.g., user_id)
        5. Filter out invalid records

    Known data issues to handle:
        - Duplicate event_ids (evt_001, evt_016 appear twice) --Done
        - Multiple timestamp formats: --Done
            * ISO 8601: "2024-01-15T10:30:00Z"
            * ISO with offset: "2024-01-15T08:00:00-05:00"
            * Space-separated: "2024-01-15 10:25:00"
            * US format: "01/15/2024 11:00:00 AM EST"
            * Unix epoch: "1705320000"
            * Human readable: "Jan 16, 2024 10:15:30 AM"
            * European: "17-01-2024 20:15:00"
        - Empty event_id (one record) -- Done
        - Null device_id (one record) -- Done
        - Case mismatch: "USR_100" vs "usr_100" -- Done
        - Invalid timestamp: "not_a_timestamp" -- Done

    Considerations:
        - Idempotency: dbt handles this via materialization
        - Document your assumptions in README
        - Consider: should invalid records be filtered or flagged?

    Hint: You may want to use CTEs to break this into steps:
        1. source_data - reference the raw source
        2. normalized - handle timestamp parsing
        3. deduplicated - remove duplicates
        4. final - apply remaining transformations
#}

with source_data as (

    select * from {{ source('raw', 'raw_events') }}

),

clean_up as (
    select 
        event_id,
        event_type,
        device_id,
        lower(user_id) as user_id, 
        case    
            when regexp_matches(timestamp, '^[0-9]{10}$')
                then to_timestamp(timestamp::bigint)
            when try_strptime(timestamp, '%Y-%m-%dT%H:%M:%SZ') is not null
                then try_strptime(timestamp, '%Y-%m-%dT%H:%M:%SZ')
            when try_strptime(timestamp, '%Y-%m-%dT%H:%M:%S%z') is not null
                then try_strptime(timestamp, '%Y-%m-%dT%H:%M:%S%z') AT TIME ZONE 'UTC'
            when try_strptime(timestamp, '%Y-%m-%d %H:%M:%S') is not null
                then try_strptime(timestamp, '%Y-%m-%d %H:%M:%S') AT TIME ZONE 'UTC'
            when try_strptime(timestamp, '%m/%d/%Y %I:%M:%S %p') is not null
                then try_strptime(timestamp, '%m/%d/%Y %I:%M:%S %p') AT TIME ZONE 'UTC'
            when try_strptime(timestamp, '%b %d, %Y %I:%M:%S %p') is not null
                then try_strptime(timestamp, '%b %d, %Y %I:%M:%S %p') AT TIME ZONE 'UTC'
            when try_strptime(timestamp, '%d-%m-%Y %H:%M:%S') is not null
                then try_strptime(timestamp, '%d-%m-%Y %H:%M:%S') AT TIME ZONE 'UTC'
            else null
        end as event_ts_utc,
        payload,
        location,
        device_metadata,
        case 
            when event_id is null or event_id = ''
                then true
            else false
        end as is_missing_event_id,
        case    
            when event_type is null
                then true
            else false
        end as is_missing_event_type,
        case 
            when timestamp is null 
                then true
            else false
        end as is_missing_timestamp,
        case when
            case when regexp_matches(timestamp, '^[0-9]{10}$')
                    then to_timestamp(timestamp::bigint)
                when try_strptime(timestamp, '%Y-%m-%dT%H:%M:%SZ') is not null
                    then try_strptime(timestamp, '%Y-%m-%dT%H:%M:%SZ')
                when try_strptime(timestamp, '%Y-%m-%dT%H:%M:%S%z') is not null
                    then try_strptime(timestamp, '%Y-%m-%dT%H:%M:%S%z')
                when try_strptime(timestamp, '%Y-%m-%d %H:%M:%S') is not null
                    then try_strptime(timestamp, '%Y-%m-%d %H:%M:%S')
                else null
            end is null
            then true
            else false
        end as is_invalid_timestamp
    from source_data
    ),

deduped as (
    select *
    from (
        select 
            *,
            row_number() over(
                partition by event_id
                order by event_ts_utc desc nulls last
            ) as rn 
        from clean_up
    )
    where rn = 1
),


final as (

    select

        event_id,
        event_type,
        device_id,
        user_id,
        event_ts_utc,  -- normalized to UTC
        payload,
        is_missing_event_id,
        is_missing_event_type,
        is_missing_timestamp,
        is_invalid_timestamp
    from deduped
    where is_missing_event_id = false
        and is_invalid_timestamp = false

)

select * from final
