{#
    Task 3: Analytics Queries

    Goal: Write SQL to answer 2-3 of the following business questions

    Choose from:
        1. Daily active devices
        2. Events per device per day
        3. Top event types in the last 7 days
        4. Average events per user by device type
        5. Event count trends over time

    Focus on:
        - Correct joins between fact and dimension tables
        - Readability (use CTEs, meaningful aliases)
        - Performance considerations (mention indexes that would help)

    Instructions:
        - Write each query below with a comment explaining:
            * What question it answers
            * Business value
            * Performance notes (what indexes would help)
        - You can run these with: dbt compile --select analytics_queries
          Then execute the compiled SQL in your database

    Note: Analyses are SQL files that dbt compiles but doesn't execute.
    They're useful for ad-hoc queries that reference your models.
#}


-- =============================================================================
-- Query 1: [Choose a question from the list above]
-- =============================================================================
-- Question: Events per device per day
-- Business value: Breakdown of events per device per day can show who is using the service the most.
-- Performance notes: Index on device_id will speed things up if the table gets too large.
-- =============================================================================

-- YOUR QUERY HERE
-- Example: select * from {{ ref('fact_events') }} limit 10

with daily_device_events as (
    select
        date_trunc('day', event_ts_utc)::date as event_date,
        device_id,
        count(*) as event_count,
        count(distinct user_id) as unique_users,
        count(distinct event_type) as unique_event_types,
    from {{ ref('fact_events')}}
    group by event_date, device_id
)

select 
    event_date,
    dd.device_id,
    dd.device_model,
    daily_device_events.event_count,
    daily_device_events.unique_event_types,
    daily_device_events.unique_users
from daily_device_events
left join main_marts.dim_devices dd
    on daily_device_events.device_id = dd.device_id
order by
    event_date desc,
    event_count desc;

-- =============================================================================
-- Query 2: [Choose a question from the list above]
-- =============================================================================
-- Question: Average events per user by device type
-- Business value: Getting a baseline average can help you compare devices to see trends
-- Performance notes: simple query just getting some counts.
-- =============================================================================

-- YOUR QUERY HERE

with events_per_user_devices as (
    select
        d.device_model,
        f.user_id,
        count(*) as event_count
    from  {{ ref('fact_events') }} f
    join {{ ref('dim_devices') }} d
        on f.device_id = d.device_id
    where d.device_model is not null
    group by 
        d.device_model, 
        f.user_id
)

select 
    device_model,
    avg(event_count) as avg_events_per_user,
    count(distinct user_id) as user_count
from events_per_user_devices
group by device_model
order by avg_events_per_user desc;
-- =============================================================================
-- Query 3 (Optional): [Choose a question from the list above]
-- =============================================================================
-- Question: Daily active devices
-- Business value: Active devices would be a good trend line to help us see what direction the business is going.
-- Performance notes: Keeping it simple because I'm running out of time. 
-- =============================================================================

-- YOUR QUERY HERE
 select 
    event_date,
    count(distinct device_id) as daily_active_devices
from {{ ref('fact_events') }}
where device_id is not null
group by event_date
order by event_date;