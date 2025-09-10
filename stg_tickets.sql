{{ config(materialized='view') }}

with base as (
    select
        inc_number,
        inc_business_service,
        inc_category,
        inc_priority,
        inc_sla_due,
        inc_sys_created_on::text as inc_sys_created_on_raw,
        inc_resolved_at::text as inc_resolved_at_raw,
        inc_assigned_to,
        inc_state,
        inc_cmdb_ci,
        inc_caller_id,
        inc_short_description,
        inc_assignment_group,
        inc_close_code,
        inc_close_notes
    from {{ source('servicenow','tickets') }}
),

cleaned as (
    select
        distinct *,
        
        nullif(nullif(inc_sys_created_on_raw, ''), 'UNKNOWN')::timestamp as created_at,
        nullif(nullif(inc_resolved_at_raw, ''), 'UNKNOWN')::timestamp as resolved_at,

        extract(year from nullif(nullif(inc_sys_created_on_raw, ''), 'UNKNOWN')::timestamp) as created_year,
        extract(month from nullif(nullif(inc_sys_created_on_raw, ''), 'UNKNOWN')::timestamp) as created_month,
        extract(day from nullif(nullif(inc_sys_created_on_raw, ''), 'UNKNOWN')::timestamp) as created_day
    from base
)

select * from cleaned
