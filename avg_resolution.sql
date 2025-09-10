
with resolved_tickets as (
    select *
         , (extract(epoch from (resolved_at - created_at))/3600) as resolution_hours
    from {{ ref('stg_tickets') }}
    where resolved_at is not null
)

select
    inc_category,
    inc_priority,
    avg(resolution_hours) as avg_resolution_hours,
    count(*) as total_tickets
from resolved_tickets
group by 1,2
order by inc_category, inc_priority
