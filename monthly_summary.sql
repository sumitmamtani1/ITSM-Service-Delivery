
with base as (
    select *
         , (extract(epoch from (resolved_at - created_at))/3600) as resolution_hours
         , case when inc_state='Closed' then 1 else 0 end as is_closed
    from {{ ref('stg_tickets') }}
)

select
    created_year,
    created_month,
    count(*) as total_tickets,
    round(avg(resolution_hours),2) as avg_resolution_hours,
    round(sum(is_closed)::decimal / count(*) * 100,2) as closure_rate_percent
from base
group by created_year, created_month
order by created_year, created_month
