
with ticket_status as (
    select
        inc_assignment_group,
        count(*) as total_tickets,
        sum(case when inc_state = 'Closed' then 1 else 0 end) as closed_tickets
    from {{ ref('stg_tickets') }}
    group by inc_assignment_group
)

select
    inc_assignment_group,
    total_tickets,
    closed_tickets,
    round((closed_tickets::decimal / total_tickets) * 100,2) as closure_rate_percent
from ticket_status
order by closure_rate_percent desc
