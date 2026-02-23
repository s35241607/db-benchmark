{{ config(materialized='table') }}

with base as (
    select a.id + (b.id - 1) * 1000 as pr_id
    from {{ ref('numbers_1000') }} as a
    cross join {{ ref('numbers_1000') }} as b
    where a.id + (b.id - 1) * 1000 <= 150000
)
select 
    pr_id,
    {{ generate_string_id('PR2024_', 'pr_id') }} as pr_no,
    {{ date_add_days("'2024-01-01 08:00:00'", mod_func('pr_id', '1500')) }} as pr_date,
    {{ mod_func('pr_id', '500') }} + 1 as requester_id,
    {{ mod_func('pr_id', '10') }} + 1 as department_id,
    case {{ mod_func('pr_id', '3') }}
        when 0 then 'APPROVED'
        when 1 then 'PENDING'
        else 'REJECTED'
    end as pr_status
from base
