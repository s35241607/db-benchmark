

with base as (
    select a.id + (b.id - 1) * 1000 as pr_id
    from `procurement`.`numbers_1000` as a
    cross join `procurement`.`numbers_1000` as b
    where a.id + (b.id - 1) * 1000 <= 150000
)
select 
    pr_id,
    
    
        concat('PR2024_', cast(pr_id as TEXT))
    
 as pr_no,
    
    
        date_add(cast('2024-01-01 08:00:00' as datetime), interval cast(
    
        mod(cast(pr_id as int), cast(1500 as int))
    
 as integer) day)
    
 as pr_date,
    
    
        mod(cast(pr_id as int), cast(500 as int))
    
 + 1 as requester_id,
    
    
        mod(cast(pr_id as int), cast(10 as int))
    
 + 1 as department_id,
    case 
    
        mod(cast(pr_id as int), cast(3 as int))
    

        when 0 then 'APPROVED'
        when 1 then 'PENDING'
        else 'REJECTED'
    end as pr_status
from base