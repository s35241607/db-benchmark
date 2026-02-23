

with base as (
    select pr_id, pr_no, pr_date
    from `procurement`.`fct_pr`
    where pr_status = 'APPROVED'
)
select 
    pr_id as rfq_id,
    
    
        concat('RFQ_', cast(pr_id as TEXT))
    
 as rfq_no,
    pr_id,
    
    
        date_add(cast(pr_date as datetime), interval cast(cast(
    
        mod(cast(pr_id as int), cast(4 as int))
    
 as int) + 1 as integer) day)
    
 as rfq_date,
    cast(
    
        mod(cast(pr_id as int), cast(1000 as int))
    
 + 1 as int) as supplier_id,
    case 
    
        mod(cast(pr_id as int), cast(2 as int))
    

        when 0 then 'AWARDED'
        else 'CLOSED'
    end as rfq_status
from base