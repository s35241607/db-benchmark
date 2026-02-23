

with base as (
    select a.id + (b.id - 1) * 1000 as po_id
    from `procurement`.`numbers_1000` as a
    cross join `procurement`.`numbers_1000` as b
    where a.id + (b.id - 1) * 1000 <= 120000
)
select 
    po_id,
    
    
        concat('PO2024_', cast(po_id as TEXT))
    
 as po_no,
    
    
        date_add(cast('2024-01-01 08:00:00' as datetime), interval cast(
    
        mod(cast(po_id as int), cast(1500 as int))
    
 as integer) day)
    
 as po_date,
    cast(
    
        mod(cast(po_id as int), cast(1000 as int))
    
 + 1 as int) as supplier_id,
    case 
    
        mod(cast(po_id as int), cast(5 as int))
    

        when 0 then 'CLOSED'
        when 1 then 'CANCELLED'
        else 'OPEN'
    end as po_status
from base