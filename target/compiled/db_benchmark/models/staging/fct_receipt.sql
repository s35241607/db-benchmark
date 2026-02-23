

with base as (
    select a.id + (b.id - 1) * 1000 as receipt_id
    from `procurement`.`numbers_1000` as a
    cross join `procurement`.`numbers_1000` as b
    where a.id + (b.id - 1) * 1000 <= 160000
),
po_lines as (
    select po_line_id, expected_date, po_qty from `procurement`.`fct_po_line`
)
select 
    b.receipt_id,
    cast(
    
        mod(cast(b.receipt_id - 1 as int), cast(120000 as int))
    
 + 1 as 
    
        bigint
    
) as po_line_id,
    
    
        concat('GRN_', cast(b.receipt_id as TEXT))
    
 as receipt_no,
    
    
        date_add(cast(pl.expected_date as datetime), interval cast(cast(
    
        mod(cast(b.receipt_id as int), cast(21 as int))
    
 as int) as integer) day)
    
 as receipt_date_base,
    
    
        date_add(cast(pl.expected_date as datetime), interval cast(0 as integer) day)
    
 as receipt_date,
    
        case 
    
        mod(cast(b.receipt_id as int), cast(5 as int))
    

            when 0 then cast(pl.po_qty / 2 as int)
            else pl.po_qty
        end
     as received_qty
from base b
left join po_lines pl on cast(
    
        mod(cast(b.receipt_id - 1 as int), cast(120000 as int))
    
 + 1 as 
    
        bigint
    
) = pl.po_line_id