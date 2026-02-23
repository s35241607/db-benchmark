

with base as (
    select a.id + (b.id - 1) * 1000 as po_line_id
    from `procurement`.`numbers_1000` as a
    cross join `procurement`.`numbers_1000` as b
    where a.id + (b.id - 1) * 1000 <= 250000
),
po_data as (
    select po_id, po_date from `procurement`.`fct_po`
)
select 
    b.po_line_id,
    cast(
    
        mod(cast(b.po_line_id - 1 as int), cast(120000 as int))
    
 + 1 as int) as po_id,
    cast(
    
        mod(cast(b.po_line_id - 1 as int), cast(3 as int))
    
 + 1 as int) as po_line_num,
    cast(
    
        mod(cast(b.po_line_id as int), cast(10000 as int))
    
 + 1 as int) as item_id,
    cast(
    
        mod(cast(b.po_line_id as int), cast(50 as int))
    
 * 20 + 20 as int) as po_qty,
    cast(
    
        mod(cast(b.po_line_id as int), cast(100 as int))
    
 + 5 as decimal(28,6)) as unit_price,
    cast(
    
        mod(cast(b.po_line_id as int), cast(80 as int))
    
 + 10 as int) as lead_time_days,
    
    
        date_add(cast(po.po_date as datetime), interval cast(cast(
    
        mod(cast(b.po_line_id as int), cast(80 as int))
    
 as int) + 10 as integer) day)
    
 as expected_date
from base b
left join po_data po on cast(
    
        mod(cast(b.po_line_id - 1 as int), cast(120000 as int))
    
 + 1 as int) = po.po_id