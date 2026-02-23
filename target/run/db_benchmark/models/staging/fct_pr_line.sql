

  create table `procurement`.`fct_pr_line`
    PROPERTIES (
      "replication_num" = "1"
    )
  as 

with base as (
    select a.id + (b.id - 1) * 1000 as pr_line_id
    from `procurement`.`numbers_1000` as a
    cross join `procurement`.`numbers_1000` as b
    where a.id + (b.id - 1) * 1000 <= 300000
)
select 
    pr_line_id,
    cast(
    
        mod(cast(pr_line_id - 1 as int), cast(150000 as int))
    
 + 1 as int) as pr_id,
    cast(
    
        mod(cast(pr_line_id - 1 as int), cast(2 as int))
    
 + 1 as int) as pr_line_num,
    cast(
    
        mod(cast(pr_line_id as int), cast(10000 as int))
    
 + 1 as int) as item_id,
    cast(
    
        mod(cast(pr_line_id as int), cast(100 as int))
    
 * 10 + 10 as int) as pr_qty,
    
    
        date_add(cast('2024-01-01 08:00:00' as datetime), interval cast(cast(
    
        mod(cast(pr_line_id as int), cast(1500 as int))
    
 as int) + cast(
    
        mod(cast(pr_line_id as int), cast(60 as int))
    
 as int) + 30 as integer) day)
    
 as required_date
from base