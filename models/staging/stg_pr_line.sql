{{ config(materialized='table') }}
with base as (
    select a.id + (b.id - 1) * 1000 as pr_line_id
    from {{ ref('numbers_1000') }} as a
    cross join {{ ref('numbers_1000') }} as b
    where a.id + (b.id - 1) * 1000 <= 300000
)
select 
    pr_line_id,
    cast({{ mod_func('pr_line_id - 1', '150000') }} + 1 as {{ dbt.type_int() }}) as pr_id,
    cast({{ mod_func('pr_line_id - 1', '2') }} + 1 as {{ dbt.type_int() }}) as pr_line_num,
    cast({{ mod_func('pr_line_id', '10000') }} + 1 as {{ dbt.type_int() }}) as item_id,
    cast({{ mod_func('pr_line_id', '100') }} * 10 + 10 as {{ dbt.type_int() }}) as pr_qty,
    {{ date_add_days("'2024-01-01 08:00:00'", "cast(" ~ mod_func('pr_line_id', '1500') ~ " as " ~ dbt.type_int() ~ ") + cast(" ~ mod_func('pr_line_id', '60') ~ " as " ~ dbt.type_int() ~ ") + 30") }} as required_date,
    case {{ mod_func('pr_line_id', '3') }}
        when 0 then 'PCS'
        when 1 then 'EA'
        else 'LOT'
    end as uom,
    cast({{ mod_func('pr_line_id', '500') }} + 5 as {{ dbt.type_numeric() }}) as estimated_unit_price,
    case {{ mod_func('pr_line_id', '5') }}
        when 0 then 'USD'
        when 1 then 'TWD'
        when 2 then 'JPY'
        when 3 then 'EUR'
        else 'USD'
    end as currency
from base
