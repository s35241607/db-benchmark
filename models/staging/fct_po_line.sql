{{ config(materialized='table') }}

with base as (
    select a.id + (b.id - 1) * 1000 as po_line_id
    from {{ ref('numbers_1000') }} as a
    cross join {{ ref('numbers_1000') }} as b
    where a.id + (b.id - 1) * 1000 <= 250000
),
po_data as (
    select po_id, po_date from {{ ref('fct_po') }}
)
select 
    b.po_line_id,
    cast({{ mod_func('b.po_line_id - 1', '120000') }} + 1 as {{ dbt.type_int() }}) as po_id,
    cast({{ mod_func('b.po_line_id - 1', '3') }} + 1 as {{ dbt.type_int() }}) as po_line_num,
    cast({{ mod_func('b.po_line_id', '10000') }} + 1 as {{ dbt.type_int() }}) as item_id,
    cast({{ mod_func('b.po_line_id', '50') }} * 20 + 20 as {{ dbt.type_int() }}) as po_qty,
    cast({{ mod_func('b.po_line_id', '100') }} + 5 as {{ dbt.type_numeric() }}) as unit_price,
    cast({{ mod_func('b.po_line_id', '80') }} + 10 as {{ dbt.type_int() }}) as lead_time_days,
    {{ date_add_days('po.po_date', "cast(" ~ mod_func('b.po_line_id', '80') ~ " as " ~ dbt.type_int() ~ ") + 10") }} as expected_date
from base b
left join po_data po on cast({{ mod_func('b.po_line_id - 1', '120000') }} + 1 as {{ dbt.type_int() }}) = po.po_id
