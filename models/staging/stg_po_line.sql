{{ config(materialized='table') }}
with base as (
    select a.id + (b.id - 1) * 1000 + (c.id - 1) * 1000000 as po_line_id
    from {{ ref('numbers_1000') }} as a
    cross join {{ ref('numbers_1000') }} as b
    cross join (select id from {{ ref('numbers_1000') }} where id <= 5) as c
),
po_data as (
    select po_id, po_date from {{ ref('stg_po') }}
)
select 
    b.po_line_id,
    cast({{ mod_func('b.po_line_id - 1', '1000000') }} + 1 as {{ dbt.type_int() }}) as po_id,
    cast({{ mod_func('b.po_line_id - 1', '5') }} + 1 as {{ dbt.type_int() }}) as po_line_num,
    cast({{ mod_func('b.po_line_id', '120000') }} + 1 as {{ dbt.type_int() }}) as item_id,
    cast({{ mod_func('b.po_line_id', '50') }} * 20 + 20 as {{ dbt.type_int() }}) as po_qty,
    cast({{ mod_func('b.po_line_id', '100') }} + 5 as {{ dbt.type_numeric() }}) as unit_price,
    cast({{ mod_func('b.po_line_id', '80') }} + 10 as {{ dbt.type_int() }}) as lead_time_days,
    {{ date_add_days('po.po_date', "cast(" ~ mod_func('b.po_line_id', '80') ~ " as " ~ dbt.type_int() ~ ") + 10") }} as expected_date,
    {{ date_add_days('po.po_date', "cast(" ~ mod_func('b.po_line_id', '70') ~ " as " ~ dbt.type_int() ~ ") + 15") }} as promised_date,
    case 
        when {{ mod_func('b.po_line_id', '3') }} = 0 then 'PCS'
        when {{ mod_func('b.po_line_id', '3') }} = 1 then 'EA'
        else 'LOT'
    end as uom,
    cast({{ mod_func('b.po_line_id', '4') }} * 0.05 as {{ dbt.type_numeric() }}) as tax_rate,
    cast({{ mod_func('b.po_line_id', '10') }} * 1.5 as {{ dbt.type_numeric() }}) as discount_pct,
    case 
        when {{ mod_func('b.po_line_id', '3') }} = 0 then 'A-Grade'
        when {{ mod_func('b.po_line_id', '3') }} = 1 then 'B-Grade'
        else 'Standard'
    end as quality_requirement_code
from base b
left join po_data po on cast({{ mod_func('b.po_line_id - 1', '1000000') }} + 1 as {{ dbt.type_int() }}) = po.po_id
