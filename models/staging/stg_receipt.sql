{{ config(materialized='table') }}
with base as (
    select a.id + (b.id - 1) * 1000 + (c.id - 1) * 1000000 as receipt_id
    from {{ ref('numbers_1000') }} as a
    cross join {{ ref('numbers_1000') }} as b
    cross join (select id from {{ ref('numbers_1000') }} where id <= 7) as c
),
pol_data as (
    select po_line_id, expected_date, po_qty from {{ ref('stg_po_line') }}
),
joined as (
    select 
        r.receipt_id,
        cast({{ mod_func('r.receipt_id - 1', '5000000') }} + 1 as {{ dbt.type_int() }}) as po_line_id,
        pol.expected_date,
        pol.po_qty
    from base r
    left join pol_data pol on cast({{ mod_func('r.receipt_id - 1', '5000000') }} + 1 as {{ dbt.type_int() }}) = pol.po_line_id
)
select 
    receipt_id,
    {{ generate_string_id('RCV2024_', 'receipt_id') }} as receipt_no,
    po_line_id,
    {{ date_add_days('expected_date', "cast(" ~ mod_func('receipt_id', '14') ~ " as " ~ dbt.type_int() ~ ") - 7") }} as receipt_date,
    cast(po_qty * (0.8 + cast({{ mod_func('receipt_id', '20') }} as {{ dbt.type_numeric() }}) / 100) as {{ dbt.type_int() }}) as received_qty,
    case 
        when {{ mod_func('receipt_id', '4') }} = 0 then 'Pending'
        when {{ mod_func('receipt_id', '4') }} = 1 then 'Rejected'
        else 'Accepted'
    end as inspection_status,
    case 
        when {{ mod_func('receipt_id', '4') }} = 1 then cast(po_qty * 0.1 as {{ dbt.type_int() }})
        else 0
    end as rejected_qty,
    case 
        when {{ mod_func('receipt_id', '4') }} = 1 then 'Quality Issue'
        else null
    end as rejection_reason,
    case 
        when {{ mod_func('receipt_id', '4') }} = 0 then 0
        when {{ mod_func('receipt_id', '4') }} = 1 then 0
        else cast(po_qty * (0.8 + cast({{ mod_func('receipt_id', '20') }} as {{ dbt.type_numeric() }}) / 100) as {{ dbt.type_int() }})
    end as accepted_qty
from joined
