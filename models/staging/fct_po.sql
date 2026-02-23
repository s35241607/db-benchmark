{{ config(materialized='table') }}

with base as (
    select a.id + (b.id - 1) * 1000 as po_id
    from {{ ref('numbers_1000') }} as a
    cross join {{ ref('numbers_1000') }} as b
    where a.id + (b.id - 1) * 1000 <= 120000
)
select 
    po_id,
    {{ generate_string_id('PO2024_', 'po_id') }} as po_no,
    {{ date_add_days("'2024-01-01 08:00:00'", mod_func('po_id', '1500')) }} as po_date,
    cast({{ mod_func('po_id', '1000') }} + 1 as {{ dbt.type_int() }}) as supplier_id,
    case {{ mod_func('po_id', '5') }}
        when 0 then 'CLOSED'
        when 1 then 'CANCELLED'
        else 'OPEN'
    end as po_status
from base
