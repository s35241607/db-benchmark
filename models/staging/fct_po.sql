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
    end as po_status,
    {{ mod_func('po_id', '50') }} + 1 as buyer_id,
    case {{ mod_func('po_id', '4') }}
        when 0 then 'FOB'
        when 1 then 'CIF'
        when 2 then 'EXW'
        else 'DDP'
    end as incoterms,
    case {{ mod_func('po_id', '3') }}
        when 0 then 'Standard'
        when 1 then 'Blanket'
        else 'Contract'
    end as po_type
from base
