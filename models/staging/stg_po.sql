{{ config(materialized='table') }}
with base as (
    select a.id + (b.id - 1) * 1000 + (c.id - 1) * 1000000 as po_id
    from {{ ref('numbers_1000') }} as a
    cross join {{ ref('numbers_1000') }} as b
    cross join (select id from {{ ref('numbers_1000') }} where id <= 1) as c
)
select 
    po_id,
    {{ generate_string_id('PO2024_', 'po_id') }} as po_no,
    {{ date_add_days("'2024-01-01 08:00:00'", mod_func('po_id', '1500')) }} as po_date,
    cast({{ mod_func('po_id', '1000') }} + 1 as {{ dbt.type_int() }}) as supplier_id,
    case 
        when {{ mod_func('po_id', '5') }} = 0 then 'CLOSED'
        when {{ mod_func('po_id', '5') }} = 1 then 'CANCELLED'
        else 'OPEN'
    end as po_status,
    {{ mod_func('po_id', '50') }} + 1 as buyer_id,
    case 
        when {{ mod_func('po_id', '4') }} = 0 then 'FOB'
        when {{ mod_func('po_id', '4') }} = 1 then 'CIF'
        when {{ mod_func('po_id', '4') }} = 2 then 'EXW'
        else 'DDP'
    end as incoterms,
    case 
        when {{ mod_func('po_id', '3') }} = 0 then 'Standard'
        when {{ mod_func('po_id', '3') }} = 1 then 'Blanket'
        else 'Contract'
    end as po_type
from base
