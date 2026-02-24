{{ config(materialized='table') }}
with base as (
    select a.id + (b.id - 1) * 1000 as receipt_id
    from {{ ref('numbers_1000') }} as a
    cross join {{ ref('numbers_1000') }} as b
    where a.id + (b.id - 1) * 1000 <= 160000
),
po_lines as (
    select po_line_id, expected_date, po_qty from {{ ref('fct_po_line') }}
)
select 
    b.receipt_id,
    cast({{ mod_func('b.receipt_id - 1', '120000') }} + 1 as {{ cross_db_type_bigint() }}) as po_line_id,
    {{ generate_string_id('GRN_', 'b.receipt_id') }} as receipt_no,
    {{ date_add_days('pl.expected_date', "cast(" ~ mod_func('b.receipt_id', '21') ~ " as " ~ dbt.type_int() ~ ")") }} as receipt_date_base,
    {{ date_add_days('pl.expected_date', "0") }} as receipt_date,
    {% if target.type == 'clickhouse' %}
        multiIf({{ mod_func('b.receipt_id', '5') }} == 0, cast(pl.po_qty / 2 as {{ dbt.type_int() }}), pl.po_qty)
    {% else %}
        case {{ mod_func('b.receipt_id', '5') }}
            when 0 then cast(pl.po_qty / 2 as {{ dbt.type_int() }})
            else pl.po_qty
        end
    {% endif %} as received_qty,
    {% if target.type == 'clickhouse' %}
        multiIf({{ mod_func('b.receipt_id', '10') }} == 0, cast(pl.po_qty * 0.9 as {{ dbt.type_int() }}), pl.po_qty)
    {% else %}
        case {{ mod_func('b.receipt_id', '10') }}
            when 0 then cast(pl.po_qty * 0.9 as {{ dbt.type_int() }})
            else pl.po_qty
        end
    {% endif %} as accepted_qty,
    {% if target.type == 'clickhouse' %}
        multiIf({{ mod_func('b.receipt_id', '10') }} == 0, cast(pl.po_qty * 0.1 as {{ dbt.type_int() }}), 0)
    {% else %}
        case {{ mod_func('b.receipt_id', '10') }}
            when 0 then cast(pl.po_qty * 0.1 as {{ dbt.type_int() }})
            else 0
        end
    {% endif %} as rejected_qty,
    case {{ mod_func('b.receipt_id', '4') }}
        when 0 then 'Pending'
        when 1 then 'Failed'
        else 'Passed'
    end as inspection_status,
    {{ mod_func('b.receipt_id', '10') }} + 1 as warehouse_id,
    {{ generate_string_id('LOC_', mod_func('b.receipt_id', '100')) }} as locator_id
from base b
left join po_lines pl on cast({{ mod_func('b.receipt_id - 1', '120000') }} + 1 as {{ cross_db_type_bigint() }}) = pl.po_line_id
