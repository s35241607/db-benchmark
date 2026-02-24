{{ config(materialized='table') }}
with base as (
    select a.id + (b.id - 1) * 1000 as row_id
    from {{ ref('numbers_1000') }} as a
    cross join (select id from {{ ref('numbers_1000') }} where id <= 100) as b
)
select 
    row_id,
    {{ generate_string_id('W02_', 'row_id') }} as wide_id,
    cast({{ mod_func('row_id', '5') }} as {{ dbt.type_int() }}) as join_key,
    cast({{ mod_func('row_id', '100') }} as {{ dbt.type_int() }}) as group_key,
    {% for col in range(1, 101) %}
    cast({{ mod_func('row_id * ' ~ col * 2, '100000') }} / 100.0 as {{ dbt.type_numeric() }}) as metric_{{ col }}{% if not loop.last %},{% endif %}
    {% endfor %}
from base
