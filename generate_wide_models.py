import os

os.makedirs("models/staging", exist_ok=True)

for i in range(1, 11):
    table_name = f"stg_wide_{i:02d}"
    
    # 100,000 rows per table: numbers_1000 (1k) cross join numbers_1000 where id <= 100 (100) = 100k
    content = f"""{{{{ config(materialized='table') }}}}
with base as (
    select a.id + (b.id - 1) * 1000 as row_id
    from {{{{ ref('numbers_1000') }}}} as a
    cross join (select id from {{{{ ref('numbers_1000') }}}} where id <= 100) as b
)
select 
    row_id,
    {{{{ generate_string_id('W{i:02d}_', 'row_id') }}}} as wide_id,
    cast({{{{ mod_func('row_id', '5') }}}} as {{{{ dbt.type_int() }}}}) as join_key,
    cast({{{{ mod_func('row_id', '100') }}}} as {{{{ dbt.type_int() }}}}) as group_key,
    {{% for col in range(1, 101) %}}
    cast({{{{ mod_func('row_id * ' ~ col * {i}, '100000') }}}} / 100.0 as {{{{ dbt.type_numeric() }}}}) as metric_{{{{ col }}}}{{% if not loop.last %}},{{% endif %}}
    {{% endfor %}}
from base
"""
    file_path = f"models/staging/{table_name}.sql"
    with open(file_path, "w", encoding="utf-8") as f:
        f.write(content)

print("Generated 10 wide table models (stg_wide_01.sql to stg_wide_10.sql).")
