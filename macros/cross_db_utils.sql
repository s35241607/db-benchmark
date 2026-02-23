{% macro generate_string_id(prefix, id_col) %}
    {% if target.type == 'postgres' %}
        '{{ prefix }}' || cast({{ id_col }} as {{ dbt.type_string() }})
    {% elif target.type == 'clickhouse' %}
        concat('{{ prefix }}', cast({{ id_col }} as {{ dbt.type_string() }}))
    {% elif target.type == 'starrocks' %}
        concat('{{ prefix }}', cast({{ id_col }} as {{ dbt.type_string() }}))
    {% else %}
        concat('{{ prefix }}', cast({{ id_col }} as {{ dbt.type_string() }}))
    {% endif %}
{% endmacro %}

{% macro date_add_days(base_date, days_col) %}
    {% if target.type == 'postgres' %}
        cast({{ base_date }} as {{ dbt.type_timestamp() }}) + (cast({{ days_col }} as integer) * interval '1 day')
    {% elif target.type == 'clickhouse' %}
        addDays(cast({{ base_date }} as {{ dbt.type_timestamp() }}), cast({{ days_col }} as Int32))
    {% elif target.type == 'starrocks' %}
        date_add(cast({{ base_date }} as {{ dbt.type_timestamp() }}), interval cast({{ days_col }} as integer) day)
    {% else %}
        cast({{ base_date }} as {{ dbt.type_timestamp() }}) + (cast({{ days_col }} as integer) * interval '1 day')
    {% endif %}
{% endmacro %}

{% macro mod_func(num, divisor) %}
    {% if target.type == 'clickhouse' %}
        modulo(cast({{ num }} as Int64), cast({{ divisor }} as Int64))
    {% else %}
        mod(cast({{ num }} as int), cast({{ divisor }} as int))
    {% endif %}
{% endmacro %}

{% macro cross_db_type_bigint() %}
    {% if target.type == 'clickhouse' %}
        Int64
    {% else %}
        {{ dbt.type_bigint() }}
    {% endif %}
{% endmacro %}
