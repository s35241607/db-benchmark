{{ config(materialized='view') }}
with combined as (
    select 'w1' as source_name, row_id, group_key, join_key,
    {% for j in range(1, 21) %}metric_{{ j }}{% if not loop.last %},{% endif %}{% endfor %}
    from {{ ref('stg_wide_01') }}
    union all
    select 'w2' as source_name, row_id, group_key, join_key,
    {% for j in range(1, 21) %}metric_{{ j }}{% if not loop.last %},{% endif %}{% endfor %}
    from {{ ref('stg_wide_02') }}
    union all
    select 'w3' as source_name, row_id, group_key, join_key,
    {% for j in range(1, 21) %}metric_{{ j }}{% if not loop.last %},{% endif %}{% endfor %}
    from {{ ref('stg_wide_03') }}
)
select
    source_name,
    row_id,
    group_key,
    join_key,
    {% for j in range(1, 21) %}
    rank() over (partition by group_key order by metric_{{ j }} desc) as rank_{{ j }},
    avg(metric_{{ j }}) over (partition by join_key) as avg_part_{{ j }},
    sum(metric_{{ j }}) over (partition by group_key order by row_id rows between unbounded preceding and current row) as cum_sum_{{ j }}{% if not loop.last %},{% endif %}
    {% endfor %}
from combined
