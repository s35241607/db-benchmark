{{ config(materialized='view') }}
select
    t1.group_key,
    count(*) as row_count,
    {% for j in range(1, 31) %}
    sum(t1.metric_{{ j }} + t2.metric_{{ j }} + t3.metric_{{ j }} + t4.metric_{{ j }} + t5.metric_{{ j }}) as sum_metric_{{ j }},
    avg(t6.metric_{{ j }} + t7.metric_{{ j }} + t8.metric_{{ j }} + t9.metric_{{ j }} + t10.metric_{{ j }}) as avg_metric_{{ j }},
    max(t1.metric_{{ j }} + t10.metric_{{ j }}) as max_metric_{{ j }}{% if not loop.last %},{% endif %}
    {% endfor %}
from {{ ref('stg_wide_01') }} t1
join {{ ref('stg_wide_02') }} t2 on t1.row_id = t2.row_id
join {{ ref('stg_wide_03') }} t3 on t1.row_id = t3.row_id
join {{ ref('stg_wide_04') }} t4 on t1.row_id = t4.row_id
join {{ ref('stg_wide_05') }} t5 on t1.row_id = t5.row_id
join {{ ref('stg_wide_06') }} t6 on t1.row_id = t6.row_id
join {{ ref('stg_wide_07') }} t7 on t1.row_id = t7.row_id
join {{ ref('stg_wide_08') }} t8 on t1.row_id = t8.row_id
join {{ ref('stg_wide_09') }} t9 on t1.row_id = t9.row_id
join {{ ref('stg_wide_10') }} t10 on t1.row_id = t10.row_id
group by t1.group_key
