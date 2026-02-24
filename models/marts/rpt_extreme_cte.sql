{{ config(materialized='view') }}
with cte1 as (
    select group_key, 
        sum(metric_1) as s1, sum(metric_2) as s2
    from {{ ref('stg_wide_01') }}
    group by group_key
),
cte2 as (
    select t2.join_key, 
        avg(t2.metric_10) as a10, max(t2.metric_20) as m20
    from {{ ref('stg_wide_02') }} t2
    join {{ ref('stg_wide_03') }} t3 on t2.row_id = t3.row_id
    group by t2.join_key
),
cte3 as (
    select t4.join_key, count(*) as c1
    from {{ ref('stg_wide_04') }} t4
    group by t4.join_key
),
cte4 as (
    select cte2.join_key, cte2.a10, cte2.m20, cte3.c1
    from cte2
    join cte3 on cte2.join_key = cte3.join_key
)
select 
    cte1.group_key,
    cte4.join_key,
    cte1.s1, cte1.s2,
    cte4.a10, cte4.m20, cte4.c1,
    {% for j in range(30, 60) %}
    sum(t5.metric_{{ j }}) as sum_m_{{ j }}{% if not loop.last %},{% endif %}
    {% endfor %}
from cte1
cross join cte4
join {{ ref('stg_wide_05') }} t5 on t5.group_key = cte1.group_key and t5.join_key = cte4.join_key
group by 
    cte1.group_key,
    cte4.join_key,
    cte1.s1, cte1.s2,
    cte4.a10, cte4.m20, cte4.c1
