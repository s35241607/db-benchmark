{{ config(materialized='table') }}

with base as (
    select pr_id, pr_no, pr_date
    from {{ ref('stg_pr') }}
    where pr_status = 'APPROVED'
)
select 
    pr_id as rfq_id,
    {{ generate_string_id('RFQ_', 'pr_id') }} as rfq_no,
    pr_id,
    {{ date_add_days('pr_date', "cast(" ~ mod_func('pr_id', '4') ~ " as " ~ dbt.type_int() ~ ") + 1") }} as rfq_date,
    cast({{ mod_func('pr_id', '1000') }} + 1 as {{ dbt.type_int() }}) as supplier_id,
    case {{ mod_func('pr_id', '2') }}
        when 0 then 'AWARDED'
        else 'CLOSED'
    end as rfq_status
from base
