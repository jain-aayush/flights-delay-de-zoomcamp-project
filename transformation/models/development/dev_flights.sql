{{ config(materialized='view') }}

select 
    *,
    cast(concat(Year, '-', Month, '-', DayOfMonth) as date) as Date,
    {{ expand_boolean_variable('Diverted') }} as diversion_status,
    {{ expand_boolean_variable('Cancelled') }} as cancellation_status,
    {{ get_cancellation_code_description('CancellationCode') }} as cancellation_code_description
from {{ source('development', 'flights') }}

{% if var('is_test_run', default=true) %}
  limit 100
{% endif %}