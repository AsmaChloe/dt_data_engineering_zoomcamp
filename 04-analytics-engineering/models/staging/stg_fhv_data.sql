{{
    config(
        materialized='view'
    )
}}

select *
from {{ source('staging','fhv_data_2019') }} as fhv_data
where EXTRACT(YEAR FROM pickup_datetime) = 2019 and EXTRACT(YEAR FROM dropoff_datetime) = 2019

-- dbt build --select <model_name> --vars '{'is_test_run': 'false'}'
{% if var('is_test_run', default=false) %}

  limit 100

{% endif %}