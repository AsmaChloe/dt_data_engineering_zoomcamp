{{
    config(
        materialized='table'
    )
}}

with 
    zones as(
        select * 
        from {{ ref('dim_zones') }}
        where borough != 'Unknown'
    )
select 
    fhv.*,
    pu.borough as pu_borough,
    pu.zone as pu_zone,
    do.borough as do_borough,
    do.zone as do_zone
from {{ ref('stg_fhv_data') }}  as fhv
inner join zones as pu
on 
    pu.locationid = fhv.pulocationid
inner join zones as do
on 
    do.locationid = fhv.dolocationid

{% if var('is_test_run', default=false) %}

  limit 100

{% endif %}