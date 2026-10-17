{{
    config(
        materialized='incremental',
        unique_key='COURIER_ID'
    )
}}
select 
    COURIER_ID,
    COURIER_NAME,
    SERVICE_TYPE,
     COURIER_UPDATED_AT
from 
    {{ ref('couriers') }}
{% if is_incremental() %}
    where COURIER_UPDATED_AT > (select coalesce(max(COURIER_UPDATED_AT), '1900-01-01') from {{ this }})
{% endif %}