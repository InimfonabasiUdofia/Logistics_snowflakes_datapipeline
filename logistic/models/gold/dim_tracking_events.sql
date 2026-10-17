{{
    config(
        materialized='incremental',
        unique_key='EVENT_ID'
    )
}}
select 
    EVENT_ID ,
    STATUS,
    EVENT_TIME,
    LOCATION,
    TRACKING_UPDATED_AT
from   
         {{ ref('tracking_events') }}

{% if is_incremental() %}
    where TRACKING_UPDATED_AT > (select coalesce(max(TRACKING_UPDATED_AT), '1900-01-01') from {{ this }})
{% endif %}
