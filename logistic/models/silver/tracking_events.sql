
select 
    EVENT_ID,
    SHIPMENT_ID,
    STATUS,
    EVENT_TIME,
    LOCATION,
    cast(UPDATED_AT as timestamp_ntz) as TRACKING_UPDATED_AT
from   
   
        {{ source('logistics', 'tracking_events') }}
where
    EVENT_ID IS NOT NULL
