
select 
    COURIER_ID,
    COURIER_NAME,
    SERVICE_TYPE,
    cast(UPDATED_AT as timestamp_ntz) as COURIER_UPDATED_AT
from 
    {{ source('logistics', 'couriers') }}
WHERE 
COURIER_ID IS NOT NULL