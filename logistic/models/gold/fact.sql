{{
    config(
        materialized='incremental',
        unique_key='SHIPMENT_ID'
    )
}}

select
    s.SHIPMENT_ID,
    s.CUSTOMER_ID,
    s.COURIER_ID,
    e.EVENT_ID,
    s.SHIPPING_FEE
   
from {{ ref('shipments') }} s
left join (
    select
        EVENT_ID,
        SHIPMENT_ID
    from {{ ref('tracking_events') }}
) e
    on s.SHIPMENT_ID = e.SHIPMENT_ID