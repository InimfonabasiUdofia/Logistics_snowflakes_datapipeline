{{
    config(
        materialized='incremental',
        unique_key='SHIPMENT_ID'
    )
}}
select 
    SHIPMENT_ID,
    ORIGIN_CITY,
    DESTINATION_CITY,
    CREATED_AT,
    PROMISED_DELIVERY_AT,
    DELIVERED_AT,
    STATUS,
    SHIPMENT_UPDATED_AT

 from  
     {{ ref('shipments') }}
{% if is_incremental() %}
    where SHIPMENT_UPDATED_AT > (select coalesce(max(SHIPMENT_UPDATED_AT), '1900-01-01') from {{ this }})
{% endif %}