
select 
    SHIPMENT_ID,
    CUSTOMER_ID,
    COURIER_ID,
    ORIGIN_CITY,
    DESTINATION_CITY,
    CREATED_AT,
    PROMISED_DELIVERY_AT,
    DELIVERED_AT,
    SHIPPING_FEE,
    STATUS,
    cast(UPDATED_AT as timestamp_ntz) as SHIPMENT_UPDATED_AT

 from  
    {{ source('logistics', 'shipments') }}
where
    SHIPMENT_ID IS NOT NULL
