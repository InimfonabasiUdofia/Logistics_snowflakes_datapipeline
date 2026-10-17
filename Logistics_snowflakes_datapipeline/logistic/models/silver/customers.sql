
    select 
        CUSTOMER_ID,
        CUSTOMER_NAME,
        CUSTOMER_TYPE,
        CITY,
        CREATED_AT,
        cast(UPDATED_AT as timestamp_ntz) as CUSTOMER_UPDATED_AT

     from  
        {{ source('logistics', 'customers') }}
    where
        CUSTOMER_ID IS NOT NULL