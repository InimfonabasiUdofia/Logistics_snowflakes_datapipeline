{{
    config(
        materialized='incremental',
        unique_key='CUSTOMER_ID'
    )
    }}
    select 
        CUSTOMER_ID,
        CUSTOMER_NAME,
        CUSTOMER_TYPE,
        CITY,
        CREATED_AT,
        CUSTOMER_UPDATED_AT

     from  
        {{ ref('customers') }}
    {% if is_incremental() %}
    where CUSTOMER_UPDATED_AT > (select coalesce(max(CUSTOMER_UPDATED_AT), '1900-01-01') from {{ this }})
{% endif %}