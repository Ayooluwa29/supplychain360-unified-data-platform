{{
    config(
        materialized='incremental',
        unique_key = 'warehouse_id',
        tags=["staging","warehouse"]
    )
}}

SELECT

    CAST(warehouse_id AS STRING) AS warehouse_id,
    
    CAST(city AS STRING) AS city,
    CAST(state AS STRING) AS state,
    CONCAT(city, ', ', state) as warehouse_location,

    {{ to_utc('_airbyte_extracted_at') }} AS extracted_date

FROM {{ source('supplychain360_db','raw_warehouses') }}

{% if is_incremental() %}
    WHERE extracted_date >= (SELECT MAX(extracted_date) FROM {{ this }})
{% endif %}