{{
    config(
        materialized='incremental',
        unique_key = 'store_id',
        tags=["staging","stores"]
    )
}}


SELECT

    CAST(store_id AS STRING) AS store_id,
    
    CAST(state AS STRING) AS state,
    CAST(region AS STRING) AS region,
    CAST(city AS STRING) AS city,
    CAST(store_name AS STRING) AS store_name,

    SAFE.PARSE_DATE('%d/%m/%Y', store_open_date) store_open_date

FROM {{ source('supplychain360_db', 'raw_stores') }}

{% if is_incremental() %}
    WHERE _airbyte_extracted_at >= (SELECT MAX(_airbyte_extracted_at) FROM {{ this }})
{% endif %}