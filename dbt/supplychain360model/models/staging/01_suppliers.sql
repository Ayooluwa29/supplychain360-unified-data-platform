{{
    config(
        materialized='incremental',
        unique_key = 'supplier_id',
        tags=["staging","suppliers"]
    )
}}


SELECT
 
    CAST(supplier_id AS STRING) AS supplier_id,
    
    CAST(supplier_name AS STRING) AS supplier_name,
    CAST(country AS STRING) AS country,
    CAST(category AS STRING) AS supplier_category,

    {{ to_utc('_airbyte_extracted_at') }} AS extracted_date

FROM {{ source('supplychain360_db', 'raw_suppliers') }}

{% if is_incremental() %}
    WHERE extracted_date >= (SELECT MAX(extracted_date) FROM {{ this }})
{% endif %}