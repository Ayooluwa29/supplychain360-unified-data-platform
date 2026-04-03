{{
    config(
        materialized='incremental',
        unique_key = 'product_id',
        tags=["staging","product"]
    )
}}

SELECT
    CAST(product_id AS STRING) AS product_id,
    CAST(supplier_id AS STRING) AS supplier_id,

    CAST(product_name AS STRING) AS product_name,
    CAST(brand AS STRING) AS brand,
    CAST(category AS STRING) AS category,

    CAST(unit_price AS NUMERIC) AS unit_price,

    {{ to_utc('_airbyte_extracted_at') }} AS extracted_date

FROM {{ source('supplychain360_db', 'raw_products') }}

{% if is_incremental() %}
    WHERE _airbyte_extracted_at >= (SELECT MAX(_airbyte_extracted_at) FROM {{ this }})
{% endif %}