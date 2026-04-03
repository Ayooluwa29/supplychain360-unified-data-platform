{{
    config(
        materialized='incremental',
        unique_key = 'transaction_id',
        tags=["staging","sales"]
    )
}}

SELECT
    CAST(transaction_id AS STRING) AS transaction_id,
    CAST(store_id AS STRING) AS store_id,
    CAST(product_id AS STRING) AS product_id,

    CAST(unit_price AS NUMERIC) AS unit_price,
    CAST(sale_amount AS NUMERIC) AS sale_amount,
    CAST(discount_pct AS NUMERIC) AS discount_pct,
    CAST(quantity_sold AS INTEGER) AS quantity_sold,

    {{ to_utc('transaction_timestamp') }} AS transaction_timestamp

FROM {{ source('supplychain360_db', 'raw_sales') }}

{% if is_incremental() %}
    WHERE transaction_timestamp >= (SELECT MAX(transaction_timestamp) FROM {{ this }})
{% endif %}