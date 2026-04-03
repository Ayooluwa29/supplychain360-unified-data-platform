{{
    config(
        materialized='incremental',
        unique_key = 'row_id',
        tags=["staging","inventory"]
    )
}}

SELECT
    CAST(_airbyte_raw_id AS STRING) AS row_id,
    CAST(product_id AS STRING) AS product_id,
    CAST(warehouse_id AS STRING) AS warehouse_id,

    CAST(reorder_threshold AS INTEGER) AS reorder_threshold,
    CAST(quantity_available AS INTEGER) AS quantity_available,

    CAST(snapshot_date AS DATE) AS snapshot_date

FROM {{ source('supplychain360_db', 'raw_inventory') }}

{% if is_incremental() %}
    WHERE snapshot_date >= (SELECT MAX(snapshot_date) FROM {{ this }})
{% endif %}