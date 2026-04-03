{{
    config(
        materialized='incremental',
        unique_key = 'shipment_id',
        tags=["staging","shipments"]
    )
}}

SELECT
    CAST(shipment_id AS STRING) AS shipment_id,
    CAST(warehouse_id AS STRING) AS warehouse_id,
    CAST(store_id AS STRING) AS store_id,
    CAST(product_id AS STRING) AS product_id,

    CAST(quantity_shipped AS INTEGER) AS quantity_shipped,

    CAST(carrier AS STRING) AS carrier,

    {{ to_utc('shipment_date') }} AS shipment_date,
    {{ to_utc('expected_delivery_date') }} AS expected_delivery_date,
    {{ to_utc('actual_delivery_date') }} AS actual_delivery_date

FROM {{ source('supplychain360_db', 'raw_shipments') }}

{% if is_incremental() %}
    WHERE shipment_date >= (SELECT MAX(shipment_date) FROM {{ this }})
{% endif %}