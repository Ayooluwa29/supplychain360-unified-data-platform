{{
    config(
        materialized='incremental',
        unique_key=['warehouse_id', 'snapshot_date'],
        tags=["mart", "warehouse"]
    )
}}


WITH movements AS (
    SELECT * FROM {{ ref('02_warehouse_movements') }}

    {% if is_incremental() %}
        WHERE snapshot_date > (SELECT max(snapshot_date) FROM {{ this }})
    {% endif %}
),

shipments as (
    SELECT * FROM {{ ref('01_shipments') }}

    {% if is_incremental() %}
        WHERE date(shipment_date) > (
            SELECT max(snapshot_date) FROM {{ this }}
        )
    {% endif %}
)

SELECT
    m.warehouse_id,
    m.warehouse_city,
    m.warehouse_state,
    m.snapshot_date,
    sum(m.estimated_inbound)                    AS total_inbound_units,
    sum(m.estimated_outbound)                   AS total_outbound_units,
    avg(m.quantity_available)                   AS avg_stock_held,
    count(DISTINCT m.product_id)                AS distinct_products_held,
    count(DISTINCT sh.shipment_id)              AS total_shipments_dispatched,
    sum(sh.quantity_shipped)                    AS total_units_dispatched

FROM movements m
LEFT JOIN shipments sh
    ON m.warehouse_id = sh.warehouse_id
    AND m.snapshot_date = DATE(sh.shipment_date)
GROUP BY m.warehouse_id,
    m.warehouse_city,
    m.warehouse_state,
    m.snapshot_date