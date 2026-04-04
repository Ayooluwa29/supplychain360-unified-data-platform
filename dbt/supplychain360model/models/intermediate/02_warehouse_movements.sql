{{
    config(
        materialized='view',
        tags=["intermediate", "warehouses"]
    )
}}



WITH inventory AS (
    SELECT
        row_id,
        product_id,
        warehouse_id,
        reorder_threshold,
        quantity_available,
        snapshot_date
    FROM {{ ref('01_inventory') }}
),

warehouses AS (
    SELECT
        warehouse_id,
        city,
        state,
        extracted_date
    FROM {{ ref('01_warehouses') }}
),

snapshot_deltas AS (
    SELECT
        warehouse_id,
        product_id,
        snapshot_date,
        quantity_available,
        quantity_available - LAG(quantity_available) OVER (
            PARTITION BY product_id, warehouse_id
            ORDER BY snapshot_date
        ) AS stock_delta
    FROM inventory
),

final AS (
    SELECT
        sd.warehouse_id,
        sd.product_id,
        sd.snapshot_date,
        sd.quantity_available,
        sd.stock_delta,
        CASE WHEN sd.stock_delta > 0
            THEN sd.stock_delta ELSE 0 end  AS estimated_inbound,
        CASE WHEN sd.stock_delta < 0
            THEN abs(sd.stock_delta) ELSE 0 END AS estimated_outbound,
        w.city                              AS warehouse_city,
        w.state                             AS warehouse_state,
        CONCAT(w.city, ', ', w.state)       AS warehouse_location

    FROM snapshot_deltas sd
    LEFT JOIN warehouses w USING (warehouse_id)
    WHERE sd.stock_delta IS NOT NULL
)

SELECT * FROM final