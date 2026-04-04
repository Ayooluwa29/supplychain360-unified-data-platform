{{
    config(
        materialized='view',
        tags=["intermediate", "inventory", "stock"]
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

products AS (
    SELECT
        product_id,
        supplier_id,
        product_name,
        brand,
        category,
        unit_price,
        extracted_date
    FROM {{ ref('01_products') }}
),

inventory_with_lag AS (
    SELECT
        *,
        lag(quantity_available) OVER (
            PARTITION BY product_id, warehouse_id
            ORDER BY snapshot_date
        ) AS prev_quantity_available
    FROM inventory
),

final AS (
    SELECT
        i.row_id,
        i.product_id,
        i.warehouse_id,
        i.snapshot_date,
        i.quantity_available,
        i.reorder_threshold,
        (i.quantity_available = 0)                        AS is_stockout,
        (i.quantity_available <= i.reorder_threshold)     AS is_below_reorder_threshold,
        i.quantity_available - i.prev_quantity_available  AS daily_stock_change,

        p.product_name,
        p.brand,
        p.category,
        p.supplier_id

    FROM inventory_with_lag i
    LEFT JOIN products p USING (product_id)
)

SELECT * FROM final