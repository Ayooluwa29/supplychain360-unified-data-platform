{{
    config(
        materialized='incremental',
        unique_key=['snapshot_date', 'product_id', 'warehouse_id'],
        tags=["mart", "inventory"]
    )
}}


WITH stock AS (
    SELECT * FROM {{ ref('02_inventory_stock_status') }}

    {% if is_incremental() %}
        WHERE snapshot_date > (SELECT max(snapshot_date) FROM {{ this }})
    {% endif %}
)

SELECT
    snapshot_date,
    product_id,
    product_name,
    category,
    warehouse_id,
    count(*)                                    AS total_snapshots,
    sum(CASE WHEN is_stockout THEN 1 ELSE 0 END) AS stockout_days,
    round(
        sum(CASE WHEN is_stockout THEN 1 ELSE 0 END) * 100.0 / count(*), 2
    )                                           AS stockout_rate_pct,
    sum(CASE WHEN is_below_reorder_threshold then 1 ELSE 0 END) AS low_stock_days,
    min(quantity_available)                     AS min_stock,
    avg(quantity_available)                     AS avg_stock

FROM stock
GROUP BY snapshot_date,
    product_id,
    product_name,
    category,
    warehouse_id