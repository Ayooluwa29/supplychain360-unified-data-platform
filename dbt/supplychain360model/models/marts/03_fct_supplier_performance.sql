{{
    config(
        materialized='incremental',
        unique_key=['supplier_id', 'carrier', 'shipment_date'],
        tags=["mart", "supplier"]
    )
}}


WITH deliveries AS (
    SELECT * FROM {{ ref('02_supplier_delivery_performance') }}

    {% if is_incremental() %}
        WHERE shipment_date > (SELECT max(shipment_date) FROM {{ this }})
    {% endif %}
)

SELECT
    supplier_id,
    supplier_name,
    supplier_country,
    supplier_category,
    carrier,
    count(shipment_id)                                          AS total_shipments,
    sum(quantity_shipped)                                       AS total_units_shipped,
    sum(CASE WHEN is_on_time THEN 1 ELSE 0 END)                AS on_time_deliveries,
    round(
        sum(CASE WHEN is_on_time THEN 1 ELSE 0 END) * 100.0
        / count(shipment_id), 2
    )                                                           AS on_time_pct,
    round(avg(delivery_delay_days), 1)                         AS avg_delay_days,
    max(delivery_delay_days)                                    AS max_delay_days,
    round(avg(actual_lead_time_days), 1)                       AS avg_lead_time_days

FROM deliveries
GROUP BY supplier_id,
    supplier_name,
    supplier_country,
    supplier_category,
    carrier