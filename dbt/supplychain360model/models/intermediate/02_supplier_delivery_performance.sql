{{
    config(
        materialized='view',
        tags=["intermediate", "supplier", "delivery"]
    )
}}


WITH shipments AS (
    SELECT
        shipment_id,
        warehouse_id,
        store_id,
        product_id,
        quantity_shipped,
        carrier,
        shipment_date,
        expected_delivery_date,
        actual_delivery_date
    FROM {{ ref('01_shipments') }}
),

suppliers AS (
    SELECT
        supplier_id,
        supplier_name,
        country,
        supplier_category,
        extracted_date
    FROM {{ ref('01_suppliers') }}
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

final AS (
    SELECT
        sh.shipment_id,
        sh.warehouse_id,
        sh.store_id,
        sh.product_id,
        sh.carrier,
        sh.quantity_shipped,
        date(sh.shipment_date)          AS shipment_date,
        date(sh.expected_delivery_date) AS expected_delivery_date,
        date(sh.actual_delivery_date)   AS actual_delivery_date,

        date_diff(
            date(sh.actual_delivery_date),
            date(sh.expected_delivery_date),
            day
        )                               AS delivery_delay_days,

        (date(sh.actual_delivery_date)
            <= date(sh.expected_delivery_date)) AS is_on_time,

        date_diff(
            date(sh.actual_delivery_date),
            date(sh.shipment_date),
            day
        )                               AS actual_lead_time_days,

        p.supplier_id,
        su.supplier_name,
        su.country                      AS supplier_country,
        su.supplier_category                     AS supplier_category

    FROM shipments sh
    LEFT JOIN products p USING (product_id)
    LEFT JOIN suppliers su USING (supplier_id)
)

SELECT * FROM final