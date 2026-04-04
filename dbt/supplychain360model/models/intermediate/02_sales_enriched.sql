{{
    config(
        materialized='view',
        tags=["intermediate", "sales", "enriched"]
    )
}}


WITH sales AS (
    SELECT
        transaction_id,
        store_id,
        product_id,
        unit_price,
        sale_amount,
        discount_pct,
        quantity_sold,
        transaction_timestamp
    FROM {{ ref('01_sales') }}
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

stores AS (
    SELECT
        store_id,
        state,
        region,
        city,
        store_name,
        store_open_date
    FROM {{ ref('01_stores') }}
),

final AS (
    SELECT
        s.transaction_id,
        date(s.transaction_timestamp)            AS sale_date,
        timestamp_trunc(
            s.transaction_timestamp, hour
        )                                        AS sale_hour,

        s.store_id,
        st.store_name,
        st.city                                  AS store_city,
        st.state                                 AS store_state,
        st.region,

        s.product_id,
        p.product_name,
        p.brand,
        p.category,
        p.supplier_id,

        s.quantity_sold,
        s.unit_price,
        s.discount_pct,
        s.sale_amount,
        round(
            s.sale_amount * (1 - s.discount_pct / 100), 2
        )                                        AS net_sale_amount

    FROM sales s
    LEFT JOIN products p USING (product_id)
    LEFT JOIN stores st USING (store_id)
)

SELECT * FROM final