{{
    config(
        materialized='incremental',
        unique_key=['sale_month', 'region', 'store_id', 'product_id'],
        tags=["mart", "sales"]
    )
}}


WITH sales AS (
    SELECT * FROM {{ ref('02_sales_enriched') }}

    {% if is_incremental() %}
        WHERE sale_date > (SELECT max(sale_month) FROM {{ this }})
    {% endif %}
)

SELECT
    region,
    store_state,
    store_city,
    store_id,
    store_name,
    category,
    product_id,
    product_name,
    date_trunc(sale_date, MONTH)                AS sale_month,
    count(DISTINCT transaction_id)              AS total_transactions,
    sum(quantity_sold)                          AS total_units_sold,
    round(sum(sale_amount), 2)                  AS gross_revenue,
    round(sum(net_sale_amount), 2)              AS net_revenue,
    round(avg(discount_pct), 2)                 AS avg_discount_pct

FROM sales
GROUP BY region,
    store_state,
    store_city,
    store_id,
    store_name,
    category,
    product_id,
    product_name,
    sale_month