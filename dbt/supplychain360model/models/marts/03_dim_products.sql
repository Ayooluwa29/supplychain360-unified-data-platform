{{
    config(
        materialized='table',
        tags=["mart", "products"]
    )
}}


select
    product_id,
    product_name,
    brand,
    category,
    unit_price,
    supplier_id
from {{ ref('01_products') }}