{{
    config(
        materialized='table',
        tags=["mart", "suppliers"]
    )
}}

select
    supplier_id,
    supplier_name,
    country,
    supplier_category
from {{ ref('01_suppliers') }}