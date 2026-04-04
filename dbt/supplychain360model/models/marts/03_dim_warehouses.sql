{{
    config(
        materialized='table',
        tags=["mart", "warehouses"]
    )
}}


select
    warehouse_id,
    city,
    state,
    warehouse_location
from {{ ref('01_warehouses') }}