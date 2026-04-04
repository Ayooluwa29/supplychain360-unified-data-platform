{{
    config(
        materialized='table',
        tags=["mart", "stores"]
    )
}}


SELECT
    store_id,
    store_name,
    city,
    state,
    region,
    store_open_date
FROM {{ ref('01_stores') }}