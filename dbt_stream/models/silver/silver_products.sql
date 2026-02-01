{{ config(
    materialized='table',
    schema='dbt_silver'
) }}

SELECT * FROM {{ ref('raw_products') }}
WHERE product_id IS NOT NULL
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY product_id 
    ORDER BY file_time DESC
) = 1