{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='payment_id',
    schema='dbt_silver',
    liquid_clustered_by=['customer_id', 'payment_date'],
    tblproperties={
      'delta.autoOptimize.optimizeWrite': 'true',
      'delta.autoOptimize.autoCompact': 'true'
    },
    post_hook=[
      "OPTIMIZE {{ this }}"
    ]
) }}

SELECT * FROM {{ ref('raw_payments') }}
WHERE payment_id IS NOT NULL
{% if is_incremental() %}
  AND file_time > (SELECT MAX(file_time) FROM {{ this }})
{% endif %}
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY payment_id 
    ORDER BY file_time DESC
) = 1