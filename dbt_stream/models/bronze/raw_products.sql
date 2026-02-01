{{ config(materialized='materialized_view', schema='dbt_bronze') }}

SELECT 
    *,
    _metadata.file_path AS source_file,
    _metadata.file_modification_time AS file_time
FROM read_files(
    '/Volumes/madhan_poc/dbt_test/dbt_streaming_test/files/products/', 
    format => 'csv', 
    header => true
)

