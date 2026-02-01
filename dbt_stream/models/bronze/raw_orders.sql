{{ config(materialized='streaming_table', schema='dbt_bronze') }}

SELECT 
    *,
    _metadata.file_path AS source_file,
    _metadata.file_modification_time AS file_time
FROM STREAM read_files(
    '/Volumes/madhan_poc/dbt_test/dbt_streaming_test/files/orders/', 
    format => 'csv', 
    header => true
)
