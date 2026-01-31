{{config(materialized='streaming_table', schema='bronze')}}

SELECT 
    *,
    _metadata.file_path AS source_file,
    _metadata.file_modification_time AS file_time
FROM read_files(
    '/Volumes/main/volume/task/dbt_pipeline/customers/', 
    format => 'csv', 
    header => true
)
-- {{ source('external_volume', 'customers') }}