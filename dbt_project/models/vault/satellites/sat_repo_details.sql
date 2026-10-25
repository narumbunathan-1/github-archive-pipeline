{{ config(materialized='incremental', unique_key='repo_hk') }}
SELECT 
    MD5(CAST(repo_id AS VARCHAR)) AS repo_hk,
    repo_name,
    ingested_at AS load_date,
    'GH_ARCHIVE' AS record_source
FROM {{ ref('stg_bronze_events') }}
WHERE repo_id IS NOT NULL
QUALIFY ROW_NUMBER() OVER (PARTITION BY MD5(CAST(repo_id AS VARCHAR)) ORDER BY ingested_at DESC) = 1
