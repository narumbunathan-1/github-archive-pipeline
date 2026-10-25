{{ config(materialized='incremental', unique_key='user_hk') }}
SELECT 
    MD5(CAST(actor_id AS VARCHAR)) AS user_hk,
    actor_login,
    ingested_at AS load_date,
    'GH_ARCHIVE' AS record_source
FROM {{ ref('stg_bronze_events') }}
WHERE actor_id IS NOT NULL
QUALIFY ROW_NUMBER() OVER (PARTITION BY MD5(CAST(actor_id AS VARCHAR)) ORDER BY ingested_at DESC) = 1
