{{ config(materialized='incremental', unique_key='event_hk') }}
SELECT
    MD5(CAST(event_id AS VARCHAR)) AS event_hk,
    ingested_at AS load_date,
    event_payload:forkee:full_name::STRING AS new_forked_repo_name,
    event_payload:forkee:created_at::TIMESTAMP_NTZ AS fork_created_at,
    'GH_ARCHIVE' AS record_source
FROM {{ ref('stg_bronze_events') }}
WHERE event_type = 'ForkEvent'
