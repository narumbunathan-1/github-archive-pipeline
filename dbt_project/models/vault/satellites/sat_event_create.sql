{{ config(materialized='incremental', unique_key='event_hk') }}
SELECT
    MD5(CAST(event_id AS VARCHAR)) AS event_hk,
    ingested_at AS load_date,
    event_payload:ref_type::STRING AS created_type,
    event_payload:ref::STRING AS created_name,
    event_payload:master_branch::STRING AS master_branch,
    event_payload:description::STRING AS repo_description,
    'GH_ARCHIVE' AS record_source
FROM {{ ref('stg_bronze_events') }}
WHERE event_type = 'CreateEvent'
