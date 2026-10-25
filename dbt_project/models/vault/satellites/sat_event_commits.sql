{{ config(materialized='incremental', unique_key='event_hk') }}
SELECT 
    MD5(CAST(e.event_id AS VARCHAR)) AS event_hk,
    e.ingested_at AS load_date,
    c.value:sha::STRING AS commit_sha,
    c.value:message::STRING AS commit_message,
    c.value:author:name::STRING AS commit_author_name,
    c.value:author:email::STRING AS commit_author_email,
    'GH_ARCHIVE' AS record_source
FROM {{ ref('stg_bronze_events') }} e,
LATERAL FLATTEN(input => e.event_payload:commits, outer => TRUE) c
WHERE e.event_type = 'PushEvent'
