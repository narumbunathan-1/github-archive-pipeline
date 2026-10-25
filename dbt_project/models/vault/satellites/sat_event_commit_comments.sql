{{ config(materialized='incremental', unique_key='event_hk') }}
SELECT
    MD5(CAST(event_id AS VARCHAR)) AS event_hk,
    ingested_at AS load_date,
    event_payload:comment:commit_id::STRING AS commit_sha,
    event_payload:comment:path::STRING AS file_path,
    event_payload:comment:position::NUMBER AS line_number,
    event_payload:comment:body::STRING AS comment_body,
    'GH_ARCHIVE' AS record_source
FROM {{ ref('stg_bronze_events') }}
WHERE event_type = 'CommitCommentEvent'
