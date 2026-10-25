{{ config(materialized='incremental', unique_key='event_hk') }}
SELECT
    MD5(CAST(event_id AS VARCHAR)) AS event_hk,
    ingested_at AS load_date,
    event_payload:action::STRING AS action,
    event_payload:issue:number::NUMBER AS issue_number,
    event_payload:comment:body::STRING AS comment_body,
    'GH_ARCHIVE' AS record_source
FROM {{ ref('stg_bronze_events') }}
WHERE event_type = 'IssueCommentEvent'
