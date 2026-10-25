{{ config(materialized='incremental', unique_key='event_hk') }}
SELECT
    MD5(CAST(event_id AS VARCHAR)) AS event_hk,
    ingested_at AS load_date,
    event_payload:action::STRING AS issue_action,
    event_payload:issue:number::NUMBER AS issue_number,
    event_payload:issue:title::STRING AS issue_title,
    event_payload:issue:state::STRING AS issue_state,
    event_payload:issue:labels AS issue_labels,
    'GH_ARCHIVE' AS record_source
FROM {{ ref('stg_bronze_events') }}
WHERE event_type = 'IssuesEvent'
