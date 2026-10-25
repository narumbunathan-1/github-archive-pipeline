{{ config(materialized='incremental', unique_key='event_hk') }}
SELECT
    MD5(CAST(event_id AS VARCHAR)) AS event_hk,
    ingested_at AS load_date,
    event_payload:action::STRING AS pr_action,
    event_payload:pull_request:number::NUMBER AS pr_number,
    event_payload:pull_request:title::STRING AS pr_title,
    event_payload:pull_request:state::STRING AS pr_state,
    event_payload:pull_request:merged::BOOLEAN AS is_merged,
    event_payload:pull_request:additions::NUMBER AS additions,
    event_payload:pull_request:deletions::NUMBER AS deletions,
    event_payload:pull_request:changed_files::NUMBER AS changed_files,
    event_payload:pull_request:user:login::STRING AS pr_author,
    'GH_ARCHIVE' AS record_source
FROM {{ ref('stg_bronze_events') }}
WHERE event_type = 'PullRequestEvent'
