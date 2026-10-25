{{ config(materialized='incremental', unique_key='event_hk') }}
SELECT
    MD5(CAST(event_id AS VARCHAR)) AS event_hk,
    ingested_at AS load_date,
    event_payload:action::STRING AS release_action,
    event_payload:release:tag_name::STRING AS release_tag,
    event_payload:release:name::STRING AS release_name,
    event_payload:release:prerelease::BOOLEAN AS is_prerelease,
    event_payload:release:author:login::STRING AS release_author,
    'GH_ARCHIVE' AS record_source
FROM {{ ref('stg_bronze_events') }}
WHERE event_type = 'ReleaseEvent'
