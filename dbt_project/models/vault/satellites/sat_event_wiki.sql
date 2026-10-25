{{ config(materialized='incremental', unique_key='event_hk') }}
SELECT
    MD5(CAST(e.event_id AS VARCHAR)) AS event_hk,
    e.ingested_at AS load_date,
    p.value:action::STRING AS wiki_action,
    p.value:page_name::STRING AS page_name,
    p.value:title::STRING AS page_title,
    'GH_ARCHIVE' AS record_source
FROM {{ ref('stg_bronze_events') }} e,
LATERAL FLATTEN(input => e.event_payload:pages, outer => TRUE) p
WHERE e.event_type = 'GollumEvent'
