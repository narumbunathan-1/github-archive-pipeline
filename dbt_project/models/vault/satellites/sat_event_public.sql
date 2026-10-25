{{ config(materialized='incremental', unique_key='event_hk') }}
SELECT
    MD5(CAST(event_id AS VARCHAR)) AS event_hk,
    ingested_at AS load_date,
    'Made Public' AS public_status,
    'GH_ARCHIVE' AS record_source
FROM {{ ref('stg_bronze_events') }}
WHERE event_type = 'PublicEvent'
