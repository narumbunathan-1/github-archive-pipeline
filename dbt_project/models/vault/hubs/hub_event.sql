{{ config(materialized='incremental', unique_key='event_hk') }}
WITH raw_events AS (
    SELECT DISTINCT
        MD5(CAST(event_id AS VARCHAR)) AS event_hk,
        event_id,
        ingested_at AS load_date,
        'GH_ARCHIVE' AS record_source
    FROM {{ ref('stg_bronze_events') }}
    WHERE event_id IS NOT NULL
)
SELECT * FROM raw_events
{% if is_incremental() %}
WHERE event_hk NOT IN (SELECT event_hk FROM {{ this }})
{% endif %}
