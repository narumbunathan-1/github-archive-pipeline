{{ config(materialized='incremental', unique_key='link_hk') }}
WITH raw_links AS (
    SELECT DISTINCT
        MD5(CONCAT_WS('||', CAST(event_id AS VARCHAR), CAST(actor_id AS VARCHAR), CAST(repo_id AS VARCHAR))) AS link_hk,
        MD5(CAST(event_id AS VARCHAR)) AS event_hk,
        MD5(CAST(actor_id AS VARCHAR)) AS user_hk,
        MD5(CAST(repo_id AS VARCHAR)) AS repo_hk,
        ingested_at AS load_date,
        'GH_ARCHIVE' AS record_source
    FROM {{ ref('stg_bronze_events') }}
    WHERE event_id IS NOT NULL AND actor_id IS NOT NULL AND repo_id IS NOT NULL
)
SELECT * FROM raw_links
{% if is_incremental() %}
WHERE link_hk NOT IN (SELECT link_hk FROM {{ this }})
{% endif %}
