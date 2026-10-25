{{ config(materialized='incremental', unique_key='repo_hk') }}

WITH deduplicated_raw_repos AS (
    SELECT 
        MD5(CAST(repo_id AS STRING)) AS repo_hk,
        repo_id,
        ingested_at AS load_date,
        'GH_ARCHIVE' AS record_source
    FROM {{ ref('stg_bronze_events') }}
    WHERE repo_id IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY MD5(CAST(repo_id AS STRING)) ORDER BY ingested_at ASC) = 1
)

SELECT * FROM deduplicated_raw_repos
{% if is_incremental() %}
WHERE repo_hk NOT IN (SELECT repo_hk FROM {{ this }})
{% endif %}
