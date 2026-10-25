{{ config(materialized='incremental', unique_key='user_hk') }}

WITH deduplicated_raw_users AS (
    SELECT 
        -- Force explicit casting to string before hashing to prevent type mismatch duplicates
        MD5(CAST(actor_id AS STRING)) AS user_hk,
        actor_id AS user_id,
        ingested_at AS load_date,
        'GH_ARCHIVE' AS record_source
    FROM {{ ref('stg_bronze_events') }}
    WHERE actor_id IS NOT NULL
    -- Deduplicate the incoming batch FIRST
    QUALIFY ROW_NUMBER() OVER (PARTITION BY MD5(CAST(actor_id AS STRING)) ORDER BY ingested_at ASC) = 1
)

SELECT * FROM deduplicated_raw_users
{% if is_incremental() %}
-- Then check against existing records
WHERE user_hk NOT IN (SELECT user_hk FROM {{ this }})
{% endif %}
