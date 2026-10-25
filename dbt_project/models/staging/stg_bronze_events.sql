{{ config(
    materialized='incremental',
    unique_key='event_id'
) }}

WITH raw_data AS (
    SELECT
        raw_payload:id::VARCHAR AS event_id,
        raw_payload:type::STRING AS event_type,
        raw_payload:created_at::TIMESTAMP_NTZ AS created_at,
        raw_payload:repo:id::NUMBER AS repo_id,
        raw_payload:repo:name::STRING AS repo_name,
        raw_payload:actor:id::NUMBER AS actor_id,
        raw_payload:actor:login::STRING AS actor_login,
        raw_payload:payload AS event_payload,
        ingestion_time AS ingested_at
    FROM GH_ARCHIVE_LOCAL.BRONZE.RAW_EVENTS 
    {% if is_incremental() %}
    WHERE ingestion_time > (SELECT MAX(ingested_at) FROM {{ this }})
    {% endif %}
)
SELECT * FROM raw_data QUALIFY ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY ingested_at DESC) = 1