{{ config(materialized='view') }}

WITH new_members AS (
    SELECT
        sr.repo_name,
        sat.member_added_login,
        sat.load_date AS member_added_at
    FROM {{ ref('sat_event_member') }} sat
    JOIN {{ ref('link_user_repo_event') }} l ON sat.event_hk = l.event_hk
    JOIN {{ ref('sat_repo_details') }} sr ON l.repo_hk = sr.repo_hk
),
repo_made_public AS (
    SELECT
        sr.repo_name,
        sat.load_date AS made_public_at
    FROM {{ ref('sat_event_public') }} sat
    JOIN {{ ref('link_user_repo_event') }} l ON sat.event_hk = l.event_hk
    JOIN {{ ref('sat_repo_details') }} sr ON l.repo_hk = sr.repo_hk
),
suspicious_commits AS (
    SELECT
        sr.repo_name,
        su.actor_login,
        COUNT(DISTINCT sat.commit_sha) AS massive_commit_volume,
        MIN(sat.load_date) AS first_suspicious_commit,
        MAX(sat.load_date) AS last_suspicious_commit
    FROM {{ ref('sat_event_commits') }} sat
    JOIN {{ ref('link_user_repo_event') }} l ON sat.event_hk = l.event_hk
    JOIN {{ ref('sat_user_details') }} su ON l.user_hk = su.user_hk
    JOIN {{ ref('sat_repo_details') }} sr ON l.repo_hk = sr.repo_hk
    GROUP BY 1, 2
    HAVING massive_commit_volume > 50
)

SELECT
    m.repo_name,
    m.member_added_login AS suspect_developer,
    m.member_added_at,
    s.massive_commit_volume,
    p.made_public_at,
    'HIGH' AS threat_level,
    'Suspicious volume of commits by new member followed by repo status change' AS alert_reason
FROM new_members m
JOIN suspicious_commits s ON m.repo_name = s.repo_name AND m.member_added_login = s.actor_login
JOIN repo_made_public p ON m.repo_name = p.repo_name
WHERE p.made_public_at >= s.last_suspicious_commit
