{{ config(materialized='table') }}

WITH stars AS (
    SELECT sr.repo_name, COUNT(*) AS new_stars
    FROM {{ ref('sat_event_watch') }} sat
    JOIN {{ ref('link_user_repo_event') }} l ON sat.event_hk = l.event_hk
    JOIN {{ ref('sat_repo_details') }} sr ON l.repo_hk = sr.repo_hk
    GROUP BY 1
),
forks AS (
    SELECT sr.repo_name, COUNT(*) AS new_forks
    FROM {{ ref('sat_event_forks') }} sat
    JOIN {{ ref('link_user_repo_event') }} l ON sat.event_hk = l.event_hk
    JOIN {{ ref('sat_repo_details') }} sr ON l.repo_hk = sr.repo_hk
    GROUP BY 1
),
issues AS (
    SELECT 
        sr.repo_name, 
        SUM(IFF(sat.issue_action = 'opened', 1, 0)) AS bugs_opened,
        SUM(IFF(sat.issue_action = 'closed', 1, 0)) AS bugs_closed
    FROM {{ ref('sat_event_issues') }} sat
    JOIN {{ ref('link_user_repo_event') }} l ON sat.event_hk = l.event_hk
    JOIN {{ ref('sat_repo_details') }} sr ON l.repo_hk = sr.repo_hk
    GROUP BY 1
)

SELECT
    COALESCE(s.repo_name, f.repo_name, i.repo_name) AS repo_name,
    COALESCE(s.new_stars, 0) AS total_new_stars,
    COALESCE(f.new_forks, 0) AS total_new_forks,
    COALESCE(i.bugs_opened, 0) AS issues_opened,
    COALESCE(i.bugs_closed, 0) AS issues_closed,
    (COALESCE(s.new_stars, 0) + COALESCE(f.new_forks, 0) - COALESCE(i.bugs_opened, 0)) AS repo_health_score
FROM stars s
FULL OUTER JOIN forks f ON s.repo_name = f.repo_name
FULL OUTER JOIN issues i ON s.repo_name = i.repo_name
ORDER BY repo_health_score DESC
