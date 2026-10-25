{{ config(materialized='table') }}

WITH pr_stats AS (
    SELECT
        su.actor_login AS developer_login,
        sr.repo_name,
        COUNT(DISTINCT l.event_hk) AS total_prs_opened,
        SUM(IFF(sat.is_merged, 1, 0)) AS total_prs_merged,
        SUM(sat.additions) AS total_lines_added,
        SUM(sat.deletions) AS total_lines_deleted
    FROM {{ ref('sat_event_pull_requests') }} sat
    JOIN {{ ref('link_user_repo_event') }} l ON sat.event_hk = l.event_hk
    JOIN {{ ref('sat_user_details') }} su ON l.user_hk = su.user_hk
    JOIN {{ ref('sat_repo_details') }} sr ON l.repo_hk = sr.repo_hk
    GROUP BY 1, 2
),
commit_stats AS (
    SELECT
        su.actor_login AS developer_login,
        sr.repo_name,
        COUNT(DISTINCT sat.commit_sha) AS total_commits
    FROM {{ ref('sat_event_commits') }} sat
    JOIN {{ ref('link_user_repo_event') }} l ON sat.event_hk = l.event_hk
    JOIN {{ ref('sat_user_details') }} su ON l.user_hk = su.user_hk
    JOIN {{ ref('sat_repo_details') }} sr ON l.repo_hk = sr.repo_hk
    GROUP BY 1, 2
),
review_stats AS (
    SELECT
        su.actor_login AS developer_login,
        sr.repo_name,
        COUNT(DISTINCT sat.comment_body) AS total_reviews_given
    FROM {{ ref('sat_event_pr_review_comments') }} sat
    JOIN {{ ref('link_user_repo_event') }} l ON sat.event_hk = l.event_hk
    JOIN {{ ref('sat_user_details') }} su ON l.user_hk = su.user_hk
    JOIN {{ ref('sat_repo_details') }} sr ON l.repo_hk = sr.repo_hk
    GROUP BY 1, 2
)

SELECT
    COALESCE(p.developer_login, c.developer_login, r.developer_login) AS developer_login,
    COALESCE(p.repo_name, c.repo_name, r.repo_name) AS repo_name,
    COALESCE(p.total_prs_opened, 0) AS prs_opened,
    COALESCE(p.total_prs_merged, 0) AS prs_merged,
    COALESCE(c.total_commits, 0) AS total_commits,
    COALESCE(r.total_reviews_given, 0) AS reviews_given,
    COALESCE(p.total_lines_added, 0) AS lines_added,
    COALESCE(p.total_lines_deleted, 0) AS lines_deleted,
    IFF(prs_opened > 0, (prs_merged::FLOAT / prs_opened) * 100, 0) AS merge_success_rate
FROM pr_stats p
FULL OUTER JOIN commit_stats c ON p.developer_login = c.developer_login AND p.repo_name = c.repo_name
FULL OUTER JOIN review_stats r ON p.developer_login = r.developer_login AND p.repo_name = r.repo_name
