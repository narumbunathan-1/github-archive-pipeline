{{ config(materialized='view') }}

WITH hub_count AS (
    SELECT COUNT(*) AS total_hub_events
    FROM {{ ref('hub_event') }}
),
spoke_counts AS (
    SELECT 'PushEvent' AS event_type, COUNT(*) AS event_count FROM {{ ref('sat_event_commits') }}
    UNION ALL SELECT 'IssuesEvent', COUNT(*) FROM {{ ref('sat_event_issues') }}
    UNION ALL SELECT 'PullRequestEvent', COUNT(*) FROM {{ ref('sat_event_pull_requests') }}
    UNION ALL SELECT 'WatchEvent', COUNT(*) FROM {{ ref('sat_event_watch') }}
    UNION ALL SELECT 'ForkEvent', COUNT(*) FROM {{ ref('sat_event_forks') }}
    UNION ALL SELECT 'PullRequestReviewCommentEvent', COUNT(*) FROM {{ ref('sat_event_pr_review_comments') }}
    UNION ALL SELECT 'IssueCommentEvent', COUNT(*) FROM {{ ref('sat_event_issue_comments') }}
    UNION ALL SELECT 'DeleteEvent', COUNT(*) FROM {{ ref('sat_event_delete') }}
    UNION ALL SELECT 'CreateEvent', COUNT(*) FROM {{ ref('sat_event_create') }}
    UNION ALL SELECT 'ReleaseEvent', COUNT(*) FROM {{ ref('sat_event_release') }}
    UNION ALL SELECT 'GollumEvent', COUNT(*) FROM {{ ref('sat_event_wiki') }}
    UNION ALL SELECT 'MemberEvent', COUNT(*) FROM {{ ref('sat_event_member') }}
    UNION ALL SELECT 'PublicEvent', COUNT(*) FROM {{ ref('sat_event_public') }}
    UNION ALL SELECT 'CommitCommentEvent', COUNT(*) FROM {{ ref('sat_event_commit_comments') }}
),
total_spoke_count AS (
    SELECT SUM(event_count) AS total_spoke_events
    FROM spoke_counts
)

SELECT
    h.total_hub_events AS vault_hub_count,
    s.total_spoke_events AS vault_satellite_count,
    h.total_hub_events - s.total_spoke_events AS discrepancy_count,
    IFF(h.total_hub_events = s.total_spoke_events, 'PASSED', 'FAILED - DATA LOSS DETECTED') AS validation_status
FROM hub_count h
CROSS JOIN total_spoke_count s
