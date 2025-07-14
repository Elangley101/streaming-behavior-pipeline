-- Test for data completeness
SELECT COUNT(*) as missing_user_ids
FROM {{ ref('fact_user_watch_sessions') }}
WHERE user_id IS NULL OR user_id = ''

-- Test for data validity
SELECT COUNT(*) as invalid_durations
FROM {{ ref('fact_user_watch_sessions') }}
WHERE watch_duration_minutes < 0 OR watch_duration_minutes > 1440

-- Test for engagement score range
SELECT COUNT(*) as invalid_engagement_scores
FROM {{ ref('fact_user_watch_sessions') }}
WHERE engagement_score < 0 OR engagement_score > 100

-- Test for completion rate range
SELECT COUNT(*) as invalid_completion_rates
FROM {{ ref('fact_user_watch_sessions') }}
WHERE completion_rate < 0 OR completion_rate > 100

-- Test for referential integrity
SELECT COUNT(*) as orphaned_user_records
FROM {{ ref('dim_users') }} u
LEFT JOIN {{ ref('fact_user_watch_sessions') }} f ON u.user_id = f.user_id
WHERE f.user_id IS NULL

-- Test for data freshness
SELECT COUNT(*) as stale_data
FROM {{ ref('fact_user_watch_sessions') }}
WHERE watch_date < DATEADD('day', -90, CURRENT_DATE()) 