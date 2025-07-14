

WITH user_analytics AS (
    SELECT
        u.user_id,
        u.total_sessions,
        u.total_watch_time_minutes,
        u.avg_session_duration,
        u.avg_engagement_score,
        u.avg_completion_rate,
        u.total_binge_sessions,
        u.binge_session_ratio,
        u.unique_shows_watched,
        u.days_active,
        u.activity_status,
        u.watch_intensity,
        u.engagement_level,
        u.watch_time_percentile,
        u.engagement_percentile,
        u.binge_percentile,
        
        -- Recent activity (last 7 days)
        COUNT(CASE WHEN f.watch_date >= DATEADD('day', -7, CURRENT_DATE()) THEN 1 END) as sessions_last_7_days,
        SUM(CASE WHEN f.watch_date >= DATEADD('day', -7, CURRENT_DATE()) THEN f.watch_duration_minutes END) as watch_time_last_7_days,
        AVG(CASE WHEN f.watch_date >= DATEADD('day', -7, CURRENT_DATE()) THEN f.engagement_score END) as engagement_last_7_days,
        
        -- Recent activity (last 30 days)
        COUNT(CASE WHEN f.watch_date >= DATEADD('day', -30, CURRENT_DATE()) THEN 1 END) as sessions_last_30_days,
        SUM(CASE WHEN f.watch_date >= DATEADD('day', -30, CURRENT_DATE()) THEN f.watch_duration_minutes END) as watch_time_last_30_days,
        
        -- Top shows watched
        ARRAY_AGG(DISTINCT f.show_name) WITHIN GROUP (ORDER BY f.show_name) as top_shows,
        
        -- Peak activity times
        u.most_common_watch_hour,
        u.most_common_watch_day,
        
        -- User lifecycle
        u.first_watch_date,
        u.last_watch_date,
        u.days_since_last_watch,
        
        -- Engagement trends
        AVG(f.rolling_avg_engagement_7sessions) as avg_rolling_engagement,
        AVG(f.rolling_avg_duration_7sessions) as avg_rolling_duration,
        
        -- Binge patterns
        AVG(f.binge_sessions_last_7) as avg_binge_sessions_per_week,
        
        -- Retention metrics
        CASE 
            WHEN u.days_since_last_watch <= 1 THEN 'Retained'
            WHEN u.days_since_last_watch <= 7 THEN 'At Risk'
            ELSE 'Churned'
        END as retention_status
        
    FROM NETFLIX_ANALYTICS.MARTS.dim_users u
    LEFT JOIN NETFLIX_ANALYTICS.MARTS.fact_user_watch_sessions f ON u.user_id = f.user_id
    GROUP BY 
        u.user_id, u.total_sessions, u.total_watch_time_minutes, u.avg_session_duration,
        u.avg_engagement_score, u.avg_completion_rate, u.total_binge_sessions, u.binge_session_ratio,
        u.unique_shows_watched, u.days_active, u.activity_status, u.watch_intensity, u.engagement_level,
        u.watch_time_percentile, u.engagement_percentile, u.binge_percentile, u.most_common_watch_hour,
        u.most_common_watch_day, u.first_watch_date, u.last_watch_date, u.days_since_last_watch
)

SELECT * FROM user_analytics