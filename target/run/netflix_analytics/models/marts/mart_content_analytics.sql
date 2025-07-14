
  
    

create or replace transient table NETFLIX_ANALYTICS.MARTS.mart_content_analytics
    

    
    as (

WITH content_analytics AS (
    SELECT
        s.show_name,
        s.total_views,
        s.unique_viewers,
        s.total_watch_time_minutes,
        s.avg_watch_duration,
        s.avg_engagement_score,
        s.avg_completion_rate,
        s.total_binge_sessions,
        s.binge_session_ratio,
        s.avg_views_per_viewer,
        s.avg_watch_time_per_viewer,
        s.avg_daily_views,
        s.popularity_category,
        s.engagement_level,
        s.binge_category,
        s.trend_status,
        
        -- Recent performance (last 7 days)
        COUNT(CASE WHEN f.watch_date >= DATEADD('day', -7, CURRENT_DATE()) THEN 1 END) as views_last_7_days,
        COUNT(DISTINCT CASE WHEN f.watch_date >= DATEADD('day', -7, CURRENT_DATE()) THEN f.user_id END) as unique_viewers_last_7_days,
        AVG(CASE WHEN f.watch_date >= DATEADD('day', -7, CURRENT_DATE()) THEN f.engagement_score END) as engagement_last_7_days,
        
        -- Recent performance (last 30 days)
        COUNT(CASE WHEN f.watch_date >= DATEADD('day', -30, CURRENT_DATE()) THEN 1 END) as views_last_30_days,
        COUNT(DISTINCT CASE WHEN f.watch_date >= DATEADD('day', -30, CURRENT_DATE()) THEN f.user_id END) as unique_viewers_last_30_days,
        
        -- Time-based patterns
        s.most_common_watch_hour,
        s.most_common_watch_day,
        
        -- Content lifecycle
        s.first_watched_date,
        s.last_watched_date,
        s.days_in_catalog,
        s.days_since_last_view,
        
        -- Viewer demographics (simplified)
        COUNT(DISTINCT f.user_id) as total_unique_viewers,
        ROUND(COUNT(*) * 1.0 / COUNT(DISTINCT f.user_id), 2) as avg_sessions_per_viewer,
        
        -- Engagement depth
        AVG(f.rolling_avg_engagement_7sessions) as avg_viewer_engagement,
        AVG(f.rolling_avg_duration_7sessions) as avg_viewer_duration,
        
        -- Binge patterns
        AVG(f.binge_sessions_last_7) as avg_binge_sessions_per_viewer,
        
        -- Content stickiness
        CASE 
            WHEN s.avg_views_per_viewer >= 3 THEN 'High Stickiness'
            WHEN s.avg_views_per_viewer >= 2 THEN 'Medium Stickiness'
            ELSE 'Low Stickiness'
        END as content_stickiness,
        
        -- Growth indicators

        
    FROM NETFLIX_ANALYTICS.MARTS.dim_shows s
    LEFT JOIN NETFLIX_ANALYTICS.MARTS.fact_user_watch_sessions f ON s.show_name = f.show_name
    GROUP BY 
        s.show_name, s.total_views, s.unique_viewers, s.total_watch_time_minutes, s.avg_watch_duration,
        s.avg_engagement_score, s.avg_completion_rate, s.total_binge_sessions, s.binge_session_ratio,
        s.avg_views_per_viewer, s.avg_watch_time_per_viewer, s.avg_daily_views, s.popularity_category,
        s.engagement_level, s.binge_category, s.trend_status, s.most_common_watch_hour, s.most_common_watch_day, s.first_watched_date,
        s.last_watched_date, s.days_in_catalog, s.days_since_last_view
)

SELECT * FROM content_analytics
    )
;


  