{{
  config(
    materialized='table',
    tags=['marts', 'netflix', 'dimension']
  )
}}

WITH show_aggregates AS (
    SELECT
        show_name,
        
        -- Basic metrics
        COUNT(*) as total_views,
        COUNT(DISTINCT user_id) as unique_viewers,
        SUM(watch_duration_minutes) as total_watch_time_minutes,
        AVG(watch_duration_minutes) as avg_watch_duration,
        AVG(engagement_score) as avg_engagement_score,
        AVG(completion_rate) as avg_completion_rate,
        
        -- Binge watching metrics
        SUM(is_binge_session) as total_binge_sessions,
        ROUND(SUM(is_binge_session) * 100.0 / COUNT(*), 2) as binge_session_ratio,
        
        -- Time-based metrics
        MIN(watch_date) as first_watched_date,
        MAX(watch_date) as last_watched_date,
        DATEDIFF('day', MIN(watch_date), MAX(watch_date)) as days_in_catalog,
        
        -- Viewer engagement
        ROUND(COUNT(*) * 1.0 / COUNT(DISTINCT user_id), 2) as avg_views_per_viewer,
        ROUND(SUM(watch_duration_minutes) * 1.0 / COUNT(DISTINCT user_id), 2) as avg_watch_time_per_viewer,
        
        -- Popularity metrics
        COUNT(DISTINCT DATE(watch_date)) as unique_watch_days,
        ROUND(COUNT(*) * 1.0 / COUNT(DISTINCT DATE(watch_date)), 2) as avg_daily_views,
        
        -- Peak activity
        MODE(watch_hour) as most_common_watch_hour,
        MODE(day_name) as most_common_watch_day,
        
        -- Recency
        DATEDIFF('day', MAX(watch_date), CURRENT_DATE()) as days_since_last_view
        
    FROM {{ ref('fact_user_watch_sessions') }}
    GROUP BY show_name
),

show_rankings AS (
    SELECT
        *,
        -- Percentile rankings
        PERCENT_RANK() OVER (ORDER BY total_views) as views_percentile,
        PERCENT_RANK() OVER (ORDER BY avg_engagement_score) as engagement_percentile,
        PERCENT_RANK() OVER (ORDER BY binge_session_ratio) as binge_percentile,
        -- Top show flags
        CASE 
            WHEN total_views >= PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY total_views) OVER () THEN 'Blockbuster'
            WHEN total_views >= PERCENTILE_CONT(0.7) WITHIN GROUP (ORDER BY total_views) OVER () THEN 'Hit'
            ELSE 'Regular'
        END as popularity_category,
        CASE 
            WHEN avg_engagement_score >= 80 THEN 'Highly Engaging'
            WHEN avg_engagement_score >= 60 THEN 'Moderately Engaging'
            ELSE 'Low Engagement'
        END as engagement_level,
        
        CASE 
            WHEN binge_session_ratio >= 50 THEN 'Binge-Worthy'
            WHEN binge_session_ratio >= 25 THEN 'Occasionally Binged'
            ELSE 'Regular Viewing'
        END as binge_category,
        
        -- Trending indicators
        CASE 
            WHEN days_since_last_view <= 1 THEN 'Trending'
            WHEN days_since_last_view <= 7 THEN 'Recent'
            WHEN days_since_last_view <= 30 THEN 'Stable'
            ELSE 'Declining'
        END as trend_status
        
    FROM show_aggregates
)

SELECT * FROM show_rankings 