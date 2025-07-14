{{
  config(
    materialized='table',
    tags=['marts', 'netflix', 'dimension']
  )
}}

WITH user_aggregates AS (
    SELECT
        user_id,
        
        -- Basic metrics
        COUNT(*) as total_sessions,
        SUM(watch_duration_minutes) as total_watch_time_minutes,
        AVG(watch_duration_minutes) as avg_session_duration,
        AVG(engagement_score) as avg_engagement_score,
        AVG(completion_rate) as avg_completion_rate,
        
        -- Binge watching metrics
        SUM(is_binge_session) as total_binge_sessions,
        ROUND(SUM(is_binge_session) * 100.0 / COUNT(*), 2) as binge_session_ratio,
        
        -- Time-based metrics
        MIN(watch_date) as first_watch_date,
        MAX(watch_date) as last_watch_date,
        DATEDIFF('day', MIN(watch_date), MAX(watch_date)) as days_active,
        
        -- Engagement patterns
        COUNT(DISTINCT show_name) as unique_shows_watched,
        COUNT(DISTINCT DATE(watch_date)) as unique_watch_days,
        
        -- Peak activity
        MODE(watch_hour) as most_common_watch_hour,
        MODE(day_name) as most_common_watch_day,
        
        -- Current segments
        MAX(engagement_segment) as current_engagement_segment,
        MAX(binge_segment) as current_binge_segment,
        MAX(frequency_segment) as current_frequency_segment,
        
        -- Recency metrics
        DATEDIFF('day', MAX(watch_date), CURRENT_DATE()) as days_since_last_watch,
        
        -- Activity level
        CASE 
            WHEN days_since_last_watch <= 1 THEN 'Active'
            WHEN days_since_last_watch <= 7 THEN 'Recent'
            WHEN days_since_last_watch <= 30 THEN 'Occasional'
            ELSE 'Inactive'
        END as activity_status
        
    FROM {{ ref('fact_user_watch_sessions') }}
    GROUP BY user_id
),

user_rankings AS (
    SELECT
        *,
        -- Percentile rankings
        PERCENT_RANK() OVER (ORDER BY total_watch_time_minutes) as watch_time_percentile,
        PERCENT_RANK() OVER (ORDER BY avg_engagement_score) as engagement_percentile,
        PERCENT_RANK() OVER (ORDER BY binge_session_ratio) as binge_percentile,
        
        -- Top user flags
        CASE 
            WHEN total_watch_time_minutes >= PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY total_watch_time_minutes) OVER () THEN 'Heavy Watcher'
            WHEN total_watch_time_minutes >= PERCENTILE_CONT(0.7) WITHIN GROUP (ORDER BY total_watch_time_minutes) OVER () THEN 'Regular Watcher'
            ELSE 'Light Watcher'
        END as watch_intensity,
        
        CASE 
            WHEN avg_engagement_score >= 80 THEN 'Highly Engaged'
            WHEN avg_engagement_score >= 60 THEN 'Moderately Engaged'
            ELSE 'Low Engagement'
        END as engagement_level
        
    FROM user_aggregates
)

SELECT * FROM user_rankings 