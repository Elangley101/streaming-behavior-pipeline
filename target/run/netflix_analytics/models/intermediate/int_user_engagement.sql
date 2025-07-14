
  create or replace   view NETFLIX_ANALYTICS.MARTS.int_user_engagement
  
   as (
    

WITH user_sessions AS (
    SELECT * FROM NETFLIX_ANALYTICS.MARTS.stg_user_watch_sessions
),

user_metrics AS (
    SELECT
        user_id,
        show_name,
        watch_date,
        watch_duration_minutes,
        engagement_score,
        completion_rate,
        is_binge_session,
        
        -- Session-level metrics
        SUM(watch_duration_minutes) OVER (
            PARTITION BY user_id 
            ORDER BY watch_date 
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) as cumulative_watch_time,
        
        COUNT(*) OVER (
            PARTITION BY user_id 
            ORDER BY watch_date 
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) as session_number,
        
        -- Rolling averages
        AVG(engagement_score) OVER (
            PARTITION BY user_id 
            ORDER BY watch_date 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) as rolling_avg_engagement_7sessions,
        
        AVG(watch_duration_minutes) OVER (
            PARTITION BY user_id 
            ORDER BY watch_date 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) as rolling_avg_duration_7sessions,
        
        -- Binge watching patterns
        SUM(is_binge_session) OVER (
            PARTITION BY user_id 
            ORDER BY watch_date 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) as binge_sessions_last_7,
        
        -- Time-based patterns
        LAG(watch_date) OVER (
            PARTITION BY user_id 
            ORDER BY watch_date
        ) as previous_session_date,
        
        DATEDIFF('hour', 
            LAG(watch_date) OVER (PARTITION BY user_id ORDER BY watch_date),
            watch_date
        ) as hours_since_last_session
        
    FROM user_sessions
),

user_segments AS (
    SELECT
        *,
        CASE 
            WHEN rolling_avg_engagement_7sessions >= 80 THEN 'High Engagement'
            WHEN rolling_avg_engagement_7sessions >= 60 THEN 'Medium Engagement'
            ELSE 'Low Engagement'
        END as engagement_segment,
        
        CASE 
            WHEN binge_sessions_last_7 >= 3 THEN 'Binge Watcher'
            WHEN binge_sessions_last_7 >= 1 THEN 'Occasional Binge'
            ELSE 'Regular Watcher'
        END as binge_segment,
        
        CASE 
            WHEN hours_since_last_session <= 24 THEN 'Daily User'
            WHEN hours_since_last_session <= 168 THEN 'Weekly User'  -- 7 days
            ELSE 'Occasional User'
        END as frequency_segment
        
    FROM user_metrics
)

SELECT * FROM user_segments
  );

