
  
    

create or replace transient table NETFLIX_ANALYTICS.MARTS.fact_user_watch_sessions
    

    
    as (

WITH user_engagement AS (
    SELECT * FROM NETFLIX_ANALYTICS.MARTS.int_user_engagement
),

final AS (
    SELECT
        -- Primary key
        user_id,
        show_name,
        watch_date,
        HOUR(watch_date) as watch_hour,
        DAYOFWEEK(watch_date) as day_of_week,
        DAYNAME(watch_date) as day_name,
        MONTH(watch_date) as month,
        QUARTER(watch_date) as quarter,
        YEAR(watch_date) as year,
        
        -- Fact metrics
        watch_duration_minutes,
        engagement_score,
        completion_rate,
        is_binge_session,
        
        -- Derived metrics
        cumulative_watch_time,
        session_number,
        rolling_avg_engagement_7sessions,
        rolling_avg_duration_7sessions,
        binge_sessions_last_7,
        hours_since_last_session,
        
        -- Segmentation
        engagement_segment,
        binge_segment,
        frequency_segment,
        
        -- Time dimensions
        DATE(watch_date) as watch_date_only,
        
        CURRENT_TIMESTAMP() as processed_at
        
    FROM user_engagement
)

SELECT * FROM final
    )
;


  