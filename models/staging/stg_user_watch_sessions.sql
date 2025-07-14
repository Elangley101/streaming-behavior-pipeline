{{
  config(
    materialized='view',
    tags=['staging', 'netflix']
  )
}}

WITH source AS (
    SELECT * FROM {{ source('raw', 'user_watch_sessions') }}
),

cleaned AS (
    SELECT
        -- Primary key
        user_id,
        show_name,
        watch_date,
        
        -- Metrics
        COALESCE(watch_duration_minutes, 0) as watch_duration_minutes,
        COALESCE(engagement_score, 0) as engagement_score,
        COALESCE(completion_rate, 0) as completion_rate,
        
        -- Derived fields
        CASE 
            WHEN watch_duration_minutes >= 120 THEN 1 
            ELSE 0 
        END as is_binge_session,
        
        -- Time dimensions
        DATE(watch_date) as watch_date_only,
        HOUR(watch_date) as watch_hour,
        DAYOFWEEK(watch_date) as day_of_week,
        DAYNAME(watch_date) as day_name,
        MONTH(watch_date) as month,
        QUARTER(watch_date) as quarter,
        YEAR(watch_date) as year,
        
        -- Metadata
        CURRENT_TIMESTAMP() as processed_at
        
    FROM source
    WHERE watch_date IS NOT NULL
      AND user_id IS NOT NULL
      AND show_name IS NOT NULL
)

SELECT * FROM cleaned 