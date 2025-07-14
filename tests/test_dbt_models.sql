-- DBT Model Tests for Netflix Analytics Pipeline

-- Uniqueness and not null for FACT_USER_WATCH_SESSIONS
SELECT * FROM {{ ref('fact_user_watch_sessions') }}
WHERE user_id IS NULL OR show_name IS NULL OR watch_date IS NULL;

-- Uniqueness for DIM_USERS
SELECT user_id, COUNT(*)
FROM {{ ref('dim_users') }}
GROUP BY user_id
HAVING COUNT(*) > 1;

-- Uniqueness for DIM_SHOWS
SELECT show_name, COUNT(*)
FROM {{ ref('dim_shows') }}
GROUP BY show_name
HAVING COUNT(*) > 1;

-- Relationship: FACT_USER_WATCH_SESSIONS.user_id should exist in DIM_USERS
SELECT f.user_id
FROM {{ ref('fact_user_watch_sessions') }} f
LEFT JOIN {{ ref('dim_users') }} u ON f.user_id = u.user_id
WHERE u.user_id IS NULL;

-- Relationship: FACT_USER_WATCH_SESSIONS.show_name should exist in DIM_SHOWS
SELECT f.show_name
FROM {{ ref('fact_user_watch_sessions') }} f
LEFT JOIN {{ ref('dim_shows') }} s ON f.show_name = s.show_name
WHERE s.show_name IS NULL;

-- Custom: completion_rate should be between 0 and 1
SELECT * FROM {{ ref('fact_user_watch_sessions') }}
WHERE completion_rate < 0 OR completion_rate > 1; 