# Data Dictionary

## USER_WATCH_SESSIONS
| Column                  | Type      | Description                                 |
|-------------------------|-----------|---------------------------------------------|
| user_id                 | VARCHAR   | Unique user identifier                      |
| show_name               | VARCHAR   | Name of the show watched                    |
| watch_duration_minutes  | FLOAT     | Duration of the watch session (minutes)     |
| watch_date              | TIMESTAMP | When the session occurred                   |
| completion_rate         | FLOAT     | Fraction of show completed (0-1)            |
| is_binge_session        | BOOLEAN   | True if part of a binge session             |
| engagement_score        | FLOAT     | Calculated engagement metric                |

## FACT_USER_WATCH_SESSIONS
| Column                  | Type      | Description                                 |
|-------------------------|-----------|---------------------------------------------|
| user_id                 | VARCHAR   | Unique user identifier                      |
| show_name               | VARCHAR   | Name of the show watched                    |
| watch_date              | TIMESTAMP | When the session occurred                   |
| watch_duration_minutes  | FLOAT     | Duration of the watch session (minutes)     |
| completion_rate         | FLOAT     | Fraction of show completed (0-1)            |
| is_binge_session        | BOOLEAN   | True if part of a binge session             |
| engagement_score        | FLOAT     | Calculated engagement metric                |

## DIM_USERS
| Column                  | Type      | Description                                 |
|-------------------------|-----------|---------------------------------------------|
| user_id                 | VARCHAR   | Unique user identifier                      |
| total_sessions          | INT       | Number of sessions for the user             |
| avg_watch_time          | FLOAT     | Average watch time per session              |
| avg_engagement          | FLOAT     | Average engagement score                    |
| binge_sessions          | INT       | Number of binge sessions                    |

## DIM_SHOWS
| Column                  | Type      | Description                                 |
|-------------------------|-----------|---------------------------------------------|
| show_name               | VARCHAR   | Name of the show                            |
| total_views             | INT       | Number of times the show was watched        |
| avg_completion_rate     | FLOAT     | Average completion rate for the show        |
| avg_engagement          | FLOAT     | Average engagement score for the show       |

## MART_CONTENT_ANALYTICS
| Column                  | Type      | Description                                 |
|-------------------------|-----------|---------------------------------------------|
| show_name               | VARCHAR   | Name of the show                            |
| total_watch_time        | FLOAT     | Total watch time for the show               |
| unique_viewers          | INT       | Number of unique viewers                    |
| avg_engagement          | FLOAT     | Average engagement score                    |
| binge_sessions          | INT       | Number of binge sessions for the show       |

## MART_USER_ANALYTICS
| Column                  | Type      | Description                                 |
|-------------------------|-----------|---------------------------------------------|
| user_id                 | VARCHAR   | Unique user identifier                      |
| total_watch_time        | FLOAT     | Total watch time for the user               |
| unique_shows            | INT       | Number of unique shows watched              |
| avg_completion_rate     | FLOAT     | Average completion rate                     |
| binge_sessions          | INT       | Number of binge sessions                    | 