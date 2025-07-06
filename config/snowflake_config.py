import os
from typing import Dict, Any
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Snowflake connection configuration
SNOWFLAKE_CONFIG: Dict[str, Any] = {
    "account": os.getenv("SNOWFLAKE_ACCOUNT"),
    "user": os.getenv("SNOWFLAKE_USER"),
    "password": os.getenv("SNOWFLAKE_PASSWORD"),
    "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
    "database": os.getenv("SNOWFLAKE_DATABASE", "NETFLIX_ANALYTICS"),
    "schema": os.getenv("SNOWFLAKE_SCHEMA", "PUBLIC"),
    "role": os.getenv("SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
}

# Table configurations
TABLE_CONFIGS = {
    "watch_facts": {
        "table_name": "WATCH_FACTS",
        "columns": [
            {"name": "watch_id", "type": "VARCHAR", "primary_key": True},
            {"name": "user_id", "type": "VARCHAR", "primary_key": True},
            {"name": "show_id", "type": "VARCHAR", "primary_key": True},
            {"name": "watch_date", "type": "TIMESTAMP_NTZ"},
            {"name": "watch_duration_minutes", "type": "FLOAT"},
            {"name": "completion_rate", "type": "FLOAT"},
            {"name": "is_binge_session", "type": "BOOLEAN"},
            {"name": "engagement_score", "type": "FLOAT"},
            {"name": "created_at", "type": "TIMESTAMP_NTZ"},
            {"name": "updated_at", "type": "TIMESTAMP_NTZ"},
        ],
        "clustering_keys": ["watch_date", "user_id"],
    },
    "user_dim": {
        "table_name": "USER_DIM",
        "columns": [
            {"name": "user_id", "type": "VARCHAR", "primary_key": True},
            {"name": "total_watch_time", "type": "FLOAT"},
            {"name": "avg_watch_time", "type": "FLOAT"},
            {"name": "total_sessions", "type": "INTEGER"},
            {"name": "binge_sessions", "type": "INTEGER"},
            {"name": "is_binge_watcher", "type": "BOOLEAN"},
            {"name": "created_at", "type": "TIMESTAMP_NTZ"},
            {"name": "updated_at", "type": "TIMESTAMP_NTZ"},
        ],
        "clustering_keys": ["user_id"],
    },
    "show_dim": {
        "table_name": "SHOW_DIM",
        "columns": [
            {"name": "show_id", "type": "VARCHAR", "primary_key": True},
            {"name": "show_name", "type": "VARCHAR"},
            {"name": "total_watch_time", "type": "FLOAT"},
            {"name": "total_views", "type": "INTEGER"},
            {"name": "avg_completion_rate", "type": "FLOAT"},
            {"name": "created_at", "type": "TIMESTAMP_NTZ"},
            {"name": "updated_at", "type": "TIMESTAMP_NTZ"},
        ],
        "clustering_keys": ["show_id"],
    },
}

# SQL statements for table creation
CREATE_TABLE_STATEMENTS = {
    "watch_facts": """
    CREATE TABLE IF NOT EXISTS {database}.{schema}.{table_name} (
        watch_id VARCHAR,
        user_id VARCHAR,
        show_id VARCHAR,
        watch_date TIMESTAMP_NTZ,
        watch_duration_minutes FLOAT,
        completion_rate FLOAT,
        is_binge_session BOOLEAN,
        engagement_score FLOAT,
        created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
        updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
        PRIMARY KEY (watch_id, user_id, show_id)
    )
    CLUSTER BY (watch_date, user_id)
    """,
    "user_dim": """
    CREATE TABLE IF NOT EXISTS {database}.{schema}.{table_name} (
        user_id VARCHAR PRIMARY KEY,
        total_watch_time FLOAT,
        avg_watch_time FLOAT,
        total_sessions INTEGER,
        binge_sessions INTEGER,
        is_binge_watcher BOOLEAN,
        created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
        updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
    )
    CLUSTER BY (user_id)
    """,
    "show_dim": """
    CREATE TABLE IF NOT EXISTS {database}.{schema}.{table_name} (
        show_id VARCHAR PRIMARY KEY,
        show_name VARCHAR,
        total_watch_time FLOAT,
        total_views INTEGER,
        avg_completion_rate FLOAT,
        created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
        updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
    )
    CLUSTER BY (show_id)
    """
} 