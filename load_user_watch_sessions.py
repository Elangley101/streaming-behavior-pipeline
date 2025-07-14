import os
import pandas as pd
from dotenv import load_dotenv
import snowflake.connector

# Load environment variables from .env
load_dotenv()

SNOWFLAKE_ACCOUNT = os.getenv('SNOWFLAKE_ACCOUNT')
SNOWFLAKE_USER = os.getenv('SNOWFLAKE_USER')
SNOWFLAKE_PASSWORD = os.getenv('SNOWFLAKE_PASSWORD')
SNOWFLAKE_WAREHOUSE = os.getenv('SNOWFLAKE_WAREHOUSE')
SNOWFLAKE_DATABASE = os.getenv('SNOWFLAKE_DATABASE')
SNOWFLAKE_SCHEMA = os.getenv('SNOWFLAKE_SCHEMA', 'MARTS')

# Read the CSV file
df = pd.read_csv('data/raw/watch_logs.csv')

# Connect to Snowflake
ctx = snowflake.connector.connect(
    user=SNOWFLAKE_USER,
    password=SNOWFLAKE_PASSWORD,
    account=SNOWFLAKE_ACCOUNT,
    warehouse=SNOWFLAKE_WAREHOUSE,
    database=SNOWFLAKE_DATABASE,
    schema=SNOWFLAKE_SCHEMA
)
cs = ctx.cursor()

try:
    # Create table if not exists
    cs.execute(f'''
        CREATE TABLE IF NOT EXISTS USER_WATCH_SESSIONS (
            user_id VARCHAR,
            show_name VARCHAR,
            watch_duration_minutes FLOAT,
            watch_date TIMESTAMP_NTZ,
            engagement_score FLOAT,
            completion_rate FLOAT,
            created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        )
    ''')

    # Insert data row by row (for simplicity; for large data, use Snowflake's bulk loading)
    for _, row in df.iterrows():
        cs.execute(f'''
            INSERT INTO USER_WATCH_SESSIONS (user_id, show_name, watch_duration_minutes, watch_date, engagement_score, completion_rate)
            VALUES (:user_id, :show_name, :watch_duration_minutes, :watch_date, :engagement_score, :completion_rate)
        ''', {
            'user_id': row['user_id'],
            'show_name': row['show_name'],
            'watch_duration_minutes': row['watch_duration_minutes'],
            'watch_date': row['watch_date'],
            'engagement_score': row.get('engagement_score', None),
            'completion_rate': row.get('completion_rate', None)
        })
    print(f"Inserted {len(df)} rows into USER_WATCH_SESSIONS.")
finally:
    cs.close()
    ctx.close() 