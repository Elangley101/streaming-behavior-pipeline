#!/usr/bin/env python3
"""
Load sample Netflix data into Snowflake USER_WATCH_SESSIONS table only.
This script creates realistic sample data and loads it into your Snowflake instance.
Downstream marts tables should be built by running dbt.
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import uuid
import logging
from pathlib import Path
import sys

# Add src to path
sys.path.append(str(Path(__file__).parent / "src"))

from snowflake_manager import SnowflakeManager
from config.config import PIPELINE_CONFIG

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SnowflakeDataLoader:
    """Load sample data into Snowflake USER_WATCH_SESSIONS table only."""
    
    def __init__(self):
        """Initialize the data loader."""
        try:
            self.snowflake = SnowflakeManager()
            logger.info("✅ Connected to Snowflake successfully")
        except Exception as e:
            logger.error(f"❌ Failed to connect to Snowflake: {e}")
            logger.info("💡 Make sure you have set up your Snowflake credentials in .env file")
            raise
    
    def generate_sample_data(self, num_records: int = 10000) -> pd.DataFrame:
        """Generate realistic Netflix watch session data."""
        logger.info(f"🎬 Generating {num_records:,} sample watch sessions...")
        
        # Generate date range (last 30 days)
        end_date = datetime.now()
        start_date = end_date - timedelta(days=30)
        dates = pd.date_range(start=start_date, end=end_date, freq='H')
        
        shows = [
            "Stranger Things", "The Crown", "Breaking Bad", "Friends", "The Office",
            "Wednesday", "Bridgerton", "The Witcher", "Squid Game", "Black Mirror",
            "The Umbrella Academy", "You", "Sex Education", "Money Heist", "Dark",
            "Narcos", "Queens Gambit", "Ozark", "The Mandalorian", "WandaVision"
        ]
        users = [f"user_{i:04d}" for i in range(1, 1001)]  # 1000 users
        data = []
        for _ in range(num_records):
            watch_date = np.random.choice(dates)
            show_name = np.random.choice(shows)
            user_id = np.random.choice(users)
            watch_duration = np.random.normal(45, 20)
            watch_duration = max(15, min(180, watch_duration))
            completion_rate = np.random.beta(2, 1)
            is_binge = watch_duration > 60 and np.random.random() > 0.6
            engagement_score = (watch_duration / 180) * 0.6 + completion_rate * 0.4
            data.append({
                "user_id": user_id,
                "show_name": show_name,
                "watch_duration_minutes": float(round(watch_duration, 2)),
                "watch_date": watch_date,
                "completion_rate": float(round(completion_rate, 3)),
                "is_binge_session": bool(is_binge),
                "engagement_score": float(round(engagement_score, 3))
            })
        df = pd.DataFrame(data)
        df['watch_duration_minutes'] = df['watch_duration_minutes'].astype(float)
        df['completion_rate'] = df['completion_rate'].astype(float)
        df['engagement_score'] = df['engagement_score'].astype(float)
        df['is_binge_session'] = df['is_binge_session'].astype(bool)
        logger.info(f"✅ Generated {len(df):,} watch sessions")
        return df
    
    def load_data_to_snowflake(self, watch_data: pd.DataFrame):
        """Load watch session data into USER_WATCH_SESSIONS table only."""
        logger.info("🚀 Loading data into Snowflake USER_WATCH_SESSIONS...")
        try:
            # Use fully qualified table name for clarity
            self.snowflake.load_dataframe(watch_data, "USER_WATCH_SESSIONS", "append")
            logger.info(f"✅ Loaded {len(watch_data):,} records into USER_WATCH_SESSIONS.")
        except Exception as e:
            logger.error(f"❌ Error loading data: {e}")
            raise
    
    def verify_data_loaded(self):
        """Verify that data was loaded correctly."""
        logger.info("🔍 Verifying data load...")
        try:
            watch_count = self.snowflake.execute_query("SELECT COUNT(*) as count FROM USER_WATCH_SESSIONS")
            logger.info(f"🎬 USER_WATCH_SESSIONS: {watch_count['count'].iloc[0]:,} records")
            logger.info("✅ Data verification complete!")
        except Exception as e:
            logger.error(f"❌ Error verifying data: {e}")
    
    def run_full_load(self, num_records: int = 10000):
        """Run the complete data loading process."""
        logger.info("🎬 Starting Netflix data load to Snowflake USER_WATCH_SESSIONS...")
        logger.info("=" * 60)
        try:
            watch_data = self.generate_sample_data(num_records)
            self.load_data_to_snowflake(watch_data)
            self.verify_data_loaded()
            logger.info("=" * 60)
            logger.info("🎉 Data loading completed successfully!")
            logger.info("💡 Next: Run 'dbt run' to build marts tables from USER_WATCH_SESSIONS.")
        except Exception as e:
            logger.error(f"❌ Data loading failed: {e}")
            raise

def main():
    """Main function to run the data loader."""
    print("🎬 Netflix Data Loader for Snowflake (USER_WATCH_SESSIONS only)")
    print("=" * 50)
    env_file = Path(".env")
    if not env_file.exists():
        print("❌ .env file not found!")
        print("💡 Please create a .env file with your Snowflake credentials:")
        print("   SNOWFLAKE_ACCOUNT=your_account")
        print("   SNOWFLAKE_USER=your_username")
        print("   SNOWFLAKE_PASSWORD=your_password")
        print("   SNOWFLAKE_WAREHOUSE=COMPUTE_WH")
        print("   SNOWFLAKE_DATABASE=NETFLIX_ANALYTICS")
        print("   SNOWFLAKE_SCHEMA=MARTS")
        print("   SNOWFLAKE_ROLE=ACCOUNTADMIN")
        return
    try:
        loader = SnowflakeDataLoader()
        loader.run_full_load(num_records=10000)
        print("\n🏆 Next Steps:")
        print("   1. Run 'dbt run' to build marts tables from USER_WATCH_SESSIONS.")
        print("   2. Test your dashboards with the new data.")
    except Exception as e:
        print(f"❌ Failed to load data: {e}")
        print("\n💡 Troubleshooting:")
        print("   - Check your Snowflake credentials in .env")
        print("   - Make sure your Snowflake account is active")
        print("   - Verify your warehouse is running")

if __name__ == "__main__":
    main() 