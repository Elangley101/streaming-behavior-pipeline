#!/usr/bin/env python3
"""
Simple script to create sample Netflix-style data for the ETL pipeline.
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os

def create_sample_data():
    """Create sample Netflix-style user behavior data."""
    
    # Create data directory if it doesn't exist
    os.makedirs('data/raw', exist_ok=True)
    
    # Sample show names
    shows = [
        "Stranger Things", "The Crown", "Wednesday", "Bridgerton", 
        "The Witcher", "Money Heist", "Dark", "Ozark", "The Queen's Gambit",
        "Squid Game", "Black Mirror", "The Umbrella Academy", "You",
        "Sex Education", "The Good Place", "Breaking Bad", "Better Call Saul",
        "Narcos", "Mindhunter", "The Haunting of Hill House"
    ]
    
    # Generate sample data
    np.random.seed(42)  # For reproducible results
    
    # Generate 1000 sample records
    n_records = 1000
    
    data = []
    base_date = datetime.now() - timedelta(days=30)
    
    for i in range(n_records):
        user_id = f"user_{np.random.randint(1, 101):03d}"
        show_name = np.random.choice(shows)
        watch_duration = np.random.randint(15, 180)  # 15 minutes to 3 hours
        watch_date = base_date + timedelta(
            days=np.random.randint(0, 30),
            hours=np.random.randint(0, 24),
            minutes=np.random.randint(0, 60)
        )
        
        data.append({
            'user_id': user_id,
            'show_name': show_name,
            'watch_duration_minutes': watch_duration,
            'watch_date': watch_date.strftime('%Y-%m-%d %H:%M:%S')
        })
    
    # Create DataFrame and save to CSV
    df = pd.DataFrame(data)
    df.to_csv('data/raw/watch_logs.csv', index=False)
    
    print(f"✅ Created sample data with {len(data)} records")
    print(f"📁 Saved to: data/raw/watch_logs.csv")
    print(f"📊 Sample data preview:")
    print(df.head())
    
    return df

if __name__ == "__main__":
    create_sample_data() 