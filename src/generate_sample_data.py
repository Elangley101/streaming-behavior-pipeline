import random
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import numpy as np
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.config import PIPELINE_CONFIG

def generate_sample_data(
    num_records: int = 1000,
    output_path: str = None,
    start_date: datetime = None
) -> None:
    """
    Generate sample watch data for testing the pipeline.
    
    Args:
        num_records: Number of records to generate
        output_path: Path to save the CSV file
        start_date: Start date for the data
    """
    if output_path is None:
        output_path = PIPELINE_CONFIG["raw_data_path"]
    
    if start_date is None:
        start_date = datetime.now() - timedelta(days=30)
    
    # Sample data
    show_names = [
        "Stranger Things", "The Crown", "Breaking Bad", "Friends",
        "The Office", "Black Mirror", "House of Cards", "Mindhunter",
        "Dark", "The Witcher", "Ozark", "Narcos"
    ]
    
    # Generate random user IDs
    user_ids = [f"user_{i:04d}" for i in range(1, 101)]
    
    # Generate data
    data = {
        "user_id": random.choices(user_ids, k=num_records),
        "show_name": random.choices(show_names, k=num_records),
        "watch_duration_minutes": np.random.normal(45, 15, num_records).clip(1, 180),
        "watch_date": [
            start_date + timedelta(
                days=random.randint(0, 30),
                hours=random.randint(0, 23),
                minutes=random.randint(0, 59)
            )
            for _ in range(num_records)
        ]
    }
    
    # Create DataFrame
    df = pd.DataFrame(data)
    
    # Ensure user_id is string type
    df["user_id"] = df["user_id"].astype(str)
    
    # Ensure output directory exists
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    
    # Save to CSV
    df.to_csv(output_path, index=False)
    print(f"Generated {num_records} sample records at {output_path}")

if __name__ == "__main__":
    generate_sample_data() 