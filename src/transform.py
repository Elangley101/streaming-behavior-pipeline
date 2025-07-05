from datetime import datetime, timedelta
from typing import Dict, List, Optional

import numpy as np
import pandas as pd
from config.config import PIPELINE_CONFIG
from src.utils import PipelineError, handle_pipeline_error, setup_logging

logger = setup_logging("transform")

class DataTransformer:
    """Handles the transformation and feature engineering of watch data."""
    
    def __init__(self):
        """Initialize the DataTransformer with configuration parameters."""
        self.binge_threshold = PIPELINE_CONFIG["binge_watch_threshold_minutes"]
        self.completion_threshold = PIPELINE_CONFIG["completion_rate_threshold"]
    
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Apply all transformations to the input DataFrame.
        
        Args:
            df: Input DataFrame with raw watch data
            
        Returns:
            pd.DataFrame: Transformed DataFrame with engineered features
        """
        try:
            logger.info("Starting data transformation")
            
            # Create a copy to avoid modifying the original
            transformed_df = df.copy()
            
            # Apply transformations
            transformed_df = self._clean_data(transformed_df)
            transformed_df = self._engineer_features(transformed_df)
            transformed_df = self._calculate_metrics(transformed_df)
            
            logger.info(f"Successfully transformed {len(transformed_df)} records")
            return transformed_df
            
        except Exception as e:
            handle_pipeline_error(e, {
                "step": "transform",
                "error_type": type(e).__name__
            })
    
    def _clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean and standardize the input data.
        
        Args:
            df: Input DataFrame
            
        Returns:
            pd.DataFrame: Cleaned DataFrame
        """
        # Remove duplicates
        df = df.drop_duplicates()
        
        # Handle missing values
        df["watch_duration_minutes"] = df["watch_duration_minutes"].fillna(0)
        df["show_name"] = df["show_name"].fillna("Unknown")
        
        # Standardize show names
        df["show_name"] = df["show_name"].str.strip().str.title()
        
        # Ensure watch_date is datetime
        df["watch_date"] = pd.to_datetime(df["watch_date"])
        
        return df
    
    def _engineer_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Engineer new features from the raw data.
        
        Args:
            df: Input DataFrame
            
        Returns:
            pd.DataFrame: DataFrame with new features
        """
        # Extract temporal features
        df["watch_hour"] = df["watch_date"].dt.hour
        df["watch_day"] = df["watch_date"].dt.day_name()
        df["watch_month"] = df["watch_date"].dt.month_name()
        
        # Calculate session features
        df["is_binge_session"] = df["watch_duration_minutes"] >= self.binge_threshold
        
        # Calculate user engagement metrics
        user_stats = df.groupby("user_id").agg({
            "watch_duration_minutes": ["sum", "mean", "count"],
            "is_binge_session": "sum"
        }).reset_index()
        
        user_stats.columns = [
            "user_id",
            "total_watch_time",
            "avg_watch_time",
            "total_sessions",
            "binge_sessions"
        ]
        
        # Merge user stats back to main DataFrame
        df = df.merge(user_stats, on="user_id", how="left")
        
        return df
    
    def _calculate_metrics(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate advanced metrics and KPIs.
        
        Args:
            df: Input DataFrame
            
        Returns:
            pd.DataFrame: DataFrame with calculated metrics
        """
        # Calculate completion rate (assuming we have show duration data)
        # This is a placeholder - in production, you'd have actual show durations
        df["completion_rate"] = df["watch_duration_minutes"] / 60  # Assuming 1-hour shows
        
        # Calculate binge watching patterns
        df["is_binge_watcher"] = df["binge_sessions"] / df["total_sessions"] > 0.5
        
        # Calculate engagement score (custom metric)
        df["engagement_score"] = (
            (df["total_watch_time"] / df["total_watch_time"].max()) * 0.4 +
            (df["binge_sessions"] / df["total_sessions"]) * 0.3 +
            (df["completion_rate"]) * 0.3
        )
        
        # Add time-based features
        df["is_weekend"] = df["watch_day"].isin(["Saturday", "Sunday"])
        df["is_primetime"] = df["watch_hour"].between(19, 23)
        
        return df
    
    def transform_batch(self, batch: pd.DataFrame) -> pd.DataFrame:
        """
        Transform a single batch of data.
        
        Args:
            batch: Input DataFrame batch
            
        Returns:
            pd.DataFrame: Transformed batch
        """
        return self.transform(batch) 