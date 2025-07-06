import argparse
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional
import uuid
import pandas as pd

from extract import DataExtractor
from load import DataLoader
from snowflake_manager import SnowflakeManager
from transform import DataTransformer
from utils import PipelineError, handle_pipeline_error, setup_logging

logger = setup_logging("etl_runner")

class ETLPipeline:
    """Orchestrates the ETL pipeline process."""
    
    def __init__(
        self,
        input_path: Optional[str] = None,
        output_path: Optional[str] = None,
        batch_mode: bool = False,
        use_snowflake: bool = False
    ):
        """
        Initialize the ETL pipeline.
        
        Args:
            input_path: Optional path to input data
            output_path: Optional path for output data
            batch_mode: Whether to process data in batches
            use_snowflake: Whether to load data to Snowflake
        """
        self.extractor = DataExtractor(input_path)
        self.transformer = DataTransformer()
        self.loader = DataLoader(output_path)
        self.batch_mode = batch_mode
        self.use_snowflake = use_snowflake
        self.snowflake_manager = SnowflakeManager() if use_snowflake else None
    
    def run(self) -> None:
        """
        Run the complete ETL pipeline.
        
        Raises:
            PipelineError: If any step fails
        """
        try:
            start_time = datetime.now()
            logger.info("Starting ETL pipeline")
            
            if self.batch_mode:
                self._run_batch_pipeline()
            else:
                self._run_full_pipeline()
            
            duration = (datetime.now() - start_time).total_seconds()
            logger.info(f"ETL pipeline completed successfully in {duration:.2f} seconds")
            
        except Exception as e:
            handle_pipeline_error(e, {
                "step": "pipeline",
                "error_type": type(e).__name__
            })
            sys.exit(1)
        finally:
            if self.snowflake_manager:
                self.snowflake_manager.close()
    
    def _run_full_pipeline(self) -> None:
        """Run the pipeline on the full dataset."""
        # Extract
        df = self.extractor.read_csv()
        
        # Transform
        transformed_df = self.transformer.transform(df)
        
        # Load to Parquet
        self.loader.save_to_parquet(
            transformed_df,
            partition_cols=["watch_month", "watch_day"]
        )
        
        # Load to Snowflake if enabled
        if self.use_snowflake:
            self._load_to_snowflake(transformed_df)
    
    def _run_batch_pipeline(self) -> None:
        """Run the pipeline in batch mode."""
        for batch in self.extractor.read_csv_in_batches():
            # Transform batch
            transformed_batch = self.transformer.transform_batch(batch)
            
            # Load batch to Parquet
            self.loader.save_batch_to_parquet(
                transformed_batch,
                partition_cols=["watch_month", "watch_day"]
            )
            
            # Load batch to Snowflake if enabled
            if self.use_snowflake:
                self._load_to_snowflake(transformed_batch)
    
    def _load_to_snowflake(self, df: pd.DataFrame) -> None:
        """
        Load transformed data to Snowflake tables.
        
        Args:
            df: Transformed DataFrame to load
        """
        if not self.snowflake_manager:
            return
        
        try:
            # Prepare watch facts
            watch_facts = df[[
                "user_id", "show_name", "watch_date",
                "watch_duration_minutes", "completion_rate",
                "is_binge_session", "engagement_score"
            ]].copy()
            watch_facts["watch_id"] = [str(uuid.uuid4()) for _ in range(len(watch_facts))]
            watch_facts["show_id"] = watch_facts["show_name"].apply(lambda x: str(hash(x)))
            
            # Load watch facts
            self.snowflake_manager.load_dataframe(watch_facts, "watch_facts")
            
            # Prepare and load user dimension
            user_dim = df.groupby("user_id", as_index=False).agg({
                "total_watch_time": "sum",
                "avg_watch_time": "mean",
                "total_sessions": "count",
                "binge_sessions": "sum",
                "is_binge_watcher": "first"
            })
            self.snowflake_manager.load_dataframe(user_dim, "user_dim")
            
            # Prepare and load show dimension
            show_dim = df.groupby("show_name", as_index=False).agg({
                "total_watch_time": "sum",
                "total_views": "count",
                "avg_completion_rate": "mean"
            })
            show_dim["show_id"] = show_dim["show_name"].apply(lambda x: str(hash(x)))
            self.snowflake_manager.load_dataframe(show_dim, "show_dim")
            
            # Log table statistics
            for table in ["watch_facts", "user_dim", "show_dim"]:
                stats = self.snowflake_manager.get_table_stats(table)
                logger.info(f"Table {table} stats: {stats}")
                
        except Exception as e:
            handle_pipeline_error(e, {
                "step": "snowflake_load",
                "error_type": type(e).__name__
            })

def main():
    """Main entry point for the ETL pipeline."""
    parser = argparse.ArgumentParser(description="Netflix-style behavioral data ETL pipeline")
    parser.add_argument(
        "--input",
        type=str,
        help="Path to input CSV file"
    )
    parser.add_argument(
        "--output",
        type=str,
        help="Path to output Parquet file"
    )
    parser.add_argument(
        "--batch",
        action="store_true",
        help="Process data in batches"
    )
    parser.add_argument(
        "--snowflake",
        action="store_true",
        help="Load data to Snowflake"
    )
    
    args = parser.parse_args()
    
    pipeline = ETLPipeline(
        input_path=args.input,
        output_path=args.output,
        batch_mode=args.batch,
        use_snowflake=args.snowflake
    )
    
    pipeline.run()

if __name__ == "__main__":
    main() 