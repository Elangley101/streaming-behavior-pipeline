from typing import Optional

import pandas as pd
from config.config import PIPELINE_CONFIG
from src.utils import PipelineError, handle_pipeline_error, setup_logging, validate_dataframe

logger = setup_logging("extract")

class DataExtractor:
    """Handles the extraction of raw data from various sources."""
    
    def __init__(self, file_path: Optional[str] = None):
        """
        Initialize the DataExtractor.
        
        Args:
            file_path: Optional path to the data file. If not provided, uses config default.
        """
        self.file_path = file_path or PIPELINE_CONFIG["raw_data_path"]
        self.batch_size = PIPELINE_CONFIG["batch_size"]
    
    def read_csv(self) -> pd.DataFrame:
        """
        Read and validate CSV data.
        
        Returns:
            pd.DataFrame: Validated DataFrame containing raw data
            
        Raises:
            PipelineError: If data loading or validation fails
        """
        try:
            logger.info(f"Reading data from {self.file_path}")
            
            # Read CSV with optimized settings
            df = pd.read_csv(
                self.file_path,
                parse_dates=["watch_date"],
                dtype={
                    "user_id": "string",
                    "show_name": "string",
                    "watch_duration_minutes": "float64"
                },
                na_values=["", "NA", "null"],
                encoding="utf-8"
            )
            
            # Validate the data
            validate_dataframe(df)
            
            logger.info(f"Successfully loaded {len(df)} records")
            return df
            
        except Exception as e:
            handle_pipeline_error(e, {
                "step": "extract",
                "file_path": self.file_path,
                "error_type": type(e).__name__
            })
    
    def read_csv_in_batches(self):
        """
        Generator that reads CSV data in batches.
        
        Yields:
            pd.DataFrame: Batches of validated data
        """
        try:
            logger.info(f"Reading data in batches from {self.file_path}")
            
            for batch in pd.read_csv(
                self.file_path,
                parse_dates=["watch_date"],
                dtype={
                    "user_id": "string",
                    "show_name": "string",
                    "watch_duration_minutes": "float64"
                },
                na_values=["", "NA", "null"],
                encoding="utf-8",
                chunksize=self.batch_size
            ):
                validate_dataframe(batch)
                yield batch
                
        except Exception as e:
            handle_pipeline_error(e, {
                "step": "extract_batch",
                "file_path": self.file_path,
                "batch_size": self.batch_size,
                "error_type": type(e).__name__
            }) 