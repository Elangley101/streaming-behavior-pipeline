import os
import logging
from pathlib import Path
from typing import Optional, List
import pandas as pd
import pyarrow as pa
from pyarrow import parquet as pq
from config.config import PIPELINE_CONFIG
from utils import PipelineError, handle_pipeline_error, setup_logging

logger = setup_logging("load")


class DataLoader:
    """Handles the loading and storage of processed data."""

    def __init__(self, output_path: Optional[str] = None):
        """
        Initialize the DataLoader.

        Args:
            output_path: Optional path for output file. If not provided, uses config default.
        """
        self.output_path = output_path or PIPELINE_CONFIG["processed_data_path"]
        self._ensure_output_directory()

    def _ensure_output_directory(self) -> None:
        """Ensure the output directory exists."""
        output_dir = Path(self.output_path).parent
        output_dir.mkdir(parents=True, exist_ok=True)

    def save_to_parquet(
        self, df: pd.DataFrame, partition_cols: Optional[list] = None
    ) -> None:
        """
        Save DataFrame to Parquet format with optional partitioning.

        Args:
            df: DataFrame to save
            partition_cols: Optional list of columns to partition by

        Raises:
            PipelineError: If saving fails
        """
        try:
            logger.info(f"Saving data to {self.output_path}")

            # Convert to PyArrow Table for better performance
            table = pa.Table.from_pandas(df)

            # Write to Parquet with compression
            if partition_cols:
                # Use write_to_dataset for partitioning
                pq.write_to_dataset(
                    table,
                    self.output_path,
                    partition_cols=partition_cols,
                    compression="snappy",
                    coerce_timestamps="ms",
                    allow_truncated_timestamps=True,
                )
            else:
                # Use write_table for single file
                pq.write_table(
                    table,
                    self.output_path,
                    compression="snappy",
                    coerce_timestamps="ms",
                    allow_truncated_timestamps=True,
                )

            logger.info(f"Successfully saved {len(df)} records to Parquet")

        except Exception as e:
            handle_pipeline_error(
                e,
                {
                    "step": "load",
                    "output_path": self.output_path,
                    "error_type": type(e).__name__,
                },
            )

    def save_batch_to_parquet(
        self, batch: pd.DataFrame, partition_cols: Optional[list] = None
    ) -> None:
        """
        Save a batch of data to Parquet format.

        Args:
            batch: DataFrame batch to save
            partition_cols: Optional list of columns to partition by
        """
        self.save_to_parquet(batch, partition_cols)

    def read_parquet(self) -> pd.DataFrame:
        """
        Read data from Parquet file.

        Returns:
            pd.DataFrame: Loaded DataFrame

        Raises:
            PipelineError: If loading fails
        """
        try:
            logger.info(f"Reading data from {self.output_path}")

            df = pd.read_parquet(self.output_path, engine="pyarrow")

            logger.info(f"Successfully loaded {len(df)} records from Parquet")
            return df

        except Exception as e:
            handle_pipeline_error(
                e,
                {
                    "step": "load_read",
                    "input_path": self.output_path,
                    "error_type": type(e).__name__,
                },
            )

    def get_parquet_metadata(self) -> dict:
        """
        Get metadata about the Parquet file.

        Returns:
            dict: Metadata about the Parquet file
        """
        try:
            parquet_file = pq.ParquetFile(self.output_path)
            return {
                "num_rows": parquet_file.metadata.num_rows,
                "num_row_groups": parquet_file.num_row_groups,
                "schema": parquet_file.schema.to_arrow_schema().to_pylist(),
            }
        except Exception as e:
            handle_pipeline_error(
                e,
                {
                    "step": "load_metadata",
                    "input_path": self.output_path,
                    "error_type": type(e).__name__,
                },
            )
