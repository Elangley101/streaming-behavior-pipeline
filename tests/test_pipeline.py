import os
import tempfile
from pathlib import Path

import pandas as pd
import pytest
from src.extract import DataExtractor
from src.load import DataLoader
from src.transform import DataTransformer
from src.generate_sample_data import generate_sample_data


@pytest.fixture
def sample_data():
    """Create temporary sample data for testing."""
    with tempfile.TemporaryDirectory() as temp_dir:
        input_path = Path(temp_dir) / "test_data.csv"
        generate_sample_data(num_records=100, output_path=str(input_path))
        yield str(input_path)


@pytest.fixture
def output_path():
    """Create temporary output path for testing."""
    with tempfile.TemporaryDirectory() as temp_dir:
        yield str(Path(temp_dir) / "output.parquet")


def test_extract(sample_data):
    """Test data extraction."""
    extractor = DataExtractor(sample_data)
    df = extractor.read_csv()

    assert isinstance(df, pd.DataFrame)
    assert len(df) > 0
    assert all(
        col in df.columns
        for col in ["user_id", "show_name", "watch_duration_minutes", "watch_date"]
    )


def test_transform(sample_data):
    """Test data transformation."""
    extractor = DataExtractor(sample_data)
    transformer = DataTransformer()

    df = extractor.read_csv()
    transformed_df = transformer.transform(df)

    assert isinstance(transformed_df, pd.DataFrame)
    assert len(transformed_df) > 0
    assert "is_binge_session" in transformed_df.columns
    assert "completion_rate" in transformed_df.columns
    assert "engagement_score" in transformed_df.columns


def test_load(sample_data, output_path):
    """Test data loading."""
    extractor = DataExtractor(sample_data)
    transformer = DataTransformer()
    loader = DataLoader(output_path)

    df = extractor.read_csv()
    transformed_df = transformer.transform(df)
    loader.save_to_parquet(transformed_df)

    # Verify the file was created
    assert os.path.exists(output_path)

    # Verify we can read it back
    loaded_df = loader.read_parquet()
    assert isinstance(loaded_df, pd.DataFrame)
    assert len(loaded_df) == len(transformed_df)


def test_batch_processing(sample_data, output_path):
    """Test batch processing functionality."""
    extractor = DataExtractor(sample_data)
    transformer = DataTransformer()
    loader = DataLoader(output_path)

    # Process in batches
    for batch in extractor.read_csv_in_batches():
        transformed_batch = transformer.transform_batch(batch)
        loader.save_batch_to_parquet(transformed_batch)

    # Verify the file was created
    assert os.path.exists(output_path)

    # Verify we can read it back
    loaded_df = loader.read_parquet()
    assert isinstance(loaded_df, pd.DataFrame)
    assert len(loaded_df) > 0


def test_data_validation(sample_data):
    """Test data validation rules."""
    extractor = DataExtractor(sample_data)
    df = extractor.read_csv()

    # Verify data types
    assert df["user_id"].dtype == "string"
    assert df["show_name"].dtype == "string"
    assert pd.api.types.is_numeric_dtype(df["watch_duration_minutes"])
    assert pd.api.types.is_datetime64_any_dtype(df["watch_date"])

    # Verify value ranges
    assert df["watch_duration_minutes"].min() >= 0
    assert df["watch_duration_minutes"].max() <= 1440  # 24 hours
