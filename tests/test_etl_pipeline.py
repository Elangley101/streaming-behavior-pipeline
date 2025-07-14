import pytest
import pandas as pd
from src.etl_runner import ETLPipeline


@pytest.fixture
def sample_data():
    return pd.DataFrame(
        {
            "user_id": ["user_001", "user_002", "user_003"],
            "show_name": ["Show A", "Show B", "Show C"],
            "watch_duration_minutes": [45, 60, 30],
            "watch_date": pd.to_datetime(["2024-01-01", "2024-01-02", "2024-01-03"]),
            "completion_rate": [0.9, 0.8, 1.0],
            "is_binge_session": [False, True, False],
            "engagement_score": [0.7, 0.8, 0.9],
        }
    )


def test_etl_load_and_transform(sample_data):
    pipeline = ETLPipeline()
    # Simulate loading and transformation
    transformed = pipeline._transform(sample_data)
    # Check that no user_id is null
    assert transformed["user_id"].notna().all()
    # Check that all watch_duration_minutes are positive
    assert (transformed["watch_duration_minutes"] > 0).all()
    # Check that completion_rate is between 0 and 1
    assert (
        (transformed["completion_rate"] >= 0) & (transformed["completion_rate"] <= 1)
    ).all()
