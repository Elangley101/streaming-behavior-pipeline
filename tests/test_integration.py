"""
Integration Tests for Netflix Behavioral Data Pipeline
Tests the complete pipeline end-to-end functionality.
"""

import pytest
import pandas as pd
import tempfile
import os
import time
from pathlib import Path
from unittest.mock import patch, MagicMock

from src.extract import DataExtractor
from src.transform import DataTransformer
from src.load import DataLoader
from src.api_service import app
from src.streaming_processor import StreamingProcessor
from src.monitoring import metrics_collector, health_checker
from src.data_lineage import track_data_source, track_transformation, get_data_lineage
from fastapi.testclient import TestClient


@pytest.fixture
def client():
    """Create test client for FastAPI."""
    return TestClient(app)


@pytest.fixture
def sample_data_file():
    """Create temporary sample data file."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        f.write("user_id,show_name,watch_duration_minutes,watch_date\n")
        f.write("user_001,Stranger Things,45,2024-01-15 20:30:00\n")
        f.write("user_002,The Crown,60,2024-01-15 19:15:00\n")
        f.write("user_003,Wednesday,30,2024-01-15 21:00:00\n")
        f.write("user_004,Bridgerton,90,2024-01-15 18:45:00\n")
        f.write("user_005,The Witcher,75,2024-01-15 22:15:00\n")
        yield f.name
    os.unlink(f.name)


@pytest.fixture
def kafka_mock():
    """Mock Kafka producer and consumer."""
    with patch("kafka.KafkaProducer") as mock_producer, patch(
        "kafka.KafkaConsumer"
    ) as mock_consumer:

        # Mock producer
        mock_producer_instance = MagicMock()
        mock_producer.return_value = mock_producer_instance

        # Mock consumer
        mock_consumer_instance = MagicMock()
        mock_consumer_instance.__iter__.return_value = []
        mock_consumer.return_value = mock_consumer_instance

        yield {"producer": mock_producer_instance, "consumer": mock_consumer_instance}


class TestEndToEndPipeline:
    """Test the complete ETL pipeline."""

    def test_complete_pipeline_flow(self, sample_data_file):
        """Test complete pipeline from extract to load."""
        # Extract
        extractor = DataExtractor(sample_data_file)
        df = extractor.read_csv()
        assert len(df) == 5
        assert all(
            col in df.columns
            for col in ["user_id", "show_name", "watch_duration_minutes", "watch_date"]
        )

        # Transform
        transformer = DataTransformer()
        transformed_df = transformer.transform(df)
        assert "is_binge_session" in transformed_df.columns
        assert "completion_rate" in transformed_df.columns
        assert "engagement_score" in transformed_df.columns

        # Load
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
            output_path = f.name

        try:
            loader = DataLoader(output_path)
            loader.save_to_parquet(transformed_df)

            # Verify file was created and can be read
            assert os.path.exists(output_path)
            loaded_df = loader.read_parquet()
            assert len(loaded_df) == len(transformed_df)
            assert all(col in loaded_df.columns for col in transformed_df.columns)
        finally:
            os.unlink(output_path)

    def test_pipeline_with_data_validation(self, sample_data_file):
        """Test pipeline with data validation."""
        extractor = DataExtractor(sample_data_file)
        transformer = DataTransformer()

        df = extractor.read_csv()
        transformed_df = transformer.transform(df)

        # Test data validation
        assert transformed_df["watch_duration_minutes"].min() >= 0
        assert transformed_df["watch_duration_minutes"].max() <= 1440
        assert transformed_df["completion_rate"].min() >= 0
        assert transformed_df["completion_rate"].max() <= 1
        assert transformed_df["engagement_score"].min() >= 0
        assert transformed_df["engagement_score"].max() <= 100


class TestAPIEndpoints:
    """Test FastAPI endpoints."""

    def test_health_endpoint(self, client):
        """Test health check endpoint."""
        response = client.get("/health")
        assert response.status_code == 200
        data = response.json()
        assert "status" in data
        assert "timestamp" in data

    def test_analytics_endpoint(self, client):
        """Test analytics endpoint."""
        response = client.get("/analytics?days=7")
        assert response.status_code == 200
        data = response.json()
        assert "total_users" in data
        assert "total_shows" in data
        assert "total_watch_time" in data

    def test_user_analytics_endpoint(self, client):
        """Test user analytics endpoint."""
        response = client.get("/users/user_001/analytics?days=30")
        assert response.status_code == 200
        data = response.json()
        assert "user_id" in data
        assert "total_watch_time" in data
        assert "favorite_shows" in data

    def test_show_analytics_endpoint(self, client):
        """Test show analytics endpoint."""
        response = client.get("/shows/Stranger%20Things/analytics?days=30")
        assert response.status_code == 200
        data = response.json()
        assert "show_name" in data
        assert "total_watch_time" in data
        assert "unique_viewers" in data

    def test_create_event_endpoint(self, client):
        """Test event creation endpoint."""
        event_data = {
            "user_id": "test_user",
            "show_name": "Test Show",
            "watch_duration_minutes": 45.5,
            "watch_date": "2024-01-15T20:30:00",
        }
        response = client.post("/events", json=event_data)
        assert response.status_code == 201
        data = response.json()
        assert data["user_id"] == "test_user"
        assert data["show_name"] == "Test Show"


class TestStreamingProcessor:
    """Test streaming processor functionality."""

    def test_streaming_processor_initialization(self, kafka_mock):
        """Test streaming processor initialization."""
        processor = StreamingProcessor()
        assert processor is not None

    def test_event_processing(self, kafka_mock):
        """Test event processing in streaming mode."""
        processor = StreamingProcessor()

        # Mock event data
        event_data = {
            "user_id": "test_user",
            "show_name": "Test Show",
            "watch_duration_minutes": 45.5,
            "watch_date": "2024-01-15T20:30:00",
        }

        # Test event processing
        processed_event = processor.process_event(event_data)
        assert processed_event["user_id"] == "test_user"
        assert "engagement_score" in processed_event
        assert "is_binge_session" in processed_event


class TestMonitoring:
    """Test monitoring and observability features."""

    def test_health_checker(self):
        """Test health checker functionality."""
        health_status = health_checker.get_health_status()
        assert "status" in health_status
        assert "timestamp" in health_status
        assert "checks" in health_status

    def test_metrics_collection(self):
        """Test metrics collection."""
        # Test API metrics
        metrics_collector.api_requests_total.labels(
            method="GET", endpoint="/health", status=200
        ).inc()

        # Test pipeline metrics
        metrics_collector.pipeline_events_processed.labels(
            status="success", source="kafka"
        ).inc()

        # Verify metrics are being collected
        assert metrics_collector.api_requests_total._value.sum() > 0
        assert metrics_collector.pipeline_events_processed._value.sum() > 0


class TestDataLineage:
    """Test data lineage tracking."""

    def test_data_source_tracking(self):
        """Test data source tracking."""
        source_id = track_data_source(
            dataset_name="test_dataset",
            source_name="test_source",
            source_type="csv",
            location="data/test.csv",
            schema={"user_id": "string", "show_name": "string"},
            content="test content",
        )
        assert source_id is not None

        # Verify lineage was created
        lineage = get_data_lineage("test_dataset")
        assert lineage is not None
        assert len(lineage.sources) == 1
        assert lineage.sources[0].name == "test_source"

    def test_transformation_tracking(self):
        """Test transformation tracking."""
        # First create a source
        source_id = track_data_source(
            dataset_name="test_dataset",
            source_name="test_source",
            source_type="csv",
            location="data/test.csv",
            schema={"user_id": "string"},
        )

        # Then track a transformation
        transformation_id = track_transformation(
            dataset_name="test_dataset",
            transformation_name="test_transform",
            transformation_type="cleaning",
            description="Test transformation",
            input_sources=[source_id],
            output_targets=["data/output.parquet"],
            parameters={"param1": "value1"},
            duration_seconds=1.5,
            records_processed=100,
            records_failed=0,
        )

        assert transformation_id is not None

        # Verify transformation was tracked
        lineage = get_data_lineage("test_dataset")
        assert len(lineage.transformations) == 1
        assert lineage.transformations[0].name == "test_transform"


class TestPerformance:
    """Test performance characteristics."""

    def test_large_dataset_processing(self):
        """Test processing of large datasets."""
        # Create large test dataset
        large_df = pd.DataFrame(
            {
                "user_id": [f"user_{i:06d}" for i in range(10000)],
                "show_name": ["Test Show"] * 10000,
                "watch_duration_minutes": [45.5] * 10000,
                "watch_date": ["2024-01-15 20:30:00"] * 10000,
            }
        )

        # Test processing time
        start_time = time.time()
        transformer = DataTransformer()
        transformed_df = transformer.transform(large_df)
        processing_time = time.time() - start_time

        # Should process 10k records in reasonable time
        assert processing_time < 10  # Less than 10 seconds
        assert len(transformed_df) == 10000

    def test_memory_usage(self):
        """Test memory usage during processing."""
        import psutil
        import os

        process = psutil.Process(os.getpid())
        initial_memory = process.memory_info().rss

        # Process large dataset
        large_df = pd.DataFrame(
            {
                "user_id": [f"user_{i:06d}" for i in range(5000)],
                "show_name": ["Test Show"] * 5000,
                "watch_duration_minutes": [45.5] * 5000,
                "watch_date": ["2024-01-15 20:30:00"] * 5000,
            }
        )

        transformer = DataTransformer()
        transformed_df = transformer.transform(large_df)

        final_memory = process.memory_info().rss
        memory_increase = final_memory - initial_memory

        # Memory increase should be reasonable (less than 100MB)
        assert memory_increase < 100 * 1024 * 1024  # 100MB


class TestErrorHandling:
    """Test error handling and recovery."""

    def test_invalid_data_handling(self):
        """Test handling of invalid data."""
        # Create data with invalid values
        invalid_df = pd.DataFrame(
            {
                "user_id": ["user_001", "user_002", "user_003"],
                "show_name": ["Show 1", "Show 2", "Show 3"],
                "watch_duration_minutes": [45, -10, 1500],  # Invalid values
                "watch_date": [
                    "2024-01-15 20:30:00",
                    "invalid_date",
                    "2024-01-15 21:00:00",
                ],
            }
        )

        transformer = DataTransformer()

        # Should handle invalid data gracefully
        try:
            transformed_df = transformer.transform(invalid_df)
            # Should still process valid records
            assert len(transformed_df) > 0
        except Exception as e:
            # Should provide meaningful error message
            assert "validation" in str(e).lower() or "invalid" in str(e).lower()

    def test_missing_data_handling(self):
        """Test handling of missing data."""
        # Create data with missing values
        missing_df = pd.DataFrame(
            {
                "user_id": ["user_001", None, "user_003"],
                "show_name": ["Show 1", "Show 2", None],
                "watch_duration_minutes": [45, 60, None],
                "watch_date": ["2024-01-15 20:30:00", "2024-01-15 19:15:00", None],
            }
        )

        transformer = DataTransformer()

        # Should handle missing data gracefully
        try:
            transformed_df = transformer.transform(missing_df)
            # Should still process records with valid data
            assert len(transformed_df) > 0
        except Exception as e:
            # Should provide meaningful error message
            assert "missing" in str(e).lower() or "null" in str(e).lower()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
