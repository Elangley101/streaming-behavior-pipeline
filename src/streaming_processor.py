import json
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Generator
import uuid
import random
import threading
from queue import Queue
import logging

import pandas as pd
import numpy as np
from kafka import KafkaProducer, KafkaConsumer
from transform import DataTransformer
from snowflake_manager import SnowflakeManager
from utils import PipelineError, handle_pipeline_error, setup_logging

logger = setup_logging("streaming_processor")


class WatchEvent(BaseModel):
    """Data model for watch events."""

    event_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    user_id: str
    show_name: str
    watch_duration_minutes: float
    watch_date: datetime
    event_type: str = "watch_session"
    session_id: str = Field(default_factory=lambda: str(uuid.uuid4()))


class StreamingProcessor:
    """Real-time streaming processor for watch events."""

    def __init__(
        self,
        kafka_bootstrap_servers: str = None,
        input_topic: str = "watch_events",
        output_topic: str = "processed_events",
        error_topic: str = "error_events",
        batch_size: int = 100,
        batch_timeout: int = 30,
    ):
        # Use environment variable or default
        if kafka_bootstrap_servers is None:
            import os

            kafka_bootstrap_servers = os.getenv(
                "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
            )
        """
        Initialize the streaming processor.
        
        Args:
            kafka_bootstrap_servers: Kafka broker addresses
            input_topic: Topic to consume from
            output_topic: Topic to produce to
            error_topic: Topic for error events
            batch_size: Number of events to process in batch
            batch_timeout: Timeout for batch processing (seconds)
        """
        self.kafka_bootstrap_servers = kafka_bootstrap_servers
        self.input_topic = input_topic
        self.output_topic = output_topic
        self.error_topic = error_topic
        self.batch_size = batch_size
        self.batch_timeout = batch_timeout

        self.producer = None
        self.consumer = None
        self.transformer = DataTransformer()
        self.snowflake_manager = None
        self.running = False

        self._setup_kafka()

    def _setup_kafka(self):
        """Setup Kafka producer and consumer."""
        try:
            # Setup producer
            self.producer = KafkaProducer(
                bootstrap_servers=self.kafka_bootstrap_servers,
                value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                acks="all",
                retries=3,
            )

            # Setup consumer
            self.consumer = KafkaConsumer(
                self.input_topic,
                bootstrap_servers=self.kafka_bootstrap_servers,
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                group_id="netflix_analytics_group",
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                auto_commit_interval_ms=1000,
            )

            logger.info("Successfully setup Kafka producer and consumer")

        except Exception as e:
            handle_pipeline_error(
                e, {"step": "kafka_setup", "error_type": type(e).__name__}
            )

    def _setup_snowflake(self):
        """Setup Snowflake connection for real-time loading."""
        try:
            self.snowflake_manager = SnowflakeManager()
            logger.info("Successfully setup Snowflake connection")
        except Exception as e:
            logger.warning(f"Could not setup Snowflake: {str(e)}")

    def process_event(self, event_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Process a single watch event.

        Args:
            event_data: Raw event data

        Returns:
            Dict[str, Any]: Processed event data
        """
        try:
            # Validate event
            event = WatchEvent(**event_data)

            # Convert to DataFrame for transformation
            df = pd.DataFrame([asdict(event)])
            df["watch_date"] = pd.to_datetime(df["watch_date"])

            # Apply transformations
            transformed_df = self.transformer.transform(df)

            # Convert back to dict
            processed_event = transformed_df.iloc[0].to_dict()
            processed_event["processed_at"] = datetime.now().isoformat()

            return processed_event

        except Exception as e:
            handle_pipeline_error(
                e,
                {
                    "step": "event_processing",
                    "event_data": event_data,
                    "error_type": type(e).__name__,
                },
            )

    def process_batch(self, events: list) -> list:
        """
        Process a batch of events.

        Args:
            events: List of event data

        Returns:
            list: List of processed events
        """
        try:
            if not events:
                return []

            # Convert to DataFrame
            df = pd.DataFrame(events)
            df["watch_date"] = pd.to_datetime(df["watch_date"])

            # Apply transformations
            transformed_df = self.transformer.transform(df)
            transformed_df["processed_at"] = datetime.now()

            # Convert back to list of dicts
            processed_events = transformed_df.to_dict("records")

            logger.info(f"Successfully processed batch of {len(events)} events")
            return processed_events

        except Exception as e:
            handle_pipeline_error(
                e,
                {
                    "step": "batch_processing",
                    "batch_size": len(events),
                    "error_type": type(e).__name__,
                },
            )

    def send_event(self, topic: str, event: Dict[str, Any], key: Optional[str] = None):
        """
        Send an event to a Kafka topic.

        Args:
            topic: Target topic
            event: Event data
            key: Optional message key
        """
        try:
            future = self.producer.send(topic, value=event, key=key)
            future.get(timeout=10)  # Wait for send confirmation

        except Exception as e:
            handle_pipeline_error(
                e,
                {"step": "kafka_send", "topic": topic, "error_type": type(e).__name__},
            )

    def load_to_snowflake(self, events: list):
        """
        Load processed events to Snowflake.

        Args:
            events: List of processed events
        """
        if not self.snowflake_manager or not events:
            return

        try:
            df = pd.DataFrame(events)

            # Prepare watch facts
            watch_facts = df[
                [
                    "user_id",
                    "show_name",
                    "watch_date",
                    "watch_duration_minutes",
                    "completion_rate",
                    "is_binge_session",
                    "engagement_score",
                ]
            ].copy()
            watch_facts["watch_id"] = [
                str(uuid.uuid4()) for _ in range(len(watch_facts))
            ]
            watch_facts["show_id"] = watch_facts["show_name"].apply(
                lambda x: str(hash(x))
            )

            # Load to Snowflake
            self.snowflake_manager.load_dataframe(watch_facts, "watch_facts")

            logger.info(f"Successfully loaded {len(events)} events to Snowflake")

        except Exception as e:
            handle_pipeline_error(
                e,
                {
                    "step": "snowflake_load",
                    "batch_size": len(events),
                    "error_type": type(e).__name__,
                },
            )

    def start_streaming(self, callback: Optional[Callable] = None):
        """
        Start the streaming processor.

        Args:
            callback: Optional callback function for processed events
        """
        self.running = True
        self._setup_snowflake()

        logger.info("Starting streaming processor")

        try:
            batch = []
            last_batch_time = time.time()

            for message in self.consumer:
                if not self.running:
                    break

                try:
                    # Add event to batch
                    batch.append(message.value)

                    # Check if batch is ready to process
                    batch_ready = (
                        len(batch) >= self.batch_size
                        or (time.time() - last_batch_time) >= self.batch_timeout
                    )

                    if batch_ready and batch:
                        # Process batch
                        processed_events = self.process_batch(batch)

                        # Send processed events
                        for event in processed_events:
                            self.send_event(
                                self.output_topic, event, key=event.get("user_id")
                            )

                        # Load to Snowflake
                        self.load_to_snowflake(processed_events)

                        # Call callback if provided
                        if callback:
                            callback(processed_events)

                        # Reset batch
                        batch = []
                        last_batch_time = time.time()

                except Exception as e:
                    # Send error event
                    error_event = {
                        "error": str(e),
                        "original_event": message.value,
                        "timestamp": datetime.now().isoformat(),
                    }
                    self.send_event(self.error_topic, error_event)
                    logger.error(f"Error processing message: {str(e)}")

        except Exception as e:
            handle_pipeline_error(
                e, {"step": "streaming", "error_type": type(e).__name__}
            )
        finally:
            self.stop_streaming()

    def stop_streaming(self):
        """Stop the streaming processor."""
        self.running = False

        if self.consumer:
            self.consumer.close()

        if self.producer:
            self.producer.close()

        if self.snowflake_manager:
            self.snowflake_manager.close()

        logger.info("Stopped streaming processor")

    def generate_sample_events(self, num_events: int = 10):
        """
        Generate sample events for testing.

        Args:
            num_events: Number of events to generate
        """
        shows = [
            "Stranger Things",
            "The Crown",
            "Breaking Bad",
            "Friends",
            "The Office",
        ]
        users = [f"user_{i:04d}" for i in range(1, 101)]

        for _ in range(num_events):
            event = WatchEvent(
                user_id=random.choice(users),
                show_name=random.choice(shows),
                watch_duration_minutes=random.uniform(20, 120),
                watch_date=datetime.now(),
            )

            self.send_event(self.input_topic, event.dict(), key=event.user_id)
            time.sleep(0.1)  # Small delay between events


class EventGenerator:
    """Generates realistic watch events for testing."""

    def __init__(self, kafka_bootstrap_servers: str = None):
        # Use environment variable or default
        if kafka_bootstrap_servers is None:
            import os

            kafka_bootstrap_servers = os.getenv(
                "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
            )
        self.producer = KafkaProducer(
            bootstrap_servers=kafka_bootstrap_servers,
            value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k else None,
        )

    def generate_realistic_events(self, duration_minutes: int = 60):
        """
        Generate realistic watch events over a time period.

        Args:
            duration_minutes: Duration to generate events for
        """
        import random
        from datetime import datetime, timedelta

        shows = [
            "Stranger Things",
            "The Crown",
            "Breaking Bad",
            "Friends",
            "The Office",
        ]
        users = [f"user_{i:04d}" for i in range(1, 101)]

        start_time = datetime.now()
        end_time = start_time + timedelta(minutes=duration_minutes)

        current_time = start_time

        while current_time < end_time:
            # Generate 1-5 events per minute
            num_events = random.randint(1, 5)

            for _ in range(num_events):
                event = WatchEvent(
                    user_id=random.choice(users),
                    show_name=random.choice(shows),
                    watch_duration_minutes=random.uniform(20, 120),
                    watch_date=current_time,
                )

                self.producer.send(
                    "watch_events", value=event.dict(), key=event.user_id
                )

            current_time += timedelta(minutes=1)
            time.sleep(1)  # Wait 1 second per minute

        self.producer.close()
