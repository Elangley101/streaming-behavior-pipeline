"""
Monitoring and Observability Module
Provides comprehensive monitoring, metrics, and health checks for the Netflix pipeline.
"""

import logging
import time
from datetime import datetime
from typing import Dict, Any, Optional
from prometheus_client import (
    Counter,
    Histogram,
    Gauge,
    generate_latest,
    CONTENT_TYPE_LATEST,
)
from prometheus_client.exposition import start_http_server
import psutil
import json

# Configure structured logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("logs/pipeline.log"), logging.StreamHandler()],
)

logger = logging.getLogger(__name__)


class MetricsCollector:
    """Collects and exposes Prometheus metrics for the pipeline."""

    def __init__(self, port: int = 8001):
        self.port = port

        # API Metrics
        self.api_requests_total = Counter(
            "api_requests_total",
            "Total number of API requests",
            ["method", "endpoint", "status"],
        )

        self.api_request_duration = Histogram(
            "api_request_duration_seconds",
            "API request duration in seconds",
            ["method", "endpoint"],
        )

        # Pipeline Metrics
        self.pipeline_events_processed = Counter(
            "pipeline_events_processed_total",
            "Total number of events processed",
            ["status", "source"],
        )

        self.pipeline_processing_duration = Histogram(
            "pipeline_processing_duration_seconds",
            "Event processing duration in seconds",
            ["stage"],
        )

        self.pipeline_queue_size = Gauge(
            "pipeline_queue_size", "Current size of processing queue"
        )

        # Data Quality Metrics
        self.data_quality_score = Gauge(
            "data_quality_score", "Data quality score (0-100)", ["dataset"]
        )

        self.data_validation_errors = Counter(
            "data_validation_errors_total",
            "Total number of data validation errors",
            ["validation_type", "severity"],
        )

        # System Metrics
        self.system_memory_usage = Gauge(
            "system_memory_usage_bytes", "System memory usage in bytes"
        )

        self.system_cpu_usage = Gauge(
            "system_cpu_usage_percent", "System CPU usage percentage"
        )

        # Business Metrics
        self.active_users = Gauge("active_users_total", "Number of active users")

        self.content_engagement = Gauge(
            "content_engagement_score", "Content engagement score", ["content_type"]
        )

        # Start metrics server
        start_http_server(self.port)
        logger.info(f"Metrics server started on port {self.port}")


class HealthChecker:
    """Provides health check functionality for the pipeline."""

    def __init__(self):
        self.health_status = {
            "status": "healthy",
            "timestamp": datetime.utcnow().isoformat(),
            "version": "1.0.0",
            "checks": {},
        }

    def check_kafka_connection(self) -> Dict[str, Any]:
        """Check Kafka connectivity."""
        try:
            from kafka import KafkaProducer

            producer = KafkaProducer(bootstrap_servers=["localhost:9092"])
            producer.close()
            return {"status": "healthy", "message": "Kafka connection successful"}
        except Exception as e:
            return {
                "status": "unhealthy",
                "message": f"Kafka connection failed: {str(e)}",
            }

    def check_snowflake_connection(self) -> Dict[str, Any]:
        """Check Snowflake connectivity."""
        try:
            import snowflake.connector

            # Add your Snowflake connection check here
            return {"status": "healthy", "message": "Snowflake connection successful"}
        except Exception as e:
            return {
                "status": "unhealthy",
                "message": f"Snowflake connection failed: {str(e)}",
            }

    def check_system_resources(self) -> Dict[str, Any]:
        """Check system resource usage."""
        try:
            memory = psutil.virtual_memory()
            cpu = psutil.cpu_percent(interval=1)

            return {
                "status": "healthy" if memory.percent < 90 and cpu < 90 else "warning",
                "message": f"Memory: {memory.percent}%, CPU: {cpu}%",
                "details": {
                    "memory_percent": memory.percent,
                    "cpu_percent": cpu,
                    "disk_usage": psutil.disk_usage("/").percent,
                },
            }
        except Exception as e:
            return {"status": "unhealthy", "message": f"System check failed: {str(e)}"}

    def get_health_status(self) -> Dict[str, Any]:
        """Get comprehensive health status."""
        self.health_status["checks"] = {
            "kafka": self.check_kafka_connection(),
            "snowflake": self.check_snowflake_connection(),
            "system": self.check_system_resources(),
        }

        # Determine overall status
        all_healthy = all(
            check["status"] == "healthy"
            for check in self.health_status["checks"].values()
        )

        self.health_status["status"] = "healthy" if all_healthy else "unhealthy"
        self.health_status["timestamp"] = datetime.utcnow().isoformat()

        return self.health_status


class StructuredLogger:
    """Provides structured logging with JSON format."""

    def __init__(self, service_name: str):
        self.service_name = service_name
        self.logger = logging.getLogger(service_name)

    def log_event(self, event_type: str, message: str, **kwargs):
        """Log an event with structured data."""
        log_entry = {
            "timestamp": datetime.utcnow().isoformat(),
            "service": self.service_name,
            "event_type": event_type,
            "message": message,
            **kwargs,
        }

        self.logger.info(json.dumps(log_entry))

    def log_error(self, error: Exception, context: Optional[Dict[str, Any]] = None):
        """Log an error with structured data."""
        log_entry = {
            "timestamp": datetime.utcnow().isoformat(),
            "service": self.service_name,
            "event_type": "error",
            "error_type": type(error).__name__,
            "error_message": str(error),
            "context": context or {},
        }

        self.logger.error(json.dumps(log_entry))

    def log_metric(self, metric_name: str, value: float, **kwargs):
        """Log a custom metric."""
        log_entry = {
            "timestamp": datetime.utcnow().isoformat(),
            "service": self.service_name,
            "event_type": "metric",
            "metric_name": metric_name,
            "value": value,
            **kwargs,
        }

        self.logger.info(json.dumps(log_entry))


class PerformanceMonitor:
    """Monitors and tracks performance metrics."""

    def __init__(self, metrics_collector: MetricsCollector):
        self.metrics = metrics_collector
        self.start_times = {}

    def start_timer(self, operation: str):
        """Start timing an operation."""
        self.start_times[operation] = time.time()

    def end_timer(self, operation: str, labels: Optional[Dict[str, str]] = None):
        """End timing an operation and record the duration."""
        if operation in self.start_times:
            duration = time.time() - self.start_times[operation]
            self.metrics.pipeline_processing_duration.labels(stage=operation).observe(
                duration
            )
            del self.start_times[operation]

    def update_system_metrics(self):
        """Update system resource metrics."""
        memory = psutil.virtual_memory()
        cpu = psutil.cpu_percent(interval=1)

        self.metrics.system_memory_usage.set(memory.used)
        self.metrics.system_cpu_usage.set(cpu)

    def update_business_metrics(
        self, active_users: int, engagement_scores: Dict[str, float]
    ):
        """Update business metrics."""
        self.metrics.active_users.set(active_users)

        for content_type, score in engagement_scores.items():
            self.metrics.content_engagement.labels(content_type=content_type).set(score)


# Global instances
metrics_collector = MetricsCollector()
health_checker = HealthChecker()
performance_monitor = PerformanceMonitor(metrics_collector)


def get_metrics():
    """Get Prometheus metrics."""
    return generate_latest(), CONTENT_TYPE_LATEST


def get_health():
    """Get health status."""
    return health_checker.get_health_status()


def log_pipeline_event(event_type: str, message: str, **kwargs):
    """Log a pipeline event."""
    logger = StructuredLogger("pipeline")
    logger.log_event(event_type, message, **kwargs)


def log_api_request(method: str, endpoint: str, status: int, duration: float):
    """Log an API request."""
    metrics_collector.api_requests_total.labels(
        method=method, endpoint=endpoint, status=status
    ).inc()

    metrics_collector.api_request_duration.labels(
        method=method, endpoint=endpoint
    ).observe(duration)

    logger = StructuredLogger("api")
    logger.log_event(
        "api_request", f"{method} {endpoint}", status=status, duration=duration
    )


def log_data_quality_issue(validation_type: str, severity: str, details: str):
    """Log a data quality issue."""
    metrics_collector.data_validation_errors.labels(
        validation_type=validation_type, severity=severity
    ).inc()

    logger = StructuredLogger("data_quality")
    logger.log_event(
        "validation_error", details, validation_type=validation_type, severity=severity
    )
