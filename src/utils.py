import logging
import logging.config
from datetime import datetime
from typing import Any, Dict, Optional
import os

import pandas as pd

# Try to import config with fallback
try:
    from config.config import LOGGING_CONFIG, VALIDATION_RULES

    CONFIG_AVAILABLE = True
except ImportError:
    CONFIG_AVAILABLE = False
    # Fallback configurations
    LOGGING_CONFIG = {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "standard": {"format": "%(asctime)s [%(levelname)s] %(name)s: %(message)s"},
        },
        "handlers": {
            "console": {
                "class": "logging.StreamHandler",
                "level": "INFO",
                "formatter": "standard",
                "stream": "ext://sys.stdout",
            },
        },
        "loggers": {
            "": {  # root logger
                "handlers": ["console"],
                "level": "INFO",
                "propagate": True,
            },
        },
    }

    VALIDATION_RULES = {
        "user_id": {
            "type": "string",
            "required": True,
            "min_length": 1,
        },
        "show_name": {
            "type": "string",
            "required": True,
            "min_length": 1,
        },
        "watch_duration_minutes": {
            "type": "number",
            "required": True,
            "minimum": 0,
            "maximum": 1440,  # 24 hours
        },
        "watch_date": {
            "type": "string",
            "required": True,
            "format": "datetime",
        },
    }

# Configure logging
try:
    logging.config.dictConfig(LOGGING_CONFIG)
except Exception as e:
    # Fallback to basic logging if config fails
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    )


class PipelineError(Exception):
    """Custom exception for pipeline-specific errors."""

    pass


def setup_logging(name: str) -> logging.Logger:
    """Set up and return a logger instance."""
    return logging.getLogger(name)


def validate_dataframe(df: pd.DataFrame) -> None:
    """
    Validate DataFrame against defined rules.

    Args:
        df: DataFrame to validate

    Raises:
        PipelineError: If validation fails
    """
    if not CONFIG_AVAILABLE:
        # Skip validation if config not available
        return

    for column, rules in VALIDATION_RULES.items():
        if rules["required"] and column not in df.columns:
            raise PipelineError(f"Required column '{column}' not found in DataFrame")

        if column in df.columns:
            # Type validation
            if rules["type"] == "string":
                # Convert to string if not already
                if df[column].dtype != "object":
                    df[column] = df[column].astype(str)
                # Additional check for string-like content
                if not all(isinstance(val, str) for val in df[column].dropna()):
                    df[column] = df[column].astype(str)
            elif rules["type"] == "number":
                if not pd.api.types.is_numeric_dtype(df[column]):
                    raise PipelineError(f"Column '{column}' must be numeric type")

            # Length validation for strings
            if rules["type"] == "string" and "min_length" in rules:
                if df[column].str.len().min() < rules["min_length"]:
                    raise PipelineError(
                        f"Column '{column}' contains values shorter than minimum length"
                    )

            # Range validation for numbers
            if rules["type"] == "number":
                if "minimum" in rules and df[column].min() < rules["minimum"]:
                    raise PipelineError(
                        f"Column '{column}' contains values below minimum"
                    )
                if "maximum" in rules and df[column].max() > rules["maximum"]:
                    raise PipelineError(
                        f"Column '{column}' contains values above maximum"
                    )

            # Date format validation
            if rules["type"] == "string" and rules.get("format") == "datetime":
                try:
                    pd.to_datetime(df[column])
                except ValueError:
                    raise PipelineError(
                        f"Column '{column}' contains invalid datetime values"
                    )


def log_pipeline_step(
    step_name: str, start_time: Optional[datetime] = None
) -> datetime:
    """
    Log the start or completion of a pipeline step with timing information.

    Args:
        step_name: Name of the pipeline step
        start_time: Optional start time for calculating duration

    Returns:
        Current timestamp
    """
    current_time = datetime.now()
    utils_logger = setup_logging("utils")

    if start_time:
        duration = (current_time - start_time).total_seconds()
        utils_logger.info(f"Completed {step_name} in {duration:.2f} seconds")
    else:
        utils_logger.info(f"Starting {step_name}")

    return current_time


def handle_pipeline_error(error: Exception, context: Dict[str, Any]) -> None:
    """
    Handle pipeline errors with proper logging and context.

    Args:
        error: The exception that occurred
        context: Additional context about the error
    """
    error_msg = f"Pipeline error in {context.get('step', 'unknown step')}: {str(error)}"
    utils_logger = setup_logging("utils")
    utils_logger.error(error_msg, extra=context)
    raise PipelineError(error_msg) from error
