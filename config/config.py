import os
from pathlib import Path
from typing import Dict, Any

# Base paths
BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / "data"
RAW_DATA_DIR = DATA_DIR / "raw"
PROCESSED_DATA_DIR = DATA_DIR / "processed"

# Create directories if they don't exist
RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)
PROCESSED_DATA_DIR.mkdir(parents=True, exist_ok=True)

# Pipeline configuration
PIPELINE_CONFIG: Dict[str, Any] = {
    "raw_data_path": str(RAW_DATA_DIR / "watch_logs.csv"),
    "processed_data_path": str(PROCESSED_DATA_DIR / "processed_watch_data.parquet"),
    "binge_watch_threshold_minutes": 120,  
    "completion_rate_threshold": 0.8,  
    "batch_size": 10000,
    "log_level": os.getenv("LOG_LEVEL", "INFO"),
}

# Data validation rules
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

# Logging configuration
LOGGING_CONFIG = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "standard": {
            "format": "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
        },
    },
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            "level": "INFO",
            "formatter": "standard",
            "stream": "ext://sys.stdout",
        },
        "file": {
            "class": "logging.FileHandler",
            "level": "DEBUG",
            "formatter": "standard",
            "filename": str(BASE_DIR / "logs" / "pipeline.log"),
            "mode": "a",
        },
    },
    "loggers": {
        "": {  # root logger
            "handlers": ["console", "file"],
            "level": "INFO",
            "propagate": True
        },
    },
} 