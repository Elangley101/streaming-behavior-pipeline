import os
import logging
from typing import Dict, List, Optional, Any, TYPE_CHECKING, Generator
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from sqlalchemy import (
    create_engine,
    text,
    MetaData,
    Table,
    Column,
    Integer,
    String,
    Float,
    DateTime,
    Boolean,
)
from sqlalchemy.exc import SQLAlchemyError
from snowflake.connector import connect, SnowflakeConnection
from snowflake.connector.errors import ProgrammingError, DatabaseError
import json
from pathlib import Path
from contextlib import contextmanager

# Configuration imports
from config.snowflake_config import SNOWFLAKE_CONFIG
from config.config import PIPELINE_CONFIG

# Utility imports
from src.utils import PipelineError, handle_pipeline_error, setup_logging

# Try to import Snowflake-specific dependencies
try:
    from snowflake.sqlalchemy import URL

    SNOWFLAKE_AVAILABLE = True
except ImportError:
    SNOWFLAKE_AVAILABLE = False
    URL = None

# Try to import config with fallback
try:
    from config.snowflake_config import (
        CREATE_TABLE_STATEMENTS,
        SNOWFLAKE_CONFIG,
        TABLE_CONFIGS,
    )

    CONFIG_AVAILABLE = True
except ImportError:
    CONFIG_AVAILABLE = False
    CREATE_TABLE_STATEMENTS = {}
    SNOWFLAKE_CONFIG = {}
    TABLE_CONFIGS = {}

# Try to import utils with fallback
try:
    from src.utils import PipelineError, handle_pipeline_error, setup_logging

    UTILS_AVAILABLE = True
except ImportError:
    UTILS_AVAILABLE = False
    PipelineError = Exception

    def handle_pipeline_error(error, context):
        print(f"Pipeline error in {context.get('step', 'unknown step')}: {str(error)}")

    def setup_logging(name):
        def log(message):
            print(f"[{name}] {message}")

        return log


logger = setup_logging("snowflake_manager")

if TYPE_CHECKING:
    from sqlalchemy.engine import Engine


class SnowflakeManager:
    """Manages Snowflake connections and operations."""

    def __init__(self):
        """Initialize the Snowflake manager with connection configuration."""
        if not SNOWFLAKE_AVAILABLE:
            logger.warning("Snowflake SQLAlchemy not available - running in local mode")
            self.enabled = False
            return

        if not CONFIG_AVAILABLE:
            logger.warning(
                "Snowflake configuration not available - running in local mode"
            )
            self.enabled = False
            return

        # Check if we have valid credentials
        if (
            not SNOWFLAKE_CONFIG.get("account")
            or not SNOWFLAKE_CONFIG.get("user")
            or not SNOWFLAKE_CONFIG.get("password")
        ):
            logger.warning(
                "Snowflake credentials not configured - running in local mode"
            )
            self.enabled = False
            return

        try:
            self.config = SNOWFLAKE_CONFIG
            self.engine = self._create_engine()
            self._initialize_tables()
            self.enabled = True
            logger.info("Snowflake connection established successfully")
        except Exception as e:
            logger.warning(
                f"Failed to connect to Snowflake: {str(e)} - running in local mode"
            )
            self.enabled = False

    def _create_engine(self):
        """
        Create a SQLAlchemy engine for Snowflake.

        Returns:
            SQLAlchemy engine instance

        Raises:
            PipelineError: If engine creation fails
        """
        try:
            return create_engine(
                URL(
                    account=self.config["account"],
                    user=self.config["user"],
                    password=self.config["password"],
                    warehouse=self.config["warehouse"],
                    database=self.config["database"],
                    schema=self.config["schema"],
                    role=self.config["role"],
                ),
                pool_size=5,
                max_overflow=10,
                pool_timeout=30,
                pool_recycle=1800,
            )
        except Exception as e:
            handle_pipeline_error(
                e, {"step": "snowflake_connection", "error_type": type(e).__name__}
            )

    def _initialize_tables(self) -> None:
        """
        Initialize Snowflake tables if they don't exist.

        Raises:
            PipelineError: If table creation fails
        """
        try:
            with self.engine.connect() as conn:
                for table_name, create_statement in CREATE_TABLE_STATEMENTS.items():
                    conn.execute(
                        text(
                            create_statement.format(
                                database=self.config["database"],
                                schema=self.config["schema"],
                                table_name=TABLE_CONFIGS[table_name]["table_name"],
                            )
                        )
                    )
                    conn.commit()
            logger.info("Successfully initialized Snowflake tables")
        except Exception as e:
            handle_pipeline_error(
                e, {"step": "snowflake_init", "error_type": type(e).__name__}
            )

    @contextmanager
    def get_connection(self) -> Generator[Any, None, None]:
        """
        Get a Snowflake connection from the pool.

        Yields:
            Connection: Snowflake connection

        Raises:
            PipelineError: If connection fails
        """
        try:
            connection = self.engine.connect()
            yield connection
            connection.close()
        except Exception as e:
            handle_pipeline_error(
                e, {"step": "snowflake_connection", "error_type": type(e).__name__}
            )

    def load_dataframe(
        self, df: pd.DataFrame, table_name: str, if_exists: str = "append"
    ) -> None:
        """
        Load a DataFrame into a Snowflake table.

        Args:
            df: DataFrame to load
            table_name: Target table name
            if_exists: How to behave if the table exists

        Raises:
            PipelineError: If loading fails
        """
        if not getattr(self, "enabled", False):
            logger.info(
                f"Snowflake disabled - saving {len(df)} rows to local file for {table_name}"
            )
            # Save to local parquet file instead
            output_path = f"data/processed/{table_name}.parquet"
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            df.to_parquet(output_path, index=False)
            return

        try:
            # Add metadata columns
            df["created_at"] = pd.Timestamp.now()
            df["updated_at"] = pd.Timestamp.now()

            # Load to Snowflake
            df.to_sql(
                name=TABLE_CONFIGS[table_name]["table_name"],
                con=self.engine,
                if_exists=if_exists,
                index=False,
                method="multi",
                chunksize=10000,
            )

            logger.info(f"Successfully loaded {len(df)} rows to {table_name}")

        except Exception as e:
            handle_pipeline_error(
                e,
                {
                    "step": "snowflake_load",
                    "table": table_name,
                    "error_type": type(e).__name__,
                },
            )

    def execute_query(self, query: str, params: Optional[Dict] = None) -> pd.DataFrame:
        """
        Execute a SQL query and return results as a DataFrame.

        Args:
            query: SQL query to execute
            params: Optional query parameters

        Returns:
            pd.DataFrame: Query results

        Raises:
            PipelineError: If query execution fails
        """
        if not getattr(self, "enabled", False):
            logger.info("Snowflake disabled - returning empty DataFrame")
            return pd.DataFrame()

        try:
            with self.get_connection() as conn:
                result = conn.execute(text(query), params or {})
                return pd.DataFrame(result.fetchall(), columns=result.keys())
        except Exception as e:
            handle_pipeline_error(
                e,
                {
                    "step": "snowflake_query",
                    "query": query,
                    "error_type": type(e).__name__,
                },
            )

    def get_table_stats(self, table_name: str) -> Dict[str, Any]:
        """
        Get statistics about a table.

        Args:
            table_name: Name of the table

        Returns:
            Dict[str, Any]: Table statistics

        Raises:
            PipelineError: If getting stats fails
        """
        if not getattr(self, "enabled", False):
            logger.info(f"Snowflake disabled - returning mock stats for {table_name}")
            return {
                "row_count": 0,
                "unique_users": 0,
                "avg_duration": 0.0,
                "latest_watch": None,
            }

        try:
            query = f"""
            SELECT
                COUNT(*) as row_count,
                COUNT(DISTINCT user_id) as unique_users,
                AVG(watch_duration_minutes) as avg_duration,
                MAX(watch_date) as latest_watch
            FROM {self.config["database"]}.{self.config["schema"]}.{TABLE_CONFIGS[table_name]["table_name"]}
            """

            return self.execute_query(query).iloc[0].to_dict()

        except Exception as e:
            handle_pipeline_error(
                e,
                {
                    "step": "snowflake_stats",
                    "table": table_name,
                    "error_type": type(e).__name__,
                },
            )

    def close(self) -> None:
        """Close the Snowflake connection."""
        try:
            if hasattr(self, "engine"):
                self.engine.dispose()
                logger.info("Snowflake connection closed")
        except Exception as e:
            logger.error(f"Error closing Snowflake connection: {str(e)}")
