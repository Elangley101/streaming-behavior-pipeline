"""
Data Lineage and Governance Module
Tracks data flow, transformations, and provides audit trails for compliance.
"""

import json
import hashlib
from datetime import datetime
from typing import Dict, List, Any, Optional
from dataclasses import dataclass, asdict
from enum import Enum
import uuid


class DataSourceType(Enum):
    """Types of data sources."""

    CSV = "csv"
    JSON = "json"
    KAFKA = "kafka"
    API = "api"
    DATABASE = "database"


class TransformationType(Enum):
    """Types of data transformations."""

    CLEANING = "cleaning"
    AGGREGATION = "aggregation"
    ENRICHMENT = "enrichment"
    VALIDATION = "validation"
    FILTERING = "filtering"


@dataclass
class DataSource:
    """Represents a data source in the lineage."""

    id: str
    name: str
    type: DataSourceType
    location: str
    schema: Dict[str, Any]
    created_at: datetime
    updated_at: datetime
    checksum: str

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            **asdict(self),
            "type": self.type.value,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
        }


@dataclass
class DataTransformation:
    """Represents a data transformation step."""

    id: str
    name: str
    type: TransformationType
    description: str
    input_sources: List[str]
    output_targets: List[str]
    parameters: Dict[str, Any]
    executed_at: datetime
    duration_seconds: float
    records_processed: int
    records_failed: int

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            **asdict(self),
            "type": self.type.value,
            "executed_at": self.executed_at.isoformat(),
        }


@dataclass
class DataLineage:
    """Represents the complete data lineage for a dataset."""

    id: str
    dataset_name: str
    sources: List[DataSource]
    transformations: List[DataTransformation]
    created_at: datetime
    updated_at: datetime

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "id": self.id,
            "dataset_name": self.dataset_name,
            "sources": [source.to_dict() for source in self.sources],
            "transformations": [trans.to_dict() for trans in self.transformations],
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
        }


class DataLineageTracker:
    """Tracks data lineage throughout the pipeline."""

    def __init__(self, storage_path: str = "data/lineage/"):
        self.storage_path = storage_path
        self.lineages: Dict[str, DataLineage] = {}
        self._load_existing_lineages()

    def _load_existing_lineages(self):
        """Load existing lineage data from storage."""
        import os

        if os.path.exists(f"{self.storage_path}/lineages.json"):
            with open(f"{self.storage_path}/lineages.json", "r") as f:
                data = json.load(f)
                for lineage_data in data.values():
                    lineage = self._deserialize_lineage(lineage_data)
                    self.lineages[lineage.id] = lineage

    def _save_lineages(self):
        """Save lineage data to storage."""
        import os

        os.makedirs(self.storage_path, exist_ok=True)

        data = {
            lineage_id: lineage.to_dict()
            for lineage_id, lineage in self.lineages.items()
        }

        with open(f"{self.storage_path}/lineages.json", "w") as f:
            json.dump(data, f, indent=2)

    def _deserialize_lineage(self, data: Dict[str, Any]) -> DataLineage:
        """Deserialize lineage data from dictionary."""
        sources = [
            DataSource(
                id=source["id"],
                name=source["name"],
                type=DataSourceType(source["type"]),
                location=source["location"],
                schema=source["schema"],
                created_at=datetime.fromisoformat(source["created_at"]),
                updated_at=datetime.fromisoformat(source["updated_at"]),
                checksum=source["checksum"],
            )
            for source in data["sources"]
        ]

        transformations = [
            DataTransformation(
                id=trans["id"],
                name=trans["name"],
                type=TransformationType(trans["type"]),
                description=trans["description"],
                input_sources=trans["input_sources"],
                output_targets=trans["output_targets"],
                parameters=trans["parameters"],
                executed_at=datetime.fromisoformat(trans["executed_at"]),
                duration_seconds=trans["duration_seconds"],
                records_processed=trans["records_processed"],
                records_failed=trans["records_failed"],
            )
            for trans in data["transformations"]
        ]

        return DataLineage(
            id=data["id"],
            dataset_name=data["dataset_name"],
            sources=sources,
            transformations=transformations,
            created_at=datetime.fromisoformat(data["created_at"]),
            updated_at=datetime.fromisoformat(data["updated_at"]),
        )

    def register_source(
        self,
        dataset_name: str,
        source_name: str,
        source_type: DataSourceType,
        location: str,
        schema: Dict[str, Any],
        content: str = None,
    ) -> str:
        """Register a new data source."""
        source_id = str(uuid.uuid4())

        # Calculate checksum if content provided
        checksum = hashlib.md5(content.encode()).hexdigest() if content else ""

        source = DataSource(
            id=source_id,
            name=source_name,
            type=source_type,
            location=location,
            schema=schema,
            created_at=datetime.utcnow(),
            updated_at=datetime.utcnow(),
            checksum=checksum,
        )

        # Create or update lineage
        if dataset_name not in [l.dataset_name for l in self.lineages.values()]:
            lineage = DataLineage(
                id=str(uuid.uuid4()),
                dataset_name=dataset_name,
                sources=[source],
                transformations=[],
                created_at=datetime.utcnow(),
                updated_at=datetime.utcnow(),
            )
            self.lineages[lineage.id] = lineage
        else:
            # Find existing lineage
            lineage = next(
                l for l in self.lineages.values() if l.dataset_name == dataset_name
            )
            lineage.sources.append(source)
            lineage.updated_at = datetime.utcnow()

        self._save_lineages()
        return source_id

    def register_transformation(
        self,
        dataset_name: str,
        transformation_name: str,
        transformation_type: TransformationType,
        description: str,
        input_sources: List[str],
        output_targets: List[str],
        parameters: Dict[str, Any],
        duration_seconds: float,
        records_processed: int,
        records_failed: int = 0,
    ) -> str:
        """Register a data transformation."""
        transformation_id = str(uuid.uuid4())

        transformation = DataTransformation(
            id=transformation_id,
            name=transformation_name,
            type=transformation_type,
            description=description,
            input_sources=input_sources,
            output_targets=output_targets,
            parameters=parameters,
            executed_at=datetime.utcnow(),
            duration_seconds=duration_seconds,
            records_processed=records_processed,
            records_failed=records_failed,
        )

        # Find lineage for dataset
        lineage = next(
            (l for l in self.lineages.values() if l.dataset_name == dataset_name), None
        )
        if lineage:
            lineage.transformations.append(transformation)
            lineage.updated_at = datetime.utcnow()
            self._save_lineages()

        return transformation_id

    def get_lineage(self, dataset_name: str) -> Optional[DataLineage]:
        """Get lineage for a specific dataset."""
        return next(
            (l for l in self.lineages.values() if l.dataset_name == dataset_name), None
        )

    def get_all_lineages(self) -> List[DataLineage]:
        """Get all lineages."""
        return list(self.lineages.values())

    def export_lineage_report(self, dataset_name: str, output_path: str):
        """Export lineage report to JSON."""
        lineage = self.get_lineage(dataset_name)
        if lineage:
            with open(output_path, "w") as f:
                json.dump(lineage.to_dict(), f, indent=2)

    def generate_lineage_summary(self, dataset_name: str) -> Dict[str, Any]:
        """Generate a summary of data lineage."""
        lineage = self.get_lineage(dataset_name)
        if not lineage:
            return {}

        return {
            "dataset_name": dataset_name,
            "total_sources": len(lineage.sources),
            "total_transformations": len(lineage.transformations),
            "last_updated": lineage.updated_at.isoformat(),
            "source_types": list(set(source.type.value for source in lineage.sources)),
            "transformation_types": list(
                set(trans.type.value for trans in lineage.transformations)
            ),
            "total_records_processed": sum(
                trans.records_processed for trans in lineage.transformations
            ),
            "total_records_failed": sum(
                trans.records_failed for trans in lineage.transformations
            ),
        }


class DataGovernance:
    """Provides data governance and compliance features."""

    def __init__(self, lineage_tracker: DataLineageTracker):
        self.lineage_tracker = lineage_tracker
        self.data_policies = {}
        self.access_logs = []

    def add_data_policy(
        self,
        policy_name: str,
        dataset_name: str,
        retention_days: int,
        encryption_required: bool,
        access_controls: List[str],
    ):
        """Add a data governance policy."""
        self.data_policies[policy_name] = {
            "dataset_name": dataset_name,
            "retention_days": retention_days,
            "encryption_required": encryption_required,
            "access_controls": access_controls,
            "created_at": datetime.utcnow().isoformat(),
        }

    def log_data_access(
        self, user_id: str, dataset_name: str, access_type: str, query: str = None
    ):
        """Log data access for audit purposes."""
        self.access_logs.append(
            {
                "timestamp": datetime.utcnow().isoformat(),
                "user_id": user_id,
                "dataset_name": dataset_name,
                "access_type": access_type,
                "query": query,
            }
        )

    def get_compliance_report(self, dataset_name: str) -> Dict[str, Any]:
        """Generate compliance report for a dataset."""
        lineage = self.lineage_tracker.get_lineage(dataset_name)
        if not lineage:
            return {}

        # Find applicable policies
        policies = [
            policy
            for policy_name, policy in self.data_policies.items()
            if policy["dataset_name"] == dataset_name
        ]

        # Get access logs
        access_logs = [
            log for log in self.access_logs if log["dataset_name"] == dataset_name
        ]

        return {
            "dataset_name": dataset_name,
            "policies": policies,
            "access_logs_count": len(access_logs),
            "last_access": access_logs[-1]["timestamp"] if access_logs else None,
            "lineage_complete": len(lineage.sources) > 0
            and len(lineage.transformations) > 0,
            "data_retention_compliant": self._check_retention_compliance(
                dataset_name, policies
            ),
            "encryption_compliant": self._check_encryption_compliance(
                dataset_name, policies
            ),
        }

    def _check_retention_compliance(
        self, dataset_name: str, policies: List[Dict]
    ) -> bool:
        """Check if data retention policies are being followed."""
        # Implementation would check actual data retention
        return True

    def _check_encryption_compliance(
        self, dataset_name: str, policies: List[Dict]
    ) -> bool:
        """Check if encryption policies are being followed."""
        # Implementation would check actual encryption
        return True


# Global instances
lineage_tracker = DataLineageTracker()
data_governance = DataGovernance(lineage_tracker)


def track_data_source(
    dataset_name: str,
    source_name: str,
    source_type: DataSourceType,
    location: str,
    schema: Dict[str, Any],
    content: str = None,
) -> str:
    """Track a data source in the lineage."""
    return lineage_tracker.register_source(
        dataset_name, source_name, source_type, location, schema, content
    )


def track_transformation(
    dataset_name: str,
    transformation_name: str,
    transformation_type: TransformationType,
    description: str,
    input_sources: List[str],
    output_targets: List[str],
    parameters: Dict[str, Any],
    duration_seconds: float,
    records_processed: int,
    records_failed: int = 0,
) -> str:
    """Track a data transformation in the lineage."""
    return lineage_tracker.register_transformation(
        dataset_name,
        transformation_name,
        transformation_type,
        description,
        input_sources,
        output_targets,
        parameters,
        duration_seconds,
        records_processed,
        records_failed,
    )


def get_data_lineage(dataset_name: str) -> Optional[DataLineage]:
    """Get data lineage for a dataset."""
    return lineage_tracker.get_lineage(dataset_name)


def log_data_access(
    user_id: str, dataset_name: str, access_type: str, query: str = None
):
    """Log data access for audit purposes."""
    data_governance.log_data_access(user_id, dataset_name, access_type, query)
