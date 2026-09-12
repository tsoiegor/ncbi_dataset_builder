"""Internal durable records for workspace execution."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any

from ..models import ProcessingUnit


@dataclass(frozen=True)
class UnitResources:
    """Hold the execution-owned minimum resources for one queue item.

    Args:
        cpus: Minimum CPUs allocated to the processor.
        memory_gb: Slurm memory reservation, or ``None`` for local execution.
        time_limit: Slurm worker time limit, or ``None`` for local execution.
    """

    cpus: int
    memory_gb: float | None = None
    time_limit: str | None = None


@dataclass(frozen=True)
class QueueItem:
    """Bind one processing unit to its durable workspace identity.

    Args:
        item_id: Filesystem-safe stable identifier.
        unit: Catalog-derived sample description.
        resources: Minimum resources derived from the execution system.
        genome_pin: Optional exact assembly accession.
        fingerprint: Semantic identity used for resume decisions.
    """

    item_id: str
    unit: ProcessingUnit
    resources: UnitResources
    genome_pin: str | None = None
    fingerprint: str = ""

    def to_dict(self) -> dict[str, Any]:
        """Serialize this queue item to JSON-compatible values."""

        return {
            "item_id": self.item_id,
            "unit": self.unit.to_dict(),
            "resources": asdict(self.resources),
            "genome_pin": self.genome_pin,
            "fingerprint": self.fingerprint,
        }

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> QueueItem:
        """Restore a queue item from serialized mapping *value*."""

        return cls(
            item_id=str(value["item_id"]),
            unit=ProcessingUnit.from_dict(value["unit"]),
            resources=UnitResources(**value["resources"]),
            genome_pin=value.get("genome_pin"),
            fingerprint=str(value.get("fingerprint", "")),
        )


@dataclass(frozen=True)
class ExecutionRecord:
    """Persist one automatic execution snapshot inside a workspace.

    Args:
        execution_id: Timestamp and content-hash identifier.
        created_at: UTC creation timestamp.
        query: Optional source NCBI query.
        group_by: Catalog entity represented by one queue item.
        items: Ordered sample queue.
        processor_identity: Stable processor identity.
        execution_type: Local, single-node Slurm, or distributed Slurm.
        execution_config: Serialized execution-system configuration.
        queue_config: Serialized sample-queue configuration.
        catalog_audit: Catalog operations preceding execution.
        metadata: Additional workspace provenance.
    """

    execution_id: str
    created_at: str
    query: str | None
    group_by: str
    items: tuple[QueueItem, ...]
    processor_identity: str
    execution_type: str
    execution_config: dict[str, Any]
    queue_config: dict[str, Any]
    catalog_audit: tuple[str, ...] = ()
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Serialize this execution record to JSON-compatible values."""

        return {
            "execution_id": self.execution_id,
            "created_at": self.created_at,
            "query": self.query,
            "group_by": self.group_by,
            "items": [item.to_dict() for item in self.items],
            "processor_identity": self.processor_identity,
            "execution_type": self.execution_type,
            "execution_config": self.execution_config,
            "queue_config": self.queue_config,
            "catalog_audit": list(self.catalog_audit),
            "metadata": self.metadata,
        }

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> ExecutionRecord:
        """Restore an execution record from serialized mapping *value*."""

        return cls(
            execution_id=str(value["execution_id"]),
            created_at=str(value["created_at"]),
            query=value.get("query"),
            group_by=str(value["group_by"]),
            items=tuple(QueueItem.from_dict(item) for item in value["items"]),
            processor_identity=str(value["processor_identity"]),
            execution_type=str(value["execution_type"]),
            execution_config=dict(value.get("execution_config", {})),
            queue_config=dict(value.get("queue_config", {})),
            catalog_audit=tuple(value.get("catalog_audit", ())),
            metadata=dict(value.get("metadata", {})),
        )
