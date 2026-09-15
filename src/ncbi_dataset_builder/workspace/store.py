"""Workspace layout and durable execution records."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, ClassVar

from ..execution.records import ExecutionRecord
from ..support.util import atomic_write_json, exclusive_file_lock, read_json, utc_timestamp

WORKSPACE_SCHEMA_VERSION = 1


@dataclass(frozen=True)
class WorkspaceConfig:
    """Describe the stable meaning and visible layout of one workspace.

    Args:
        schema_version: Version of the workspace metadata schema.
        created_at: UTC timestamp when the workspace was initialized.
        group_by: Catalog entity represented by one processing unit.
        description_profile: Metadata projection used for descriptions.
        genome_policy: Serialized genome-selection policy.
        directories: Human-readable role of each workspace directory.
    """

    schema_version: int
    created_at: str
    group_by: str
    description_profile: str
    genome_policy: dict[str, Any] = field(default_factory=dict)
    directories: dict[str, str] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Serialize this configuration to JSON-compatible values."""

        return asdict(self)

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> WorkspaceConfig:
        """Restore a workspace configuration from *value*."""

        return cls(
            schema_version=int(value["schema_version"]),
            created_at=str(value["created_at"]),
            group_by=str(value["group_by"]),
            description_profile=str(value.get("description_profile", "training")),
            genome_policy=dict(value.get("genome_policy", {})),
            directories=dict(value.get("directories", {})),
        )


class WorkspaceStore:
    """Create and maintain the package-owned workspace layout."""

    DIRECTORIES: ClassVar[dict[str, str]] = {
        "bigWig": "published experiment BigWig files",
        "descriptions": "published experiment descriptions",
        "genomes": "published genome FASTA files",
        "catalogs": "cached and selected run catalogs",
        "metadata": "normalized NCBI metadata",
        "metadata_cache": "reusable NCBI response cache",
        "fastq": "bounded provider-owned sample inputs",
        "work": "processor and download intermediates",
        "outputs": "processor outputs by sample",
        "state": "durable per-sample state",
        "executions": "automatic execution snapshots",
        "slurm": "generated scheduler scripts",
        "logs": "sample and scheduler logs",
    }

    def __init__(self, root: Path) -> None:
        """Create a workspace rooted at *root*."""

        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        for name in self.DIRECTORIES:
            (self.root / name).mkdir(parents=True, exist_ok=True)
        self.config_path = self.root / "workspace.json"
        self.manifest_path = self.root / "manifest.json"
        self.executions = self.root / "executions"

    def configure(
        self,
        *,
        group_by: str,
        description_profile: str,
        genome_policy: dict[str, Any],
    ) -> WorkspaceConfig:
        """Create or validate stable workspace semantics.

        Args:
            group_by: Catalog entity represented by one processing unit.
            description_profile: Metadata projection used for descriptions.
            genome_policy: Serialized genome-selection policy.

        Stable semantic settings cannot change after sample state exists.
        """

        requested = WorkspaceConfig(
            schema_version=WORKSPACE_SCHEMA_VERSION,
            created_at=utc_timestamp(),
            group_by=group_by,
            description_profile=description_profile,
            genome_policy=dict(genome_policy),
            directories=dict(self.DIRECTORIES),
        )
        lock = self.root / "state" / "workspace-config.lock"
        with exclusive_file_lock(lock):
            if not self.config_path.is_file():
                atomic_write_json(self.config_path, requested.to_dict())
                return requested
            current = WorkspaceConfig.from_dict(read_json(self.config_path))
            if current.schema_version != WORKSPACE_SCHEMA_VERSION:
                raise ValueError(
                    f"Unsupported workspace schema {current.schema_version}; "
                    f"expected {WORKSPACE_SCHEMA_VERSION}"
                )
            changed = {
                name: (getattr(current, name), getattr(requested, name))
                for name in ("group_by", "description_profile", "genome_policy")
                if getattr(current, name) != getattr(requested, name)
            }
            has_state = any((self.root / "state" / "units").glob("*.json"))
            if changed and has_state:
                details = ", ".join(
                    f"{name}: {old!r} -> {new!r}" for name, (old, new) in changed.items()
                )
                raise ValueError(
                    "Workspace semantic configuration cannot change after processing: " + details
                )
            if changed or current.directories != requested.directories:
                updated = WorkspaceConfig(
                    schema_version=current.schema_version,
                    created_at=current.created_at,
                    group_by=requested.group_by,
                    description_profile=requested.description_profile,
                    genome_policy=requested.genome_policy,
                    directories=requested.directories,
                )
                atomic_write_json(self.config_path, updated.to_dict())
                return updated
            return current

    def save_execution(self, execution: ExecutionRecord) -> Path:
        """Persist automatic *execution* and return its JSON path."""

        path = self.executions / f"{execution.execution_id}.json"
        atomic_write_json(path, execution.to_dict())
        return path

    def load_execution(self, path_or_id: str | Path) -> ExecutionRecord:
        """Load an execution by identifier or JSON *path_or_id*."""

        candidate = Path(path_or_id)
        path = candidate if candidate.is_file() else self.executions / f"{path_or_id}.json"
        if not path.is_file():
            raise FileNotFoundError(f"Execution record does not exist: {path}")
        return ExecutionRecord.from_dict(read_json(path))

    def latest_execution(self) -> ExecutionRecord:
        """Load the most recently written execution record."""

        records = sorted(self.executions.glob("*.json"), key=lambda path: path.stat().st_mtime)
        if not records:
            raise FileNotFoundError("Workspace has no execution records")
        return ExecutionRecord.from_dict(read_json(records[-1]))

    def sync_manifest(
        self,
        execution: ExecutionRecord,
        states: dict[str, dict[str, Any] | None],
    ) -> Path:
        """Write the current workspace manifest for *execution* and *states*."""

        units: dict[str, Any] = {}
        for item in execution.items:
            state = states.get(item.item_id)
            units[item.item_id] = {
                "unit": item.unit.to_dict(),
                "fingerprint": item.fingerprint,
                "status": (state or {}).get("status", "pending"),
                "state": state,
            }
        atomic_write_json(
            self.manifest_path,
            {
                "updated_at": utc_timestamp(),
                "latest_execution_id": execution.execution_id,
                "group_by": execution.group_by,
                "processor_identity": execution.processor_identity,
                "units": units,
            },
        )
        return self.manifest_path
