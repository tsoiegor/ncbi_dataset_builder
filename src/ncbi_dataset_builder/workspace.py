from __future__ import annotations

from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, ClassVar

from .models import WorkspaceJob
from .util import atomic_write_json, exclusive_file_lock, read_json, utc_timestamp

WORKSPACE_SCHEMA_VERSION = 1


@dataclass(frozen=True)
class WorkspaceConfig:
    """Describe the stable meaning and visible layout of one workspace.

    Args:
        schema_version: Version of the workspace metadata schema.
        created_at: UTC timestamp when the workspace was initialized.
        group_by: Catalog entity represented by one processing unit.
        description_profile: Metadata projection used for model descriptions.
        genome_policy: Serialized genome-selection policy.
        directories: Human-readable role of every top-level data directory.
    """

    schema_version: int
    created_at: str
    group_by: str
    description_profile: str
    genome_policy: dict[str, Any] = field(default_factory=dict)
    directories: dict[str, str] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Serialize this workspace config to JSON-compatible values."""

        return asdict(self)

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> WorkspaceConfig:
        """Restore a workspace config from serialized mapping *value*."""

        return cls(
            schema_version=int(value["schema_version"]),
            created_at=str(value["created_at"]),
            group_by=str(value["group_by"]),
            description_profile=str(value.get("description_profile", "training")),
            genome_policy=dict(value.get("genome_policy", {})),
            directories=dict(value.get("directories", {})),
        )


class WorkspaceStore:
    """Persist workspace configuration, job snapshots, and the unit manifest."""

    DIRECTORIES: ClassVar[dict[str, str]] = {
        "bigWig": "final experiment BigWig files",
        "descriptions": "final experiment training descriptions",
        "genomes": "final species genome FASTA files",
        "catalogs": "cached and selected run catalogs",
        "metadata": "normalized NCBI metadata",
        "metadata_cache": "reusable NCBI response and bundle cache",
        "fastq": "bounded unit-local staged FASTQ data",
        "work": "processor and download intermediates",
        "outputs": "generic processor outputs",
        "state": "durable unit and batch state",
        "jobs": "automatically generated execution snapshots",
        "slurm": "generated scheduler scripts",
        "logs": "unit and scheduler logs",
    }

    def __init__(self, root: Path) -> None:
        """Create a workspace store rooted at *root* and its visible directories."""

        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        for name in self.DIRECTORIES:
            (self.root / name).mkdir(parents=True, exist_ok=True)
        self.config_path = self.root / "workspace.json"
        self.manifest_path = self.root / "manifest.json"
        self.jobs = self.root / "jobs"

    def configure(
        self,
        *,
        group_by: str,
        description_profile: str,
        genome_policy: dict[str, Any],
    ) -> WorkspaceConfig:
        """Create or validate config for *group_by*, *description_profile*, and *genome_policy*.

        Stable semantic settings cannot change after unit state exists because
        doing so would make one workspace represent incompatible datasets.
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

    def save_job(self, job: WorkspaceJob) -> Path:
        """Atomically save *job*, update the latest pointer, and return its path."""

        path = self.jobs / f"{job.job_id}.json"
        atomic_write_json(path, job.to_dict())
        atomic_write_json(
            self.jobs / "latest.json",
            {"job_id": job.job_id, "path": str(path), "updated_at": utc_timestamp()},
        )
        return path

    def load_job(self, path_or_id: str | Path) -> WorkspaceJob:
        """Load a job identified by *path_or_id* from this workspace."""

        candidate = Path(path_or_id)
        path = candidate if candidate.is_file() else self.jobs / f"{candidate.name}.json"
        if not path.is_file():
            raise FileNotFoundError(f"Workspace job does not exist: {path_or_id}")
        return WorkspaceJob.from_dict(read_json(path))

    def latest_job(self) -> WorkspaceJob:
        """Load the most recently generated workspace job snapshot."""

        pointer = self.jobs / "latest.json"
        if not pointer.is_file():
            raise FileNotFoundError(f"Workspace has no generated jobs: {self.jobs}")
        return self.load_job(str(read_json(pointer)["job_id"]))

    def sync_manifest(self, job: WorkspaceJob, states: dict[str, dict[str, Any] | None]) -> Path:
        """Merge *job* and current unit *states* into the root manifest."""

        lock = self.root / "state" / "workspace-manifest.lock"
        with exclusive_file_lock(lock):
            manifest = (
                read_json(self.manifest_path)
                if self.manifest_path.is_file()
                else {"schema_version": 1, "units": {}, "dataset": {}}
            )
            units = manifest.setdefault("units", {})
            requested = {task.task_id for task in job.tasks}
            for record in units.values():
                if isinstance(record, dict):
                    record["requested_by_latest_job"] = False
            for task in job.tasks:
                state = states.get(task.task_id) or {}
                same_work = state.get("fingerprint") == task.fingerprint
                units[task.task_id] = {
                    "fingerprint": task.fingerprint,
                    "status": state.get("status", "pending") if same_work else "pending",
                    "job_id": state.get("job_id", job.job_id) if same_work else job.job_id,
                    "requested_by_latest_job": task.task_id in requested,
                    "unit": task.unit.to_dict(),
                    "result": state.get("result") if same_work else None,
                    "error": state.get("error") if same_work else None,
                    "updated_at": state.get("finished_at") or state.get("started_at"),
                }
            manifest.update(
                {
                    "schema_version": 1,
                    "updated_at": utc_timestamp(),
                    "latest_job_id": job.job_id,
                    "group_by": job.group_by,
                    "processor_identity": job.processor_identity,
                    "units": units,
                }
            )
            atomic_write_json(self.manifest_path, manifest)
        return self.manifest_path
