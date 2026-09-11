from __future__ import annotations

import shutil
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Literal

from .util import (
    atomic_write_json,
    bytes_to_gb,
    read_json,
    sanitize_identifier,
    utc_timestamp,
)

BatchStatus = Literal[
    "pending",
    "streaming",
    "staging",
    "ready",
    "processing",
    "completed",
    "partially_failed",
    "cleaned",
]
CleanupPolicy = Literal["after_success", "never"]


def path_size_gb(path: Path) -> float:
    """Return the recursive regular-file size below *path* in decimal GB."""

    if path.is_file():
        return bytes_to_gb(path.stat().st_size)
    if not path.exists():
        return 0.0
    return sum(bytes_to_gb(item.stat().st_size) for item in path.rglob("*") if item.is_file())


def paths_size_gb(paths: tuple[Path, ...] | list[Path]) -> float:
    """Return the non-overlapping recursive size of *paths* in decimal GB."""

    resolved: list[Path] = []
    for path in sorted({item.resolve() for item in paths}, key=lambda item: len(item.parts)):
        if not any(path == parent or path.is_relative_to(parent) for parent in resolved):
            resolved.append(path)
    return sum(path_size_gb(path) for path in resolved)


def free_space_gb(path: Path) -> float:
    """Return available storage at *path* in decimal GB."""

    path.mkdir(parents=True, exist_ok=True)
    return bytes_to_gb(shutil.disk_usage(path).free)


def remove_owned_roots(paths: tuple[Path, ...], *, allowed_root: Path) -> tuple[Path, ...]:
    """Remove explicit *paths* only when each lies safely below *allowed_root*.

    Returned paths are the roots that existed and were removed. Nested roots
    are collapsed before deletion.
    """

    boundary = allowed_root.resolve()
    candidates: list[Path] = []
    for raw in sorted({path.resolve() for path in paths}, key=lambda item: len(item.parts)):
        if raw == boundary or not raw.is_relative_to(boundary):
            raise ValueError(f"Refuse to clean path outside the FASTQ cache: {raw}")
        if any(raw == parent or raw.is_relative_to(parent) for parent in candidates):
            continue
        candidates.append(raw)
    removed: list[Path] = []
    for path in candidates:
        if path.is_dir():
            shutil.rmtree(path)
            removed.append(path)
        elif path.exists():
            path.unlink()
            removed.append(path)
    return tuple(removed)


@dataclass(frozen=True)
class PipelinePolicy:
    """Control unit-level streaming, storage checks, cleanup, and logs.

    Args:
        prefetch_batches: Number of raw-data windows allowed ahead of the
            currently processing window.
        max_staged_gb: Optional maximum estimated resident input and processing
            storage in GB.
        minimum_free_gb: Free storage that must remain before staging a batch.
        cleanup: Whether successful unit inputs are deleted after verified processing.
        keep_failed_inputs: Preserve staged inputs for failed units.
        fsync_logs: Synchronize each unit log at the end of every phase.
        download_workers: Maximum simultaneous unit downloads.
        processing_storage_multiplier: Estimated peak processing storage divided
            by raw SRA size. Four means four times total, not four extra copies.
        max_threads_per_unit: Optional CPU ceiling for one adaptively allocated
            unit. ``ResourceSpec.threads`` is its hard minimum.
        scheduler_poll_seconds: Maximum wait between streaming scheduler checks.
    """

    prefetch_batches: int = 1
    max_staged_gb: float | None = None
    minimum_free_gb: float = 0.0
    cleanup: CleanupPolicy = "after_success"
    keep_failed_inputs: bool = True
    fsync_logs: bool = True
    download_workers: int = 2
    processing_storage_multiplier: float = 1.0
    max_threads_per_unit: int | None = None
    scheduler_poll_seconds: float = 1.0

    def __post_init__(self) -> None:
        """Validate prefetch, storage, cleanup, and logging policy values."""

        if self.prefetch_batches < 0:
            raise ValueError("prefetch_batches cannot be negative")
        if self.max_staged_gb is not None and self.max_staged_gb <= 0:
            raise ValueError("max_staged_gb must be positive")
        if self.minimum_free_gb < 0:
            raise ValueError("minimum_free_gb cannot be negative")
        if self.cleanup not in {"after_success", "never"}:
            raise ValueError(f"Unknown cleanup policy: {self.cleanup!r}")
        if self.download_workers < 1:
            raise ValueError("download_workers must be positive")
        if self.processing_storage_multiplier < 1:
            raise ValueError("processing_storage_multiplier must be at least one")
        if self.max_threads_per_unit is not None and self.max_threads_per_unit < 1:
            raise ValueError("max_threads_per_unit must be positive")
        if self.scheduler_poll_seconds <= 0:
            raise ValueError("scheduler_poll_seconds must be positive")


@dataclass(frozen=True)
class BatchManifest:
    """Persist the durable lifecycle and storage facts for one batch.

    Args:
        job_id: Owning execution-snapshot identifier.
        batch_id: Integer batch identifier.
        status: Current batch lifecycle state.
        unit_ids: Ordered unit identifiers in the batch.
        estimated_size_gb: Catalog estimate for all units.
        staged_size_gb: Measured staged-input size.
        retained_size_gb: Measured input size remaining after cleanup.
        free_space_gb: Measured free storage at the latest transition.
        task_statuses: Latest status keyed by task identifier.
        log_paths: One durable log path keyed by task identifier.
        cleanup_roots: Manifest-owned input roots keyed by task identifier.
        started_at: Timestamp of the first transition.
        updated_at: Timestamp of the latest transition.
        completed_at: Timestamp when processing reached a terminal state.
    """

    job_id: str
    batch_id: int
    status: BatchStatus
    unit_ids: tuple[str, ...]
    estimated_size_gb: float
    staged_size_gb: float = 0.0
    retained_size_gb: float = 0.0
    free_space_gb: float | None = None
    task_statuses: dict[str, str] = field(default_factory=dict)
    log_paths: dict[str, str] = field(default_factory=dict)
    cleanup_roots: dict[str, list[str]] = field(default_factory=dict)
    started_at: str = field(default_factory=utc_timestamp)
    updated_at: str = field(default_factory=utc_timestamp)
    completed_at: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Serialize this batch manifest to JSON-compatible values."""

        value = asdict(self)
        value["unit_ids"] = list(self.unit_ids)
        return value

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> BatchManifest:
        """Restore a batch manifest from serialized mapping *value*."""

        copied = dict(value)
        copied["unit_ids"] = tuple(copied.get("unit_ids", ()))
        return cls(**copied)


class BatchStateStore:
    """Persist atomic per-batch manifests under a shared *root*."""

    def __init__(self, root: Path) -> None:
        """Create a batch state store below *root*."""

        self.root = root
        self.root.mkdir(parents=True, exist_ok=True)

    def _path(self, job_id: str, batch_id: int) -> Path:
        """Return the state path for *job_id* and *batch_id*."""

        return self.root / sanitize_identifier(job_id) / f"batch-{batch_id:06d}.json"

    def save(self, manifest: BatchManifest) -> Path:
        """Atomically persist *manifest* and return its path."""

        path = self._path(manifest.job_id, manifest.batch_id)
        atomic_write_json(path, manifest.to_dict())
        return path

    def get(self, job_id: str, batch_id: int) -> BatchManifest | None:
        """Return saved state for *job_id* and *batch_id*, when present."""

        path = self._path(job_id, batch_id)
        return BatchManifest.from_dict(read_json(path)) if path.is_file() else None
