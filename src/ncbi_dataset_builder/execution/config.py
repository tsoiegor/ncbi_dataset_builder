"""Execution-system and sample-queue configuration."""

from __future__ import annotations

import os
import re
import shutil
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Literal, Protocol

from ..support.util import bytes_to_gb
from .storage import path_size_gb

CleanupPolicy = Literal["after_success", "never"]


class StoragePolicy(Protocol):
    """Describe storage capacity available to the sample queue."""

    def available_gb(self, workspace: Path) -> float:
        """Return usable storage remaining for *workspace* in decimal GB."""


@dataclass(frozen=True)
class FilesystemStorage:
    """Reserve free filesystem space during local execution.

    Args:
        reserve_free_gb: Free space that must remain on the filesystem holding
            the workspace after admitting another sample.
    """

    reserve_free_gb: float = 0.0

    def __post_init__(self) -> None:
        """Validate :attr:`reserve_free_gb`."""

        if self.reserve_free_gb < 0:
            raise ValueError("reserve_free_gb cannot be negative")

    def available_gb(self, workspace: Path) -> float:
        """Return free space above the reserve on the filesystem containing *workspace*."""

        workspace.mkdir(parents=True, exist_ok=True)
        free_gb = bytes_to_gb(shutil.disk_usage(workspace).free)
        return max(0.0, free_gb - self.reserve_free_gb)


@dataclass(frozen=True)
class QuotaStorage:
    """Model a user quota instead of shared-filesystem free space.

    Args:
        quota_gb: Total storage quota in decimal GB.
        reserve_gb: Quota capacity that the queue must leave unused.
        usage_root: Directory whose current size counts against the quota.
            ``None`` uses the builder workspace. Set this to a quota root when
            the quota includes files outside the workspace.
    """

    quota_gb: float
    reserve_gb: float = 0.0
    usage_root: Path | None = None

    def __post_init__(self) -> None:
        """Normalize :attr:`usage_root` and validate quota values."""

        if self.quota_gb <= 0:
            raise ValueError("quota_gb must be positive")
        if self.reserve_gb < 0 or self.reserve_gb >= self.quota_gb:
            raise ValueError("reserve_gb must be non-negative and below quota_gb")
        if self.usage_root is not None:
            object.__setattr__(self, "usage_root", Path(self.usage_root))

    def available_gb(self, workspace: Path) -> float:
        """Return unused quota above :attr:`reserve_gb`, defaulting usage to *workspace*."""

        used_gb = path_size_gb(self.usage_root or workspace)
        return max(0.0, self.quota_gb - self.reserve_gb - used_gb)


@dataclass(frozen=True)
class QueuePolicy:
    """Control sample streaming independently of the execution system.

    Args:
        download_workers: Maximum simultaneous sample downloads.
        max_inflight_gb: Optional maximum estimated raw and processing storage
            for downloading, ready, and processing samples together.
        processing_storage_multiplier: Estimated peak processor storage divided
            by raw sample size. ``2`` means twice the raw size in total.
        cleanup: Remove provider-owned inputs after a verified success, or keep
            them indefinitely.
        keep_failed_inputs: Preserve downloaded input after a failed processor.
        fsync_logs: Flush and synchronize each sample log at phase boundaries.
        scheduler_poll_seconds: Maximum wait between queue state checks.
    """

    download_workers: int = 2
    max_inflight_gb: float | None = None
    processing_storage_multiplier: float = 1.0
    cleanup: CleanupPolicy = "after_success"
    keep_failed_inputs: bool = True
    fsync_logs: bool = True
    scheduler_poll_seconds: float = 1.0

    def __post_init__(self) -> None:
        """Validate queue concurrency, storage, cleanup, and polling values."""

        if self.download_workers < 1:
            raise ValueError("download_workers must be positive")
        if self.max_inflight_gb is not None and self.max_inflight_gb <= 0:
            raise ValueError("max_inflight_gb must be positive")
        if self.processing_storage_multiplier < 1:
            raise ValueError("processing_storage_multiplier must be at least one")
        if self.cleanup not in {"after_success", "never"}:
            raise ValueError(f"Unknown cleanup policy: {self.cleanup!r}")
        if self.scheduler_poll_seconds <= 0:
            raise ValueError("scheduler_poll_seconds must be positive")


def _validate_scheduler_value(name: str, value: str | None, *, commas: bool = False) -> None:
    """Validate an optional Slurm *value* for scheduler field *name*."""

    if value is None:
        return
    pattern = r"[A-Za-z0-9_.-]+(?:,[A-Za-z0-9_.-]+)*" if commas else r"[A-Za-z0-9_.-]+"
    if not re.fullmatch(pattern, value):
        raise ValueError(f"Unsafe or invalid Slurm {name}: {value!r}")


def _validate_time(name: str, value: str) -> None:
    """Validate Slurm time *value* stored in field *name*."""

    if not re.fullmatch(r"[0-9:-]+", value):
        raise ValueError(f"Unsafe or invalid {name}: {value!r}")


@dataclass(frozen=True)
class LocalExecution:
    """Configure execution on one ordinary server.

    Args:
        total_cpus: CPU budget shared by processing samples.
        min_cpus_per_job: Minimum CPUs supplied to one sample processor.
        max_cpus_per_job: Maximum CPUs supplied to one sample processor.
        max_running_jobs: Maximum concurrently processing samples. Downloads do
            not count toward this limit.
        storage: Free-filesystem-space policy used for queue admission.

    Local execution deliberately has no memory setting or memory enforcement.
    """

    total_cpus: int = max(1, os.cpu_count() or 1)
    min_cpus_per_job: int = 1
    max_cpus_per_job: int | None = None
    max_running_jobs: int = 1
    storage: FilesystemStorage = field(default_factory=FilesystemStorage)

    def __post_init__(self) -> None:
        """Resolve the CPU ceiling and validate local resource limits."""

        maximum = self.max_cpus_per_job or self.total_cpus
        object.__setattr__(self, "max_cpus_per_job", maximum)
        if min(self.total_cpus, self.min_cpus_per_job, maximum, self.max_running_jobs) < 1:
            raise ValueError("Local CPU and job limits must be positive")
        if self.min_cpus_per_job > maximum:
            raise ValueError("min_cpus_per_job cannot exceed max_cpus_per_job")
        if maximum > self.total_cpus:
            raise ValueError("max_cpus_per_job cannot exceed total_cpus")


@dataclass(frozen=True)
class SlurmSingleNodeExecution:
    """Configure a streaming queue inside one Slurm allocation.

    Args:
        allocation_cpus: CPUs requested for the coordinator allocation.
        allocation_memory_gb: Memory requested for the allocation in GB.
        allocation_time_limit: Slurm wall time for the allocation.
        min_cpus_per_job: Minimum CPUs supplied to one sample processor.
        max_cpus_per_job: Maximum CPUs supplied to one sample processor.
        memory_gb_per_job: Memory reserved per concurrently processing sample.
        max_running_jobs: Maximum concurrent sample processors inside the node.
        partition: Optional comma-separated Slurm partitions.
        account: Optional Slurm account.
        qos: Optional Slurm quality-of-service name.
        storage: Quota-aware storage policy.
    """

    allocation_cpus: int
    allocation_memory_gb: float
    allocation_time_limit: str
    storage: QuotaStorage
    min_cpus_per_job: int = 1
    max_cpus_per_job: int | None = None
    memory_gb_per_job: float = 1.0
    max_running_jobs: int = 1
    partition: str | None = None
    account: str | None = None
    qos: str | None = None

    def __post_init__(self) -> None:
        """Resolve the CPU ceiling and validate Slurm allocation limits."""

        maximum = self.max_cpus_per_job or self.allocation_cpus
        object.__setattr__(self, "max_cpus_per_job", maximum)
        if min(self.allocation_cpus, self.min_cpus_per_job, maximum, self.max_running_jobs) < 1:
            raise ValueError("Single-node Slurm CPU and job limits must be positive")
        if self.allocation_memory_gb <= 0 or self.memory_gb_per_job <= 0:
            raise ValueError("Single-node Slurm memory limits must be positive")
        if self.min_cpus_per_job > maximum or maximum > self.allocation_cpus:
            raise ValueError("Per-job CPU limits must fit inside allocation_cpus")
        if self.memory_gb_per_job > self.allocation_memory_gb:
            raise ValueError("memory_gb_per_job cannot exceed allocation_memory_gb")
        _validate_time("allocation_time_limit", self.allocation_time_limit)
        _validate_scheduler_value("partition", self.partition, commas=True)
        _validate_scheduler_value("account", self.account)
        _validate_scheduler_value("qos", self.qos)


@dataclass(frozen=True)
class SlurmDistributedExecution:
    """Configure one coordinator and a queue of Slurm sample jobs.

    Args:
        total_cpu_quota: Maximum CPUs used by coordinator and sample jobs.
        max_running_jobs: Maximum simultaneously active sample jobs. The
            coordinator is additional and does not count toward this limit.
        cpus_per_node: Largest worker CPU request supported by a node.
        min_cpus_per_job: Minimum CPUs requested for one sample job.
        max_cpus_per_job: Maximum CPUs requested for one sample job.
        memory_gb_per_job: Hard Slurm memory request for each sample job.
        worker_time_limit: Slurm wall time for each sample job.
        coordinator_cpus: CPUs reserved for the coordinator.
        coordinator_memory_gb: Hard memory request for the coordinator.
        coordinator_time_limit: Slurm wall time for the coordinator.
        partition: Optional comma-separated Slurm partitions.
        account: Optional Slurm account.
        qos: Optional Slurm quality-of-service name.
        storage: Quota-aware storage policy.
    """

    total_cpu_quota: int
    max_running_jobs: int
    cpus_per_node: int
    min_cpus_per_job: int
    max_cpus_per_job: int
    memory_gb_per_job: float
    worker_time_limit: str
    storage: QuotaStorage
    coordinator_cpus: int = 1
    coordinator_memory_gb: float = 4.0
    coordinator_time_limit: str = "7-00:00:00"
    partition: str | None = None
    account: str | None = None
    qos: str | None = None

    def __post_init__(self) -> None:
        """Validate distributed CPU, memory, time, and scheduler settings."""

        numeric = (
            self.total_cpu_quota,
            self.max_running_jobs,
            self.cpus_per_node,
            self.min_cpus_per_job,
            self.max_cpus_per_job,
            self.coordinator_cpus,
        )
        if min(numeric) < 1:
            raise ValueError("Distributed Slurm CPU and job limits must be positive")
        if self.memory_gb_per_job <= 0 or self.coordinator_memory_gb <= 0:
            raise ValueError("Distributed Slurm memory limits must be positive")
        if self.coordinator_cpus >= self.total_cpu_quota:
            raise ValueError("coordinator_cpus must be below total_cpu_quota")
        if self.min_cpus_per_job > self.max_cpus_per_job:
            raise ValueError("min_cpus_per_job cannot exceed max_cpus_per_job")
        if self.max_cpus_per_job > self.cpus_per_node:
            raise ValueError("max_cpus_per_job cannot exceed cpus_per_node")
        if self.min_cpus_per_job > self.total_cpu_quota - self.coordinator_cpus:
            raise ValueError("CPU quota cannot admit one sample job")
        _validate_time("worker_time_limit", self.worker_time_limit)
        _validate_time("coordinator_time_limit", self.coordinator_time_limit)
        _validate_scheduler_value("partition", self.partition, commas=True)
        _validate_scheduler_value("account", self.account)
        _validate_scheduler_value("qos", self.qos)

    def worker_capacity(self, cpus_per_job: int | None = None) -> int:
        """Return jobs allowed by CPU quota for optional *cpus_per_job*."""

        cpus = cpus_per_job or self.min_cpus_per_job
        if cpus < self.min_cpus_per_job or cpus > self.max_cpus_per_job:
            raise ValueError("cpus_per_job is outside configured per-job limits")
        by_cpu = (self.total_cpu_quota - self.coordinator_cpus) // cpus
        return min(self.max_running_jobs, by_cpu)


ExecutionSystem = LocalExecution | SlurmSingleNodeExecution | SlurmDistributedExecution


def queue_policy_to_dict(policy: QueuePolicy) -> dict[str, Any]:
    """Serialize *policy* for an internal execution record."""

    return asdict(policy)


def queue_policy_from_dict(value: dict[str, Any]) -> QueuePolicy:
    """Restore a queue policy from serialized *value*."""

    return QueuePolicy(**value)


def execution_to_dict(execution: ExecutionSystem) -> dict[str, Any]:
    """Serialize an execution-system configuration for workspace state."""

    value = asdict(execution)
    storage = value.get("storage")
    if isinstance(storage, dict) and storage.get("usage_root") is not None:
        storage["usage_root"] = str(storage["usage_root"])
    return {"kind": execution.__class__.__name__, "config": value}


def execution_from_dict(value: dict[str, Any]) -> ExecutionSystem:
    """Restore an execution-system configuration from serialized *value*."""

    kind = str(value["kind"])
    config = dict(value["config"])
    storage = config.get("storage")
    if storage is not None:
        if kind == "LocalExecution":
            config["storage"] = FilesystemStorage(**storage)
        else:
            config["storage"] = QuotaStorage(**storage)
    classes = {
        "LocalExecution": LocalExecution,
        "SlurmSingleNodeExecution": SlurmSingleNodeExecution,
        "SlurmDistributedExecution": SlurmDistributedExecution,
    }
    try:
        execution_class = classes[kind]
    except KeyError as exc:
        raise ValueError(f"Unknown execution-system kind: {kind!r}") from exc
    return execution_class(**config)
