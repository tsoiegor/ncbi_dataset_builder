"""Atomic per-sample workspace state."""

from __future__ import annotations

import socket
import time
from pathlib import Path
from typing import Any

from ..errors import UnitAlreadyRunning
from ..support.util import (
    atomic_write_json,
    exclusive_file_lock,
    read_json,
    sanitize_identifier,
    utc_timestamp,
)


class UnitStateStore:
    """Persist restart-safe state for independently processed samples."""

    def __init__(self, root: Path) -> None:
        """Create the state store below *root*."""

        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)

    def _path(self, unit_id: str) -> Path:
        """Return the JSON state path for *unit_id*."""

        return self.root / f"{sanitize_identifier(unit_id)}.json"

    def _lock(self, unit_id: str) -> Path:
        """Return the state-update lock path for *unit_id*."""

        return self.root.parent / "locks" / f"{sanitize_identifier(unit_id)}.state.lock"

    def _archive(self, unit_id: str, previous: dict[str, Any]) -> None:
        """Archive *previous* state before changing a sample fingerprint."""

        history = self.root.parent / "history" / sanitize_identifier(unit_id)
        stamp = utc_timestamp().replace(":", "-")
        atomic_write_json(history / f"{stamp}.json", previous)

    def get(self, unit_id: str) -> dict[str, Any] | None:
        """Return current state for *unit_id*, or ``None`` when absent."""

        path = self._path(unit_id)
        return read_json(path) if path.is_file() else None

    def start(
        self,
        unit_id: str,
        *,
        fingerprint: str,
        execution_id: str,
        item: dict[str, Any],
        log_path: Path,
        retry_failed: bool = False,
        reclaim_running: bool = False,
        stale_after_seconds: float = 7 * 24 * 60 * 60,
        force: bool = False,
    ) -> bool:
        """Claim one sample for execution.

        Args:
            unit_id: Stable sample identifier.
            fingerprint: Semantic work identity.
            execution_id: Automatic workspace execution identifier.
            item: Serialized queue item.
            log_path: Durable per-sample log path.
            retry_failed: Retry an otherwise matching failed sample.
            reclaim_running: Immediately reclaim an interrupted running state.
            stale_after_seconds: Age after which a running claim is stale.
            force: Repair a success whose declared outputs are invalid.

        Returns:
            ``True`` when claimed and ``False`` when reusable or non-retryable.
        """

        with exclusive_file_lock(self._lock(unit_id), timeout_seconds=60):
            previous = self.get(unit_id)
            if previous:
                status = previous.get("status")
                same_work = previous.get("fingerprint") == fingerprint
                if status == "succeeded" and same_work and not force:
                    return False
                if status == "failed" and same_work and not retry_failed and not force:
                    return False
                if status in {"running", "submitted"}:
                    started = float(
                        previous.get("started_epoch")
                        or previous.get("submitted_epoch")
                        or time.time()
                    )
                    if not reclaim_running and time.time() - started <= stale_after_seconds:
                        raise UnitAlreadyRunning(f"Sample is already running: {unit_id}")
                if not same_work or force:
                    self._archive(unit_id, previous)
            attempts = (
                int((previous or {}).get("attempts", 0)) + 1
                if previous and previous.get("fingerprint") == fingerprint
                else 1
            )
            submitted = previous or {}
            atomic_write_json(
                self._path(unit_id),
                {
                    "unit_id": unit_id,
                    "status": "running",
                    "attempts": attempts,
                    "started_at": utc_timestamp(),
                    "started_epoch": time.time(),
                    "host": socket.gethostname(),
                    "phase": "staging",
                    "log_path": str(log_path),
                    "fingerprint": fingerprint,
                    "execution_id": execution_id,
                    "item": item,
                    "slurm_job_id": submitted.get("slurm_job_id"),
                    "allocated_cpus": submitted.get("allocated_cpus"),
                    "allocated_memory_gb": submitted.get("allocated_memory_gb"),
                },
            )
            return True

    def record_submission(
        self,
        unit_id: str,
        *,
        slurm_job_id: str,
        cpus: int,
        memory_gb: float,
        fingerprint: str,
        execution_id: str,
        item: dict[str, Any],
        log_path: Path,
    ) -> None:
        """Record one submitted distributed Slurm sample job.

        Args:
            unit_id: Stable sample identifier.
            slurm_job_id: Scheduler-assigned job identifier.
            cpus: CPU allocation for the sample job.
            memory_gb: Memory allocation for the sample job.
            fingerprint: Semantic work identity.
            execution_id: Automatic workspace execution identifier.
            item: Serialized queue item.
            log_path: Durable per-sample log path.
        """

        if not slurm_job_id or cpus < 1 or memory_gb <= 0:
            raise ValueError("Submission needs a job ID and positive resources")
        with exclusive_file_lock(self._lock(unit_id), timeout_seconds=60):
            previous = self.get(unit_id)
            if previous and previous.get("fingerprint") != fingerprint:
                self._archive(unit_id, previous)
                previous = None
            atomic_write_json(
                self._path(unit_id),
                {
                    **(previous or {}),
                    "unit_id": unit_id,
                    "status": "submitted",
                    "phase": "queued",
                    "submitted_at": utc_timestamp(),
                    "submitted_epoch": time.time(),
                    "slurm_job_id": slurm_job_id,
                    "allocated_cpus": cpus,
                    "allocated_memory_gb": memory_gb,
                    "fingerprint": fingerprint,
                    "execution_id": execution_id,
                    "item": item,
                    "log_path": str(log_path),
                    "result": None,
                    "error": None,
                },
            )

    def set_phase(self, unit_id: str, phase: str) -> None:
        """Set processing *phase* for a claimed *unit_id*."""

        with exclusive_file_lock(self._lock(unit_id), timeout_seconds=60):
            previous = self.get(unit_id)
            if not previous or previous.get("status") != "running":
                raise ValueError(f"Cannot update an unclaimed sample: {unit_id}")
            atomic_write_json(
                self._path(unit_id),
                {**previous, "phase": phase, "phase_updated_at": utc_timestamp()},
            )

    def set_runtime_resources(
        self,
        unit_id: str,
        *,
        cpus: int,
        memory_gb: float | None,
    ) -> None:
        """Persist runtime *cpus* and optional Slurm *memory_gb* for *unit_id*."""

        with exclusive_file_lock(self._lock(unit_id), timeout_seconds=60):
            previous = self.get(unit_id)
            if not previous or previous.get("status") != "running":
                raise ValueError(f"Cannot allocate an unclaimed sample: {unit_id}")
            atomic_write_json(
                self._path(unit_id),
                {
                    **previous,
                    "allocated_cpus": cpus,
                    "allocated_memory_gb": memory_gb,
                    "resources_updated_at": utc_timestamp(),
                },
            )

    def succeed(self, unit_id: str, result: dict[str, Any]) -> None:
        """Mark *unit_id* succeeded and persist serialized *result*."""

        with exclusive_file_lock(self._lock(unit_id), timeout_seconds=60):
            previous = self.get(unit_id) or {}
            atomic_write_json(
                self._path(unit_id),
                {
                    **previous,
                    "status": "succeeded",
                    "finished_at": utc_timestamp(),
                    "phase": "completed",
                    "result": result,
                    "error": None,
                },
            )

    def fail(self, unit_id: str, error: str) -> None:
        """Mark *unit_id* failed and persist a bounded tail of *error*."""

        with exclusive_file_lock(self._lock(unit_id), timeout_seconds=60):
            previous = self.get(unit_id) or {}
            atomic_write_json(
                self._path(unit_id),
                {
                    **previous,
                    "status": "failed",
                    "finished_at": utc_timestamp(),
                    "phase": "failed",
                    "error": error[-20_000:],
                },
            )

    def summary(self, unit_ids: list[str] | None = None) -> dict[str, Any]:
        """Summarize all states, or only *unit_ids*."""

        paths = (
            [self._path(item) for item in unit_ids]
            if unit_ids is not None
            else sorted(self.root.glob("*.json"))
        )
        records = [read_json(path) for path in paths if path.is_file()]
        counts = {"pending": 0, "submitted": 0, "running": 0, "succeeded": 0, "failed": 0}
        for record in records:
            status = str(record.get("status", "pending"))
            counts[status] = counts.get(status, 0) + 1
        if unit_ids is not None:
            counts["pending"] += len(unit_ids) - len(records)
        return {"counts": counts, "units": records}
