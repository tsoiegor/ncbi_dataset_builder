from __future__ import annotations

import socket
import time
from pathlib import Path
from typing import Any

from .errors import TaskAlreadyRunning
from .util import (
    atomic_write_json,
    exclusive_file_lock,
    read_json,
    sanitize_identifier,
    utc_timestamp,
)


class TaskStateStore:
    """Persist atomic task-state files below a shared-storage *root* directory."""

    def __init__(self, root: Path) -> None:
        """Create the state store below directory *root*."""

        self.root = root
        self.root.mkdir(parents=True, exist_ok=True)

    def _path(self, task_id: str) -> Path:
        """Return the JSON state path for *task_id*."""

        return self.root / f"{sanitize_identifier(task_id)}.json"

    def _lock(self, task_id: str) -> Path:
        """Return the state-update lock path for *task_id*."""

        return self.root.parent / "locks" / f"{sanitize_identifier(task_id)}.state.lock"

    def _archive(self, task_id: str, previous: dict[str, Any]) -> None:
        """Archive *previous* state for *task_id* before replacing its fingerprint."""

        history = self.root.parent / "history" / sanitize_identifier(task_id)
        stamp = utc_timestamp().replace(":", "-")
        atomic_write_json(history / f"{stamp}.json", previous)

    def get(self, task_id: str) -> dict[str, Any] | None:
        """Return current state for *task_id*, or ``None`` when it has not run."""

        path = self._path(task_id)
        return read_json(path) if path.is_file() else None

    def import_record(self, task_id: str, record: dict[str, Any]) -> bool:
        """Import *record* for *task_id* only when no current state exists."""

        with exclusive_file_lock(self._lock(task_id), timeout_seconds=60):
            path = self._path(task_id)
            if path.is_file():
                return False
            atomic_write_json(path, {**record, "task_id": task_id})
            return True

    def start(
        self,
        task_id: str,
        *,
        retry_failed: bool = False,
        stale_after_seconds: float = 7 * 24 * 60 * 60,
        log_path: Path | None = None,
        reclaim_running: bool = False,
        fingerprint: str = "",
        job_id: str | None = None,
        task: dict[str, Any] | None = None,
        force: bool = False,
    ) -> bool:
        """Claim *task_id* unless completed, non-retryable, or already running.

        *retry_failed* permits another attempt after failure, while
        *stale_after_seconds* permits reclaiming an abandoned running claim,
        *log_path* records the unit's single durable log file, and
        *reclaim_running* lets a workspace coordinator recover an interrupted
        claim immediately. *fingerprint* identifies semantic work, *job_id*
        records the latest operational snapshot, *task* stores its unit
        specification, and *force* repairs invalid successful artifacts.
        """

        with exclusive_file_lock(self._lock(task_id), timeout_seconds=60):
            previous = self.get(task_id)
            if previous:
                status = previous.get("status")
                same_work = previous.get("fingerprint", "") == fingerprint
                if status == "succeeded" and same_work and not force:
                    return False
                if status == "failed" and same_work and not retry_failed and not force:
                    return False
                if status == "running":
                    started_epoch = float(previous.get("started_epoch", time.time()))
                    if not reclaim_running and time.time() - started_epoch <= stale_after_seconds:
                        raise TaskAlreadyRunning(f"Task is already running: {task_id}")
                if not same_work or force:
                    self._archive(task_id, previous)
            attempts = (
                int((previous or {}).get("attempts", 0)) + 1
                if previous and previous.get("fingerprint", "") == fingerprint
                else 1
            )
            atomic_write_json(
                self._path(task_id),
                {
                    "task_id": task_id,
                    "status": "running",
                    "attempts": attempts,
                    "started_at": utc_timestamp(),
                    "started_epoch": time.time(),
                    "host": socket.gethostname(),
                    "phase": "staging",
                    "log_path": str(log_path) if log_path is not None else None,
                    "fingerprint": fingerprint,
                    "job_id": job_id,
                    "task": task,
                },
            )
            return True

    def set_phase(self, task_id: str, phase: str) -> None:
        """Set the current *phase* for a claimed *task_id*."""

        with exclusive_file_lock(self._lock(task_id), timeout_seconds=60):
            previous = self.get(task_id)
            if not previous or previous.get("status") != "running":
                raise ValueError(f"Cannot update phase for an unclaimed task: {task_id}")
            atomic_write_json(
                self._path(task_id),
                {**previous, "phase": phase, "phase_updated_at": utc_timestamp()},
            )

    def succeed(self, task_id: str, result: dict[str, Any]) -> None:
        """Mark *task_id* succeeded and persist serialized processor *result*."""

        with exclusive_file_lock(self._lock(task_id), timeout_seconds=60):
            previous = self.get(task_id) or {}
            atomic_write_json(
                self._path(task_id),
                {
                    **previous,
                    "status": "succeeded",
                    "finished_at": utc_timestamp(),
                    "phase": "completed",
                    "result": result,
                    "error": None,
                },
            )

    def fail(self, task_id: str, error: str) -> None:
        """Mark *task_id* failed and persist a bounded tail of *error*."""

        with exclusive_file_lock(self._lock(task_id), timeout_seconds=60):
            previous = self.get(task_id) or {}
            atomic_write_json(
                self._path(task_id),
                {
                    **previous,
                    "status": "failed",
                    "finished_at": utc_timestamp(),
                    "phase": "failed",
                    "error": error[-20_000:],
                },
            )

    def summary(self, task_ids: list[str] | None = None) -> dict[str, Any]:
        """Summarize all states, or only the requested *task_ids*."""

        paths = (
            [self._path(item) for item in task_ids]
            if task_ids is not None
            else sorted(self.root.glob("*.json"))
        )
        records = [read_json(path) for path in paths if path.is_file()]
        counts = {"pending": 0, "running": 0, "succeeded": 0, "failed": 0}
        for record in records:
            counts[record.get("status", "pending")] = (
                counts.get(record.get("status", "pending"), 0) + 1
            )
        if task_ids is not None:
            counts["pending"] += len(task_ids) - len(records)
        return {"counts": counts, "tasks": records}
