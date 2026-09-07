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
    """Atomic per-task state files; safe for many Slurm workers on shared storage."""

    def __init__(self, root: Path) -> None:
        self.root = root
        self.root.mkdir(parents=True, exist_ok=True)

    def _path(self, task_id: str) -> Path:
        return self.root / f"{sanitize_identifier(task_id)}.json"

    def _lock(self, task_id: str) -> Path:
        return self.root / f".{sanitize_identifier(task_id)}.state.lock"

    def get(self, task_id: str) -> dict[str, Any] | None:
        path = self._path(task_id)
        return read_json(path) if path.is_file() else None

    def start(
        self,
        task_id: str,
        *,
        retry_failed: bool = False,
        stale_after_seconds: float = 7 * 24 * 60 * 60,
    ) -> bool:
        with exclusive_file_lock(self._lock(task_id), timeout_seconds=60):
            previous = self.get(task_id)
            if previous:
                status = previous.get("status")
                if status == "succeeded":
                    return False
                if status == "failed" and not retry_failed:
                    return False
                if status == "running":
                    started_epoch = float(previous.get("started_epoch", time.time()))
                    if time.time() - started_epoch <= stale_after_seconds:
                        raise TaskAlreadyRunning(f"Task is already running: {task_id}")
            attempts = int((previous or {}).get("attempts", 0)) + 1
            atomic_write_json(
                self._path(task_id),
                {
                    "task_id": task_id,
                    "status": "running",
                    "attempts": attempts,
                    "started_at": utc_timestamp(),
                    "started_epoch": time.time(),
                    "host": socket.gethostname(),
                },
            )
            return True

    def succeed(self, task_id: str, result: dict[str, Any]) -> None:
        with exclusive_file_lock(self._lock(task_id), timeout_seconds=60):
            previous = self.get(task_id) or {}
            atomic_write_json(
                self._path(task_id),
                {
                    **previous,
                    "status": "succeeded",
                    "finished_at": utc_timestamp(),
                    "result": result,
                    "error": None,
                },
            )

    def fail(self, task_id: str, error: str) -> None:
        with exclusive_file_lock(self._lock(task_id), timeout_seconds=60):
            previous = self.get(task_id) or {}
            atomic_write_json(
                self._path(task_id),
                {
                    **previous,
                    "status": "failed",
                    "finished_at": utc_timestamp(),
                    "error": error[-20_000:],
                },
            )

    def summary(self, task_ids: list[str] | None = None) -> dict[str, Any]:
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
