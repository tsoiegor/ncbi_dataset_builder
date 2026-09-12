from __future__ import annotations

import contextlib
import hashlib
import json
import os
import re
import socket
import tempfile
import threading
import time
import uuid
from collections.abc import Iterator
from pathlib import Path
from typing import Any

from .progress import ProgressReporter, get_progress

GB = 1_000_000_000


def bytes_to_gb(value: float) -> float:
    """Convert a byte *value* to decimal gigabytes."""

    return float(value) / GB


def gb_to_bytes(value: float) -> int:
    """Convert a decimal-gigabyte *value* to bytes for system-level operations."""

    return round(float(value) * GB)


def sanitize_identifier(value: str) -> str:
    """Convert *value* to a non-empty identifier safe for file names."""

    cleaned = re.sub(r"[^A-Za-z0-9._-]+", "_", value.strip())
    if not cleaned or cleaned in {".", ".."}:
        raise ValueError(f"Unsafe or empty identifier: {value!r}")
    return cleaned


def sha256_file(
    path: Path,
    chunk_size: int = 8 * 1024 * 1024,
    *,
    progress: ProgressReporter | None = None,
) -> str:
    """Return the SHA-256 of *path* using *chunk_size* and optional *progress*."""

    digest = hashlib.sha256()
    reporter = get_progress(progress)
    with (
        path.open("rb") as handle,
        reporter.task(
            f"Checksum {path.name}", total=bytes_to_gb(path.stat().st_size), unit="GB"
        ) as task,
    ):
        for chunk in iter(lambda: handle.read(chunk_size), b""):
            digest.update(chunk)
            task.update(bytes_to_gb(len(chunk)))
    return digest.hexdigest()


def atomic_write_bytes(path: Path, data: bytes) -> bool:
    """Atomically write *data* to *path* when its content changed.

    Return ``True`` when a file was replaced and ``False`` when the existing
    file already contained exactly the same bytes.
    """

    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        if path.is_file() and path.read_bytes() == data:
            return False
    except FileNotFoundError:
        pass
    fd, temporary = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".part", dir=path.parent)
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    except BaseException:
        with contextlib.suppress(FileNotFoundError):
            os.unlink(temporary)
        raise
    return True


def atomic_write_text(path: Path, text: str) -> bool:
    """Atomically write UTF-8 *text* to *path* when its content changed."""

    return atomic_write_bytes(path, text.encode("utf-8"))


def atomic_write_json(path: Path, value: Any) -> bool:
    """Atomically serialize *value* to *path*, returning whether it changed."""

    return atomic_write_text(
        path, json.dumps(value, indent=2, sort_keys=True, ensure_ascii=False) + "\n"
    )


def read_json(path: Path) -> Any:
    """Read and decode a UTF-8 JSON document from *path*."""

    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def _local_process_alive(pid: int) -> bool:
    """Return whether local process *pid* still appears to exist."""

    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


@contextlib.contextmanager
def exclusive_file_lock(
    path: Path,
    *,
    timeout_seconds: float = 120.0,
    stale_after_seconds: float = 24 * 60 * 60,
    heartbeat_seconds: float | None = None,
) -> Iterator[None]:
    """Acquire the lock at *path* until exit or *timeout_seconds*.

    It is appropriate for shared filesystems because ownership is persisted in a
    normal file. A lock older than *stale_after_seconds* may be reclaimed.
    Optional *heartbeat_seconds* refreshes long-running lock ownership.
    """

    if heartbeat_seconds is not None and (
        heartbeat_seconds <= 0 or heartbeat_seconds >= stale_after_seconds
    ):
        raise ValueError("heartbeat_seconds must be positive and below stale_after_seconds")
    path.parent.mkdir(parents=True, exist_ok=True)
    deadline = time.monotonic() + timeout_seconds
    owner = str(uuid.uuid4())
    payload = json.dumps(
        {
            "pid": os.getpid(),
            "host": socket.gethostname(),
            "created_at": time.time(),
            "owner": owner,
        }
    )
    while True:
        try:
            descriptor = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
                handle.write(payload)
            break
        except FileExistsError:
            try:
                age = time.time() - path.stat().st_mtime
                current = json.loads(path.read_text(encoding="utf-8"))
                dead_local_owner = (
                    current.get("host") == socket.gethostname()
                    and isinstance(current.get("pid"), int)
                    and not _local_process_alive(current["pid"])
                )
                if dead_local_owner or age > stale_after_seconds:
                    path.unlink(missing_ok=True)
                    continue
            except FileNotFoundError:
                continue
            except (json.JSONDecodeError, OSError):
                pass
            if time.monotonic() >= deadline:
                raise TimeoutError(f"Timed out waiting for lock {path}")
            time.sleep(0.2)
    stop_heartbeat = threading.Event()
    heartbeat: threading.Thread | None = None
    if heartbeat_seconds is not None:

        def refresh_lock() -> None:
            """Refresh this lock until the owning context signals completion."""

            while not stop_heartbeat.wait(heartbeat_seconds):
                try:
                    current = json.loads(path.read_text(encoding="utf-8"))
                    if current.get("owner") != owner:
                        return
                    path.touch()
                except (FileNotFoundError, json.JSONDecodeError, OSError):
                    return

        heartbeat = threading.Thread(
            target=refresh_lock,
            name="dataset-lock-heartbeat",
            daemon=True,
        )
        heartbeat.start()
    try:
        yield
    finally:
        stop_heartbeat.set()
        if heartbeat is not None:
            heartbeat.join(timeout=max(1.0, heartbeat_seconds or 1.0))
        try:
            current = json.loads(path.read_text(encoding="utf-8"))
            if current.get("owner") == owner:
                path.unlink(missing_ok=True)
        except (FileNotFoundError, json.JSONDecodeError, OSError):
            pass


def utc_timestamp() -> str:
    """Return the current UTC time as an ISO 8601 string."""

    from datetime import datetime, timezone

    return datetime.now(timezone.utc).isoformat()


def existing_nonempty(path: Path) -> bool:
    """Return whether *path* is an existing regular file with content."""

    return path.is_file() and path.stat().st_size > 0
