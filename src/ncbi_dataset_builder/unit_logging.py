from __future__ import annotations

import logging
import os
import sys
import threading
from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from pathlib import Path
from typing import IO, Any, TextIO

from .util import utc_timestamp


@dataclass
class _UnitLogSession:
    """Hold the active unit log state.

    Args:
        path: Durable log-file path.
        handle: Open text handle for the log.
        fsync: Flush the file to durable storage when the session closes.
        lock: Lock serializing writes from logging and stream adapters.
    """

    path: Path
    handle: TextIO
    fsync: bool
    lock: threading.RLock

    def write(self, value: str) -> None:
        """Append text *value* to the log and flush it."""

        if not value:
            return
        with self.lock:
            self.handle.write(value)
            self.handle.flush()

    def close(self) -> None:
        """Flush, optionally synchronize, and close the active handle."""

        with self.lock:
            self.handle.flush()
            if self.fsync:
                os.fsync(self.handle.fileno())
            self.handle.close()


_CURRENT_SESSION: ContextVar[_UnitLogSession | None] = ContextVar(
    "ncbi_dataset_builder_unit_log_session", default=None
)


class _UnitLogHandler(logging.Handler):
    """Route Python log records to the unit log active in their context."""

    def emit(self, record: logging.LogRecord) -> None:
        """Format and append *record* when a unit session is active."""

        session = _CURRENT_SESSION.get()
        if session is None:
            return
        try:
            session.write(self.format(record) + "\n")
        except BaseException:  # noqa: BLE001 - logging must never break unit execution
            self.handleError(record)


class _ContextStream:
    """Proxy one process stream while routing contextual writes to unit logs."""

    def __init__(self, fallback: TextIO, label: str) -> None:
        """Use *fallback* outside unit work and label captured text with *label*."""

        self._fallback = fallback
        self._label = label

    def write(self, value: str) -> int:
        """Write *value* to the current unit log or the fallback stream."""

        session = _CURRENT_SESSION.get()
        if session is None:
            return self._fallback.write(value)
        session.write(value)
        return len(value)

    def flush(self) -> None:
        """Flush the current unit log and the fallback stream when applicable."""

        session = _CURRENT_SESSION.get()
        if session is None:
            self._fallback.flush()
        else:
            session.handle.flush()

    def isatty(self) -> bool:
        """Return the fallback stream's terminal status."""

        return self._fallback.isatty()

    def fileno(self) -> int:
        """Return the current unit log descriptor or the fallback descriptor."""

        session = _CURRENT_SESSION.get()
        return (session.handle if session is not None else self._fallback).fileno()

    @property
    def encoding(self) -> str | None:
        """Return the fallback stream encoding."""

        return self._fallback.encoding

    @property
    def errors(self) -> str | None:
        """Return the fallback stream error policy."""

        return self._fallback.errors

    def __getattr__(self, name: str) -> Any:
        """Delegate unsupported attribute *name* to the fallback stream."""

        return getattr(self._fallback, name)


_INSTALL_LOCK = threading.Lock()
_INSTALLED = False


def install_unit_logging() -> None:
    """Install context-aware package logging and stdout/stderr routing once."""

    global _INSTALLED
    with _INSTALL_LOCK:
        if not _INSTALLED:
            handler = _UnitLogHandler(level=logging.DEBUG)
            handler.setFormatter(
                logging.Formatter(
                    "%(asctime)s %(levelname)s %(name)s [%(threadName)s] %(message)s",
                    datefmt="%Y-%m-%dT%H:%M:%S",
                )
            )
            package_logger = logging.getLogger("ncbi_dataset_builder")
            package_logger.addHandler(handler)
            package_logger.setLevel(logging.DEBUG)
            _INSTALLED = True
        if not isinstance(sys.stdout, _ContextStream):
            sys.stdout = _ContextStream(sys.stdout, "stdout")  # type: ignore[assignment]
        if not isinstance(sys.stderr, _ContextStream):
            sys.stderr = _ContextStream(sys.stderr, "stderr")  # type: ignore[assignment]


@contextmanager
def unit_log(
    path: Path,
    *,
    phase: str,
    unit_id: str,
    fsync: bool = True,
) -> Iterator[Path]:
    """Append one *phase* for *unit_id* to *path* and yield the log path.

    *fsync* controls durable synchronization when the context exits. Repeated
    staging, processing, and retry phases append to the same file.
    """

    install_unit_logging()
    path.parent.mkdir(parents=True, exist_ok=True)
    handle = path.open("a", encoding="utf-8", buffering=1)
    session = _UnitLogSession(path=path, handle=handle, fsync=fsync, lock=threading.RLock())
    token = _CURRENT_SESSION.set(session)
    session.write(f"\n===== {utc_timestamp()} unit={unit_id} phase={phase} started =====\n")
    try:
        yield path
    except BaseException:
        session.write(f"===== {utc_timestamp()} unit={unit_id} phase={phase} failed =====\n")
        raise
    else:
        session.write(f"===== {utc_timestamp()} unit={unit_id} phase={phase} completed =====\n")
    finally:
        _CURRENT_SESSION.reset(token)
        session.close()


def current_unit_log_path() -> Path | None:
    """Return the active unit-log path, or ``None`` outside unit execution."""

    session = _CURRENT_SESSION.get()
    return session.path if session is not None else None


def current_unit_log_handle() -> IO[str] | None:
    """Return the active text handle for direct subprocess redirection."""

    session = _CURRENT_SESSION.get()
    if session is not None:
        session.handle.flush()
        return session.handle
    return None


def write_unit_output(label: str, value: str | bytes | None) -> None:
    """Append command or processor *value* under a *label* in the active log."""

    session = _CURRENT_SESSION.get()
    if session is None or value in (None, "", b""):
        return
    rendered = value.decode("utf-8", errors="replace") if isinstance(value, bytes) else value
    session.write(f"--- {label} ---\n{rendered}")
    if not rendered.endswith("\n"):
        session.write("\n")


def _reset_unit_logging_for_tests() -> None:
    """Reset no runtime state; retained only as an explicit test hook."""

    return
