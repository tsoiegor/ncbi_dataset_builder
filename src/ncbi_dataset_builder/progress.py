from __future__ import annotations

import logging
import sys
import threading
import time
from collections.abc import Iterable, Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from types import TracebackType
from typing import Any, Self, TextIO, TypeVar

T = TypeVar("T")

LOGGER = logging.getLogger("ncbi_dataset_builder")
_MINIMUM_CONSOLE_LEVEL: ContextVar[int] = ContextVar(
    "ncbi_dataset_builder_minimum_console_level",
    default=logging.NOTSET,
)


def progress_level_enabled(level: int) -> bool:
    """Return whether *level* is visible in the current progress context."""

    return level >= _MINIMUM_CONSOLE_LEVEL.get()


class ProgressTask:
    """Track one operation with an optional progress bar or text updates."""

    def __init__(
        self,
        reporter: ProgressReporter,
        description: str,
        *,
        total: float | None,
        unit: str,
    ) -> None:
        """Create a task owned by *reporter* for *description*.

        *total* is the expected amount, and *unit* labels that amount.
        """

        self.reporter = reporter
        self.description = description
        self.total = total
        self.unit = unit
        self.completed = 0.0
        self.started_at = time.monotonic()
        self._last_report = self.started_at
        self._closed = False
        self._visible = progress_level_enabled(logging.INFO)
        self._bar = (
            reporter._create_bar(description, total=total, unit=unit) if self._visible else None
        )
        LOGGER.info(self._render("started"))
        if self._visible and reporter.enabled and self._bar is None:
            reporter._write(self._render("started"))

    def _render(self, state: str) -> str:
        """Render textual *state* with current counts and elapsed time."""

        elapsed = time.monotonic() - self.started_at
        count = f"{self.completed:,.3f}" if self.unit == "GB" else f"{self.completed:,.0f}"
        if self.total is not None:
            total = f"{self.total:,.3f}" if self.unit == "GB" else f"{self.total:,.0f}"
            count += f"/{total}"
        return f"{self.description}: {state} ({count} {self.unit}, {elapsed:.1f}s)"

    def update(self, amount: float = 1) -> None:
        """Advance the task by non-negative *amount*."""

        if amount < 0:
            raise ValueError("Progress amount cannot be negative")
        if self._closed:
            return
        self.completed += amount
        if self._bar is not None:
            self._bar.update(amount)
        now = time.monotonic()
        finished = self.total is not None and self.completed >= self.total
        if not finished and now - self._last_report >= self.reporter.text_interval_seconds:
            rendered = self._render("progress")
            LOGGER.info(rendered)
            if self._visible and self.reporter.enabled and self._bar is None:
                self.reporter._write(rendered)
            self._last_report = now

    def close(self, *, status: str = "complete") -> None:
        """Close the task and report final *status* once."""

        if self._closed:
            return
        self._closed = True
        if self._bar is not None:
            self._bar.close()
        elif self._visible and self.reporter.enabled:
            self.reporter._write(self._render(status))
        LOGGER.info(self._render(status))

    def __enter__(self) -> Self:
        """Return this task for use in a context manager."""

        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Close with success or failure from *exc_type*, *exc*, and *traceback*."""

        del exc, traceback
        self.close(status="failed" if exc_type is not None else "complete")


class ProgressReporter:
    """Display progress bars when available and emit standard logging events."""

    def __init__(
        self,
        *,
        enabled: bool = True,
        use_bars: bool = True,
        stream: TextIO | None = None,
        text_interval_seconds: float = 5.0,
    ) -> None:
        """Configure progress output.

        *enabled* controls direct display, *use_bars* enables optional tqdm,
        *stream* receives fallback text, and *text_interval_seconds* limits
        fallback update frequency.
        """

        if text_interval_seconds <= 0:
            raise ValueError("text_interval_seconds must be positive")
        self.enabled = enabled
        self.use_bars = use_bars
        self.stream = stream or sys.stderr
        self.text_interval_seconds = text_interval_seconds
        self._lock = threading.Lock()
        self._tqdm = self._load_tqdm() if enabled and use_bars else None

    @staticmethod
    def _load_tqdm() -> Any | None:
        """Return ``tqdm.auto.tqdm`` when the optional package is installed."""

        try:
            from tqdm.auto import tqdm
        except ImportError:
            return None
        return tqdm

    def _create_bar(self, description: str, *, total: float | None, unit: str) -> Any | None:
        """Create a tqdm bar for *description*, *total*, and *unit* when enabled."""

        if self._tqdm is None:
            return None
        return self._tqdm(
            total=total,
            desc=description,
            unit=unit,
            dynamic_ncols=True,
            leave=True,
            file=self.stream,
            unit_scale=False,
        )

    def _write(self, message: str) -> None:
        """Write one thread-safe progress *message* to the configured stream."""

        with self._lock:
            if self._tqdm is not None:
                self._tqdm.write(message, file=self.stream)
            else:
                print(message, file=self.stream, flush=True)

    def message(self, message: str, *, level: int = logging.INFO) -> None:
        """Log *message* at *level* and display it when progress is enabled."""

        LOGGER.log(level, message)
        if self.enabled and progress_level_enabled(level):
            self._write(message)

    @contextmanager
    def minimum_level(self, level: int) -> Iterator[None]:
        """Temporarily hide console events below *level* while retaining log records."""

        current = _MINIMUM_CONSOLE_LEVEL.get()
        token = _MINIMUM_CONSOLE_LEVEL.set(max(current, level))
        try:
            yield
        finally:
            _MINIMUM_CONSOLE_LEVEL.reset(token)

    def cache_summary(
        self,
        description: str,
        *,
        cached: int,
        missing: int,
        unit: str = "items",
    ) -> None:
        """Report cached and missing counts for *description* in *unit*."""

        self.message(
            f"{description}: {cached:,} {unit} loaded from cache; {missing:,} {unit} require work"
        )

    def network_summary(
        self,
        description: str,
        *,
        cached: int,
        to_fetch: int,
        unit: str = "request batches",
    ) -> None:
        """Report *cached* and *to_fetch* counts for *description* in *unit*."""

        self.message(
            f"{description}: {cached:,} {unit} loaded from raw cache; "
            f"{to_fetch:,} {unit} will be fetched from NCBI"
        )

    def task(
        self, description: str, *, total: float | None = None, unit: str = "items"
    ) -> ProgressTask:
        """Create a progress task for *description*, optional *total*, and *unit*."""

        return ProgressTask(self, description, total=total, unit=unit)

    def track(
        self,
        items: Iterable[T],
        description: str,
        *,
        total: float | None = None,
        unit: str = "items",
    ) -> Iterator[T]:
        """Yield *items* while tracking *description*, *total*, and *unit*."""

        if total is None:
            try:
                total = len(items)  # type: ignore[arg-type]
            except TypeError:
                pass
        with self.task(description, total=total, unit=unit) as task:
            for item in items:
                yield item
                task.update()


NULL_PROGRESS = ProgressReporter(enabled=False, use_bars=False)


def get_progress(progress: ProgressReporter | None) -> ProgressReporter:
    """Return *progress* or the shared disabled reporter when it is ``None``."""

    return progress or NULL_PROGRESS
