from __future__ import annotations

import logging
import os
import shutil
import subprocess
import time
from collections.abc import Mapping, Sequence
from pathlib import Path

from ..errors import ExternalToolError
from .unit_logging import write_unit_output

LOGGER = logging.getLogger("ncbi_dataset_builder.commands")


class CommandRunner:
    """Run external tools without a shell, adding optional base environment values."""

    def __init__(self, *, base_env: Mapping[str, str] | None = None) -> None:
        """Store *base_env* values that every child command should receive."""

        self.base_env = dict(base_env or {})

    def which(self, executable: str) -> str | None:
        """Return the resolved path for *executable*, or ``None`` when absent."""

        return shutil.which(executable)

    def require(self, *executables: str) -> None:
        """Raise when any name in *executables* is unavailable on ``PATH``."""

        missing = [name for name in executables if self.which(name) is None]
        if missing:
            raise ExternalToolError(
                "Missing required executable(s): "
                + ", ".join(missing)
                + ". Install them or provide a CommandRunner configured for your environment."
            )

    def run(
        self,
        command: Sequence[str | os.PathLike[str]],
        *,
        cwd: Path | None = None,
        env: Mapping[str, str] | None = None,
        timeout: float | None = None,
        capture_output: bool = True,
        text: bool = True,
        check: bool = True,
        stdout=None,
        stderr=None,
    ) -> subprocess.CompletedProcess:
        """Run *command* with explicit process controls.

        *cwd* selects the directory, *env* adds variables, *timeout* bounds the
        run, *capture_output* and *text* control returned output, *check* raises
        on failure, and *stdout*/*stderr* may provide explicit streams.
        """

        rendered = [str(item) for item in command]
        if not rendered:
            raise ValueError("Command cannot be empty")
        merged_env = os.environ.copy()
        merged_env.update(self.base_env)
        if env:
            merged_env.update(env)
        if stdout is not None or stderr is not None:
            capture_output = False
        LOGGER.info("Run external command: %s", rendered)
        started = time.monotonic()
        try:
            completed = subprocess.run(
                rendered,
                cwd=cwd,
                env=merged_env,
                timeout=timeout,
                capture_output=capture_output,
                text=text,
                check=False,
                stdout=stdout,
                stderr=stderr,
            )
        except FileNotFoundError as exc:
            raise ExternalToolError(f"Executable not found: {rendered[0]}") from exc
        except subprocess.TimeoutExpired as exc:
            raise ExternalToolError(f"Command timed out after {timeout}s: {rendered!r}") from exc
        write_unit_output(f"command stdout: {' '.join(rendered)}", completed.stdout)
        write_unit_output(f"command stderr: {' '.join(rendered)}", completed.stderr)
        if check and completed.returncode != 0:
            error = completed.stderr if isinstance(completed.stderr, str) else ""
            output = completed.stdout if isinstance(completed.stdout, str) else ""
            detail = (error or output).strip()[-4000:]
            raise ExternalToolError(
                f"Command failed with exit code {completed.returncode}: {rendered!r}"
                + (f"\n{detail}" if detail else "")
            )
        LOGGER.info(
            "External command finished in %.1fs with exit code %d: %s",
            time.monotonic() - started,
            completed.returncode,
            rendered[0],
        )
        return completed

    def version(self, executable: str, *arguments: str) -> str:
        """Return the first version-output line from *executable* and *arguments*."""

        completed = self.run([executable, *(arguments or ("--version",))])
        value = (completed.stdout or completed.stderr or "").strip().splitlines()
        return value[0] if value else "unknown"
