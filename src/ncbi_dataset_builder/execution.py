from __future__ import annotations

import re
import shlex
import sys
from collections.abc import Callable, Iterable
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from pathlib import Path
from typing import Generic, TypeVar

from .commands import CommandRunner
from .models import ResourceSpec
from .util import atomic_write_text

T = TypeVar("T")
R = TypeVar("R")


class LocalExecutor(Generic[T, R]):
    def __init__(self, *, max_workers: int = 1, total_threads: int | None = None) -> None:
        if max_workers < 1:
            raise ValueError("max_workers must be positive")
        self.max_workers = max_workers
        self.total_threads = total_threads

    def map(
        self, items: Iterable[T], function: Callable[[T], R], *, threads_per_task: int
    ) -> list[R]:
        materialized = list(items)
        if not materialized:
            return []
        workers = self.max_workers
        if self.total_threads is not None:
            workers = min(workers, max(1, self.total_threads // max(1, threads_per_task)))
        results: dict[int, R] = {}
        with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="dataset-task") as pool:
            futures: dict[Future[R], int] = {
                pool.submit(function, item): index for index, item in enumerate(materialized)
            }
            for future in as_completed(futures):
                results[futures[future]] = future.result()
        return [results[index] for index in range(len(materialized))]


@dataclass(frozen=True)
class SlurmOptions:
    resources: ResourceSpec = field(default_factory=ResourceSpec)
    max_parallel: int | None = None
    partition: str | None = None
    account: str | None = None
    qos: str | None = None

    def __post_init__(self) -> None:
        if self.max_parallel is not None and self.max_parallel < 1:
            raise ValueError("max_parallel must be positive")
        for name, value in (
            ("partition", self.partition),
            ("account", self.account),
            ("qos", self.qos),
        ):
            if value is not None and not re.fullmatch(r"[A-Za-z0-9_.-]+", value):
                raise ValueError(f"Unsafe or invalid Slurm {name}: {value!r}")


class SlurmExecutor:
    """Generate and submit a real sbatch job array, one durable task per index."""

    def __init__(
        self,
        *,
        runner: CommandRunner | None = None,
        python_executable: str | None = None,
    ) -> None:
        self.runner = runner or CommandRunner()
        self.python_executable = python_executable or sys.executable

    def create_script(
        self,
        *,
        plan_path: Path,
        task_count: int,
        processor_reference: str,
        workspace: Path,
        email: str | None,
        output_path: Path,
        options: SlurmOptions,
        retry_failed: bool = False,
        task_indices: list[int] | None = None,
    ) -> Path:
        if task_count < 1:
            raise ValueError("Cannot create a Slurm array for an empty plan")
        resources = options.resources
        indices = list(range(task_count)) if task_indices is None else sorted(set(task_indices))
        if not indices or indices[0] < 0 or indices[-1] >= task_count:
            raise ValueError("Slurm task indices must be a non-empty subset of the plan")
        array = self._array_spec(indices)
        if options.max_parallel:
            array += f"%{options.max_parallel}"
        logs = workspace / "logs" / "slurm"
        lines = [
            "#!/usr/bin/env bash",
            "#SBATCH --job-name=ncbi-dataset",
            f"#SBATCH --array={array}",
            f"#SBATCH --cpus-per-task={resources.threads}",
            f"#SBATCH --mem={resources.memory_gb}G",
            f"#SBATCH --time={resources.time_limit}",
            f"#SBATCH --output={shlex.quote(str(logs / '%A_%a.out'))}",
            f"#SBATCH --error={shlex.quote(str(logs / '%A_%a.err'))}",
        ]
        for flag, value in (
            ("partition", options.partition),
            ("account", options.account),
            ("qos", options.qos),
        ):
            if value:
                lines.append(f"#SBATCH --{flag}={value}")
        command = [
            self.python_executable,
            "-m",
            "ncbi_dataset_builder.worker",
            "--plan",
            str(plan_path.resolve()),
            "--task-index",
            "${SLURM_ARRAY_TASK_ID}",
            "--processor",
            processor_reference,
            "--workspace",
            str(workspace.resolve()),
        ]
        if email:
            command.extend(("--email", email))
        if retry_failed:
            command.append("--retry-failed")
        rendered = " ".join(
            item if item == "${SLURM_ARRAY_TASK_ID}" else shlex.quote(item) for item in command
        )
        lines.extend(
            (
                "",
                "set -euo pipefail",
                f"mkdir -p {shlex.quote(str(logs))}",
                rendered,
                "",
            )
        )
        atomic_write_text(output_path, "\n".join(lines))
        return output_path

    @staticmethod
    def _array_spec(indices: list[int]) -> str:
        ranges: list[str] = []
        start = previous = indices[0]
        for index in indices[1:]:
            if index == previous + 1:
                previous = index
                continue
            ranges.append(str(start) if start == previous else f"{start}-{previous}")
            start = previous = index
        ranges.append(str(start) if start == previous else f"{start}-{previous}")
        return ",".join(ranges)

    def submit(self, script: Path) -> str:
        self.runner.require("sbatch")
        completed = self.runner.run(["sbatch", "--parsable", str(script)])
        job_id = (completed.stdout or "").strip().split(";", 1)[0]
        if not job_id:
            raise RuntimeError(f"sbatch returned no job id: {completed.stdout!r}")
        return job_id
