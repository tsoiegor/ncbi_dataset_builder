from __future__ import annotations

import math
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
from .progress import ProgressReporter, get_progress
from .util import atomic_write_text

T = TypeVar("T")
R = TypeVar("R")


class LocalExecutor(Generic[T, R]):
    """Run independent items concurrently while respecting a total thread budget."""

    def __init__(
        self,
        *,
        max_workers: int = 1,
        total_threads: int | None = None,
        total_memory_gb: float | None = None,
        progress: ProgressReporter | None = None,
    ) -> None:
        """Set worker limits and optional *progress* reporting.

        *max_workers* bounds tasks, *total_threads* is the process-wide CPU
        budget, and *total_memory_gb* is the process-wide memory budget.
        """

        if max_workers < 1:
            raise ValueError("max_workers must be positive")
        self.max_workers = max_workers
        self.total_threads = total_threads
        self.total_memory_gb = total_memory_gb
        if total_memory_gb is not None and total_memory_gb <= 0:
            raise ValueError("total_memory_gb must be positive")
        self.progress = get_progress(progress)

    def map(
        self,
        items: Iterable[T],
        function: Callable[[T], R],
        *,
        threads_per_task: int,
        memory_gb_per_task: float = 1.0,
        on_result: Callable[[T, R], None] | None = None,
    ) -> list[R]:
        """Apply *function* to *items*, budgeting *threads_per_task* each.

        *memory_gb_per_task* reserves memory per concurrent item. Results retain
        input order. Optional *on_result* runs in the caller thread as each item
        completes.
        """

        materialized = list(items)
        if not materialized:
            return []
        if threads_per_task < 1:
            raise ValueError("threads_per_task must be positive")
        workers = self.max_workers
        if self.total_threads is not None:
            if self.total_threads < threads_per_task:
                raise ValueError(
                    f"total_threads={self.total_threads} cannot satisfy a "
                    f"{threads_per_task}-thread task"
                )
            workers = min(workers, self.total_threads // max(1, threads_per_task))
        if memory_gb_per_task <= 0:
            raise ValueError("memory_gb_per_task must be positive")
        if self.total_memory_gb is not None:
            if self.total_memory_gb < memory_gb_per_task:
                raise ValueError(
                    f"total_memory_gb={self.total_memory_gb:g} cannot satisfy a "
                    f"{memory_gb_per_task:g} GB task"
                )
            workers = min(
                workers,
                int(self.total_memory_gb // memory_gb_per_task),
            )
        results: dict[int, R] = {}
        with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="dataset-task") as pool:
            futures: dict[Future[R], int] = {
                pool.submit(function, item): index for index, item in enumerate(materialized)
            }
            with self.progress.task(
                "Execute local dataset tasks", total=len(futures), unit="tasks"
            ) as progress:
                for future in as_completed(futures):
                    index = futures[future]
                    result = future.result()
                    results[index] = result
                    if on_result is not None:
                        on_result(materialized[index], result)
                    progress.update()
        return [results[index] for index in range(len(materialized))]


@dataclass(frozen=True)
class SlurmOptions:
    """Configure one Slurm allocation or legacy array submission.

    Args:
        resources: CPU, memory, and time requested per task.
        max_parallel: Optional maximum simultaneous units inside the coordinator.
        partition: Optional Slurm partition name.
        account: Optional allocation account.
        qos: Optional quality-of-service name.
    """

    resources: ResourceSpec = field(default_factory=ResourceSpec)
    max_parallel: int | None = None
    partition: str | None = None
    account: str | None = None
    qos: str | None = None

    def __post_init__(self) -> None:
        """Validate positive concurrency and shell-safe scheduler identifiers."""

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
    """Generate and submit Slurm scripts for durable dataset execution."""

    def __init__(
        self,
        *,
        runner: CommandRunner | None = None,
        python_executable: str | None = None,
        progress: ProgressReporter | None = None,
    ) -> None:
        """Use optional *runner*, *python_executable*, and *progress* reporter."""

        self.runner = runner or CommandRunner()
        self.python_executable = python_executable or sys.executable
        self.progress = get_progress(progress)

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
        """Write a Slurm array script for selected tasks in a saved plan.

        *plan_path* identifies the saved plan, *task_count* bounds valid indices,
        *processor_reference* names the importable processor, *workspace* stores
        logs and state, *email* configures NCBI access, *output_path* receives the
        script, and *options* supplies scheduler settings. *retry_failed* enables
        failed-task retries; *task_indices* optionally submits a subset.
        """

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
        self.progress.message(f"Slurm script created: {output_path}")
        return output_path

    @staticmethod
    def _array_spec(indices: list[int]) -> str:
        """Compress sorted array *indices* into Slurm range syntax."""

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
        """Submit *script* with ``sbatch`` and return the scheduler job ID."""

        self.runner.require("sbatch")
        self.progress.message(f"Submit Slurm script: {script}")
        completed = self.runner.run(["sbatch", "--parsable", str(script)])
        job_id = (completed.stdout or "").strip().split(";", 1)[0]
        if not job_id:
            raise RuntimeError(f"sbatch returned no job id: {completed.stdout!r}")
        self.progress.message(f"Slurm job submitted: {job_id}")
        return job_id

    def create_coordinator_script(
        self,
        *,
        plan_path: Path,
        processor_reference: str,
        workspace: Path,
        email: str | None,
        output_path: Path,
        options: SlurmOptions,
        max_workers: int,
        total_threads: int,
        total_memory_gb: float,
        prefetch_batches: int,
        max_staged_gb: float | None,
        minimum_free_gb: float,
        cleanup: str,
        keep_failed_inputs: bool,
        fsync_logs: bool,
        retry_failed: bool = False,
        batch_ids: set[int] | None = None,
    ) -> Path:
        """Write one coordinator job that preserves bounded batch ordering.

        *plan_path* and *processor_reference* select work, *workspace* stores
        durable state, *email* configures NCBI, and *output_path* receives the
        script. *options* supplies scheduler flags. *max_workers*,
        *total_threads*, and *total_memory_gb* control allocation-wide resources.
        *prefetch_batches*, *max_staged_gb*, *minimum_free_gb*, *cleanup*,
        *keep_failed_inputs*, and *fsync_logs* configure the pipeline.
        *retry_failed* enables retries and *batch_ids* optionally limits batches.
        """

        if max_workers < 1 or total_threads < 1 or total_memory_gb <= 0:
            raise ValueError("Coordinator workers, threads, and memory must be positive")
        logs = workspace / "logs" / "slurm"
        lines = [
            "#!/usr/bin/env bash",
            "#SBATCH --job-name=ncbi-dataset",
            f"#SBATCH --cpus-per-task={total_threads}",
            f"#SBATCH --mem={math.ceil(total_memory_gb)}G",
            f"#SBATCH --time={options.resources.time_limit}",
            f"#SBATCH --output={shlex.quote(str(logs / '%j.coordinator.log'))}",
            f"#SBATCH --error={shlex.quote(str(logs / '%j.coordinator.log'))}",
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
            "ncbi_dataset_builder.pipeline_worker",
            "--plan",
            str(plan_path.resolve()),
            "--processor",
            processor_reference,
            "--workspace",
            str(workspace.resolve()),
            "--max-workers",
            str(max_workers),
            "--total-threads",
            str(total_threads),
            "--total-memory-gb",
            str(total_memory_gb),
            "--prefetch-batches",
            str(prefetch_batches),
            "--minimum-free-gb",
            str(minimum_free_gb),
            "--cleanup",
            cleanup,
        ]
        if max_staged_gb is not None:
            command.extend(("--max-staged-gb", str(max_staged_gb)))
        if email:
            command.extend(("--email", email))
        if retry_failed:
            command.append("--retry-failed")
        if not keep_failed_inputs:
            command.append("--discard-failed-inputs")
        if not fsync_logs:
            command.append("--no-fsync-logs")
        for batch_id in sorted(batch_ids or ()):
            command.extend(("--batch-id", str(batch_id)))
        rendered = " ".join(shlex.quote(item) for item in command)
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
        self.progress.message(f"Slurm coordinator script created: {output_path}")
        return output_path
