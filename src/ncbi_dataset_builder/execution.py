from __future__ import annotations

import math
import re
import shlex
import sys
from collections.abc import Callable, Iterable
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from pathlib import Path
from typing import Generic, Literal, TypeVar

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
    """Configure single-node or quota-aware distributed Slurm execution.

    Args:
        resources: CPU, memory, and time requested per task.
        max_parallel: Optional maximum simultaneous units inside the coordinator.
        partition: Optional Slurm partition name.
        account: Optional allocation account.
        qos: Optional quality-of-service name.
        mode: ``single_node`` or quota-aware ``distributed`` execution.
        total_cpu_quota: Maximum CPUs used across coordinator and worker jobs.
        max_running_jobs: Maximum running coordinator and worker jobs.
        coordinator_cpus: CPUs reserved for the distributed coordinator.
        coordinator_memory_gb: Memory reserved for the coordinator in GB.
        coordinator_time_limit: Slurm wall time for the complete coordinator.
        cpus_per_node: Optional CPU capacity used to reject impossible requests.
    """

    resources: ResourceSpec = field(default_factory=ResourceSpec)
    max_parallel: int | None = None
    partition: str | None = None
    account: str | None = None
    qos: str | None = None
    mode: Literal["single_node", "distributed"] = "single_node"
    total_cpu_quota: int | None = None
    max_running_jobs: int | None = None
    coordinator_cpus: int = 1
    coordinator_memory_gb: int = 4
    coordinator_time_limit: str = "7-00:00:00"
    cpus_per_node: int | None = None

    def __post_init__(self) -> None:
        """Validate positive concurrency and shell-safe scheduler identifiers."""

        if self.max_parallel is not None and self.max_parallel < 1:
            raise ValueError("max_parallel must be positive")
        if self.mode not in {"single_node", "distributed"}:
            raise ValueError(f"Unknown Slurm mode: {self.mode!r}")
        if self.coordinator_cpus < 1 or self.coordinator_memory_gb < 1:
            raise ValueError("Coordinator CPU and memory must be positive")
        if self.cpus_per_node is not None and self.cpus_per_node < 1:
            raise ValueError("cpus_per_node must be positive")
        if (
            self.cpus_per_node is not None
            and self.coordinator_cpus > self.cpus_per_node
        ):
            raise ValueError(
                f"Coordinator request exceeds cpus_per_node={self.cpus_per_node}"
            )
        if not re.fullmatch(r"[0-9:-]+", self.coordinator_time_limit):
            raise ValueError(
                f"Unsafe or invalid coordinator time: {self.coordinator_time_limit!r}"
            )
        if self.partition is not None:
            partitions = self.partition.split(",")
            if not partitions or any(
                not re.fullmatch(r"[A-Za-z0-9_.-]+", value) for value in partitions
            ):
                raise ValueError(f"Unsafe or invalid Slurm partition: {self.partition!r}")
        for name, value in (("account", self.account), ("qos", self.qos)):
            if value is not None and not re.fullmatch(r"[A-Za-z0-9_.-]+", value):
                raise ValueError(f"Unsafe or invalid Slurm {name}: {value!r}")
        if self.mode == "distributed":
            if self.total_cpu_quota is None or self.total_cpu_quota < 2:
                raise ValueError("Distributed Slurm mode requires total_cpu_quota >= 2")
            if self.max_running_jobs is None or self.max_running_jobs < 2:
                raise ValueError("Distributed Slurm mode requires max_running_jobs >= 2")
            if self.coordinator_cpus >= self.total_cpu_quota:
                raise ValueError("Coordinator CPUs must be below total_cpu_quota")

    def worker_parallelism(self, threads_per_unit: int) -> int:
        """Return worker slots allowed for *threads_per_unit* by all configured quotas."""

        if threads_per_unit < 1:
            raise ValueError("threads_per_unit must be positive")
        if self.cpus_per_node is not None and threads_per_unit > self.cpus_per_node:
            raise ValueError(
                f"A {threads_per_unit}-CPU unit exceeds cpus_per_node={self.cpus_per_node}"
            )
        if self.mode != "distributed":
            return self.max_parallel or 1
        assert self.total_cpu_quota is not None
        assert self.max_running_jobs is not None
        by_cpu = (self.total_cpu_quota - self.coordinator_cpus) // threads_per_unit
        by_jobs = self.max_running_jobs - 1
        workers = min(by_cpu, by_jobs)
        if self.max_parallel is not None:
            workers = min(workers, self.max_parallel)
        if workers < 1:
            raise ValueError(
                f"CPU quota cannot satisfy one {threads_per_unit}-CPU worker after reserving "
                f"{self.coordinator_cpus} coordinator CPUs"
            )
        return workers


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
        cleanup: str = "after_success",
        keep_failed_inputs: bool = True,
        fsync_logs: bool = True,
    ) -> Path:
        """Write a Slurm array script for selected tasks in a saved plan.

        *plan_path* identifies the saved plan, *task_count* bounds valid indices,
        *processor_reference* names the importable processor, *workspace* stores
        logs and state, *email* configures NCBI access, *output_path* receives the
        script, and *options* supplies scheduler settings. *retry_failed* enables
        failed-task retries; *task_indices* optionally submits a subset.
        *cleanup*, *keep_failed_inputs*, and *fsync_logs* configure worker cleanup
        and logging behavior.
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
            f"#SBATCH --output={shlex.quote(str(logs / '%A.batch.log'))}",
            f"#SBATCH --error={shlex.quote(str(logs / '%A.batch.log'))}",
            "#SBATCH --open-mode=append",
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
        command.extend(("--cleanup", cleanup))
        if not keep_failed_inputs:
            command.append("--discard-failed-inputs")
        if not fsync_logs:
            command.append("--no-fsync-logs")
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

    def create_dispatcher_script(
        self,
        *,
        plan_path: Path,
        processor_reference: str,
        workspace: Path,
        email: str | None,
        output_path: Path,
        options: SlurmOptions,
        prefetch_batches: int,
        max_staged_gb: float | None,
        minimum_free_gb: float,
        cleanup: str,
        keep_failed_inputs: bool,
        fsync_logs: bool,
        retry_failed: bool = False,
        batch_ids: set[int] | None = None,
    ) -> Path:
        """Write a quota-aware dispatcher for *plan_path* and *processor_reference*.

        *workspace*, *email*, and *output_path* configure execution paths;
        *options* supplies Slurm limits; *prefetch_batches*, *max_staged_gb*,
        *minimum_free_gb*, *cleanup*, *keep_failed_inputs*, and *fsync_logs*
        configure storage and logs. *retry_failed* and *batch_ids* select work.
        """

        if options.mode != "distributed":
            raise ValueError("Dispatcher scripts require distributed Slurm mode")
        logs = workspace / "logs" / "slurm"
        lines = [
            "#!/usr/bin/env bash",
            "#SBATCH --job-name=ncbi-dispatch",
            f"#SBATCH --cpus-per-task={options.coordinator_cpus}",
            f"#SBATCH --mem={options.coordinator_memory_gb}G",
            f"#SBATCH --time={options.coordinator_time_limit}",
            f"#SBATCH --output={shlex.quote(str(logs / '%j.coordinator.log'))}",
            f"#SBATCH --error={shlex.quote(str(logs / '%j.coordinator.log'))}",
            "#SBATCH --open-mode=append",
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
            "ncbi_dataset_builder.slurm_dispatcher",
            "--plan",
            str(plan_path.resolve()),
            "--processor",
            processor_reference,
            "--workspace",
            str(workspace.resolve()),
            "--total-cpu-quota",
            str(options.total_cpu_quota),
            "--max-running-jobs",
            str(options.max_running_jobs),
            "--coordinator-cpus",
            str(options.coordinator_cpus),
            "--prefetch-batches",
            str(prefetch_batches),
            "--minimum-free-gb",
            str(minimum_free_gb),
            "--cleanup",
            cleanup,
        ]
        if options.max_parallel is not None:
            command.extend(("--max-parallel", str(options.max_parallel)))
        if options.cpus_per_node is not None:
            command.extend(("--cpus-per-node", str(options.cpus_per_node)))
        if max_staged_gb is not None:
            command.extend(("--max-staged-gb", str(max_staged_gb)))
        if options.partition:
            command.extend(("--partition", options.partition))
        if options.account:
            command.extend(("--account", options.account))
        if options.qos:
            command.extend(("--qos", options.qos))
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
        self.progress.message(f"Slurm dispatcher script created: {output_path}")
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
