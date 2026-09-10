from __future__ import annotations

import argparse
import os
import re
import shutil
import subprocess
from pathlib import Path

from .execution import SlurmExecutor, SlurmOptions
from .models import DatasetTask, WorkspaceJob
from .pipeline import PipelinePolicy
from .util import exclusive_file_lock, sanitize_identifier
from .workflow import BuilderConfig, DatasetBuilder


def _selected_batches(job: WorkspaceJob, batch_ids: set[int] | None) -> list[int]:
    """Return ordered batch IDs in *job*, optionally restricted by *batch_ids*."""

    available = {task.batch_id for task in job.tasks}
    if batch_ids is not None:
        missing = batch_ids - available
        if missing:
            raise ValueError(f"Job has no requested batches: {sorted(missing)}")
        available &= batch_ids
    if not available:
        raise ValueError("No batches were selected for distributed execution")
    return sorted(available)


def _batch_tasks(job: WorkspaceJob, batch_id: int) -> list[tuple[int, DatasetTask]]:
    """Return global task indices and tasks from *job* belonging to *batch_id*."""

    return [(index, task) for index, task in enumerate(job.tasks) if task.batch_id == batch_id]


def _start_array(script: Path) -> subprocess.Popen[str]:
    """Submit worker-array *script* asynchronously and return its waiting process."""

    executable = shutil.which("sbatch")
    if executable is None:
        raise RuntimeError("Required executable is not available: sbatch")
    return subprocess.Popen(
        [executable, "--parsable", "--wait", str(script)],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )


def _finish_array(process: subprocess.Popen[str], batch_id: int) -> tuple[str | None, int]:
    """Wait for *process* from *batch_id* and return its job ID and exit code."""

    output, _ = process.communicate()
    if output:
        print(output, end="" if output.endswith("\n") else "\n", flush=True)
    job_ids = re.findall(r"(?m)^\s*(\d+)(?:;[^\s]+)?\s*$", output or "")
    job_id = job_ids[0] if job_ids else None
    if job_id is None:
        raise RuntimeError(f"Slurm did not accept worker array for batch {batch_id}")
    return job_id, process.returncode


def _validate_uniform_resources(tasks: list[tuple[int, DatasetTask]], batch_id: int) -> None:
    """Require all *tasks* in *batch_id* to use one Slurm resource specification."""

    resources = {task.resources for _, task in tasks}
    if len(resources) != 1:
        raise ValueError(
            f"Distributed batch {batch_id} has heterogeneous resources; "
            "submit separate jobs or resource classes"
        )


def main(argv: list[str] | None = None) -> int:
    """Dispatch quota-aware batch arrays from optional command-line *argv*."""

    parser = argparse.ArgumentParser(
        description="Stage bounded batches and dispatch one quota-limited Slurm array per batch"
    )
    parser.add_argument("--job", type=Path, required=True)
    parser.add_argument("--processor", required=True)
    parser.add_argument("--workspace", type=Path, required=True)
    parser.add_argument("--email", default=os.environ.get("NCBI_EMAIL"))
    parser.add_argument("--partition")
    parser.add_argument("--account")
    parser.add_argument("--qos")
    parser.add_argument("--total-cpu-quota", type=int, required=True)
    parser.add_argument("--max-running-jobs", type=int, required=True)
    parser.add_argument("--coordinator-cpus", type=int, default=1)
    parser.add_argument("--cpus-per-node", type=int)
    parser.add_argument("--max-parallel", type=int)
    parser.add_argument("--prefetch-batches", type=int, choices=(0, 1), default=1)
    parser.add_argument("--max-staged-gb", type=float)
    parser.add_argument("--minimum-free-gb", type=float, default=0.0)
    parser.add_argument("--cleanup", choices=("after_success", "never"), default="after_success")
    parser.add_argument("--discard-failed-inputs", action="store_true")
    parser.add_argument("--no-fsync-logs", action="store_true")
    parser.add_argument("--retry-failed", action="store_true")
    parser.add_argument("--batch-id", type=int, action="append")
    parser.add_argument("--prefetch-max-size", default="u")
    parser.add_argument("--download-workers", type=int, default=2)
    arguments = parser.parse_args(argv)

    policy = PipelinePolicy(
        prefetch_batches=arguments.prefetch_batches,
        max_staged_gb=arguments.max_staged_gb,
        minimum_free_gb=arguments.minimum_free_gb,
        cleanup=arguments.cleanup,
        keep_failed_inputs=not arguments.discard_failed_inputs,
        fsync_logs=not arguments.no_fsync_logs,
        download_workers=arguments.download_workers,
    )
    builder = DatasetBuilder(
        BuilderConfig(
            workspace=arguments.workspace,
            email=arguments.email,
            ncbi_api_key=os.environ.get("NCBI_API_KEY"),
            pipeline_policy=policy,
            prefetch_max_size=arguments.prefetch_max_size,
            progress_bars=False,
        )
    )
    job = builder.load_job(arguments.job)
    batches = _selected_batches(
        job, set(arguments.batch_id) if arguments.batch_id is not None else None
    )
    all_tasks = [item for batch_id in batches for item in _batch_tasks(job, batch_id)]
    resources = {task.resources for _, task in all_tasks}
    if len(resources) != 1:
        raise ValueError(
            "Distributed execution currently requires one ResourceSpec across selected tasks"
        )
    per_unit = next(iter(resources))
    quota = SlurmOptions(
        resources=per_unit,
        max_parallel=arguments.max_parallel,
        partition=arguments.partition,
        account=arguments.account,
        qos=arguments.qos,
        mode="distributed",
        total_cpu_quota=arguments.total_cpu_quota,
        max_running_jobs=arguments.max_running_jobs,
        coordinator_cpus=arguments.coordinator_cpus,
        cpus_per_node=arguments.cpus_per_node,
    )
    worker_slots = quota.worker_parallelism(per_unit.threads)
    print(
        f"Distributed Slurm: {worker_slots} worker jobs x {per_unit.threads} CPUs; "
        f"quota {arguments.total_cpu_quota} CPUs/{arguments.max_running_jobs} jobs",
        flush=True,
    )
    lock = arguments.workspace / "state" / "pipeline-coordinator.lock"
    executor = SlurmExecutor()
    with exclusive_file_lock(
        lock,
        timeout_seconds=5,
        stale_after_seconds=5 * 60,
        heartbeat_seconds=30,
    ):
        current = builder.prefetch_batch(
            job,
            batches[0],
            retry_failed=arguments.retry_failed,
            policy=policy,
        )
        failed = False
        for position, batch_id in enumerate(batches):
            indexed_tasks = _batch_tasks(job, batch_id)
            _validate_uniform_resources(indexed_tasks, batch_id)
            worker_options = SlurmOptions(
                resources=indexed_tasks[0][1].resources,
                max_parallel=worker_slots,
                partition=arguments.partition,
                account=arguments.account,
                qos=arguments.qos,
            )
            worker_script = (
                arguments.workspace
                / "slurm"
                / sanitize_identifier(job.job_id)
                / f"batch-{batch_id:06d}.sbatch"
            )
            executor.create_script(
                job_path=arguments.job,
                task_count=len(job.tasks),
                processor_reference=arguments.processor,
                workspace=arguments.workspace,
                email=arguments.email,
                output_path=worker_script,
                options=worker_options,
                retry_failed=arguments.retry_failed,
                task_indices=[index for index, _ in indexed_tasks],
                cleanup=policy.cleanup,
                keep_failed_inputs=policy.keep_failed_inputs,
                fsync_logs=policy.fsync_logs,
                prefetch_max_size=arguments.prefetch_max_size,
            )
            process = _start_array(worker_script)
            next_manifest = None
            stage_error: BaseException | None = None
            if position + 1 < len(batches) and policy.prefetch_batches == 1:
                try:
                    next_manifest = builder.prefetch_batch(
                        job,
                        batches[position + 1],
                        retry_failed=arguments.retry_failed,
                        policy=policy,
                        occupied_size_gb=current.staged_size_gb,
                    )
                except Exception as exc:  # noqa: BLE001 - wait for submitted workers
                    stage_error = exc
            job_id, return_code = _finish_array(process, batch_id)
            print(
                f"Batch {batch_id} worker array {job_id} exited with code {return_code}",
                flush=True,
            )
            finalized = builder.finalize_distributed_batch(job, batch_id)
            failed = failed or return_code != 0 or any(
                status != "succeeded" for status in finalized.task_statuses.values()
            )
            if stage_error is not None:
                raise stage_error
            if position + 1 < len(batches):
                current = next_manifest or builder.prefetch_batch(
                    job,
                    batches[position + 1],
                    retry_failed=arguments.retry_failed,
                    policy=policy,
                )
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
