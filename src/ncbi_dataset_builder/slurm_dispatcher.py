from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
import time
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from dataclasses import dataclass, replace
from pathlib import Path

from .execution import SlurmExecutor, SlurmOptions
from .models import DatasetTask, StagedFastq, WorkspaceJob
from .pipeline import PipelinePolicy, free_space_gb, path_size_gb
from .util import exclusive_file_lock, sanitize_identifier
from .workflow import BuilderConfig, DatasetBuilder


@dataclass(frozen=True)
class _ReadyUnit:
    """Hold a staged job *index*, its *task*, and measured *staged_size_gb*."""

    index: int
    task: DatasetTask
    staged_size_gb: float


@dataclass
class _RunningUnit:
    """Track one submitted Slurm unit.

    Args:
        index: Position in the workspace job.
        task: Original task with its minimum resources.
        slurm_job_id: Submitted Slurm job identifier.
        threads: Concrete CPU allocation.
        staged_size_gb: Measured or estimated staged size in GB.
        submitted_epoch: Submission time as Unix seconds.
        missing_since: First time the job disappeared from ``squeue``.
    """

    index: int
    task: DatasetTask
    slurm_job_id: str
    threads: int
    staged_size_gb: float
    submitted_epoch: float
    missing_since: float | None = None


def _selected_tasks(
    job: WorkspaceJob,
    batch_ids: set[int] | None,
) -> list[tuple[int, DatasetTask]]:
    """Return ordered tasks from *job*, optionally limited by *batch_ids*."""

    available = {task.batch_id for task in job.tasks}
    if batch_ids is not None:
        missing = batch_ids - available
        if missing:
            raise ValueError(f"Job has no requested batches: {sorted(missing)}")
    selected = [
        (index, task)
        for index, task in enumerate(job.tasks)
        if batch_ids is None or task.batch_id in batch_ids
    ]
    if not selected:
        raise ValueError("No tasks were selected for distributed execution")
    return selected


def _fits_window(
    occupied_gb: float,
    candidate_gb: float,
    limit_gb: float | None,
    occupied_units: int,
) -> bool:
    """Test *candidate_gb* against *occupied_gb* and optional *limit_gb*.

    *occupied_units* permits one oversized unit only in an empty window.
    """

    if limit_gb is None:
        return True
    if candidate_gb > limit_gb:
        return occupied_units == 0
    return occupied_gb + candidate_gb <= limit_gb + 1e-12


def _planned_processing_count(
    candidate: DatasetTask,
    *,
    active: list[DatasetTask],
    waiting: list[DatasetTask],
    processing_window_gb: float | None,
    processing_unit_limit: int | None,
    worker_job_limit: int,
    worker_cpu_budget: int,
) -> int:
    """Estimate a feasible cohort containing *candidate*.

    Existing *active* tasks precede *waiting* tasks. *processing_window_gb*,
    *processing_unit_limit*, *worker_job_limit*, and *worker_cpu_budget* bound
    the cohort used for equal launch-time CPU sharing.
    """

    selected_ids: set[str] = set()
    selected_count = 0
    raw_gb = 0.0
    minimum_threads = 0
    maximum_units = min(
        worker_job_limit,
        processing_unit_limit or worker_job_limit,
    )
    for task in [*active, candidate, *waiting]:
        if task.task_id in selected_ids or selected_count >= maximum_units:
            continue
        size_gb = max(0.0, task.unit.total_size_gb)
        if not _fits_window(raw_gb, size_gb, processing_window_gb, selected_count):
            continue
        next_threads = minimum_threads + task.resources.threads
        if next_threads > worker_cpu_budget:
            continue
        selected_ids.add(task.task_id)
        selected_count += 1
        raw_gb += size_gb
        minimum_threads = next_threads
    return max(1, selected_count)


def _slurm_job_statuses(job_ids: set[str]) -> dict[str, tuple[str, str]] | None:
    """Return state and reason for active *job_ids*, or ``None`` on query error."""

    if not job_ids:
        return {}
    executable = shutil.which("squeue")
    if executable is None:
        raise RuntimeError("Required executable is not available: squeue")
    completed = subprocess.run(
        [executable, "-h", "-j", ",".join(sorted(job_ids)), "-o", "%A|%T|%R"],
        capture_output=True,
        text=True,
        check=False,
    )
    if completed.returncode != 0:
        print(
            "Warning: could not query active Slurm workers; admissions are paused: "
            f"{completed.stderr.strip() or completed.stdout.strip()}",
            file=sys.stderr,
            flush=True,
        )
        return None
    statuses: dict[str, tuple[str, str]] = {}
    for line in completed.stdout.splitlines():
        fields = line.strip().split("|", 2)
        if len(fields) == 3 and fields[0]:
            statuses[fields[0]] = (fields[1], fields[2])
    return statuses


def _fastq_root(workspace: Path, task: DatasetTask) -> Path:
    """Return the package-owned FASTQ cache root for *task* in *workspace*."""

    return workspace / "fastq" / sanitize_identifier(task.task_id)


def _state_is_valid_success(builder: DatasetBuilder, task: DatasetTask) -> bool:
    """Ask *builder* whether *task* has matching success and valid artifacts."""

    state = builder.state.get(task.task_id)
    return bool(
        state
        and state.get("fingerprint") == task.fingerprint
        and state.get("status") == "succeeded"
        and builder._state_artifacts_valid(state)
    )


def _report_progress(
    *,
    total: int,
    pending: int,
    downloading: int,
    ready: int,
    running: dict[int, _RunningUnit],
    succeeded: int,
    failed: int,
) -> None:
    """Print aggregate *total*, *pending*, *downloading*, and *ready* counts.

    *running* supplies active CPU allocations; *succeeded* and *failed* supply
    terminal counts.
    """

    allocated = sum(item.threads for item in running.values())
    print(
        "Streaming progress: "
        f"{succeeded + failed:,}/{total:,} terminal "
        f"({succeeded:,} succeeded, {failed:,} failed); "
        f"{pending:,} pending, {downloading:,} downloading, {ready:,} ready, "
        f"{len(running):,} processing; {allocated:,} worker CPUs allocated",
        flush=True,
    )


def main(argv: list[str] | None = None) -> int:
    """Run the restart-safe unit streaming dispatcher from optional *argv*."""

    parser = argparse.ArgumentParser(
        description="Stream staged units into quota-limited Slurm worker jobs"
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
    parser.add_argument("--prefetch-batches", type=int, default=1)
    parser.add_argument("--max-staged-gb", type=float)
    parser.add_argument("--minimum-free-gb", type=float, default=0.0)
    parser.add_argument("--processing-storage-multiplier", type=float, default=1.0)
    parser.add_argument("--max-threads-per-unit", type=int)
    parser.add_argument("--scheduler-poll-seconds", type=float, default=1.0)
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
        processing_storage_multiplier=arguments.processing_storage_multiplier,
        max_threads_per_unit=arguments.max_threads_per_unit,
        scheduler_poll_seconds=arguments.scheduler_poll_seconds,
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
    indexed_tasks = _selected_tasks(
        job,
        set(arguments.batch_id) if arguments.batch_id is not None else None,
    )
    tasks = [task for _index, task in indexed_tasks]
    minimum_threads = max(task.resources.threads for task in tasks)
    quota = SlurmOptions(
        resources=tasks[0].resources,
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
    assert quota.total_cpu_quota is not None
    assert quota.max_running_jobs is not None
    worker_cpu_budget = quota.total_cpu_quota - quota.coordinator_cpus
    worker_job_limit = quota.max_running_jobs - 1
    if quota.max_parallel is not None:
        worker_job_limit = min(worker_job_limit, quota.max_parallel)
    maximum_threads = policy.max_threads_per_unit or quota.cpus_per_node or worker_cpu_budget
    if quota.cpus_per_node is not None:
        maximum_threads = min(maximum_threads, quota.cpus_per_node)
    maximum_threads = min(maximum_threads, worker_cpu_budget)
    if maximum_threads < minimum_threads:
        raise ValueError(
            f"The per-unit CPU ceiling {maximum_threads} is below a unit minimum "
            f"of {minimum_threads}"
        )

    processing_window_value = job.metadata.get("max_batch_gb")
    processing_window_gb = (
        float(processing_window_value) if processing_window_value is not None else None
    )
    processing_units_value = job.metadata.get("max_batch_units")
    processing_unit_limit = (
        int(processing_units_value) if processing_units_value is not None else None
    )
    total_raw_window_gb = (
        processing_window_gb * (policy.prefetch_batches + 1)
        if processing_window_gb is not None
        else None
    )
    queue_unit_limit = (
        (processing_unit_limit or worker_job_limit) * (policy.prefetch_batches + 1)
    )

    print(
        f"Distributed streaming: {len(tasks):,} units; {worker_cpu_budget:,} worker CPUs; "
        f"{worker_job_limit:,} worker jobs; per-unit CPU range "
        f"{minimum_threads:,}..{maximum_threads:,}; processing window "
        f"{processing_window_gb if processing_window_gb is not None else 'unbounded'} GB; "
        f"{policy.prefetch_batches} prefetch window(s)",
        flush=True,
    )

    lock = arguments.workspace / "state" / "pipeline-coordinator.lock"
    executor = SlurmExecutor()
    job_scripts = arguments.workspace / "slurm" / sanitize_identifier(job.job_id) / "units"
    pending: list[tuple[int, DatasetTask]] = []
    ready: list[_ReadyUnit] = []
    running: dict[int, _RunningUnit] = {}
    terminal: dict[int, str] = {}
    retry_counts: dict[int, int] = {}
    retry_after: dict[int, float] = {}
    retained_storage_gb = 0.0
    missing_grace_seconds = max(30.0, policy.scheduler_poll_seconds * 3)
    download_pool = ThreadPoolExecutor(
        max_workers=policy.download_workers,
        thread_name_prefix="distributed-download",
    )
    downloads: dict[Future[StagedFastq], tuple[int, DatasetTask]] = {}

    try:
        with exclusive_file_lock(
            lock,
            timeout_seconds=120,
            stale_after_seconds=90,
            heartbeat_seconds=15,
        ):
            builder._initialize_streaming_manifests(job, tasks)
            possible_reattach: list[tuple[int, DatasetTask, dict[str, object]]] = []
            for index, task in indexed_tasks:
                state = builder.state.get(task.task_id) or {}
                same_work = state.get("fingerprint") == task.fingerprint
                status = state.get("status") if same_work else None
                if status == "succeeded" and _state_is_valid_success(builder, task):
                    terminal[index] = "succeeded"
                    retained_storage_gb += path_size_gb(
                        _fastq_root(arguments.workspace, task)
                    )
                    continue
                if status == "failed" and not arguments.retry_failed:
                    terminal[index] = "failed"
                    retained_storage_gb += path_size_gb(_fastq_root(arguments.workspace, task))
                    continue
                slurm_job_id = state.get("slurm_job_id")
                if status in {"submitted", "running"} and isinstance(slurm_job_id, str):
                    possible_reattach.append((index, task, state))
                else:
                    pending.append((index, task))

            slurm_statuses = _slurm_job_statuses(
                {str(state["slurm_job_id"]) for _index, _task, state in possible_reattach}
            )
            if slurm_statuses is None:
                slurm_statuses = {
                    str(state["slurm_job_id"]): ("UNKNOWN", "UNKNOWN")
                    for _index, _task, state in possible_reattach
                }
            now = time.time()
            for index, task, state in possible_reattach:
                slurm_job_id = str(state["slurm_job_id"])
                if slurm_job_id not in slurm_statuses:
                    pending.append((index, task))
                    continue
                _slurm_state, reason = slurm_statuses[slurm_job_id]
                if state.get("status") == "submitted" and "held" in reason.lower():
                    executor.release(slurm_job_id)
                threads = max(
                    task.resources.threads,
                    int(state.get("allocated_threads") or task.resources.threads),
                )
                running[index] = _RunningUnit(
                    index=index,
                    task=task,
                    slurm_job_id=slurm_job_id,
                    threads=threads,
                    staged_size_gb=max(
                        task.unit.total_size_gb,
                        path_size_gb(_fastq_root(arguments.workspace, task)),
                    ),
                    submitted_epoch=float(state.get("submitted_epoch") or now),
                )
            pending.sort(key=lambda item: item[0])
            _report_progress(
                total=len(tasks),
                pending=len(pending),
                downloading=0,
                ready=0,
                running=running,
                succeeded=sum(value == "succeeded" for value in terminal.values()),
                failed=sum(value == "failed" for value in terminal.values()),
            )
            last_report = time.monotonic()

            while len(terminal) < len(tasks):
                made_progress = False
                monotonic_now = time.monotonic()

                for future in [item for item in downloads if item.done()]:
                    index, task = downloads.pop(future)
                    try:
                        staged = future.result()
                    except Exception as exc:  # noqa: BLE001 - staging is retried
                        failures = retry_counts.get(index, 0) + 1
                        retry_counts[index] = failures
                        delay = min(300.0, float(2 ** min(failures - 1, 8)))
                        retry_after[index] = monotonic_now + delay
                        pending.append((index, task))
                        pending.sort(key=lambda item: item[0])
                        print(
                            f"Warning: staging failed for {task.task_id}; retry "
                            f"{failures + 1} starts in {delay:.1f}s: {exc}",
                            file=sys.stderr,
                            flush=True,
                        )
                    else:
                        retry_counts.pop(index, None)
                        retry_after.pop(index, None)
                        ready.append(
                            _ReadyUnit(
                                index=index,
                                task=task,
                                staged_size_gb=max(task.unit.total_size_gb, staged.size_gb),
                            )
                        )
                        ready.sort(key=lambda item: item.index)
                    made_progress = True

                if running:
                    states = {
                        index: builder.state.get(item.task.task_id) or {}
                        for index, item in running.items()
                    }
                    slurm_statuses = _slurm_job_statuses(
                        {item.slurm_job_id for item in running.values()}
                    )
                    now = time.time()
                    for index, item in list(running.items()):
                        state = states[index]
                        same_work = state.get("fingerprint") == item.task.fingerprint
                        status = state.get("status") if same_work else None
                        if status == "succeeded" and _state_is_valid_success(builder, item.task):
                            terminal[index] = "succeeded"
                            running.pop(index)
                            made_progress = True
                            continue
                        if status == "failed":
                            terminal[index] = "failed"
                            running.pop(index)
                            retained_storage_gb += path_size_gb(
                                _fastq_root(arguments.workspace, item.task)
                            )
                            made_progress = True
                            continue
                        if slurm_statuses is None:
                            item.missing_since = None
                            continue
                        slurm_status = slurm_statuses.get(item.slurm_job_id)
                        if slurm_status is not None:
                            _scheduler_state, reason = slurm_status
                            if status == "submitted" and "held" in reason.lower():
                                try:
                                    executor.release(item.slurm_job_id)
                                except Exception as exc:  # noqa: BLE001 - retry next poll
                                    print(
                                        f"Warning: could not release recovered held worker "
                                        f"{item.slurm_job_id}: {exc}",
                                        file=sys.stderr,
                                        flush=True,
                                    )
                            item.missing_since = None
                            continue
                        if item.missing_since is None:
                            item.missing_since = now
                            continue
                        if now - item.missing_since < missing_grace_seconds:
                            continue
                        running.pop(index)
                        pending.append((index, item.task))
                        pending.sort(key=lambda entry: entry[0])
                        print(
                            f"Warning: Slurm worker {item.slurm_job_id} for "
                            f"{item.task.task_id} disappeared without terminal state; "
                            "the unit will be resubmitted",
                            file=sys.stderr,
                            flush=True,
                        )
                        made_progress = True

                active_tasks = [item.task for item in running.values()]
                active_raw_gb = sum(
                    max(0.0, item.task.unit.total_size_gb) for item in running.values()
                )
                active_threads = sum(item.threads for item in running.values())
                waiting_tasks = [item.task for item in ready]
                waiting_tasks.extend(task for _index, task in downloads.values())
                waiting_tasks.extend(task for _index, task in pending)
                ready_storage_gb = sum(item.staged_size_gb for item in ready)
                download_storage_gb = sum(
                    max(0.0, task.unit.total_size_gb)
                    for _index, task in downloads.values()
                )
                running_storage_gb = sum(
                    item.staged_size_gb * policy.processing_storage_multiplier
                    for item in running.values()
                )

                for item in list(ready):
                    task = item.task
                    if retry_after.get(item.index, 0.0) > monotonic_now:
                        continue
                    current_state = builder.state.get(task.task_id) or {}
                    if _state_is_valid_success(builder, task):
                        ready.remove(item)
                        waiting_tasks = [
                            entry for entry in waiting_tasks if entry.task_id != task.task_id
                        ]
                        ready_storage_gb -= item.staged_size_gb
                        terminal[item.index] = "succeeded"
                        made_progress = True
                        continue
                    if (
                        current_state.get("fingerprint") == task.fingerprint
                        and current_state.get("status") == "failed"
                    ):
                        ready.remove(item)
                        waiting_tasks = [
                            entry for entry in waiting_tasks if entry.task_id != task.task_id
                        ]
                        ready_storage_gb -= item.staged_size_gb
                        terminal[item.index] = "failed"
                        retained_storage_gb += path_size_gb(
                            _fastq_root(arguments.workspace, task)
                        )
                        made_progress = True
                        continue
                    if len(running) >= worker_job_limit:
                        break
                    if processing_unit_limit is not None and len(running) >= processing_unit_limit:
                        break
                    raw_gb = max(0.0, task.unit.total_size_gb)
                    if not _fits_window(
                        active_raw_gb,
                        raw_gb,
                        processing_window_gb,
                        len(running),
                    ):
                        continue
                    available_threads = worker_cpu_budget - active_threads
                    if available_threads < task.resources.threads:
                        continue
                    other_waiting = [
                        entry for entry in waiting_tasks if entry.task_id != task.task_id
                    ]
                    planned_count = _planned_processing_count(
                        task,
                        active=active_tasks,
                        waiting=other_waiting,
                        processing_window_gb=processing_window_gb,
                        processing_unit_limit=processing_unit_limit,
                        worker_job_limit=worker_job_limit,
                        worker_cpu_budget=worker_cpu_budget,
                    )
                    fair_share = max(
                        task.resources.threads,
                        worker_cpu_budget // planned_count,
                    )
                    allocated_threads = min(
                        maximum_threads,
                        available_threads,
                        fair_share,
                    )
                    extra_processing_gb = item.staged_size_gb * (
                        policy.processing_storage_multiplier - 1
                    )
                    resident_gb = (
                        retained_storage_gb
                        + ready_storage_gb
                        + download_storage_gb
                        + running_storage_gb
                    )
                    if policy.max_staged_gb is not None and (
                        resident_gb + extra_processing_gb > policy.max_staged_gb
                    ):
                        continue

                    allocated_task = replace(
                        task,
                        resources=replace(task.resources, threads=allocated_threads),
                    )
                    worker_script = (
                        job_scripts
                        / f"{item.index:06d}-{sanitize_identifier(task.task_id)}.sbatch"
                    )
                    worker_options = SlurmOptions(
                        resources=allocated_task.resources,
                        partition=arguments.partition,
                        account=arguments.account,
                        qos=arguments.qos,
                    )
                    slurm_job_id: str | None = None
                    try:
                        executor.create_script(
                            job_path=arguments.job,
                            task_count=len(job.tasks),
                            processor_reference=arguments.processor,
                            workspace=arguments.workspace,
                            email=arguments.email,
                            output_path=worker_script,
                            options=worker_options,
                            retry_failed=True,
                            task_indices=[item.index],
                            cleanup=policy.cleanup,
                            keep_failed_inputs=policy.keep_failed_inputs,
                            fsync_logs=policy.fsync_logs,
                            prefetch_max_size=arguments.prefetch_max_size,
                        )
                        slurm_job_id = executor.submit(worker_script, hold=True)
                        builder.state.record_submission(
                            task.task_id,
                            slurm_job_id=slurm_job_id,
                            threads=allocated_threads,
                            memory_gb=allocated_task.resources.memory_gb,
                            fingerprint=task.fingerprint,
                            job_id=job.job_id,
                            task=allocated_task.to_dict(),
                            log_path=builder._unit_log_path(task),
                        )
                        executor.release(slurm_job_id)
                    except Exception as exc:  # noqa: BLE001 - submission is retried
                        if slurm_job_id is not None:
                            try:
                                executor.cancel(slurm_job_id)
                            except Exception as cancel_exc:
                                raise RuntimeError(
                                    f"Could not release or cancel held Slurm job "
                                    f"{slurm_job_id}; manual intervention is required"
                                ) from cancel_exc
                        failures = retry_counts.get(item.index, 0) + 1
                        retry_counts[item.index] = failures
                        delay = min(300.0, float(2 ** min(failures - 1, 8)))
                        retry_after[item.index] = monotonic_now + delay
                        print(
                            f"Warning: Slurm submission failed for {task.task_id}; "
                            f"retry in {delay:.1f}s: {exc}",
                            file=sys.stderr,
                            flush=True,
                        )
                        continue

                    ready.remove(item)
                    waiting_tasks = [
                        entry for entry in waiting_tasks if entry.task_id != task.task_id
                    ]
                    ready_storage_gb -= item.staged_size_gb
                    running_storage_gb += item.staged_size_gb * (
                        policy.processing_storage_multiplier
                    )
                    running[item.index] = _RunningUnit(
                        index=item.index,
                        task=task,
                        slurm_job_id=slurm_job_id,
                        threads=allocated_threads,
                        staged_size_gb=item.staged_size_gb,
                        submitted_epoch=time.time(),
                    )
                    active_tasks.append(task)
                    active_raw_gb += raw_gb
                    active_threads += allocated_threads
                    made_progress = True

                inflight_count = len(downloads) + len(ready) + len(running)
                inflight_raw_gb = sum(
                    max(0.0, task.unit.total_size_gb)
                    for _index, task in downloads.values()
                )
                inflight_raw_gb += sum(
                    max(0.0, item.task.unit.total_size_gb) for item in ready
                )
                inflight_raw_gb += sum(
                    max(0.0, item.task.unit.total_size_gb) for item in running.values()
                )
                while pending and len(downloads) < policy.download_workers:
                    candidate_position: int | None = None
                    for position, (index, task) in enumerate(pending):
                        if retry_after.get(index, 0.0) > monotonic_now:
                            continue
                        if inflight_count >= queue_unit_limit:
                            break
                        raw_gb = max(0.0, task.unit.total_size_gb)
                        if not _fits_window(
                            inflight_raw_gb,
                            raw_gb,
                            total_raw_window_gb,
                            inflight_count,
                        ):
                            continue
                        resident_gb = retained_storage_gb + inflight_raw_gb + raw_gb
                        resident_gb += active_raw_gb * (
                            policy.processing_storage_multiplier - 1
                        )
                        if policy.max_staged_gb is not None and resident_gb > policy.max_staged_gb:
                            continue
                        available_gb = free_space_gb(arguments.workspace)
                        if available_gb - raw_gb < policy.minimum_free_gb:
                            continue
                        candidate_position = position
                        break
                    if candidate_position is None:
                        break
                    index, task = pending.pop(candidate_position)
                    future = download_pool.submit(
                        builder.prefetch_unit,
                        job,
                        index,
                        policy=policy,
                    )
                    downloads[future] = (index, task)
                    inflight_count += 1
                    inflight_raw_gb += max(0.0, task.unit.total_size_gb)
                    made_progress = True

                now_for_report = time.monotonic()
                if now_for_report - last_report >= 30.0:
                    _report_progress(
                        total=len(tasks),
                        pending=len(pending),
                        downloading=len(downloads),
                        ready=len(ready),
                        running=running,
                        succeeded=sum(value == "succeeded" for value in terminal.values()),
                        failed=sum(value == "failed" for value in terminal.values()),
                    )
                    last_report = now_for_report

                if not made_progress:
                    if downloads:
                        wait(
                            downloads,
                            timeout=policy.scheduler_poll_seconds,
                            return_when=FIRST_COMPLETED,
                        )
                    else:
                        time.sleep(policy.scheduler_poll_seconds)

            builder._finalize_streaming_manifests(job, tasks, policy=policy)
            builder.workspace.sync_manifest(
                job,
                {task.task_id: builder.state.get(task.task_id) for task in job.tasks},
            )
            _report_progress(
                total=len(tasks),
                pending=0,
                downloading=0,
                ready=0,
                running={},
                succeeded=sum(value == "succeeded" for value in terminal.values()),
                failed=sum(value == "failed" for value in terminal.values()),
            )
    finally:
        download_pool.shutdown(wait=False, cancel_futures=True)

    return 1 if any(value == "failed" for value in terminal.values()) else 0


if __name__ == "__main__":
    raise SystemExit(main())
