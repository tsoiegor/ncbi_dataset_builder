"""Quota-aware coordinator for distributed Slurm sample jobs."""

from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
import time
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from dataclasses import dataclass
from pathlib import Path

from ..api import DatasetBuilder, _PreparedUnit
from ..workspace import WorkspaceStore
from .config import (
    SlurmDistributedExecution,
    execution_from_dict,
    queue_policy_from_dict,
)
from .slurm import SlurmExecutor


@dataclass
class _RunningSample:
    """Track one submitted sample until durable state becomes terminal."""

    index: int
    slurm_job_id: str
    cpus: int
    submitted_epoch: float
    claim_id: str
    missing_since: float | None = None


def _slurm_states(job_ids: set[str]) -> dict[str, str] | None:
    """Return active Slurm states for *job_ids*, or ``None`` on query failure."""

    if not job_ids:
        return {}
    executable = shutil.which("squeue")
    if executable is None:
        raise RuntimeError("Required executable is not available: squeue")
    completed = subprocess.run(
        [executable, "-h", "-j", ",".join(sorted(job_ids)), "-o", "%A|%T"],
        capture_output=True,
        text=True,
        check=False,
    )
    if completed.returncode != 0:
        print(
            "Warning: squeue failed; new sample admissions are paused: "
            f"{completed.stderr.strip() or completed.stdout.strip()}",
            file=sys.stderr,
            flush=True,
        )
        return None
    states: dict[str, str] = {}
    for line in completed.stdout.splitlines():
        job_id, separator, state = line.strip().partition("|")
        if separator and job_id:
            states[job_id] = state
    return states


def main(argv: list[str] | None = None) -> int:
    """Coordinate a saved distributed execution from optional *argv*."""

    parser = argparse.ArgumentParser(description="Coordinate distributed NCBI sample jobs")
    parser.add_argument("--execution", type=Path, required=True)
    parser.add_argument("--processor", required=True)
    parser.add_argument("--workspace", type=Path, required=True)
    parser.add_argument("--email", default=os.environ.get("NCBI_EMAIL"))
    parser.add_argument("--retry-failed", action="store_true")
    arguments = parser.parse_args(argv)

    workspace = WorkspaceStore(arguments.workspace)
    record = workspace.load_execution(arguments.execution)
    execution = execution_from_dict(record.execution_config)
    if not isinstance(execution, SlurmDistributedExecution):
        raise TypeError("Execution record is not configured for distributed Slurm")
    queue = queue_policy_from_dict(record.queue_config)
    builder = DatasetBuilder.from_execution_record(
        workspace=arguments.workspace,
        record=record,
        email=arguments.email,
        ncbi_api_key=os.environ.get("NCBI_API_KEY"),
    )
    executor = SlurmExecutor(progress=builder.progress)
    record_path = workspace.executions / f"{record.execution_id}.json"
    pending: list[int] = []
    ready: dict[int, _PreparedUnit] = {}
    running: dict[int, _RunningSample] = {}
    for index, item in enumerate(record.items):
        state = builder.state.get(item.item_id) or {}
        same_work = state.get("fingerprint") == item.fingerprint
        reusable = bool(
            same_work
            and state.get("status") == "succeeded"
            and builder._outputs_valid(state)
        )
        failed = bool(
            same_work and state.get("status") == "failed"
        )
        if reusable or (failed and not arguments.retry_failed):
            continue
        active = state.get("status") in {"running", "submitted"}
        slurm_job_id = state.get("slurm_job_id")
        claim_id = state.get("claim_id")
        coordinator_ready = bool(
            same_work
            and state.get("status") == "running"
            and state.get("phase") == "ready"
            and isinstance(state.get("prepared"), dict)
            and not state.get("worker_activated_at")
        )
        if coordinator_ready:
            ready[index] = builder._prepared_from_state(item, state)
        elif same_work and active and isinstance(slurm_job_id, str) and slurm_job_id:
            if not isinstance(claim_id, str):
                raise ValueError(f"Active sample has no claim token: {item.item_id}")
            running[index] = _RunningSample(
                index=index,
                slurm_job_id=slurm_job_id,
                cpus=int(state.get("allocated_cpus") or execution.min_cpus_per_job),
                submitted_epoch=float(
                    state.get("submitted_epoch") or state.get("started_epoch") or time.time()
                ),
                claim_id=claim_id,
            )
        else:
            pending.append(index)

    worker_cpu_budget = execution.total_cpu_quota - execution.coordinator_cpus
    stage_threads = max(1, execution.coordinator_cpus // queue.download_workers)
    staging: dict[Future[_PreparedUnit], int] = {}
    stage_pool = ThreadPoolExecutor(
        max_workers=queue.download_workers,
        thread_name_prefix="distributed-download",
    )
    try:
        while pending or staging or ready or running:
            made_progress = False
            for future in [candidate for candidate in staging if candidate.done()]:
                index = staging.pop(future)
                prepared = future.result()
                if prepared.outcome is None:
                    ready[index] = prepared
                made_progress = True

            states = _slurm_states({value.slurm_job_id for value in running.values()})
            if states is not None:
                now = time.time()
                for index, active in list(running.items()):
                    if active.slurm_job_id in states:
                        active.missing_since = None
                        continue
                    item = record.items[index]
                    saved = builder.state.get(item.item_id) or {}
                    if saved.get("status") in {"succeeded", "failed"}:
                        running.pop(index)
                        made_progress = True
                        continue
                    if active.missing_since is None:
                        active.missing_since = now
                    elif now - active.missing_since > 30:
                        current_claim = saved.get("claim_id")
                        if (
                            saved.get("execution_id") == record.execution_id
                            and isinstance(current_claim, str)
                        ):
                            builder.state.fail(
                                item.item_id,
                                f"Slurm job {active.slurm_job_id} ended without "
                                "terminal sample state",
                                claim_id=current_claim,
                            )
                        running.pop(index)
                        made_progress = True

            active_cpus = sum(value.cpus for value in running.values())
            while (
                states is not None
                and ready
                and len(running) < execution.max_running_jobs
            ):
                available_cpus = worker_cpu_budget - active_cpus
                if available_cpus < execution.min_cpus_per_job:
                    break
                index = min(ready)
                prepared = ready[index]
                item = prepared.item
                ready_items = [ready[key].item for key in sorted(ready) if key != index]
                cohort = builder._scheduled_count(
                    item,
                    active=[record.items[key] for key in running],
                    ready=ready_items,
                    total_cpus=worker_cpu_budget,
                    total_memory_gb=None,
                    max_running_jobs=execution.max_running_jobs,
                )
                fair_share = max(
                    execution.min_cpus_per_job,
                    worker_cpu_budget // cohort,
                )
                cpus = min(execution.max_cpus_per_job, available_cpus, fair_share)
                script = executor.create_sample_script(
                    record_path=record_path,
                    item_index=index,
                    processor_reference=arguments.processor,
                    workspace=arguments.workspace,
                    email=arguments.email,
                    output_path=(
                        workspace.path("slurm")
                        / record.execution_id
                        / f"{item.item_id}.sbatch"
                    ),
                    execution=execution,
                    cpus=cpus,
                    retry_failed=arguments.retry_failed,
                )
                slurm_job_id: str | None = None
                try:
                    slurm_job_id = executor.submit(script, hold=True)
                    if prepared.claim_id is None:
                        raise ValueError(f"Ready sample has no claim token: {item.item_id}")
                    builder.state.record_ready_submission(
                        item.item_id,
                        slurm_job_id=slurm_job_id,
                        cpus=cpus,
                        memory_gb=execution.memory_gb_per_job,
                        claim_id=prepared.claim_id,
                    )
                    executor.release(slurm_job_id)
                except Exception:
                    if slurm_job_id is not None:
                        executor.cancel(slurm_job_id)
                    raise
                assert slurm_job_id is not None
                running[index] = _RunningSample(
                    index=index,
                    slurm_job_id=slurm_job_id,
                    cpus=cpus,
                    submitted_epoch=time.time(),
                    claim_id=prepared.claim_id,
                )
                ready.pop(index)
                active_cpus += cpus
                made_progress = True

            inflight_indexes = [*staging.values(), *ready, *running]
            estimated_inflight_gb = sum(
                max(0.0, record.items[index].unit.total_size_gb)
                for index in inflight_indexes
            )
            processing_extra_gb = sum(
                max(0.0, record.items[index].unit.total_size_gb)
                * (queue.processing_storage_multiplier - 1)
                for index in running
            )
            while pending and len(staging) < queue.download_workers:
                candidate_position: int | None = None
                for position, index in enumerate(pending):
                    raw_gb = max(0.0, record.items[index].unit.total_size_gb)
                    projected_gb = estimated_inflight_gb + processing_extra_gb + raw_gb
                    if (
                        queue.max_inflight_gb is not None
                        and projected_gb > queue.max_inflight_gb
                        and inflight_indexes
                    ):
                        continue
                    if raw_gb > execution.storage.available_gb(arguments.workspace):
                        continue
                    candidate_position = position
                    break
                if candidate_position is None:
                    break
                index = pending.pop(candidate_position)
                item = record.items[index]
                future = stage_pool.submit(
                    builder._claim_and_stage,
                    item,
                    execution_id=record.execution_id,
                    retry_failed=arguments.retry_failed,
                    queue=queue,
                    reclaim_running=True,
                    stage_threads=stage_threads,
                )
                staging[future] = index
                inflight_indexes.append(index)
                estimated_inflight_gb += max(0.0, item.unit.total_size_gb)
                made_progress = True

            if not made_progress:
                if staging:
                    wait(
                        staging,
                        timeout=queue.scheduler_poll_seconds,
                        return_when=FIRST_COMPLETED,
                    )
                elif running:
                    time.sleep(min(60.0, queue.scheduler_poll_seconds))
                elif ready:
                    blocked = record.items[min(ready)]
                    raise RuntimeError(
                        f"Distributed queue cannot admit ready sample {blocked.item_id}; "
                        "check CPU quota and worker limits"
                    )
                elif pending:
                    blocked = record.items[pending[0]]
                    raise RuntimeError(
                        f"Distributed queue cannot stage {blocked.item_id}; "
                        "check storage capacity"
                    )
    finally:
        stage_pool.shutdown(wait=False, cancel_futures=True)

    workspace.sync_manifest(
        record,
        {item.item_id: builder.state.get(item.item_id) for item in record.items},
    )
    summary = builder.state.summary([item.item_id for item in record.items])
    return 1 if summary["counts"].get("failed", 0) else 0


if __name__ == "__main__":
    raise SystemExit(main())
