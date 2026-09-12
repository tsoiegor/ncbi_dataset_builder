"""Quota-aware coordinator for distributed Slurm sample jobs."""

from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path

from ..api import BuilderConfig, DatasetBuilder
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
    builder = DatasetBuilder(
        BuilderConfig(
            workspace=arguments.workspace,
            email=arguments.email,
            ncbi_api_key=os.environ.get("NCBI_API_KEY"),
            group_by=record.group_by,
        )
    )
    executor = SlurmExecutor(progress=builder.progress)
    record_path = workspace.executions / f"{record.execution_id}.json"
    pending: list[int] = []
    for index, item in enumerate(record.items):
        state = builder.state.get(item.item_id)
        reusable = bool(
            state
            and state.get("fingerprint") == item.fingerprint
            and state.get("status") == "succeeded"
            and builder._outputs_valid(state)
        )
        failed = bool(
            state
            and state.get("fingerprint") == item.fingerprint
            and state.get("status") == "failed"
        )
        if not reusable and (arguments.retry_failed or not failed):
            pending.append(index)

    running: dict[int, _RunningSample] = {}
    worker_cpu_budget = execution.total_cpu_quota - execution.coordinator_cpus
    while pending or running:
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
                    continue
                if active.missing_since is None:
                    active.missing_since = now
                elif now - active.missing_since > 30:
                    builder.state.fail(
                        item.item_id,
                        f"Slurm job {active.slurm_job_id} ended without terminal sample state",
                    )
                    running.pop(index)

        made_progress = False
        active_cpus = sum(value.cpus for value in running.values())
        active_gb = sum(
            max(0.0, record.items[index].unit.total_size_gb)
            * queue.processing_storage_multiplier
            for index in running
        )
        while pending and len(running) < execution.max_running_jobs:
            available_cpus = worker_cpu_budget - active_cpus
            if available_cpus < execution.min_cpus_per_job:
                break
            selected_position: int | None = None
            for position, index in enumerate(pending):
                raw_gb = max(0.0, record.items[index].unit.total_size_gb)
                projected_gb = active_gb + raw_gb * queue.processing_storage_multiplier
                if (
                    queue.max_inflight_gb is not None
                    and projected_gb > queue.max_inflight_gb
                    and running
                ):
                    continue
                if raw_gb > execution.storage.available_gb(arguments.workspace):
                    continue
                selected_position = position
                break
            if selected_position is None:
                break
            index = pending.pop(selected_position)
            item = record.items[index]
            cohort = min(execution.max_running_jobs, len(running) + len(pending) + 1)
            fair_share = max(execution.min_cpus_per_job, worker_cpu_budget // max(1, cohort))
            cpus = min(execution.max_cpus_per_job, available_cpus, fair_share)
            script = executor.create_sample_script(
                record_path=record_path,
                item_index=index,
                processor_reference=arguments.processor,
                workspace=arguments.workspace,
                email=arguments.email,
                output_path=(
                    arguments.workspace
                    / "slurm"
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
                builder.state.record_submission(
                    item.item_id,
                    slurm_job_id=slurm_job_id,
                    cpus=cpus,
                    memory_gb=execution.memory_gb_per_job,
                    fingerprint=item.fingerprint,
                    execution_id=record.execution_id,
                    item=item.to_dict(),
                    log_path=builder._unit_log_path(item),
                )
                executor.release(slurm_job_id)
            except Exception:
                if slurm_job_id is not None:
                    executor.cancel(slurm_job_id)
                raise
            assert slurm_job_id is not None
            running[index] = _RunningSample(index, slurm_job_id, cpus, time.time())
            active_cpus += cpus
            active_gb += max(0.0, item.unit.total_size_gb) * queue.processing_storage_multiplier
            made_progress = True

        if not made_progress and not running and pending:
            blocked = record.items[pending[0]]
            raise RuntimeError(
                f"Distributed queue cannot admit {blocked.item_id}; check CPU quota and storage"
            )
        if running:
            time.sleep(min(60.0, queue.scheduler_poll_seconds))

    workspace.sync_manifest(
        record,
        {item.item_id: builder.state.get(item.item_id) for item in record.items},
    )
    summary = builder.state.summary([item.item_id for item in record.items])
    return 1 if summary["counts"].get("failed", 0) else 0


if __name__ == "__main__":
    raise SystemExit(main())
