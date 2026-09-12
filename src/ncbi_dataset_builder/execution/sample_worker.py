"""Internal entry point for one distributed Slurm sample job."""

from __future__ import annotations

import argparse
import os
from pathlib import Path

from ..api import BuilderConfig, DatasetBuilder
from ..processing.base import load_processor
from ..workspace import WorkspaceStore
from .config import (
    SlurmDistributedExecution,
    execution_from_dict,
    queue_policy_from_dict,
)


def main(argv: list[str] | None = None) -> int:
    """Run one saved sample selected by optional command-line *argv*."""

    parser = argparse.ArgumentParser(description="Run one distributed NCBI sample")
    parser.add_argument("--execution", type=Path, required=True)
    parser.add_argument("--item-index", type=int, required=True)
    parser.add_argument("--processor", required=True)
    parser.add_argument("--workspace", type=Path, required=True)
    parser.add_argument("--cpus", type=int, required=True)
    parser.add_argument("--email", default=os.environ.get("NCBI_EMAIL"))
    parser.add_argument("--retry-failed", action="store_true")
    arguments = parser.parse_args(argv)

    record = WorkspaceStore(arguments.workspace).load_execution(arguments.execution)
    execution = execution_from_dict(record.execution_config)
    if not isinstance(execution, SlurmDistributedExecution):
        raise TypeError("Execution record is not configured for distributed Slurm")
    if arguments.item_index < 0 or arguments.item_index >= len(record.items):
        raise IndexError("item-index is outside the execution queue")
    if not execution.min_cpus_per_job <= arguments.cpus <= execution.max_cpus_per_job:
        raise ValueError("Worker CPU allocation is outside execution limits")
    queue = queue_policy_from_dict(record.queue_config)
    builder = DatasetBuilder(
        BuilderConfig(
            workspace=arguments.workspace,
            email=arguments.email,
            ncbi_api_key=os.environ.get("NCBI_API_KEY"),
            group_by=record.group_by,
        )
    )
    item = record.items[arguments.item_index]
    prepared = builder._claim_and_stage(
        item,
        execution_id=record.execution_id,
        retry_failed=arguments.retry_failed,
        queue=queue,
        reclaim_running=True,
    )
    outcome = (
        prepared.outcome
        if prepared.outcome is not None
        else builder._process_prepared(
            prepared,
            load_processor(arguments.processor),
            cpus=arguments.cpus,
            queue=queue,
        )
    )
    builder._cleanup_input(prepared, outcome, queue=queue)
    builder.workspace.sync_manifest(
        record,
        {entry.item_id: builder.state.get(entry.item_id) for entry in record.items},
    )
    return 1 if outcome.status == "failed" else 0


if __name__ == "__main__":
    raise SystemExit(main())
