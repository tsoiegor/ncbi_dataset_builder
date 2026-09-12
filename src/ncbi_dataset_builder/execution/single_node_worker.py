"""Internal entry point for one-node Slurm execution."""

from __future__ import annotations

import argparse
import os
from pathlib import Path

from ..api import BuilderConfig, DatasetBuilder
from ..processing.base import load_processor
from ..workspace import WorkspaceStore
from .config import (
    SlurmSingleNodeExecution,
    execution_from_dict,
    queue_policy_from_dict,
)


def main(argv: list[str] | None = None) -> int:
    """Run the saved single-node execution described by optional *argv*."""

    parser = argparse.ArgumentParser(description="Run a sample queue in one Slurm allocation")
    parser.add_argument("--execution", type=Path, required=True)
    parser.add_argument("--processor", required=True)
    parser.add_argument("--workspace", type=Path, required=True)
    parser.add_argument("--email", default=os.environ.get("NCBI_EMAIL"))
    parser.add_argument("--retry-failed", action="store_true")
    arguments = parser.parse_args(argv)

    record = WorkspaceStore(arguments.workspace).load_execution(arguments.execution)
    execution = execution_from_dict(record.execution_config)
    if not isinstance(execution, SlurmSingleNodeExecution):
        raise TypeError("Execution record is not configured for one-node Slurm")
    queue = queue_policy_from_dict(record.queue_config)
    builder = DatasetBuilder(
        BuilderConfig(
            workspace=arguments.workspace,
            email=arguments.email,
            ncbi_api_key=os.environ.get("NCBI_API_KEY"),
            group_by=record.group_by,
        )
    )
    report = builder._run_streaming(
        record,
        load_processor(arguments.processor),
        execution=execution,
        queue=queue,
        retry_failed=arguments.retry_failed,
    )
    return 1 if report.failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
