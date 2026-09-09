from __future__ import annotations

import argparse
import os
from pathlib import Path

from .pipeline import PipelinePolicy
from .workflow import BuilderConfig, DatasetBuilder


def main(argv: list[str] | None = None) -> int:
    """Execute one plan task from optional CLI *argv* and return an exit code."""

    parser = argparse.ArgumentParser(
        description="Execute one durable dataset task (normally from Slurm)"
    )
    parser.add_argument("--plan", type=Path, required=True)
    parser.add_argument("--task-index", type=int, required=True)
    parser.add_argument("--processor", required=True)
    parser.add_argument("--workspace", type=Path, required=True)
    parser.add_argument("--email", default=os.environ.get("NCBI_EMAIL"))
    parser.add_argument("--retry-failed", action="store_true")
    parser.add_argument("--cleanup", choices=("after_success", "never"), default="after_success")
    parser.add_argument("--discard-failed-inputs", action="store_true")
    parser.add_argument("--no-fsync-logs", action="store_true")
    arguments = parser.parse_args(argv)
    policy = PipelinePolicy(
        cleanup=arguments.cleanup,
        keep_failed_inputs=not arguments.discard_failed_inputs,
        fsync_logs=not arguments.no_fsync_logs,
    )
    builder = DatasetBuilder(
        BuilderConfig(
            workspace=arguments.workspace,
            email=arguments.email,
            ncbi_api_key=os.environ.get("NCBI_API_KEY"),
            max_workers=1,
            pipeline_policy=policy,
            progress_bars=False,
        )
    )
    plan = builder.load_plan(arguments.plan)
    outcome = builder.run_task(
        plan,
        arguments.task_index,
        arguments.processor,
        retry_failed=arguments.retry_failed,
    )
    return 1 if outcome.status == "failed" else 0


if __name__ == "__main__":
    raise SystemExit(main())
