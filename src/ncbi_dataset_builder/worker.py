from __future__ import annotations

import argparse
import os
from pathlib import Path

from .pipeline import PipelinePolicy
from .workflow import BuilderConfig, DatasetBuilder


def main(argv: list[str] | None = None) -> int:
    """Execute one workspace job task from optional CLI *argv* and return an exit code."""

    parser = argparse.ArgumentParser(
        description="Execute one durable dataset task (normally from Slurm)"
    )
    parser.add_argument("--job", type=Path, required=True)
    parser.add_argument("--task-index", type=int, required=True)
    parser.add_argument("--processor", required=True)
    parser.add_argument("--workspace", type=Path, required=True)
    parser.add_argument("--email", default=os.environ.get("NCBI_EMAIL"))
    parser.add_argument("--retry-failed", action="store_true")
    parser.add_argument("--cleanup", choices=("after_success", "never"), default="after_success")
    parser.add_argument("--discard-failed-inputs", action="store_true")
    parser.add_argument("--no-fsync-logs", action="store_true")
    parser.add_argument("--prefetch-max-size", default="u")
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
            prefetch_max_size=arguments.prefetch_max_size,
            progress_bars=False,
        )
    )
    job = builder.load_job(arguments.job)
    allocated_threads_raw = os.environ.get("SLURM_CPUS_PER_TASK")
    allocated_threads = (
        int(allocated_threads_raw) if allocated_threads_raw is not None else None
    )
    outcome = builder.run_task(
        job,
        arguments.task_index,
        arguments.processor,
        retry_failed=arguments.retry_failed,
        threads_override=allocated_threads,
    )
    return 1 if outcome.status == "failed" else 0


if __name__ == "__main__":
    raise SystemExit(main())
