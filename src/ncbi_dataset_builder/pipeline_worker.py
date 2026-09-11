from __future__ import annotations

import argparse
import os
from pathlib import Path

from .pipeline import PipelinePolicy
from .workflow import BuilderConfig, DatasetBuilder


def main(argv: list[str] | None = None) -> int:
    """Execute a bounded workspace job from optional command-line *argv*."""

    parser = argparse.ArgumentParser(
        description="Coordinate unit-level streaming inside one Slurm job"
    )
    parser.add_argument("--job", type=Path, required=True)
    parser.add_argument("--processor", required=True)
    parser.add_argument("--workspace", type=Path, required=True)
    parser.add_argument("--email", default=os.environ.get("NCBI_EMAIL"))
    parser.add_argument("--max-workers", type=int, required=True)
    parser.add_argument("--total-threads", type=int, required=True)
    parser.add_argument("--total-memory-gb", type=float, required=True)
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
            max_workers=arguments.max_workers,
            total_threads=arguments.total_threads,
            total_memory_gb=arguments.total_memory_gb,
            pipeline_policy=policy,
            prefetch_max_size=arguments.prefetch_max_size,
            progress_bars=False,
        )
    )
    report = builder.run_job(
        builder.load_job(arguments.job),
        arguments.processor,
        retry_failed=arguments.retry_failed,
        batch_ids=set(arguments.batch_id) if arguments.batch_id else None,
        policy=policy,
    )
    return 1 if report.failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
