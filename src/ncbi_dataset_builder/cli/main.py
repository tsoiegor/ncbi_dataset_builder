"""Command-line interface for the workspace API."""

from __future__ import annotations

import argparse
import os
from pathlib import Path

from ..api import BuilderConfig, DatasetBuilder
from ..execution.config import (
    FilesystemStorage,
    LocalExecution,
    QueuePolicy,
    QuotaStorage,
    SlurmDistributedExecution,
    SlurmSingleNodeExecution,
)


def _add_builder_arguments(parser: argparse.ArgumentParser) -> None:
    """Add stable workspace and NCBI arguments to *parser*."""

    parser.add_argument("--workspace", type=Path, required=True)
    parser.add_argument("--email", default=os.environ.get("NCBI_EMAIL"))
    parser.add_argument("--ncbi-api-key", default=os.environ.get("NCBI_API_KEY"))
    parser.add_argument(
        "--group-by",
        choices=("run", "experiment", "sra_sample", "biosample"),
        default="experiment",
    )
    parser.add_argument("--prefetch-max-size", default="u")


def _add_catalog_arguments(parser: argparse.ArgumentParser) -> None:
    """Add mutually exclusive catalog source arguments to *parser*."""

    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--catalog", type=Path)
    source.add_argument("--query")
    parser.add_argument("--refresh", action="store_true")


def _add_queue_arguments(parser: argparse.ArgumentParser) -> None:
    """Add sample-streaming arguments to *parser*."""

    parser.add_argument("--download-workers", type=int, default=2)
    parser.add_argument("--max-inflight-gb", type=float)
    parser.add_argument("--processing-storage-multiplier", type=float, default=1.0)
    parser.add_argument("--keep-inputs", action="store_true")
    parser.add_argument("--discard-failed-inputs", action="store_true")
    parser.add_argument("--retry-failed", action="store_true")


def _add_slurm_arguments(parser: argparse.ArgumentParser) -> None:
    """Add common Slurm and quota arguments to *parser*."""

    parser.add_argument("--partition")
    parser.add_argument("--account")
    parser.add_argument("--qos")
    parser.add_argument("--quota-gb", type=float, required=True)
    parser.add_argument("--quota-reserve-gb", type=float, default=0.0)
    parser.add_argument("--quota-usage-root", type=Path)
    parser.add_argument("--script-path", type=Path)
    parser.add_argument("--no-submit", action="store_true")


def _parser() -> argparse.ArgumentParser:
    """Build and return the top-level argument parser."""

    parser = argparse.ArgumentParser(prog="ncbi-dataset")
    commands = parser.add_subparsers(dest="command", required=True)

    fetch = commands.add_parser("fetch-catalog", help="Fetch and save an NCBI RunInfo CSV")
    _add_builder_arguments(fetch)
    fetch.add_argument("--query", required=True)
    fetch.add_argument("--output", type=Path, required=True)
    fetch.add_argument("--refresh", action="store_true")

    local = commands.add_parser("build-local", help="Stream samples on one ordinary server")
    _add_builder_arguments(local)
    _add_catalog_arguments(local)
    _add_queue_arguments(local)
    local.add_argument("--processor", required=True)
    local.add_argument("--total-cpus", type=int, required=True)
    local.add_argument("--min-cpus-per-job", type=int, default=1)
    local.add_argument("--max-cpus-per-job", type=int)
    local.add_argument("--max-running-jobs", type=int, default=1)
    local.add_argument("--reserve-free-gb", type=float, default=0.0)

    single = commands.add_parser("submit-single-node", help="Submit one Slurm allocation")
    _add_builder_arguments(single)
    _add_catalog_arguments(single)
    _add_queue_arguments(single)
    _add_slurm_arguments(single)
    single.add_argument("--processor", required=True)
    single.add_argument("--allocation-cpus", type=int, required=True)
    single.add_argument("--allocation-memory-gb", type=float, required=True)
    single.add_argument("--allocation-time-limit", required=True)
    single.add_argument("--min-cpus-per-job", type=int, default=1)
    single.add_argument("--max-cpus-per-job", type=int)
    single.add_argument("--memory-gb-per-job", type=float, required=True)
    single.add_argument("--max-running-jobs", type=int, default=1)

    distributed = commands.add_parser(
        "submit-distributed", help="Submit a multi-node Slurm coordinator"
    )
    _add_builder_arguments(distributed)
    _add_catalog_arguments(distributed)
    _add_queue_arguments(distributed)
    _add_slurm_arguments(distributed)
    distributed.add_argument("--processor", required=True)
    distributed.add_argument("--total-cpu-quota", type=int, required=True)
    distributed.add_argument("--max-running-jobs", type=int, required=True)
    distributed.add_argument("--cpus-per-node", type=int, required=True)
    distributed.add_argument("--min-cpus-per-job", type=int, required=True)
    distributed.add_argument("--max-cpus-per-job", type=int, required=True)
    distributed.add_argument("--memory-gb-per-job", type=float, required=True)
    distributed.add_argument("--worker-time-limit", required=True)
    distributed.add_argument("--coordinator-cpus", type=int, default=1)
    distributed.add_argument("--coordinator-memory-gb", type=float, default=4.0)
    distributed.add_argument("--coordinator-time-limit", default="7-00:00:00")

    status = commands.add_parser("status", help="Show workspace sample status")
    _add_builder_arguments(status)
    status.add_argument("--execution-id")

    publish = commands.add_parser("publish", help="Publish verified experiment outputs")
    _add_builder_arguments(publish)
    publish.add_argument("--destination", type=Path)
    publish.add_argument("--execution-id")
    publish.add_argument("--mode", choices=("auto", "hardlink", "copy"), default="auto")
    publish.add_argument("--overwrite", action="store_true")
    return parser


def _builder(arguments: argparse.Namespace) -> DatasetBuilder:
    """Create a builder from parsed *arguments*."""

    return DatasetBuilder(
        BuilderConfig(
            workspace=arguments.workspace,
            email=arguments.email,
            ncbi_api_key=arguments.ncbi_api_key,
            group_by=arguments.group_by,
            prefetch_max_size=arguments.prefetch_max_size,
        )
    )


def _catalog(builder: DatasetBuilder, arguments: argparse.Namespace):
    """Load or fetch the catalog selected by *arguments*."""

    if arguments.catalog is not None:
        return builder.load_runs(arguments.catalog)
    return builder.fetch_runs(arguments.query, refresh=arguments.refresh)


def _queue(arguments: argparse.Namespace) -> QueuePolicy:
    """Create a queue policy from parsed *arguments*."""

    return QueuePolicy(
        download_workers=arguments.download_workers,
        max_inflight_gb=arguments.max_inflight_gb,
        processing_storage_multiplier=arguments.processing_storage_multiplier,
        cleanup="never" if arguments.keep_inputs else "after_success",
        keep_failed_inputs=not arguments.discard_failed_inputs,
    )


def _quota(arguments: argparse.Namespace) -> QuotaStorage:
    """Create quota storage from parsed *arguments*."""

    return QuotaStorage(
        quota_gb=arguments.quota_gb,
        reserve_gb=arguments.quota_reserve_gb,
        usage_root=arguments.quota_usage_root,
    )


def main(argv: list[str] | None = None) -> int:
    """Execute the command described by optional *argv*."""

    arguments = _parser().parse_args(argv)
    builder = _builder(arguments)
    if arguments.command == "fetch-catalog":
        catalog = builder.fetch_runs(arguments.query, refresh=arguments.refresh)
        arguments.output.parent.mkdir(parents=True, exist_ok=True)
        catalog.frame.write_csv(arguments.output)
        return 0
    if arguments.command == "build-local":
        report = builder.build(
            _catalog(builder, arguments),
            arguments.processor,
            execution=LocalExecution(
                total_cpus=arguments.total_cpus,
                min_cpus_per_job=arguments.min_cpus_per_job,
                max_cpus_per_job=arguments.max_cpus_per_job,
                max_running_jobs=arguments.max_running_jobs,
                storage=FilesystemStorage(reserve_free_gb=arguments.reserve_free_gb),
            ),
            queue=_queue(arguments),
            retry_failed=arguments.retry_failed,
            query=arguments.query,
        )
        return 1 if report.failed else 0
    if arguments.command in {"submit-single-node", "submit-distributed"}:
        common = {
            "partition": arguments.partition,
            "account": arguments.account,
            "qos": arguments.qos,
            "storage": _quota(arguments),
        }
        if arguments.command == "submit-single-node":
            execution = SlurmSingleNodeExecution(
                allocation_cpus=arguments.allocation_cpus,
                allocation_memory_gb=arguments.allocation_memory_gb,
                allocation_time_limit=arguments.allocation_time_limit,
                min_cpus_per_job=arguments.min_cpus_per_job,
                max_cpus_per_job=arguments.max_cpus_per_job,
                memory_gb_per_job=arguments.memory_gb_per_job,
                max_running_jobs=arguments.max_running_jobs,
                **common,
            )
        else:
            execution = SlurmDistributedExecution(
                total_cpu_quota=arguments.total_cpu_quota,
                max_running_jobs=arguments.max_running_jobs,
                cpus_per_node=arguments.cpus_per_node,
                min_cpus_per_job=arguments.min_cpus_per_job,
                max_cpus_per_job=arguments.max_cpus_per_job,
                memory_gb_per_job=arguments.memory_gb_per_job,
                worker_time_limit=arguments.worker_time_limit,
                coordinator_cpus=arguments.coordinator_cpus,
                coordinator_memory_gb=arguments.coordinator_memory_gb,
                coordinator_time_limit=arguments.coordinator_time_limit,
                **common,
            )
        builder.submit_slurm(
            _catalog(builder, arguments),
            processor_reference=arguments.processor,
            execution=execution,
            queue=_queue(arguments),
            retry_failed=arguments.retry_failed,
            script_path=arguments.script_path,
            submit=not arguments.no_submit,
            query=arguments.query,
        )
        return 0
    if arguments.command == "status":
        print(builder.status(arguments.execution_id))
        return 0
    export = builder.publish_dataset(
        arguments.destination,
        execution_id=arguments.execution_id,
        mode=arguments.mode,
        overwrite=arguments.overwrite,
    )
    print(export.manifest)
    return 0
