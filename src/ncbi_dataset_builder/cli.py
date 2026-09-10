from __future__ import annotations

import argparse
import json
import os
import re
import tempfile
from pathlib import Path
from typing import Any

import polars as pl

from .execution import SlurmOptions
from .metadata import sanitize_legacy_metadata
from .models import ResourceSpec
from .pipeline import PipelinePolicy
from .processing.base import load_processor
from .workflow import BuilderConfig, DatasetBuilder


def _write_csv(frame: pl.DataFrame, path: Path) -> None:
    """Atomically write Polars *frame* as CSV to *path*."""

    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".part", dir=path.parent
    )
    os.close(descriptor)
    temporary = Path(temporary_name)
    try:
        frame.write_csv(temporary)
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _value(raw: str) -> Any:
    """Decode JSON scalar *raw*, falling back to the original string."""

    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return raw


def _condition(specification: str) -> pl.Expr:
    """Parse a CLI filter *specification* into a safe Polars expression."""

    match = re.fullmatch(r"\s*([^<>=!~]+?)\s*(==|!=|>=|<=|>|<|~)\s*(.*?)\s*", specification)
    if not match:
        raise ValueError(f"Invalid filter {specification!r}; use COLUMN==VALUE or COLUMN~substring")
    column, operator, raw = match.groups()
    value = _value(raw)
    expression = pl.col(column)
    if operator == "==":
        return expression == value
    if operator == "!=":
        return expression != value
    if operator == ">=":
        return expression >= value
    if operator == "<=":
        return expression <= value
    if operator == ">":
        return expression > value
    if operator == "<":
        return expression < value
    return expression.cast(pl.String).str.contains(str(value), literal=True)


def _builder(arguments: argparse.Namespace) -> DatasetBuilder:
    """Create a dataset builder from parsed CLI *arguments*."""

    return DatasetBuilder(
        BuilderConfig(
            workspace=arguments.workspace,
            email=arguments.email,
            ncbi_api_key=arguments.api_key,
            max_workers=arguments.max_workers,
            total_threads=arguments.total_threads,
            total_memory_gb=arguments.total_memory_gb,
            prefetch_max_size=getattr(arguments, "prefetch_max_size", "u"),
            show_progress=not arguments.no_progress,
            progress_bars=not arguments.no_progress_bars,
        )
    )


def build_parser() -> argparse.ArgumentParser:
    """Build and return the complete ``ncbi-dataset`` argument parser."""

    parser = argparse.ArgumentParser(prog="ncbi-dataset")
    parser.add_argument("--workspace", type=Path, default=Path("workspace"))
    parser.add_argument("--email", default=os.environ.get("NCBI_EMAIL"))
    parser.add_argument("--api-key", default=os.environ.get("NCBI_API_KEY"))
    parser.add_argument("--max-workers", type=int, default=1)
    parser.add_argument("--total-threads", type=int)
    parser.add_argument("--total-memory-gb", type=float)
    parser.add_argument("--no-progress", action="store_true")
    parser.add_argument(
        "--no-progress-bars",
        action="store_true",
        help="Use periodic text progress instead of optional tqdm bars",
    )
    commands = parser.add_subparsers(dest="command", required=True)

    fetch = commands.add_parser("fetch-runs", help="Fetch complete SRA RunInfo for an Entrez query")
    fetch.add_argument("query")
    fetch.add_argument("--output", type=Path)
    fetch.add_argument("--refresh", action="store_true")

    geo = commands.add_parser("resolve-geo", help="Resolve GSE/GSM accessions to SRA RunInfo")
    geo.add_argument("accessions", nargs="+")
    geo.add_argument("--output", type=Path, required=True)

    filtering = commands.add_parser("filter", help="Apply repeatable safe filters to RunInfo CSV")
    filtering.add_argument("--catalog", type=Path, required=True)
    filtering.add_argument("--where", action="append", default=[], required=True)
    filtering.add_argument("--output", type=Path, required=True)

    enrich = commands.add_parser(
        "enrich-metadata", help="Fetch structured SRA and BioSample XML metadata"
    )
    enrich.add_argument("--catalog", type=Path, required=True)
    enrich.add_argument("--output", type=Path, required=True)
    enrich.add_argument("--include-raw-trees", action="store_true")

    metadata = commands.add_parser(
        "fetch-metadata",
        help="Fetch complete structured metadata for SRA accessions",
    )
    metadata.add_argument("accessions", nargs="+")
    metadata.add_argument("--output", type=Path, required=True)
    metadata.add_argument("--include-raw-trees", action="store_true")

    for metadata_command in (enrich, metadata):
        metadata_command.add_argument(
            "--refresh",
            action="store_true",
            help="Bypass normalized metadata and raw Entrez response caches",
        )
        metadata_command.add_argument(
            "--description-profile",
            choices=("training", "full"),
            default="training",
            help="Per-sample JSON profile; complete metadata.json is retained separately",
        )
        metadata_command.add_argument(
            "--legacy-descriptions",
            type=Path,
            help="Directory of old flat sampleDescriptions JSON whose values must be preserved",
        )

    sanitize = commands.add_parser(
        "sanitize-legacy-metadata",
        help="Remove leaked presentation HTML and decode entities in old JSON snapshots",
    )
    sanitize.add_argument("paths", nargs="+", type=Path)

    build = commands.add_parser("build", help="Reconcile a catalog and build required units")
    build.add_argument("--catalog", type=Path, required=True)
    build.add_argument("--processor", required=True)
    build.add_argument("--retry-failed", action="store_true")
    build.add_argument("--batch-id", type=int, action="append")

    submit = commands.add_parser(
        "submit-slurm", help="Generate or submit one bounded-pipeline coordinator job"
    )
    submit.add_argument("--catalog", type=Path, required=True)
    submit.add_argument("--processor", required=True)
    submit.add_argument("--script", type=Path)
    submit.add_argument("--partition")
    submit.add_argument("--account")
    submit.add_argument("--qos")
    submit.add_argument("--max-parallel", type=int)
    submit.add_argument(
        "--slurm-mode", choices=("single_node", "distributed"), default="single_node"
    )
    submit.add_argument("--total-cpu-quota", type=int)
    submit.add_argument("--max-running-jobs", type=int)
    submit.add_argument("--coordinator-cpus", type=int, default=1)
    submit.add_argument("--coordinator-memory-gb", type=int, default=4)
    submit.add_argument("--coordinator-time-limit", default="7-00:00:00")
    submit.add_argument("--cpus-per-node", type=int)
    submit.add_argument("--dry-run", action="store_true")
    submit.add_argument("--retry-failed", action="store_true")
    submit.add_argument("--batch-id", type=int, action="append")

    for pipeline_command in (build, submit):
        pipeline_command.add_argument(
            "--group-by",
            choices=("run", "experiment", "sra_sample", "biosample"),
            default="experiment",
        )
        pipeline_command.add_argument("--threads", type=int, default=4)
        pipeline_command.add_argument("--memory-gb", type=int, default=16)
        pipeline_command.add_argument("--time-limit", default="24:00:00")
        pipeline_command.add_argument("--max-batch-gb", type=float)
        pipeline_command.add_argument("--max-batch-units", type=int)
        pipeline_command.add_argument(
            "--prefetch-max-size",
            default="u",
            help="SRA Toolkit archive limit such as 200G, or u for unlimited",
        )
        pipeline_command.add_argument("--prefetch-batches", type=int, choices=(0, 1), default=1)
        pipeline_command.add_argument("--download-workers", type=int, default=2)
        pipeline_command.add_argument("--max-staged-gb", type=float)
        pipeline_command.add_argument("--minimum-free-gb", type=float, default=0.0)
        pipeline_command.add_argument(
            "--cleanup", choices=("after_success", "never"), default="after_success"
        )
        pipeline_command.add_argument("--discard-failed-inputs", action="store_true")
        pipeline_command.add_argument("--no-fsync-logs", action="store_true")

    status = commands.add_parser("status", help="Summarize durable task state")
    status.add_argument("--job-id")
    status.add_argument("--batch-id", type=int, action="append")

    publish = commands.add_parser(
        "publish", help="Publish completed experiment outputs as a compact dataset"
    )
    publish.add_argument("--job-id")
    publish.add_argument("--destination", type=Path)
    publish.add_argument("--mode", choices=("auto", "hardlink", "copy"), default="auto")
    publish.add_argument("--overwrite", action="store_true")

    preflight = commands.add_parser("preflight", help="Check external executables and versions")
    preflight.add_argument("--processor")
    return parser


def main(argv: list[str] | None = None) -> int:
    """Run the CLI with optional *argv* and return its process exit code."""

    parser = build_parser()
    arguments = parser.parse_args(argv)
    try:
        legacy_descriptions = None
        legacy_path = getattr(arguments, "legacy_descriptions", None)
        if legacy_path is not None:
            if not legacy_path.is_dir():
                raise ValueError(f"Legacy description directory does not exist: {legacy_path}")
            legacy_descriptions = {
                path.stem: json.loads(path.read_text(encoding="utf-8"))
                for path in legacy_path.glob("*.json")
            }
        builder = _builder(arguments)
        if arguments.command == "fetch-runs":
            catalog = builder.fetch_runs(arguments.query, refresh=arguments.refresh)
            output = arguments.output or arguments.workspace / "latest-runinfo.csv"
            _write_csv(catalog.frame, output)
            print(
                json.dumps(
                    {
                        "rows": catalog.frame.height,
                        "output": str(output),
                        "audit": catalog.audit,
                    }
                )
            )
        elif arguments.command == "resolve-geo":
            catalog = builder.fetch_geo_runs(arguments.accessions)
            _write_csv(catalog.frame, arguments.output)
            print(json.dumps({"rows": catalog.frame.height, "output": str(arguments.output)}))
        elif arguments.command == "filter":
            catalog = builder.load_runs(arguments.catalog)
            for condition in arguments.where:
                catalog = catalog.filter(_condition(condition), description=condition)
            _write_csv(catalog.frame, arguments.output)
            print(
                json.dumps(
                    {
                        "rows": catalog.frame.height,
                        "output": str(arguments.output),
                        "audit": catalog.audit,
                    }
                )
            )
        elif arguments.command == "enrich-metadata":
            bundle = builder.enrich_metadata(
                builder.load_runs(arguments.catalog),
                destination=arguments.output,
                include_raw=arguments.include_raw_trees,
                refresh=arguments.refresh,
                description_profile=arguments.description_profile,
                legacy_descriptions=legacy_descriptions,
            )
            print(json.dumps({key: len(value) for key, value in bundle.to_dict().items()}))
        elif arguments.command == "fetch-metadata":
            bundle = builder.fetch_metadata(
                arguments.accessions,
                destination=arguments.output,
                include_raw=arguments.include_raw_trees,
                refresh=arguments.refresh,
                description_profile=arguments.description_profile,
                legacy_descriptions=legacy_descriptions,
            )
            print(json.dumps({key: len(value) for key, value in bundle.to_dict().items()}))
        elif arguments.command == "sanitize-legacy-metadata":
            changed = sanitize_legacy_metadata(arguments.paths)
            print(json.dumps({"changed": len(changed)}))
        elif arguments.command == "build":
            policy = PipelinePolicy(
                prefetch_batches=arguments.prefetch_batches,
                max_staged_gb=arguments.max_staged_gb,
                minimum_free_gb=arguments.minimum_free_gb,
                cleanup=arguments.cleanup,
                keep_failed_inputs=not arguments.discard_failed_inputs,
                fsync_logs=not arguments.no_fsync_logs,
                download_workers=arguments.download_workers,
            )
            report = builder.build(
                builder.load_runs(arguments.catalog),
                arguments.processor,
                group_by=arguments.group_by,
                resources=ResourceSpec(
                    arguments.threads, arguments.memory_gb, arguments.time_limit
                ),
                max_batch_gb=arguments.max_batch_gb,
                max_batch_units=arguments.max_batch_units,
                retry_failed=arguments.retry_failed,
                batch_ids=set(arguments.batch_id) if arguments.batch_id else None,
                policy=policy,
            )
            print(
                json.dumps(
                    {
                        "succeeded": report.succeeded,
                        "failed": report.failed,
                        "skipped": report.skipped,
                        "job_id": report.job_id,
                    }
                )
            )
            return 1 if report.failed else 0
        elif arguments.command == "submit-slurm":
            resources = ResourceSpec(
                arguments.threads, arguments.memory_gb, arguments.time_limit
            )
            script, slurm_job_id = builder.submit_slurm(
                builder.load_runs(arguments.catalog),
                processor_reference=arguments.processor,
                options=SlurmOptions(
                    resources=resources,
                    max_parallel=arguments.max_parallel,
                    partition=arguments.partition,
                    account=arguments.account,
                    qos=arguments.qos,
                    mode=arguments.slurm_mode,
                    total_cpu_quota=arguments.total_cpu_quota,
                    max_running_jobs=arguments.max_running_jobs,
                    coordinator_cpus=arguments.coordinator_cpus,
                    coordinator_memory_gb=arguments.coordinator_memory_gb,
                    coordinator_time_limit=arguments.coordinator_time_limit,
                    cpus_per_node=arguments.cpus_per_node,
                ),
                group_by=arguments.group_by,
                max_batch_gb=arguments.max_batch_gb,
                max_batch_units=arguments.max_batch_units,
                script_path=arguments.script,
                submit=not arguments.dry_run,
                retry_failed=arguments.retry_failed,
                batch_ids=set(arguments.batch_id) if arguments.batch_id else None,
                policy=PipelinePolicy(
                    prefetch_batches=arguments.prefetch_batches,
                    max_staged_gb=arguments.max_staged_gb,
                    minimum_free_gb=arguments.minimum_free_gb,
                    cleanup=arguments.cleanup,
                    keep_failed_inputs=not arguments.discard_failed_inputs,
                    fsync_logs=not arguments.no_fsync_logs,
                    download_workers=arguments.download_workers,
                ),
            )
            print(
                json.dumps(
                    {
                        "script": str(script),
                        "workspace_job_id": builder.workspace.latest_job().job_id,
                        "slurm_job_id": slurm_job_id,
                    }
                )
            )
        elif arguments.command == "status":
            print(
                json.dumps(
                    builder.status(
                        arguments.job_id,
                        batch_ids=set(arguments.batch_id) if arguments.batch_id else None,
                    ),
                    indent=2,
                )
            )
        elif arguments.command == "publish":
            result = builder.publish_dataset(
                destination=arguments.destination,
                job_id=arguments.job_id,
                mode=arguments.mode,
                overwrite=arguments.overwrite,
            )
            print(
                json.dumps(
                    {
                        "destination": str(result.destination),
                        "manifest": str(result.manifest),
                        "experiments": result.experiments,
                        "genomes": result.genomes,
                    }
                )
            )
        elif arguments.command == "preflight":
            versions = {
                "sra": builder.fastq_provider.preflight(),
                "genomes": builder.genomes.preflight(),
            }
            if arguments.processor:
                processor = load_processor(arguments.processor)
                if hasattr(processor, "preflight"):
                    versions["processor"] = processor.preflight()
            print(json.dumps(versions, indent=2))
        return 0
    except (ValueError, OSError, RuntimeError) as exc:
        parser.exit(2, f"error: {exc}\n")


if __name__ == "__main__":
    raise SystemExit(main())
