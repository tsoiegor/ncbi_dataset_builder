"""High-level workspace API and sample-streaming scheduler."""

from __future__ import annotations

import hashlib
import inspect
import json
import logging
import os
import shutil
import tempfile
import traceback
from collections.abc import Iterable, Mapping
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from dataclasses import asdict, dataclass, field, replace
from pathlib import Path
from typing import Any

from .acquisition.fastq import FastqProvider, SraToolkitProvider
from .acquisition.genomes import GenomeManager, GenomeSelectionPolicy
from .acquisition.geo import GeoClient
from .catalog import GroupLevel, RunCatalog, validate_polars_runtime
from .errors import UnitAlreadyRunning
from .execution.config import (
    ExecutionSystem,
    LocalExecution,
    QueuePolicy,
    SlurmDistributedExecution,
    SlurmSingleNodeExecution,
    execution_to_dict,
    queue_policy_to_dict,
)
from .execution.records import ExecutionRecord, QueueItem, UnitResources
from .execution.state import UnitStateStore
from .execution.storage import paths_size_gb
from .metadata import (
    BioSampleClient,
    EntrezClient,
    MetadataBundle,
    SraClient,
    fetch_metadata_for_accessions,
    fetch_metadata_for_catalog,
)
from .metadata.descriptions import DescriptionPolicy
from .models import FastqSet, GenomeRef, ProcessingResult, ProcessingUnit, StagedFastq
from .processing.base import Processor, load_processor
from .support.progress import ProgressReporter
from .support.unit_logging import install_unit_logging, unit_log
from .support.util import exclusive_file_lock, sanitize_identifier, sha256_file, utc_timestamp
from .workspace import WorkspaceStore
from .workspace.publishing import DatasetExport, DatasetPublisher, PublishMode

LOGGER = logging.getLogger("ncbi_dataset_builder.api")


@dataclass(frozen=True)
class BuilderConfig:
    """Configure stable NCBI and workspace behavior.

    Args:
        workspace: Root directory for caches, state, work, and outputs.
        email: Contact email required by NCBI Entrez.
        ncbi_api_key: Optional NCBI key for a higher request rate.
        genome_policy: Policy used to choose NCBI assemblies.
        group_by: Catalog entity represented by one processing unit.
        description_profile: ``training`` or ``full`` metadata projection.
        prefetch_max_size: SRA Toolkit archive-size limit, or ``u`` for unlimited.
        show_progress: Display long-running operation progress.
        progress_bars: Use tqdm bars when available.

    CPU, memory, storage, and concurrency belong to an execution-system object,
    not to this stable builder configuration.
    """

    workspace: Path
    email: str | None = None
    ncbi_api_key: str | None = None
    genome_policy: GenomeSelectionPolicy = field(default_factory=GenomeSelectionPolicy)
    group_by: GroupLevel = "experiment"
    description_profile: str = "training"
    prefetch_max_size: str = "u"
    show_progress: bool = True
    progress_bars: bool = True

    def __post_init__(self) -> None:
        """Normalize the workspace and validate stable settings."""

        object.__setattr__(self, "workspace", Path(self.workspace))
        if self.group_by not in {"run", "experiment", "sra_sample", "biosample"}:
            raise ValueError(f"Unknown workspace grouping level: {self.group_by!r}")
        if self.description_profile not in {"training", "full"}:
            raise ValueError(f"Unknown description profile: {self.description_profile!r}")
        if not self.prefetch_max_size.strip():
            raise ValueError("prefetch_max_size cannot be empty")


@dataclass(frozen=True)
class UnitOutcome:
    """Record one sample outcome.

    Args:
        unit_id: Stable processing-unit identifier.
        status: ``succeeded``, ``failed``, or ``skipped``.
        result: Optional serialized successful result.
        error: Optional failure or skip explanation.
    """

    unit_id: str
    status: str
    result: dict[str, Any] | None = None
    error: str | None = None


@dataclass(frozen=True)
class BuildReport:
    """Collect ordered outcomes for one automatic workspace execution.

    Args:
        outcomes: Ordered per-sample outcomes.
        execution_id: Automatic workspace execution identifier.
    """

    outcomes: tuple[UnitOutcome, ...]
    execution_id: str

    @property
    def succeeded(self) -> int:
        """Return the number of succeeded samples."""

        return sum(item.status == "succeeded" for item in self.outcomes)

    @property
    def failed(self) -> int:
        """Return the number of failed samples."""

        return sum(item.status == "failed" for item in self.outcomes)

    @property
    def skipped(self) -> int:
        """Return the number of samples reused from workspace state."""

        return sum(item.status == "skipped" for item in self.outcomes)


@dataclass(frozen=True)
class _PreparedUnit:
    """Hold one queue item between download and processing."""

    item: QueueItem
    log_path: Path
    genome: GenomeRef | None = None
    staged: StagedFastq | None = None
    outcome: UnitOutcome | None = None
    reset_outputs: bool = False


class DatasetBuilder:
    """Build and resume NCBI datasets inside one durable workspace."""

    def __init__(
        self,
        config: BuilderConfig,
        *,
        fastq_provider: FastqProvider | None = None,
        genome_manager: GenomeManager | None = None,
        progress: ProgressReporter | None = None,
    ) -> None:
        """Initialize from *config* and optional service implementations.

        Args:
            config: Stable workspace and NCBI configuration.
            fastq_provider: Optional custom input provider.
            genome_manager: Optional custom genome manager.
            progress: Optional progress reporter.
        """

        self.config = config
        install_unit_logging()
        self.progress = progress or ProgressReporter(
            enabled=config.show_progress,
            use_bars=config.progress_bars,
        )
        self.workspace = WorkspaceStore(config.workspace)
        if config.email:
            entrez = EntrezClient(
                email=config.email,
                api_key=config.ncbi_api_key,
                cache_dir=config.workspace / "metadata_cache",
                progress=self.progress,
            )
            self.sra: SraClient | None = SraClient(entrez)
            self.biosample: BioSampleClient | None = BioSampleClient(entrez)
            self.geo: GeoClient | None = GeoClient(entrez, self.sra)
        else:
            self.sra = None
            self.biosample = None
            self.geo = None
        self.fastq_provider = fastq_provider or SraToolkitProvider(
            prefetch_max_size=config.prefetch_max_size,
            progress=self.progress,
        )
        self.genomes = genome_manager or GenomeManager(
            config.workspace / "work" / "genome_cache",
            policy=config.genome_policy,
            progress=self.progress,
        )
        self.state = UnitStateStore(config.workspace / "state" / "units")

    def fetch_runs(self, query: str, *, refresh: bool = False) -> RunCatalog:
        """Fetch an SRA RunInfo catalog for *query*.

        Args:
            query: NCBI SRA search expression, for example
                ``'"ATAC-seq"[Strategy] AND "Homo sapiens"[Organism]'``.
            refresh: Bypass a cached catalog and query NCBI again.
        """

        if self.sra is None:
            raise ValueError("fetch_runs requires an email in BuilderConfig")
        validate_polars_runtime()
        digest = hashlib.sha256(query.encode("utf-8")).hexdigest()[:16]
        cache = self.config.workspace / "catalogs" / f"sra.{digest}.csv"
        if cache.is_file() and not refresh:
            return RunCatalog.from_csv(cache).deduplicate_runs()
        catalog = self.sra.fetch_runinfo(query, refresh=refresh)
        cache.parent.mkdir(parents=True, exist_ok=True)
        descriptor, temporary_name = tempfile.mkstemp(
            prefix=f".{cache.name}.", suffix=".part", dir=cache.parent
        )
        os.close(descriptor)
        temporary = Path(temporary_name)
        try:
            catalog.frame.write_csv(temporary)
            os.replace(temporary, cache)
        finally:
            temporary.unlink(missing_ok=True)
        return catalog.replace_frame(catalog.frame, event=f"cached SRA query {query!r}")

    @staticmethod
    def load_runs(path: str | Path) -> RunCatalog:
        """Load and deduplicate a RunInfo CSV from *path*."""

        return RunCatalog.from_csv(path).deduplicate_runs()

    def fetch_geo_runs(self, accessions: list[str]) -> RunCatalog:
        """Resolve GEO GSE/GSM *accessions* to an SRA run catalog."""

        if self.geo is None:
            raise ValueError("fetch_geo_runs requires an email in BuilderConfig")
        return self.geo.resolve_to_sra(accessions)

    def fetch_metadata(
        self,
        accessions: list[str],
        *,
        destination: Path | None = None,
        include_raw: bool = False,
        refresh: bool = False,
        description_profile: str | None = None,
        description_policy: DescriptionPolicy | None = None,
    ) -> MetadataBundle:
        """Fetch normalized metadata for SRA *accessions* and save it.

        Args:
            accessions: SRA accessions such as ``SRP...``, ``SRX...``, or ``SRR...``.
            destination: Output directory; defaults to ``workspace/metadata``.
            include_raw: Retain parsed raw XML trees in the bundle.
            refresh: Bypass reusable NCBI response caches.
            description_profile: ``training`` or ``full``; defaults to builder config.
            description_policy: Optional compact-field selection policy.
        """

        if self.sra is None or self.biosample is None:
            raise ValueError("fetch_metadata requires an email in BuilderConfig")
        bundle = fetch_metadata_for_accessions(
            accessions,
            sra=self.sra,
            biosample=self.biosample,
            include_raw=include_raw,
            refresh=refresh,
        )
        bundle.save(
            destination or self.config.workspace / "metadata",
            description_profile=description_profile or self.config.description_profile,
            policy=description_policy,
            progress=self.progress,
        )
        return bundle

    def enrich_metadata(
        self,
        catalog: RunCatalog,
        *,
        destination: Path | None = None,
        include_raw: bool = False,
        refresh: bool = False,
        description_profile: str | None = None,
        description_policy: DescriptionPolicy | None = None,
    ) -> MetadataBundle:
        """Fetch and save normalized metadata for runs in *catalog*.

        Args:
            catalog: Run catalog whose linked accessions are fetched.
            destination: Output directory; defaults to ``workspace/metadata``.
            include_raw: Retain parsed raw XML trees in the bundle.
            refresh: Bypass reusable NCBI response caches.
            description_profile: ``training`` or ``full``; defaults to builder config.
            description_policy: Optional compact-field selection policy.
        """

        if self.sra is None or self.biosample is None:
            raise ValueError("enrich_metadata requires an email in BuilderConfig")
        bundle = fetch_metadata_for_catalog(
            catalog,
            sra=self.sra,
            biosample=self.biosample,
            include_raw=include_raw,
            refresh=refresh,
        )
        bundle.save(
            destination or self.config.workspace / "metadata",
            description_profile=description_profile or self.config.description_profile,
            policy=description_policy,
            progress=self.progress,
        )
        return bundle

    @staticmethod
    def _processor_identity(
        processor: Processor | str,
        processor_id: str | None,
        resolved: Processor | None = None,
    ) -> str:
        """Return a stable identity for *processor* or explicit *processor_id*."""

        if processor_id:
            return processor_id
        if isinstance(processor, str):
            return processor
        target = resolved if resolved is not None else processor
        module = getattr(target, "__module__", target.__class__.__module__)
        name = getattr(target, "__qualname__", target.__class__.__qualname__)
        identity = f"{module}:{name}"
        config = getattr(target, "config", None)
        if config is not None:
            identity += f":{config!r}"
        source_target = target if inspect.isfunction(target) else target.__class__
        source = inspect.getsourcefile(source_target)
        if source and Path(source).is_file():
            identity += f":source_sha256={sha256_file(Path(source))}"
        return identity

    def _fastq_identity(self, unit: ProcessingUnit) -> str:
        """Return the semantic input-provider identity for *unit*."""

        explicit = getattr(self.fastq_provider, "cache_identity", None)
        if callable(explicit):
            return str(explicit(unit))
        provider_class = self.fastq_provider.__class__
        identity: dict[str, Any] = {
            "provider": f"{provider_class.__module__}:{provider_class.__qualname__}"
        }
        config = getattr(self.fastq_provider, "config", None)
        if config is not None:
            identity["config"] = repr(config)
        urls = getattr(self.fastq_provider, "urls", None)
        if isinstance(urls, Mapping):
            identity["urls"] = list(urls.get(unit.unit_id, ()))
        return json.dumps(identity, sort_keys=True, separators=(",", ":"))

    def _fingerprint(
        self,
        unit: ProcessingUnit,
        *,
        group_by: GroupLevel,
        genome_pin: str | None,
        processor_identity: str,
    ) -> str:
        """Hash all semantic inputs for *unit*."""

        identity = {
            "group_by": group_by,
            "unit_id": unit.unit_id,
            "run_accessions": list(unit.run_accessions),
            "experiment_accessions": list(unit.experiment_accessions),
            "sra_sample_accessions": list(unit.sra_sample_accessions),
            "biosample_accessions": list(unit.biosample_accessions),
            "taxid": unit.taxid,
            "genome_pin": genome_pin,
            "processor_identity": processor_identity,
            "fastq_identity": self._fastq_identity(unit),
        }
        return hashlib.sha256(
            json.dumps(identity, sort_keys=True, separators=(",", ":")).encode("utf-8")
        ).hexdigest()

    @staticmethod
    def _execution_id(created_at: str, items: Iterable[QueueItem]) -> str:
        """Return a timestamp and content hash for an automatic execution."""

        digest = hashlib.sha256(
            "\n".join(item.fingerprint for item in items).encode("utf-8")
        ).hexdigest()[:10]
        stamp = (
            created_at.split("+", 1)[0]
            .replace("-", "")
            .replace(":", "")
            .replace(".", "")
            + "Z"
        )
        return f"execution-{stamp}-{digest}"

    @staticmethod
    def _unit_resources(execution: ExecutionSystem) -> UnitResources:
        """Derive per-sample minimum resources from *execution*."""

        if isinstance(execution, LocalExecution):
            return UnitResources(cpus=execution.min_cpus_per_job)
        if isinstance(execution, SlurmSingleNodeExecution):
            return UnitResources(
                cpus=execution.min_cpus_per_job,
                memory_gb=execution.memory_gb_per_job,
                time_limit=execution.allocation_time_limit,
            )
        return UnitResources(
            cpus=execution.min_cpus_per_job,
            memory_gb=execution.memory_gb_per_job,
            time_limit=execution.worker_time_limit,
        )

    def _create_execution(
        self,
        catalog: RunCatalog,
        processor: Processor | str,
        *,
        execution: ExecutionSystem,
        queue: QueuePolicy,
        group_by: GroupLevel | None,
        genome_pins: dict[int, str] | None,
        query: str | None,
        processor_id: str | None,
    ) -> ExecutionRecord:
        """Reconcile *catalog* with workspace state and save an execution record."""

        clean = catalog.deduplicate_runs()
        selected_group = group_by or self.config.group_by
        resolved = None if isinstance(processor, str) else processor
        processor_identity = self._processor_identity(processor, processor_id, resolved)
        self.workspace.configure(
            group_by=selected_group,
            description_profile=self.config.description_profile,
            genome_policy=asdict(self.config.genome_policy),
        )
        pins = genome_pins or {}
        resources = self._unit_resources(execution)
        items: list[QueueItem] = []
        for original in clean.processing_units(by=selected_group):
            unit = replace(
                original,
                run_accessions=tuple(sorted(original.run_accessions)),
                experiment_accessions=tuple(sorted(original.experiment_accessions)),
                sra_sample_accessions=tuple(sorted(original.sra_sample_accessions)),
                biosample_accessions=tuple(sorted(original.biosample_accessions)),
            )
            pin = pins.get(unit.taxid) if unit.taxid is not None else None
            items.append(
                QueueItem(
                    item_id=sanitize_identifier(unit.unit_id),
                    unit=unit,
                    resources=resources,
                    genome_pin=pin,
                    fingerprint=self._fingerprint(
                        unit,
                        group_by=selected_group,
                        genome_pin=pin,
                        processor_identity=processor_identity,
                    ),
                )
            )
        created_at = utc_timestamp()
        execution_record = ExecutionRecord(
            execution_id=self._execution_id(created_at, items),
            created_at=created_at,
            query=query,
            group_by=selected_group,
            items=tuple(items),
            processor_identity=processor_identity,
            execution_type=execution.__class__.__name__,
            execution_config=execution_to_dict(execution),
            queue_config=queue_policy_to_dict(queue),
            catalog_audit=clean.audit,
            metadata={
                "fastq_provider": (
                    f"{self.fastq_provider.__class__.__module__}:"
                    f"{self.fastq_provider.__class__.__qualname__}"
                ),
            },
        )
        self.workspace.save_execution(execution_record)
        self.workspace.sync_manifest(
            execution_record,
            {item.item_id: self.state.get(item.item_id) for item in items},
        )
        return execution_record

    def _outputs_valid(self, state: dict[str, Any]) -> bool:
        """Return whether persisted outputs and genome artifacts remain valid."""

        result = state.get("result")
        processing = result.get("processing") if isinstance(result, dict) else None
        if not isinstance(processing, dict):
            return False
        outputs = [Path(str(value)) for value in processing.get("outputs", ())]
        if not outputs or any(not path.is_file() or path.stat().st_size == 0 for path in outputs):
            return False
        expected = result.get("output_sha256", {})
        facts = result.get("output_files", {})
        for path in outputs:
            fact = facts.get(str(path), {}) if isinstance(facts, dict) else {}
            if (
                isinstance(fact, dict)
                and fact.get("size") == path.stat().st_size
                and fact.get("modified_ns") == path.stat().st_mtime_ns
            ):
                continue
            checksum = expected.get(str(path)) if isinstance(expected, dict) else None
            if not checksum or sha256_file(path, progress=self.progress) != checksum:
                return False
        genome = result.get("genome")
        if not isinstance(genome, dict):
            return False
        fasta = Path(str(genome.get("fasta", "")))
        return bool(fasta.is_file() and fasta.stat().st_size > 0)

    def _unit_log_path(self, item: QueueItem) -> Path:
        """Return the permanent workspace log path for *item*."""

        species = sanitize_identifier(item.unit.scientific_name or "unknown_species")
        return self.config.workspace / "logs" / species / f"{item.item_id}.log"

    def _stage_fastq(self, item: QueueItem) -> StagedFastq:
        """Stage input for *item*, adapting providers that expose only ``fetch``."""

        destination = self.config.workspace / "fastq"
        stage = getattr(self.fastq_provider, "stage", None)
        if callable(stage):
            return stage(item.unit, destination, threads=item.resources.cpus)
        ready = self.fastq_provider.fetch(item.unit, destination, threads=item.resources.cpus)
        return StagedFastq(
            unit_id=item.unit.unit_id,
            source=ready.source,
            size_gb=paths_size_gb([ready.work_dir]),
            cleanup_roots=(ready.work_dir,),
            ready_fastq=ready,
            metadata={"provider_mode": "fetch_during_staging"},
        )

    def _materialize_fastq(self, prepared: _PreparedUnit) -> FastqSet:
        """Materialize processor input for *prepared*."""

        if prepared.staged is None:
            raise ValueError(f"Sample {prepared.item.item_id} has no staged input")
        materialize = getattr(self.fastq_provider, "materialize", None)
        if callable(materialize):
            return materialize(
                prepared.item.unit,
                prepared.staged,
                self.config.workspace / "fastq",
                threads=prepared.item.resources.cpus,
            )
        if prepared.staged.ready_fastq is None:
            raise TypeError("Input provider supplied neither materialize() nor ready FASTQ")
        return prepared.staged.ready_fastq

    def _claim_and_stage(
        self,
        item: QueueItem,
        *,
        execution_id: str,
        retry_failed: bool,
        queue: QueuePolicy,
        reclaim_running: bool,
    ) -> _PreparedUnit:
        """Claim and download one queue *item*."""

        log_path = self._unit_log_path(item)
        previous = self.state.get(item.item_id)
        force = bool(
            previous
            and previous.get("status") == "succeeded"
            and previous.get("fingerprint") == item.fingerprint
            and not self._outputs_valid(previous)
        )
        reset_outputs = bool(
            previous is None
            or force
            or previous.get("fingerprint") != item.fingerprint
            or previous.get("status") == "failed"
            or previous.get("phase") == "processing"
        )
        try:
            claimed = self.state.start(
                item.item_id,
                fingerprint=item.fingerprint,
                execution_id=execution_id,
                item=item.to_dict(),
                log_path=log_path,
                retry_failed=retry_failed,
                reclaim_running=reclaim_running,
                force=force,
            )
        except UnitAlreadyRunning as exc:
            return _PreparedUnit(
                item,
                log_path,
                outcome=UnitOutcome(item.item_id, "skipped", error=str(exc)),
            )
        if not claimed:
            saved = self.state.get(item.item_id) or {}
            return _PreparedUnit(
                item,
                log_path,
                outcome=UnitOutcome(
                    item.item_id,
                    "skipped",
                    result=saved.get("result"),
                    error=saved.get("error"),
                ),
            )
        try:
            with (
                unit_log(
                    log_path,
                    phase="download",
                    unit_id=item.item_id,
                    fsync=queue.fsync_logs,
                ),
                self.progress.minimum_level(logging.WARNING),
            ):
                if item.unit.taxid is None:
                    raise ValueError(
                        f"Sample {item.item_id} has no TaxID; add it before execution"
                    )
                genome = self.genomes.resolve(
                    taxid=item.unit.taxid,
                    scientific_name=item.unit.scientific_name,
                    pin=item.genome_pin,
                )
                staged = self._stage_fastq(item)
                self.state.set_phase(item.item_id, "ready")
            return _PreparedUnit(
                item=item,
                log_path=log_path,
                genome=genome,
                staged=staged,
                reset_outputs=reset_outputs,
            )
        except Exception:  # noqa: BLE001 - sample boundary persists operational failures
            error = traceback.format_exc()
            self.state.fail(item.item_id, error)
            with unit_log(
                log_path,
                phase="download-error",
                unit_id=item.item_id,
                fsync=queue.fsync_logs,
            ):
                LOGGER.error("Input preparation failed:\n%s", error)
            return _PreparedUnit(
                item,
                log_path,
                outcome=UnitOutcome(item.item_id, "failed", error=error),
            )

    def _process_prepared(
        self,
        prepared: _PreparedUnit,
        processor: Processor,
        *,
        cpus: int,
        queue: QueuePolicy,
    ) -> UnitOutcome:
        """Materialize and process one downloaded sample with *cpus*."""

        item = prepared.item
        if prepared.outcome is not None:
            return prepared.outcome
        if prepared.genome is None or prepared.staged is None:
            raise ValueError(f"Prepared sample {item.item_id} is incomplete")
        try:
            self.state.set_phase(item.item_id, "processing")
            self.state.set_runtime_resources(
                item.item_id,
                cpus=cpus,
                memory_gb=item.resources.memory_gb,
            )
            with (
                unit_log(
                    prepared.log_path,
                    phase="processing",
                    unit_id=item.item_id,
                    fsync=queue.fsync_logs,
                ),
                self.progress.minimum_level(logging.WARNING),
            ):
                fastq = self._materialize_fastq(prepared)
                work_root = self.config.workspace / "work" / "units" / item.item_id
                output_root = self.config.workspace / "outputs" / item.item_id
                if prepared.reset_outputs:
                    for owned in (work_root, output_root):
                        if owned.is_dir():
                            shutil.rmtree(owned)
                        elif owned.exists():
                            owned.unlink()
                fastq = replace(
                    fastq,
                    work_dir=work_root,
                    output_dir=output_root,
                    metadata={**fastq.metadata, "unit_log_path": str(prepared.log_path)},
                )
                result = processor(fastq, prepared.genome, cpus)
                if not isinstance(result, ProcessingResult):
                    raise TypeError(
                        "Processor must return ProcessingResult, got "
                        f"{type(result).__name__} for {item.item_id}"
                    )
                result.validate()
                payload = {
                    "processing": result.to_dict(),
                    "output_sha256": {
                        str(path): sha256_file(path, progress=self.progress)
                        for path in result.outputs
                    },
                    "output_files": {
                        str(path): {
                            "size": path.stat().st_size,
                            "modified_ns": path.stat().st_mtime_ns,
                        }
                        for path in result.outputs
                    },
                    "genome": prepared.genome.to_dict(),
                    "fastq": fastq.to_dict(),
                    "log_path": str(prepared.log_path),
                }
                self.state.succeed(item.item_id, payload)
            return UnitOutcome(item.item_id, "succeeded", result=payload)
        except Exception:  # noqa: BLE001 - sample boundary persists operational failures
            error = traceback.format_exc()
            self.state.fail(item.item_id, error)
            with unit_log(
                prepared.log_path,
                phase="processing-error",
                unit_id=item.item_id,
                fsync=queue.fsync_logs,
            ):
                LOGGER.error("Processing failed:\n%s", error)
            return UnitOutcome(item.item_id, "failed", error=error)

    def _cleanup_input(
        self,
        prepared: _PreparedUnit,
        outcome: UnitOutcome,
        *,
        queue: QueuePolicy,
    ) -> bool:
        """Remove only provider-owned roots allowed by *queue*."""

        if queue.cleanup == "never" or prepared.staged is None:
            return True
        if outcome.status != "succeeded" and queue.keep_failed_inputs:
            return True
        boundary = (self.config.workspace / "fastq").resolve()
        try:
            for raw in prepared.staged.cleanup_roots:
                path = raw.resolve()
                if path == boundary or not path.is_relative_to(boundary):
                    raise ValueError(f"Refuse to clean provider input outside {boundary}: {path}")
                if path.is_dir():
                    shutil.rmtree(path)
                elif path.exists():
                    path.unlink()
            return True
        except Exception:
            LOGGER.exception("Input cleanup failed for %s", prepared.item.item_id)
            return False

    @staticmethod
    def _runtime_limits(
        execution: LocalExecution | SlurmSingleNodeExecution,
    ) -> tuple[int, int, int, float | None]:
        """Return total, minimum, maximum CPUs and optional memory."""

        if isinstance(execution, LocalExecution):
            return (
                execution.total_cpus,
                execution.min_cpus_per_job,
                execution.max_cpus_per_job or execution.total_cpus,
                None,
            )
        return (
            execution.allocation_cpus,
            execution.min_cpus_per_job,
            execution.max_cpus_per_job or execution.allocation_cpus,
            execution.allocation_memory_gb,
        )

    @staticmethod
    def _scheduled_count(
        candidate: QueueItem,
        *,
        active: list[QueueItem],
        ready: list[QueueItem],
        total_cpus: int,
        total_memory_gb: float | None,
        max_running_jobs: int,
    ) -> int:
        """Estimate the ready cohort used for fair launch-time CPU sharing."""

        selected: list[QueueItem] = []
        selected_ids: set[str] = set()
        minimum_cpus = 0
        memory_gb = 0.0
        for item in [*active, candidate, *ready]:
            if item.item_id in selected_ids or len(selected) >= max_running_jobs:
                continue
            next_cpus = minimum_cpus + item.resources.cpus
            next_memory = memory_gb + (item.resources.memory_gb or 0.0)
            if next_cpus > total_cpus:
                continue
            if total_memory_gb is not None and next_memory > total_memory_gb:
                continue
            selected.append(item)
            selected_ids.add(item.item_id)
            minimum_cpus = next_cpus
            memory_gb = next_memory
        return max(1, len(selected))

    def _run_streaming(
        self,
        record: ExecutionRecord,
        processor: Processor,
        *,
        execution: LocalExecution | SlurmSingleNodeExecution,
        queue: QueuePolicy,
        retry_failed: bool,
    ) -> BuildReport:
        """Stream samples from download through processing without grouping them."""

        items = list(record.items)
        if not items:
            return BuildReport((), record.execution_id)
        total_cpus, minimum_cpus, maximum_cpus, total_memory_gb = self._runtime_limits(execution)
        if maximum_cpus < minimum_cpus:
            raise ValueError("max_cpus_per_job is below min_cpus_per_job")
        if total_cpus < minimum_cpus:
            raise ValueError("Execution CPU budget cannot admit one sample")
        if total_memory_gb is not None and any(
            (item.resources.memory_gb or 0.0) > total_memory_gb for item in items
        ):
            raise ValueError("A sample memory request exceeds the Slurm allocation")

        pending = list(items)
        order = {item.item_id: index for index, item in enumerate(items)}
        ready: list[_PreparedUnit] = []
        downloads: dict[Future[_PreparedUnit], QueueItem] = {}
        processing: dict[Future[UnitOutcome], tuple[_PreparedUnit, int]] = {}
        outcomes: dict[str, UnitOutcome] = {}
        download_pool = ThreadPoolExecutor(
            max_workers=queue.download_workers,
            thread_name_prefix="sample-download",
        )
        process_pool = ThreadPoolExecutor(
            max_workers=execution.max_running_jobs,
            thread_name_prefix="sample-process",
        )
        progress = self.progress.task("Stream samples", total=len(items), unit="samples")
        completed_cleanly = False
        try:
            while pending or downloads or ready or processing:
                made_progress = False
                for future in [candidate for candidate in downloads if candidate.done()]:
                    item = downloads.pop(future)
                    prepared = future.result()
                    if prepared.outcome is not None:
                        outcomes[item.item_id] = prepared.outcome
                        progress.update()
                    else:
                        ready.append(prepared)
                        ready.sort(key=lambda value: order[value.item.item_id])
                    made_progress = True

                for future in [candidate for candidate in processing if candidate.done()]:
                    prepared, _cpus = processing.pop(future)
                    outcome = future.result()
                    self._cleanup_input(prepared, outcome, queue=queue)
                    outcomes[prepared.item.item_id] = outcome
                    progress.update()
                    made_progress = True

                active_prepared = [prepared for prepared, _cpus in processing.values()]
                active_items = [prepared.item for prepared in active_prepared]
                active_cpus = sum(cpus for _prepared, cpus in processing.values())
                active_memory_gb = sum(
                    prepared.item.resources.memory_gb or 0.0 for prepared in active_prepared
                )
                ready_items = [prepared.item for prepared in ready]

                for prepared in list(ready):
                    item = prepared.item
                    if len(processing) >= execution.max_running_jobs:
                        break
                    requested_memory = item.resources.memory_gb or 0.0
                    if (
                        total_memory_gb is not None
                        and active_memory_gb + requested_memory > total_memory_gb
                    ):
                        continue
                    available_cpus = total_cpus - active_cpus
                    if available_cpus < item.resources.cpus:
                        continue
                    cohort = self._scheduled_count(
                        item,
                        active=active_items,
                        ready=[value for value in ready_items if value.item_id != item.item_id],
                        total_cpus=total_cpus,
                        total_memory_gb=total_memory_gb,
                        max_running_jobs=execution.max_running_jobs,
                    )
                    fair_share = max(item.resources.cpus, total_cpus // cohort)
                    allocated_cpus = min(maximum_cpus, available_cpus, fair_share)
                    future = process_pool.submit(
                        self._process_prepared,
                        prepared,
                        processor,
                        cpus=allocated_cpus,
                        queue=queue,
                    )
                    processing[future] = (prepared, allocated_cpus)
                    ready.remove(prepared)
                    ready_items = [value for value in ready_items if value.item_id != item.item_id]
                    active_items.append(item)
                    active_cpus += allocated_cpus
                    active_memory_gb += requested_memory
                    made_progress = True

                inflight_items = [*downloads.values(), *(prepared.item for prepared in ready)]
                inflight_items.extend(prepared.item for prepared, _cpus in processing.values())
                estimated_inflight_gb = sum(
                    max(0.0, item.unit.total_size_gb) for item in inflight_items
                )
                processing_extra_gb = sum(
                    max(0.0, prepared.item.unit.total_size_gb)
                    * (queue.processing_storage_multiplier - 1)
                    for prepared, _cpus in processing.values()
                )
                while pending and len(downloads) < queue.download_workers:
                    candidate_index: int | None = None
                    for index, item in enumerate(pending):
                        raw_gb = max(0.0, item.unit.total_size_gb)
                        projected_gb = estimated_inflight_gb + processing_extra_gb + raw_gb
                        if (
                            queue.max_inflight_gb is not None
                            and projected_gb > queue.max_inflight_gb
                            and inflight_items
                        ):
                            continue
                        if raw_gb > execution.storage.available_gb(self.config.workspace):
                            continue
                        candidate_index = index
                        break
                    if candidate_index is None:
                        break
                    item = pending.pop(candidate_index)
                    future = download_pool.submit(
                        self._claim_and_stage,
                        item,
                        execution_id=record.execution_id,
                        retry_failed=retry_failed,
                        queue=queue,
                        reclaim_running=True,
                    )
                    downloads[future] = item
                    inflight_items.append(item)
                    estimated_inflight_gb += max(0.0, item.unit.total_size_gb)
                    made_progress = True

                if not made_progress:
                    active_futures = [*downloads, *processing]
                    if active_futures:
                        wait(
                            active_futures,
                            timeout=queue.scheduler_poll_seconds,
                            return_when=FIRST_COMPLETED,
                        )
                    else:
                        blocked = ready[0].item if ready else pending[0]
                        raise RuntimeError(
                            f"Sample queue cannot admit {blocked.item_id}; increase CPU, "
                            "Slurm memory, queue storage, quota, or filesystem capacity"
                        )
            completed_cleanly = True
        finally:
            download_pool.shutdown(wait=False, cancel_futures=True)
            process_pool.shutdown(wait=False, cancel_futures=True)
            progress.close(status="complete" if completed_cleanly else "failed")

        self.workspace.sync_manifest(
            record,
            {item.item_id: self.state.get(item.item_id) for item in items},
        )
        return BuildReport(
            tuple(outcomes[item.item_id] for item in items),
            record.execution_id,
        )

    def build(
        self,
        catalog: RunCatalog,
        processor: Processor | str,
        *,
        execution: LocalExecution | None = None,
        queue: QueuePolicy | None = None,
        group_by: GroupLevel | None = None,
        genome_pins: dict[int, str] | None = None,
        query: str | None = None,
        retry_failed: bool = False,
        processor_id: str | None = None,
    ) -> BuildReport:
        """Stream *catalog* samples through *processor* on a local server.

        Args:
            catalog: Catalog loaded from CSV or fetched from NCBI.
            processor: Callable or ``module:object`` reference accepting
                ``(fastq, genome, cpus)``.
            execution: Local CPU, concurrency, and free-storage settings.
            queue: Download and in-flight storage behavior.
            group_by: Optional override for the configured catalog grouping.
            genome_pins: Exact assembly accessions keyed by taxonomy ID.
            query: Optional source query recorded as provenance.
            retry_failed: Retry samples with matching failed state.
            processor_id: Explicit semantic version for a dynamic callable.

        Returns:
            Ordered outcomes and an automatic execution ID.
        """

        selected_execution = execution or LocalExecution()
        selected_queue = queue or QueuePolicy()
        callable_processor = (
            load_processor(processor) if isinstance(processor, str) else processor
        )
        record = self._create_execution(
            catalog,
            processor,
            execution=selected_execution,
            queue=selected_queue,
            group_by=group_by,
            genome_pins=genome_pins,
            query=query,
            processor_id=processor_id,
        )
        lock = self.config.workspace / "state" / "queue-coordinator.lock"
        with exclusive_file_lock(
            lock,
            timeout_seconds=120,
            stale_after_seconds=90,
            heartbeat_seconds=15,
        ):
            return self._run_streaming(
                record,
                callable_processor,
                execution=selected_execution,
                queue=selected_queue,
                retry_failed=retry_failed,
            )

    def submit_slurm(
        self,
        catalog: RunCatalog,
        *,
        processor_reference: str,
        execution: SlurmSingleNodeExecution | SlurmDistributedExecution,
        queue: QueuePolicy | None = None,
        group_by: GroupLevel | None = None,
        genome_pins: dict[int, str] | None = None,
        query: str | None = None,
        retry_failed: bool = False,
        script_path: Path | None = None,
        submit: bool = True,
    ) -> tuple[Path, str | None]:
        """Create and optionally submit a Slurm execution for *catalog*.

        Args:
            catalog: Catalog whose samples enter the queue.
            processor_reference: Importable ``module:object`` processor.
            execution: Single-node or distributed Slurm configuration.
            queue: Download and in-flight storage behavior.
            group_by: Optional catalog grouping override.
            genome_pins: Exact assembly accessions keyed by taxonomy ID.
            query: Optional source query recorded as provenance.
            retry_failed: Retry samples with matching failed state.
            script_path: Optional destination for the coordinator script.
            submit: Submit with ``sbatch`` when ``True``; otherwise only write.

        Returns:
            The script path and Slurm job ID, or ``None`` for a dry run.
        """

        from .execution.slurm import SlurmExecutor

        selected_queue = queue or QueuePolicy()
        record = self._create_execution(
            catalog,
            processor_reference,
            execution=execution,
            queue=selected_queue,
            group_by=group_by,
            genome_pins=genome_pins,
            query=query,
            processor_id=None,
        )
        record_path = self.workspace.executions / f"{record.execution_id}.json"
        target = script_path or self.config.workspace / "slurm" / f"{record.execution_id}.sbatch"
        executor = SlurmExecutor(progress=self.progress)
        if isinstance(execution, SlurmSingleNodeExecution):
            script = executor.create_single_node_script(
                record_path=record_path,
                processor_reference=processor_reference,
                builder_config=self.config,
                output_path=target,
                execution=execution,
                retry_failed=retry_failed,
            )
        else:
            script = executor.create_distributed_script(
                record_path=record_path,
                processor_reference=processor_reference,
                builder_config=self.config,
                output_path=target,
                execution=execution,
                retry_failed=retry_failed,
            )
        return script, executor.submit(script) if submit else None

    def status(self, execution_id: str | None = None) -> dict[str, Any]:
        """Return per-sample status for optional *execution_id*."""

        record = (
            self.workspace.load_execution(execution_id)
            if execution_id is not None
            else self.workspace.latest_execution()
        )
        summary = self.state.summary([item.item_id for item in record.items])
        summary["execution_id"] = record.execution_id
        return summary

    def publish_dataset(
        self,
        destination: Path | None = None,
        *,
        execution_id: str | None = None,
        mode: PublishMode = "auto",
        overwrite: bool = False,
    ) -> DatasetExport:
        """Publish verified workspace outputs into *destination*.

        Args:
            destination: Target dataset directory, or the workspace defaults.
            execution_id: Execution to publish; ``None`` selects the latest.
            mode: ``copy``, ``hardlink``, or ``auto``.
            overwrite: Atomically replace an existing published dataset.
        """

        record = (
            self.workspace.load_execution(execution_id)
            if execution_id is not None
            else self.workspace.latest_execution()
        )
        return DatasetPublisher(self.config.workspace, progress=self.progress).publish(
            record,
            destination,
            mode=mode,
            overwrite=overwrite,
        )
