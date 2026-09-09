from __future__ import annotations

import hashlib
import inspect
import json
import logging
import os
import tempfile
import threading
import traceback
import uuid
from collections.abc import Iterable, Mapping
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any

from .catalog import GroupLevel, RunCatalog, validate_polars_runtime
from .descriptions import DescriptionPolicy
from .errors import TaskAlreadyRunning
from .execution import LocalExecutor, SlurmExecutor, SlurmOptions
from .fastq import FastqProvider, SraToolkitProvider
from .genomes import GenomeManager, GenomeSelectionPolicy
from .geo import GeoClient
from .metadata import (
    BioSampleClient,
    EntrezClient,
    MetadataBundle,
    SraClient,
    fetch_metadata_for_accessions,
    fetch_metadata_for_catalog,
)
from .models import (
    DatasetPlan,
    DatasetTask,
    FastqSet,
    GenomeRef,
    ProcessingResult,
    ResourceSpec,
    StagedFastq,
)
from .pipeline import (
    BatchManifest,
    BatchStateStore,
    PipelinePolicy,
    free_space_gb,
    paths_size_gb,
    remove_owned_roots,
)
from .processing.base import Processor, load_processor
from .progress import ProgressReporter
from .publishing import DatasetExport, DatasetPublisher, PublishMode
from .state import TaskStateStore
from .unit_logging import install_unit_logging, unit_log
from .util import (
    atomic_write_json,
    exclusive_file_lock,
    read_json,
    sanitize_identifier,
    sha256_file,
    utc_timestamp,
)

_METADATA_BUNDLE_CACHE_VERSION = 2
LOGGER = logging.getLogger("ncbi_dataset_builder.workflow")


@dataclass(frozen=True)
class BuilderConfig:
    """Configure a dataset builder.

    Args:
        workspace: Root directory for caches, plans, state, and outputs.
        email: Contact email required for NCBI requests.
        ncbi_api_key: Optional key for the higher NCBI request rate.
        max_workers: Maximum simultaneous local tasks.
        total_threads: Optional thread budget shared by local tasks.
        total_memory_gb: Optional memory budget shared by local tasks.
        genome_policy: Policy used to choose NCBI assemblies.
        pipeline_policy: Bounded staging, storage, cleanup, and logging policy.
        show_progress: Display progress for long-running operations.
        progress_bars: Use tqdm bars when available instead of periodic text.
    """

    workspace: Path
    email: str | None = None
    ncbi_api_key: str | None = None
    max_workers: int = 1
    total_threads: int | None = None
    total_memory_gb: float | None = None
    genome_policy: GenomeSelectionPolicy = field(default_factory=GenomeSelectionPolicy)
    pipeline_policy: PipelinePolicy = field(default_factory=PipelinePolicy)
    show_progress: bool = True
    progress_bars: bool = True

    def __post_init__(self) -> None:
        """Normalize the workspace path and validate concurrency limits."""

        object.__setattr__(self, "workspace", Path(self.workspace))
        if self.max_workers < 1:
            raise ValueError("max_workers must be positive")
        if self.total_threads is not None and self.total_threads < 1:
            raise ValueError("total_threads must be positive")
        if self.total_memory_gb is not None and self.total_memory_gb <= 0:
            raise ValueError("total_memory_gb must be positive")


@dataclass(frozen=True)
class TaskOutcome:
    """Record one task outcome.

    Args:
        task_id: Stable task identifier.
        status: ``succeeded``, ``failed``, or ``skipped``.
        result: Optional serialized successful result.
        error: Optional failure or skip explanation.
    """

    task_id: str
    status: str
    result: dict[str, Any] | None = None
    error: str | None = None


@dataclass(frozen=True)
class BuildReport:
    """Collect ordered task outcomes from a local dataset build."""

    outcomes: tuple[TaskOutcome, ...]

    @property
    def succeeded(self) -> int:
        """Return the number of succeeded outcomes."""

        return sum(item.status == "succeeded" for item in self.outcomes)

    @property
    def failed(self) -> int:
        """Return the number of failed outcomes."""

        return sum(item.status == "failed" for item in self.outcomes)

    @property
    def skipped(self) -> int:
        """Return the number of skipped outcomes."""

        return sum(item.status == "skipped" for item in self.outcomes)


@dataclass(frozen=True)
class _PreparedTask:
    """Hold one claimed task between staging and processing.

    Args:
        task: Planned dataset task.
        state_key: Durable task-state key.
        log_path: The unit's only log file.
        genome: Resolved reference, when staging succeeded.
        staged: Downloaded input description, when staging succeeded.
        outcome: Terminal staging or skip outcome, when processing is unnecessary.
    """

    task: DatasetTask
    state_key: str
    log_path: Path
    genome: GenomeRef | None = None
    staged: StagedFastq | None = None
    outcome: TaskOutcome | None = None


@dataclass(frozen=True)
class _PreparedBatch:
    """Hold staged task entries and their durable *manifest*.

    Args:
        entries: Ordered claimed, skipped, or staging-failed tasks.
        manifest: Latest durable batch lifecycle record.
    """

    entries: tuple[_PreparedTask, ...]
    manifest: BatchManifest


class DatasetBuilder:
    """Coordinate catalog, metadata, genomes, FASTQ, local execution, and Slurm."""

    def __init__(
        self,
        config: BuilderConfig,
        *,
        fastq_provider: FastqProvider | None = None,
        genome_manager: GenomeManager | None = None,
        progress: ProgressReporter | None = None,
    ) -> None:
        """Initialize from *config* and optional service implementations.

        *fastq_provider* and *genome_manager* replace built-ins, while *progress*
        replaces the reporter configured by ``show_progress`` and
        ``progress_bars``.
        """

        self.config = config
        install_unit_logging()
        self.progress = progress or ProgressReporter(
            enabled=config.show_progress,
            use_bars=config.progress_bars,
        )
        config.workspace.mkdir(parents=True, exist_ok=True)
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
        self.fastq_provider = fastq_provider or SraToolkitProvider(progress=self.progress)
        self.genomes = genome_manager or GenomeManager(
            config.workspace / "genomes",
            policy=config.genome_policy,
            progress=self.progress,
        )
        self.state = TaskStateStore(config.workspace / "state" / "tasks")
        self.batch_state = BatchStateStore(config.workspace / "state" / "batches")

    def _metadata_cache_path(
        self,
        namespace: str,
        identifiers: Mapping[str, Iterable[str]],
        *,
        include_raw: bool,
    ) -> Path:
        """Return a bundle-cache path for *namespace* and *identifiers*.

        *include_raw* separates compact normalized bundles from bundles that
        retain complete XML trees.
        """

        identity = {
            "cache_version": _METADATA_BUNDLE_CACHE_VERSION,
            "namespace": namespace,
            "include_raw": include_raw,
            "identifiers": {
                name: sorted({str(value).strip() for value in values if str(value).strip()})
                for name, values in sorted(identifiers.items())
            },
        }
        digest = hashlib.sha256(
            json.dumps(identity, sort_keys=True, separators=(",", ":")).encode("utf-8")
        ).hexdigest()
        return self.config.workspace / "metadata_cache" / "bundles" / f"{digest}.json"

    def _load_metadata_cache(self, path: Path) -> MetadataBundle | None:
        """Load *path* as a bundle, reporting and ignoring invalid cache data."""

        if not path.is_file():
            return None
        try:
            return MetadataBundle.load(path, progress=self.progress)
        except (OSError, TypeError, ValueError, UnicodeError) as exc:
            self.progress.message(f"Ignore invalid normalized metadata cache {path}: {exc}")
            return None

    def _save_metadata_cache(self, path: Path, bundle: MetadataBundle) -> None:
        """Atomically save *bundle* to cache *path* with progress reporting."""

        with self.progress.task("Save normalized metadata cache", total=1, unit="bundle") as task:
            atomic_write_json(path, bundle.to_dict())
            task.update()

    @staticmethod
    def _catalog_description_count(catalog: RunCatalog) -> int | None:
        """Estimate distinct sample descriptions represented by *catalog*."""

        for column in ("SRA Sample", "Sample"):
            if column in catalog.frame.columns:
                return catalog.frame.get_column(column).drop_nulls().n_unique()
        return None

    def _entrez_statistics(self) -> dict[str, int]:
        """Return metadata-client cache counters or zeros for a custom client."""

        entrez = getattr(self.sra, "entrez", None)
        statistics = getattr(entrez, "statistics", None)
        if callable(statistics):
            return statistics()
        return {"raw_cache_hits": 0, "network_requests": 0}

    def _report_genome_inventory(
        self,
        tasks: Iterable[DatasetTask],
        *,
        description: str,
    ) -> None:
        """Report unique genome cache state for selected *tasks* under *description*."""

        requirements = [
            (task.unit.taxid, task.genome_pin) for task in tasks if task.unit.taxid is not None
        ]
        unique_count = len(set(requirements))
        inventory = getattr(self.genomes, "cache_inventory", None)
        if callable(inventory):
            inventory(requirements, description=description)
        else:
            self.progress.message(
                f"{description}: {unique_count:,} unique genomes required; "
                "custom genome manager does not expose cache inventory"
            )

    def fetch_runs(self, query: str, *, refresh: bool = False) -> RunCatalog:
        """Return SRA RunInfo for *query*; *refresh* bypasses a cached catalog."""

        if self.sra is None:
            raise ValueError("fetch_runs requires a contact email in BuilderConfig")
        # Check before the network request: a mixed/stale Polars installation can
        # otherwise fail only after the complete RunInfo response has downloaded.
        validate_polars_runtime()
        digest = hashlib.sha256(query.encode("utf-8")).hexdigest()[:16]
        cache = self.config.workspace / "catalogs" / f"sra.{digest}.csv"
        if cache.is_file() and not refresh:
            catalog = RunCatalog.from_csv(cache).deduplicate_runs()
            self.progress.cache_summary(
                "SRA RunInfo", cached=catalog.frame.height, missing=0, unit="runs"
            )
            return catalog
        self.progress.message(f"SRA RunInfo cache miss; fetching query {query!r}")
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
        self.progress.message(f"SRA RunInfo complete: {catalog.frame.height:,} runs")
        return catalog.replace_frame(catalog.frame, event=f"cached SRA query {query!r} at {cache}")

    @staticmethod
    def load_runs(path: str | Path) -> RunCatalog:
        """Load and deduplicate a RunInfo CSV from *path*."""

        return RunCatalog.from_csv(path).deduplicate_runs()

    def fetch_geo_runs(self, accessions: list[str]) -> RunCatalog:
        """Resolve GEO GSE/GSM *accessions* to an SRA run catalog."""

        if self.geo is None:
            raise ValueError("fetch_geo_runs requires a contact email in BuilderConfig")
        self.progress.message(f"Resolve {len(accessions):,} GEO accessions to SRA")
        return self.geo.resolve_to_sra(accessions)

    def fetch_metadata(
        self,
        accessions: list[str],
        *,
        destination: Path | None = None,
        include_raw: bool = False,
        refresh: bool = False,
        description_profile: str = "training",
        description_policy: DescriptionPolicy | None = None,
        legacy_descriptions: dict[str, dict[str, Any]] | None = None,
    ) -> MetadataBundle:
        """Fetch and save SRA *accessions* with linked BioSample metadata.

        *destination* overrides the workspace metadata directory; *include_raw*
        retains XML trees; and *refresh* bypasses bundle and Entrez caches.
        *description_profile*, *description_policy*, and *legacy_descriptions*
        control only derived per-sample files, not complete normalized metadata.
        """

        if self.sra is None or self.biosample is None:
            raise ValueError("fetch_metadata requires a contact email in BuilderConfig")
        cache = self._metadata_cache_path(
            "accessions", {"sra": accessions}, include_raw=include_raw
        )
        bundle = None if refresh else self._load_metadata_cache(cache)
        before = self._entrez_statistics()
        if bundle is not None:
            self.progress.cache_summary(
                "Sample descriptions",
                cached=len(bundle.sra_samples),
                missing=0,
                unit="samples",
            )
            self.progress.message(f"Normalized metadata cache hit: {cache}")
        if bundle is None:
            self.progress.cache_summary(
                "Normalized metadata inputs",
                cached=0,
                missing=len(set(accessions)),
                unit="SRA accessions",
            )
            self.progress.message(f"Normalized metadata cache miss: {cache}")
            bundle = fetch_metadata_for_accessions(
                accessions,
                sra=self.sra,
                biosample=self.biosample,
                include_raw=include_raw,
                refresh=refresh,
            )
            self._save_metadata_cache(cache, bundle)
        after = self._entrez_statistics()
        self.progress.message(
            "Entrez responses for this operation: "
            f"{after['raw_cache_hits'] - before['raw_cache_hits']:,} raw-cache hits; "
            f"{after['network_requests'] - before['network_requests']:,} NCBI requests"
        )
        bundle.save(
            destination or self.config.workspace / "metadata",
            description_profile=description_profile,
            policy=description_policy,
            legacy_descriptions=legacy_descriptions,
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
        description_profile: str = "training",
        description_policy: DescriptionPolicy | None = None,
        legacy_descriptions: dict[str, dict[str, Any]] | None = None,
    ) -> MetadataBundle:
        """Fetch and save metadata for runs in *catalog*.

        *destination* selects storage, *include_raw* retains XML trees, and
        *refresh* bypasses bundle and Entrez caches. *description_profile*,
        *description_policy*, and *legacy_descriptions* control derived
        per-sample files.
        """

        if self.sra is None or self.biosample is None:
            raise ValueError("enrich_metadata requires a contact email in BuilderConfig")
        runs = catalog.frame.get_column("Run").drop_nulls().unique().to_list()
        biosamples = (
            catalog.frame.get_column("BioSample").drop_nulls().unique().to_list()
            if "BioSample" in catalog.frame.columns
            else []
        )
        cache = self._metadata_cache_path(
            "catalog",
            {"runs": runs, "biosamples": biosamples},
            include_raw=include_raw,
        )
        bundle = None if refresh else self._load_metadata_cache(cache)
        before = self._entrez_statistics()
        expected = self._catalog_description_count(catalog)
        if bundle is not None:
            self.progress.cache_summary(
                "Sample descriptions",
                cached=len(bundle.sra_samples),
                missing=0,
                unit="samples",
            )
            self.progress.message(f"Normalized metadata cache hit: {cache}")
        if bundle is None:
            if expected is not None:
                self.progress.cache_summary(
                    "Sample descriptions",
                    cached=0,
                    missing=expected,
                    unit="samples",
                )
            self.progress.message(f"Normalized metadata cache miss: {cache}")
            bundle = fetch_metadata_for_catalog(
                catalog,
                sra=self.sra,
                biosample=self.biosample,
                include_raw=include_raw,
                refresh=refresh,
            )
            self._save_metadata_cache(cache, bundle)
        after = self._entrez_statistics()
        self.progress.message(
            "Entrez responses for this operation: "
            f"{after['raw_cache_hits'] - before['raw_cache_hits']:,} raw-cache hits; "
            f"{after['network_requests'] - before['network_requests']:,} NCBI requests"
        )
        self.progress.message(
            f"Metadata bundle contains {len(bundle.sra_samples):,} sample descriptions"
        )
        bundle.save(
            destination or self.config.workspace / "metadata",
            description_profile=description_profile,
            policy=description_policy,
            legacy_descriptions=legacy_descriptions,
            progress=self.progress,
        )
        return bundle

    def plan(
        self,
        catalog: RunCatalog,
        *,
        group_by: GroupLevel = "experiment",
        resources: ResourceSpec | None = None,
        max_batch_gb: float | None = None,
        max_batch_units: int | None = None,
        genome_pins: dict[int, str] | None = None,
        query: str | None = None,
    ) -> DatasetPlan:
        """Create a deterministic task plan from *catalog*.

        *group_by* defines processing units, *resources* apply to every task,
        *max_batch_gb* and *max_batch_units* form batches, *genome_pins* fix
        species assemblies, and *query* records the originating search.
        """

        self.progress.message(f"Plan dataset from {catalog.frame.height:,} catalog rows")
        clean = catalog.deduplicate_runs()
        resources = resources or ResourceSpec()
        units = clean.processing_units(by=group_by)
        batches = RunCatalog.batch_units(units, max_gb=max_batch_gb, max_units=max_batch_units)
        pins = genome_pins or {}
        tasks: list[DatasetTask] = []
        for batch_id, batch in enumerate(batches):
            for unit in batch:
                task_id = sanitize_identifier(unit.unit_id)
                tasks.append(
                    DatasetTask(
                        task_id=task_id,
                        unit=unit,
                        batch_id=batch_id,
                        resources=resources,
                        genome_pin=pins.get(unit.taxid) if unit.taxid is not None else None,
                    )
                )
        plan = DatasetPlan(
            plan_id=str(uuid.uuid4()),
            created_at=utc_timestamp(),
            query=query,
            group_by=group_by,
            tasks=tuple(tasks),
            catalog_audit=clean.audit,
            metadata={"batch_count": len(batches), "max_batch_gb": max_batch_gb},
        )
        self.progress.message(
            f"Plan complete: {len(plan.tasks):,} tasks in {len(batches):,} batches"
        )
        return plan

    def save_plan(self, plan: DatasetPlan, path: Path | None = None) -> Path:
        """Atomically save *plan* at optional *path* and return the chosen path."""

        target = path or self.config.workspace / "plans" / f"{plan.plan_id}.json"
        atomic_write_json(target, plan.to_dict())
        return target

    @staticmethod
    def load_plan(path: str | Path) -> DatasetPlan:
        """Load a serialized dataset plan from *path*."""

        return DatasetPlan.from_dict(read_json(Path(path)))

    @staticmethod
    def _processor_identity(
        processor: Processor | str,
        processor_id: str | None,
        resolved: Processor | None = None,
    ) -> str:
        """Return a stable identity for *processor* or explicit *processor_id*.

        *resolved* may provide an already imported callable, avoiding duplicate loading.
        """

        if processor_id:
            return processor_id
        target = resolved if resolved is not None else processor
        if isinstance(processor, str):
            identity = processor
        else:
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

    def _register_processor(self, plan: DatasetPlan, identity: str) -> None:
        """Bind *plan* permanently to processor *identity* or verify the prior binding."""

        directory = self.config.workspace / "state" / "plans"
        safe_plan_id = sanitize_identifier(plan.plan_id)
        path = directory / f"{safe_plan_id}.json"
        directory.mkdir(parents=True, exist_ok=True)
        with exclusive_file_lock(directory / f".{safe_plan_id}.lock"):
            if path.is_file():
                previous = read_json(path)
                if previous.get("processor_identity") != identity:
                    raise ValueError(
                        f"Plan {plan.plan_id} is already bound to processor "
                        f"{previous.get('processor_identity')!r}; create a new plan for {identity!r}"
                    )
                return
            atomic_write_json(
                path,
                {
                    "plan_id": plan.plan_id,
                    "processor_identity": identity,
                    "registered_at": utc_timestamp(),
                },
            )

    def _unit_log_path(self, plan_id: str, task: DatasetTask) -> Path:
        """Return the one log path assigned to *task* in *plan_id*."""

        return (
            self.config.workspace
            / "logs"
            / "units"
            / sanitize_identifier(plan_id)
            / f"batch-{task.batch_id:06d}"
            / f"{sanitize_identifier(task.task_id)}.log"
        )

    def _stage_fastq(self, task: DatasetTask) -> StagedFastq:
        """Stage FASTQ inputs for *task*, adapting providers with only ``fetch``."""

        destination = self.config.workspace / "fastq"
        stage = getattr(self.fastq_provider, "stage", None)
        if callable(stage):
            return stage(task.unit, destination, threads=task.resources.threads)
        ready = self.fastq_provider.fetch(task.unit, destination, threads=task.resources.threads)
        return StagedFastq(
            unit_id=task.unit.unit_id,
            source=ready.source,
            size_gb=paths_size_gb([ready.work_dir]),
            cleanup_roots=(ready.work_dir,),
            ready_fastq=ready,
            metadata={"provider_mode": "fetch_during_staging"},
        )

    def _materialize_fastq(self, prepared: _PreparedTask) -> FastqSet:
        """Materialize FASTQ for a staged *prepared* task."""

        if prepared.staged is None:
            raise ValueError(f"Task {prepared.task.task_id} has no staged FASTQ")
        destination = self.config.workspace / "fastq"
        materialize = getattr(self.fastq_provider, "materialize", None)
        if callable(materialize):
            return materialize(
                prepared.task.unit,
                prepared.staged,
                destination,
                threads=prepared.task.resources.threads,
            )
        if prepared.staged.ready_fastq is None:
            raise TypeError("FASTQ provider supplied neither materialize() nor ready FASTQ")
        return prepared.staged.ready_fastq

    def _validate_staging_capacity(
        self,
        *,
        estimated_size_gb: float,
        occupied_size_gb: float,
        policy: PipelinePolicy,
    ) -> float:
        """Validate *estimated_size_gb* against *occupied_size_gb* and *policy*.

        Return currently available storage in GB after checking configured
        staged-size and minimum-free-space limits.
        """

        if (
            policy.max_staged_gb is not None
            and occupied_size_gb + estimated_size_gb > policy.max_staged_gb
        ):
            raise RuntimeError(
                "Staging would exceed max_staged_gb: "
                f"{occupied_size_gb:.3f} GB present + {estimated_size_gb:.3f} GB estimated "
                f"> {policy.max_staged_gb:.3f} GB"
            )
        available = free_space_gb(self.config.workspace)
        if available - estimated_size_gb < policy.minimum_free_gb:
            raise RuntimeError(
                "Insufficient free storage for staging: "
                f"{available:.3f} GB free - {estimated_size_gb:.3f} GB estimated "
                f"< {policy.minimum_free_gb:.3f} GB reserve"
            )
        return available

    def _claim_and_stage_task(
        self,
        task: DatasetTask,
        *,
        plan_id: str,
        retry_failed: bool,
        policy: PipelinePolicy,
        staging_started_event: threading.Event | None = None,
        reclaim_running: bool = False,
    ) -> _PreparedTask:
        """Claim and stage *task* in *plan_id* under retry and logging policy.

        *retry_failed* controls retries, *policy* controls logging, and optional
        *staging_started_event* signals after this provider stage completes.
        *reclaim_running* recovers state while holding the workspace lock.
        """

        state_key = f"{plan_id}__{task.task_id}"
        log_path = self._unit_log_path(plan_id, task)
        try:
            claimed = self.state.start(
                state_key,
                retry_failed=retry_failed,
                log_path=log_path,
                reclaim_running=reclaim_running,
            )
        except TaskAlreadyRunning as exc:
            self.progress.message(f"Task {task.task_id} skipped: {exc}", level=logging.WARNING)
            return _PreparedTask(
                task,
                state_key,
                log_path,
                outcome=TaskOutcome(task.task_id, "skipped", error=str(exc)),
            )
        if not claimed:
            previous = self.state.get(state_key) or {}
            return _PreparedTask(
                task,
                state_key,
                log_path,
                outcome=TaskOutcome(
                    task.task_id,
                    "skipped",
                    result=previous.get("result"),
                    error=previous.get("error"),
                ),
            )
        try:
            with (
                unit_log(
                    log_path,
                    phase="staging",
                    unit_id=task.task_id,
                    fsync=policy.fsync_logs,
                ),
                self.progress.minimum_level(logging.WARNING),
            ):
                if task.unit.taxid is None:
                    raise ValueError(
                        f"Task {task.task_id} has no species taxid; "
                        "pin or add TaxID before planning"
                    )
                self.progress.message(f"Resolve genome for taxid {task.unit.taxid} and stage FASTQ")
                genome = self.genomes.resolve(
                    taxid=task.unit.taxid,
                    scientific_name=task.unit.scientific_name,
                    pin=task.genome_pin,
                )
                staged = self._stage_fastq(task)
                if staging_started_event is not None:
                    staging_started_event.set()
                self.state.set_phase(state_key, "ready")
            return _PreparedTask(task, state_key, log_path, genome=genome, staged=staged)
        except Exception:  # noqa: BLE001 - this boundary persists staging failures
            error = traceback.format_exc()
            self.state.fail(state_key, error)
            with unit_log(
                log_path,
                phase="staging-error",
                unit_id=task.task_id,
                fsync=policy.fsync_logs,
            ):
                LOGGER.error("Staging failed:\n%s", error)
            self.progress.message(
                f"Task {task.task_id} staging failed: {error.rstrip().splitlines()[-1]}",
                level=logging.ERROR,
            )
            return _PreparedTask(
                task,
                state_key,
                log_path,
                outcome=TaskOutcome(task.task_id, "failed", error=error),
            )

    def _stage_batch(
        self,
        plan: DatasetPlan,
        tasks: list[DatasetTask],
        *,
        retry_failed: bool,
        policy: PipelinePolicy,
        occupied_size_gb: float,
        started_event: threading.Event | None = None,
    ) -> _PreparedBatch:
        """Stage one *tasks* batch from *plan* under *retry_failed* and *policy*.

        *occupied_size_gb* accounts for the batch currently being processed;
        optional *started_event* is set after the staging transition is durable.
        """

        batch_id = tasks[0].batch_id
        estimated = sum(task.unit.total_size_gb for task in tasks)
        manifest = BatchManifest(
            plan_id=plan.plan_id,
            batch_id=batch_id,
            status="staging",
            unit_ids=tuple(task.task_id for task in tasks),
            estimated_size_gb=estimated,
            task_statuses={task.task_id: "pending" for task in tasks},
            log_paths={
                task.task_id: str(self._unit_log_path(plan.plan_id, task)) for task in tasks
            },
        )
        self.batch_state.save(manifest)
        self.progress.message(
            f"Batch {batch_id} staging started: {len(tasks):,} units; {estimated:.3f} GB estimated"
        )
        try:
            self._validate_staging_capacity(
                estimated_size_gb=estimated,
                occupied_size_gb=occupied_size_gb,
                policy=policy,
            )
        except Exception:
            failed = replace(
                manifest,
                status="partially_failed",
                free_space_gb=free_space_gb(self.config.workspace),
                updated_at=utc_timestamp(),
                completed_at=utc_timestamp(),
            )
            self.batch_state.save(failed)
            raise
        prepared_entries: list[_PreparedTask] = []
        for task in tasks:
            prepared_entries.append(
                self._claim_and_stage_task(
                    task,
                    plan_id=plan.plan_id,
                    retry_failed=retry_failed,
                    policy=policy,
                    staging_started_event=(
                        started_event
                        if started_event is not None and not started_event.is_set()
                        else None
                    ),
                    reclaim_running=True,
                )
            )
        if started_event is not None and not started_event.is_set():
            started_event.set()
        roots = tuple(
            path
            for entry in prepared_entries
            if entry.staged is not None
            for path in entry.staged.cleanup_roots
        )
        staged_size = paths_size_gb(list(roots))
        actual_available = free_space_gb(self.config.workspace)
        storage_error: str | None = None
        if (
            policy.max_staged_gb is not None
            and occupied_size_gb + staged_size > policy.max_staged_gb
        ):
            storage_error = (
                "Measured staged inputs exceed max_staged_gb: "
                f"{occupied_size_gb:.3f} GB present + {staged_size:.3f} GB staged "
                f"> {policy.max_staged_gb:.3f} GB"
            )
        elif actual_available < policy.minimum_free_gb:
            storage_error = (
                "Measured free storage is below minimum_free_gb after staging: "
                f"{actual_available:.3f} GB < {policy.minimum_free_gb:.3f} GB"
            )
        if storage_error is not None:
            for index, entry in enumerate(prepared_entries):
                if entry.outcome is not None:
                    continue
                self.state.fail(entry.state_key, storage_error)
                with unit_log(
                    entry.log_path,
                    phase="storage-error",
                    unit_id=entry.task.task_id,
                    fsync=policy.fsync_logs,
                ):
                    LOGGER.error(storage_error)
                prepared_entries[index] = replace(
                    entry,
                    outcome=TaskOutcome(entry.task.task_id, "failed", error=storage_error),
                )
            self.progress.message(storage_error, level=logging.ERROR)
        entries = tuple(prepared_entries)
        statuses = {
            entry.task.task_id: (entry.outcome.status if entry.outcome is not None else "ready")
            for entry in entries
        }
        manifest = replace(
            manifest,
            status="ready",
            staged_size_gb=staged_size,
            retained_size_gb=staged_size,
            free_space_gb=actual_available,
            task_statuses=statuses,
            cleanup_roots={
                entry.task.task_id: [str(path) for path in entry.staged.cleanup_roots]
                for entry in entries
                if entry.staged is not None
            },
            updated_at=utc_timestamp(),
        )
        self.batch_state.save(manifest)
        failed_count = sum(
            entry.outcome is not None and entry.outcome.status == "failed" for entry in entries
        )
        self.progress.message(
            f"Batch {batch_id} staging complete: {staged_size:.3f} GB ready; "
            f"{failed_count:,} failures",
            level=logging.WARNING if failed_count else logging.INFO,
        )
        return _PreparedBatch(entries, manifest)

    def prefetch_batch(
        self,
        plan: DatasetPlan,
        batch_id: int,
        *,
        retry_failed: bool = False,
        policy: PipelinePolicy | None = None,
        occupied_size_gb: float = 0.0,
    ) -> BatchManifest:
        """Stage *batch_id* from *plan* for distributed workers.

        *retry_failed* includes previously failed tasks, *policy* controls storage
        and logging, and *occupied_size_gb* accounts for a currently running batch.
        This method does not claim task state; Slurm workers claim their own units.
        """

        active_policy = policy or self.config.pipeline_policy
        tasks = [task for task in plan.tasks if task.batch_id == batch_id]
        if not tasks:
            raise ValueError(f"Plan {plan.plan_id} has no batch {batch_id}")
        eligible: list[DatasetTask] = []
        statuses: dict[str, str] = {}
        for task in tasks:
            state = self.state.get(f"{plan.plan_id}__{task.task_id}")
            status = (state or {}).get("status", "pending")
            statuses[task.task_id] = status
            if status == "succeeded" or status == "failed" and not retry_failed:
                continue
            eligible.append(task)
        estimated = sum(task.unit.total_size_gb for task in eligible)
        self._validate_staging_capacity(
            estimated_size_gb=estimated,
            occupied_size_gb=occupied_size_gb,
            policy=active_policy,
        )
        manifest = BatchManifest(
            plan_id=plan.plan_id,
            batch_id=batch_id,
            status="staging",
            unit_ids=tuple(task.task_id for task in tasks),
            estimated_size_gb=estimated,
            task_statuses=statuses,
            log_paths={task.task_id: str(self._unit_log_path(plan.plan_id, task)) for task in tasks},
        )
        self.batch_state.save(manifest)
        cleanup_roots: dict[str, list[str]] = {}
        staging_failed = False
        for task in eligible:
            log_path = self._unit_log_path(plan.plan_id, task)
            try:
                with (
                    unit_log(
                        log_path,
                        phase="distributed-prefetch",
                        unit_id=task.task_id,
                        fsync=active_policy.fsync_logs,
                    ),
                    self.progress.minimum_level(logging.WARNING),
                ):
                    if task.unit.taxid is None:
                        raise ValueError(f"Task {task.task_id} has no species taxid")
                    self.genomes.resolve(
                        taxid=task.unit.taxid,
                        scientific_name=task.unit.scientific_name,
                        pin=task.genome_pin,
                    )
                    staged = self._stage_fastq(task)
                    cleanup_roots[task.task_id] = [str(path) for path in staged.cleanup_roots]
                    statuses[task.task_id] = "ready"
            except Exception:  # noqa: BLE001 - worker receives a later independent retry
                staging_failed = True
                statuses[task.task_id] = "staging_failed"
                error = traceback.format_exc()
                with unit_log(
                    log_path,
                    phase="distributed-prefetch-error",
                    unit_id=task.task_id,
                    fsync=active_policy.fsync_logs,
                ):
                    LOGGER.error("Distributed prefetch failed; worker will retry:\n%s", error)
                self.progress.message(
                    f"Prefetch failed for {task.task_id}; its worker will retry",
                    level=logging.WARNING,
                )
        roots = tuple(Path(path) for values in cleanup_roots.values() for path in values)
        staged_size = paths_size_gb(list(roots))
        available = free_space_gb(self.config.workspace)
        storage_error: str | None = None
        if (
            active_policy.max_staged_gb is not None
            and occupied_size_gb + staged_size > active_policy.max_staged_gb
        ):
            storage_error = (
                "Measured distributed staging exceeds max_staged_gb: "
                f"{occupied_size_gb:.3f} GB present + {staged_size:.3f} GB staged > "
                f"{active_policy.max_staged_gb:.3f} GB"
            )
        elif available < active_policy.minimum_free_gb:
            storage_error = (
                "Measured free storage is below minimum_free_gb after distributed staging: "
                f"{available:.3f} GB < {active_policy.minimum_free_gb:.3f} GB"
            )
        manifest = replace(
            manifest,
            status="partially_failed" if staging_failed or storage_error else "ready",
            staged_size_gb=staged_size,
            retained_size_gb=staged_size,
            free_space_gb=available,
            task_statuses=statuses,
            cleanup_roots=cleanup_roots,
            updated_at=utc_timestamp(),
        )
        self.batch_state.save(manifest)
        if storage_error is not None:
            self.progress.message(storage_error, level=logging.ERROR)
            raise RuntimeError(storage_error)
        self.progress.message(
            f"Batch {batch_id} prefetched: {len(eligible):,} eligible units; "
            f"{staged_size:.3f} GB staged",
            level=logging.WARNING if staging_failed else logging.INFO,
        )
        return manifest

    def finalize_distributed_batch(self, plan: DatasetPlan, batch_id: int) -> BatchManifest:
        """Finalize distributed *batch_id* state for *plan* after its Slurm array exits."""

        tasks = [task for task in plan.tasks if task.batch_id == batch_id]
        if not tasks:
            raise ValueError(f"Plan {plan.plan_id} has no batch {batch_id}")
        prior = self.batch_state.get(plan.plan_id, batch_id)
        if prior is None:
            raise ValueError(f"Batch {batch_id} has no staging manifest")
        statuses = {
            task.task_id: (
                self.state.get(f"{plan.plan_id}__{task.task_id}") or {"status": "pending"}
            ).get("status", "pending")
            for task in tasks
        }
        roots = tuple(Path(path) for values in prior.cleanup_roots.values() for path in values)
        retained = paths_size_gb(list(roots))
        terminal = all(status in {"succeeded", "failed"} for status in statuses.values())
        all_succeeded = all(status == "succeeded" for status in statuses.values())
        status = (
            "cleaned"
            if all_succeeded and retained == 0
            else "completed"
            if all_succeeded
            else "partially_failed"
        )
        manifest = replace(
            prior,
            status=status,
            retained_size_gb=retained,
            free_space_gb=free_space_gb(self.config.workspace),
            task_statuses=statuses,
            updated_at=utc_timestamp(),
            completed_at=utc_timestamp() if terminal else None,
        )
        self.batch_state.save(manifest)
        return manifest

    def _process_prepared_task(
        self,
        prepared: _PreparedTask,
        processor: Processor,
        *,
        plan_id: str,
        policy: PipelinePolicy,
    ) -> TaskOutcome:
        """Materialize and process one staged *prepared* task with *processor*.

        *plan_id* selects work/output paths and *policy* controls durable logging.
        """

        task = prepared.task
        if prepared.outcome is not None:
            return prepared.outcome
        if prepared.genome is None or prepared.staged is None:
            raise ValueError(f"Prepared task {task.task_id} is incomplete")
        try:
            self.state.set_phase(prepared.state_key, "processing")
            with (
                unit_log(
                    prepared.log_path,
                    phase="processing",
                    unit_id=task.task_id,
                    fsync=policy.fsync_logs,
                ),
                self.progress.minimum_level(logging.WARNING),
            ):
                fastq = self._materialize_fastq(prepared)
                task_root = (
                    self.config.workspace / "work" / sanitize_identifier(plan_id) / task.task_id
                )
                fastq = replace(
                    fastq,
                    work_dir=task_root,
                    output_dir=self.config.workspace
                    / "results"
                    / sanitize_identifier(plan_id)
                    / task.task_id,
                    metadata={
                        **fastq.metadata,
                        "unit_log_path": str(prepared.log_path),
                    },
                )
                result = processor(fastq, prepared.genome, task.resources.threads)
                if not isinstance(result, ProcessingResult):
                    raise TypeError(
                        "Processor must return ProcessingResult, got "
                        f"{type(result).__name__} for {task.task_id}"
                    )
                result.validate()
                payload = {
                    "processing": result.to_dict(),
                    "output_sha256": {
                        str(path): sha256_file(path, progress=self.progress)
                        for path in result.outputs
                    },
                    "genome": prepared.genome.to_dict(),
                    "fastq": fastq.to_dict(),
                    "log_path": str(prepared.log_path),
                }
                self.state.succeed(prepared.state_key, payload)
            return TaskOutcome(task.task_id, "succeeded", result=payload)
        except Exception:  # noqa: BLE001 - a task boundary must persist every operational failure
            error = traceback.format_exc()
            self.state.fail(prepared.state_key, error)
            with unit_log(
                prepared.log_path,
                phase="processing-error",
                unit_id=task.task_id,
                fsync=policy.fsync_logs,
            ):
                LOGGER.error("Processing failed:\n%s", error)
            self.progress.message(
                f"Task {task.task_id} failed: {error.rstrip().splitlines()[-1]}",
                level=logging.ERROR,
            )
            return TaskOutcome(task.task_id, "failed", error=error)

    def _cleanup_inputs(
        self,
        prepared: _PreparedTask,
        outcome: TaskOutcome,
        *,
        policy: PipelinePolicy,
    ) -> bool:
        """Clean manifest-owned inputs for *prepared* and *outcome* under *policy*."""

        if policy.cleanup == "never" or prepared.staged is None:
            return True
        if outcome.status != "succeeded" and policy.keep_failed_inputs:
            return True
        try:
            with (
                unit_log(
                    prepared.log_path,
                    phase="cleanup",
                    unit_id=prepared.task.task_id,
                    fsync=policy.fsync_logs,
                ),
                self.progress.minimum_level(logging.WARNING),
            ):
                removed = remove_owned_roots(
                    prepared.staged.cleanup_roots,
                    allowed_root=self.config.workspace / "fastq",
                )
                self.progress.message(f"Removed {len(removed):,} manifest-owned input roots")
            return True
        except Exception:  # noqa: BLE001 - cleanup must not hide verified outputs
            error = traceback.format_exc()
            with unit_log(
                prepared.log_path,
                phase="cleanup-error",
                unit_id=prepared.task.task_id,
                fsync=policy.fsync_logs,
            ):
                LOGGER.error("Cleanup failed:\n%s", error)
            self.progress.message(
                f"Task {prepared.task.task_id} cleanup failed: {error.rstrip().splitlines()[-1]}",
                level=logging.WARNING,
            )
            return False

    def _process_batch(
        self,
        prepared_batch: _PreparedBatch,
        processor: Processor,
        *,
        plan_id: str,
        policy: PipelinePolicy,
    ) -> tuple[TaskOutcome, ...]:
        """Process and safely clean one *prepared_batch* with *processor*.

        *plan_id* selects output paths and *policy* controls resources and cleanup.
        """

        manifest = replace(
            prepared_batch.manifest,
            status="processing",
            updated_at=utc_timestamp(),
        )
        self.batch_state.save(manifest)
        self.progress.message(f"Batch {manifest.batch_id} processing started")
        active = [entry for entry in prepared_batch.entries if entry.outcome is None]
        threads = max((entry.task.resources.threads for entry in active), default=1)
        memory_gb = max((entry.task.resources.memory_gb for entry in active), default=1)
        executor: LocalExecutor[_PreparedTask, TaskOutcome] = LocalExecutor(
            max_workers=self.config.max_workers,
            total_threads=self.config.total_threads,
            total_memory_gb=self.config.total_memory_gb,
            progress=self.progress,
        )
        processed = executor.map(
            active,
            lambda entry: self._process_prepared_task(
                entry, processor, plan_id=plan_id, policy=policy
            ),
            threads_per_task=threads,
            memory_gb_per_task=memory_gb,
        )
        by_id = {outcome.task_id: outcome for outcome in processed}
        outcomes = tuple(
            entry.outcome or by_id[entry.task.task_id] for entry in prepared_batch.entries
        )
        cleanup_results = [
            self._cleanup_inputs(entry, outcome, policy=policy)
            for entry, outcome in zip(prepared_batch.entries, outcomes, strict=True)
        ]
        cleanup_ok = all(cleanup_results)
        failed = sum(outcome.status == "failed" for outcome in outcomes)
        skipped = sum(outcome.status == "skipped" for outcome in outcomes)
        succeeded = sum(outcome.status == "succeeded" for outcome in outcomes)
        retained = paths_size_gb(
            [
                path
                for entry in prepared_batch.entries
                if entry.staged is not None
                for path in entry.staged.cleanup_roots
            ]
        )
        if failed or not cleanup_ok:
            status = "partially_failed"
        elif policy.cleanup == "after_success":
            status = "cleaned"
        else:
            status = "completed"
        manifest = replace(
            manifest,
            status=status,
            retained_size_gb=retained,
            free_space_gb=free_space_gb(self.config.workspace),
            task_statuses={outcome.task_id: outcome.status for outcome in outcomes},
            updated_at=utc_timestamp(),
            completed_at=utc_timestamp(),
        )
        self.batch_state.save(manifest)
        self.progress.message(
            f"Batch {manifest.batch_id} complete: {succeeded:,} succeeded; "
            f"{failed:,} failed; {skipped:,} skipped; {retained:.3f} GB retained",
            level=logging.WARNING if failed or not cleanup_ok else logging.INFO,
        )
        return outcomes

    def _execute_task(
        self,
        task: DatasetTask,
        processor: Processor,
        *,
        plan_id: str,
        retry_failed: bool,
    ) -> TaskOutcome:
        """Execute *task* with *processor* in *plan_id* under *retry_failed*."""

        policy = self.config.pipeline_policy
        prepared = self._claim_and_stage_task(
            task,
            plan_id=plan_id,
            retry_failed=retry_failed,
            policy=policy,
        )
        outcome = self._process_prepared_task(prepared, processor, plan_id=plan_id, policy=policy)
        self._cleanup_inputs(prepared, outcome, policy=policy)
        return outcome

    def build(
        self,
        plan: DatasetPlan,
        processor: Processor | str,
        *,
        retry_failed: bool = False,
        batch_ids: set[int] | None = None,
        processor_id: str | None = None,
        policy: PipelinePolicy | None = None,
    ) -> BuildReport:
        """Execute *plan* with *processor* under one workspace coordinator lock.

        *retry_failed* enables failed tasks, *batch_ids* limits execution,
        *processor_id* versions dynamic callables, and *policy* overrides the
        configured pipeline policy.
        """

        lock = self.config.workspace / "state" / ".pipeline-coordinator.lock"
        with exclusive_file_lock(
            lock,
            timeout_seconds=5,
            stale_after_seconds=5 * 60,
            heartbeat_seconds=30,
        ):
            return self._build_bounded(
                plan,
                processor,
                retry_failed=retry_failed,
                batch_ids=batch_ids,
                processor_id=processor_id,
                policy=policy,
            )

    def _build_bounded(
        self,
        plan: DatasetPlan,
        processor: Processor | str,
        *,
        retry_failed: bool = False,
        batch_ids: set[int] | None = None,
        processor_id: str | None = None,
        policy: PipelinePolicy | None = None,
    ) -> BuildReport:
        """Run the bounded pipeline for *plan* and *processor*.

        *retry_failed* enables failed tasks, *batch_ids* limits execution, and
        *processor_id* can explicitly version dynamic processor callables.
        Optional *policy* overrides bounded staging and cleanup configuration.
        """

        callable_processor = load_processor(processor) if isinstance(processor, str) else processor
        identity = self._processor_identity(processor, processor_id, callable_processor)
        self._register_processor(plan, identity)
        tasks = [task for task in plan.tasks if batch_ids is None or task.batch_id in batch_ids]
        if batch_ids is not None and not tasks:
            raise ValueError(f"No tasks belong to requested batches: {sorted(batch_ids)}")
        batch_count = len({task.batch_id for task in tasks})
        batch_label = "batch" if batch_count == 1 else "batches"
        self._report_genome_inventory(
            tasks,
            description=(
                f"Genome references for {len(tasks):,} selected tasks in "
                f"{batch_count:,} {batch_label}"
            ),
        )
        active_policy = policy or self.config.pipeline_policy
        grouped: dict[int, list[DatasetTask]] = {}
        for task in tasks:
            grouped.setdefault(task.batch_id, []).append(task)
        batches = [grouped[batch_id] for batch_id in sorted(grouped)]
        outcomes: list[TaskOutcome] = []
        current = self._stage_batch(
            plan,
            batches[0],
            retry_failed=retry_failed,
            policy=active_policy,
            occupied_size_gb=0.0,
        )
        with ThreadPoolExecutor(max_workers=1, thread_name_prefix="batch-prefetch") as prefetch:
            next_future: Future[_PreparedBatch] | None = None
            for index, batch in enumerate(batches):
                if index + 1 < len(batches) and active_policy.prefetch_batches == 1:
                    started_event = threading.Event()
                    next_future = prefetch.submit(
                        self._stage_batch,
                        plan,
                        batches[index + 1],
                        retry_failed=retry_failed,
                        policy=active_policy,
                        occupied_size_gb=current.manifest.staged_size_gb,
                        started_event=started_event,
                    )
                    started_event.wait()
                outcomes.extend(
                    self._process_batch(
                        current,
                        callable_processor,
                        plan_id=plan.plan_id,
                        policy=active_policy,
                    )
                )
                if index + 1 >= len(batches):
                    continue
                if next_future is not None:
                    current = next_future.result()
                    next_future = None
                else:
                    current = self._stage_batch(
                        plan,
                        batches[index + 1],
                        retry_failed=retry_failed,
                        policy=active_policy,
                        occupied_size_gb=0.0,
                    )
        order = {task.task_id: index for index, task in enumerate(tasks)}
        outcomes.sort(key=lambda outcome: order[outcome.task_id])
        return BuildReport(tuple(outcomes))

    def run_task(
        self,
        plan: DatasetPlan,
        task_index: int,
        processor: Processor | str,
        *,
        retry_failed: bool = False,
        processor_id: str | None = None,
    ) -> TaskOutcome:
        """Execute one *task_index* from *plan* with the selected *processor*.

        *retry_failed* enables a failed task and *processor_id* explicitly
        identifies dynamic callables when automatic identity is insufficient.
        """

        if task_index < 0 or task_index >= len(plan.tasks):
            raise IndexError(f"Task index {task_index} is outside 0..{len(plan.tasks) - 1}")
        callable_processor = load_processor(processor) if isinstance(processor, str) else processor
        identity = self._processor_identity(processor, processor_id, callable_processor)
        self._register_processor(plan, identity)
        return self._execute_task(
            plan.tasks[task_index],
            callable_processor,
            plan_id=plan.plan_id,
            retry_failed=retry_failed,
        )

    def submit_slurm(
        self,
        plan: DatasetPlan,
        *,
        processor_reference: str,
        options: SlurmOptions,
        plan_path: Path | None = None,
        script_path: Path | None = None,
        submit: bool = True,
        retry_failed: bool = False,
        batch_ids: set[int] | None = None,
        policy: PipelinePolicy | None = None,
    ) -> tuple[Path, str | None]:
        """Create and optionally submit one Slurm coordinator for *plan*.

        *processor_reference* must be importable, *options* configures Slurm,
        *plan_path* and *script_path* control artifacts, *submit* chooses dry-run
        behavior, *retry_failed* enables retries, *batch_ids* limits tasks, and
        *policy* overrides bounded staging and cleanup behavior.
        """

        saved_plan = self.save_plan(plan, plan_path)
        target_script = script_path or self.config.workspace / "slurm" / f"{plan.plan_id}.sbatch"
        executor = SlurmExecutor(progress=self.progress)
        (self.config.workspace / "logs" / "slurm").mkdir(parents=True, exist_ok=True)
        selected_tasks = [
            task for task in plan.tasks if batch_ids is None or task.batch_id in batch_ids
        ]
        if not selected_tasks:
            raise ValueError(f"No tasks belong to requested batches: {sorted(batch_ids or set())}")
        batch_count = len({task.batch_id for task in selected_tasks})
        batch_label = "batch" if batch_count == 1 else "batches"
        self._report_genome_inventory(
            selected_tasks,
            description=(
                f"Genome references for {len(selected_tasks):,} selected Slurm tasks in "
                f"{batch_count:,} {batch_label}"
            ),
        )
        active_policy = policy or self.config.pipeline_policy
        if options.mode == "distributed":
            resource_specs = {task.resources for task in selected_tasks}
            if len(resource_specs) != 1:
                raise ValueError(
                    "Distributed Slurm execution requires one ResourceSpec across selected tasks"
                )
            per_unit_threads = max(task.resources.threads for task in selected_tasks)
            workers = options.worker_parallelism(per_unit_threads)
            self.progress.message(
                f"Distributed Slurm quota permits {workers:,} concurrent units at "
                f"{per_unit_threads:,} CPUs each"
            )
            script = executor.create_dispatcher_script(
                plan_path=saved_plan,
                processor_reference=processor_reference,
                workspace=self.config.workspace,
                email=self.config.email,
                output_path=target_script,
                options=options,
                prefetch_batches=active_policy.prefetch_batches,
                max_staged_gb=active_policy.max_staged_gb,
                minimum_free_gb=active_policy.minimum_free_gb,
                cleanup=active_policy.cleanup,
                keep_failed_inputs=active_policy.keep_failed_inputs,
                fsync_logs=active_policy.fsync_logs,
                retry_failed=retry_failed,
                batch_ids=batch_ids,
            )
            return script, executor.submit(script) if submit else None
        workers = options.max_parallel or self.config.max_workers
        per_unit_threads = max(task.resources.threads for task in selected_tasks)
        per_unit_memory_gb = max(task.resources.memory_gb for task in selected_tasks)
        total_threads = self.config.total_threads or per_unit_threads * workers
        total_memory_gb = self.config.total_memory_gb or per_unit_memory_gb * workers
        if options.cpus_per_node is not None and total_threads > options.cpus_per_node:
            raise ValueError(
                f"Single-node Slurm request needs {total_threads} CPUs but "
                f"cpus_per_node={options.cpus_per_node}"
            )
        workers = min(workers, max(1, total_threads // per_unit_threads))
        workers = min(workers, max(1, int(total_memory_gb // per_unit_memory_gb)))
        script = executor.create_coordinator_script(
            plan_path=saved_plan,
            processor_reference=processor_reference,
            workspace=self.config.workspace,
            email=self.config.email,
            output_path=target_script,
            options=options,
            max_workers=workers,
            total_threads=total_threads,
            total_memory_gb=total_memory_gb,
            prefetch_batches=active_policy.prefetch_batches,
            max_staged_gb=active_policy.max_staged_gb,
            minimum_free_gb=active_policy.minimum_free_gb,
            cleanup=active_policy.cleanup,
            keep_failed_inputs=active_policy.keep_failed_inputs,
            fsync_logs=active_policy.fsync_logs,
            retry_failed=retry_failed,
            batch_ids=batch_ids,
        )
        return script, executor.submit(script) if submit else None

    def status(self, plan: DatasetPlan, *, batch_ids: set[int] | None = None) -> dict[str, Any]:
        """Return durable status for *plan*, optionally limited to *batch_ids*."""

        tasks = [task for task in plan.tasks if batch_ids is None or task.batch_id in batch_ids]
        keys = [f"{plan.plan_id}__{task.task_id}" for task in tasks]
        summary = self.state.summary(keys)
        states = {record["task_id"]: record.get("status", "pending") for record in summary["tasks"]}
        batches: dict[int, dict[str, int]] = {}
        for task, key in zip(tasks, keys, strict=True):
            counts = batches.setdefault(
                task.batch_id,
                {"pending": 0, "running": 0, "succeeded": 0, "failed": 0},
            )
            status = states.get(key, "pending")
            counts[status] = counts.get(status, 0) + 1
        summary["batches"] = [
            {"batch_id": batch_id, "counts": counts} for batch_id, counts in sorted(batches.items())
        ]
        summary["batch_manifests"] = [
            saved.to_dict()
            for batch_id in sorted(batches)
            if (saved := self.batch_state.get(plan.plan_id, batch_id)) is not None
        ]
        return summary

    def publish_dataset(
        self,
        plan: DatasetPlan,
        destination: Path | None = None,
        *,
        mode: PublishMode = "auto",
        overwrite: bool = False,
    ) -> DatasetExport:
        """Publish completed *plan* into compact *destination* using *mode*.

        *overwrite* permits atomic replacement of an existing published dataset.
        """

        return DatasetPublisher(self.config.workspace, progress=self.progress).publish(
            plan,
            destination,
            mode=mode,
            overwrite=overwrite,
        )
