from __future__ import annotations

import hashlib
import inspect
import json
import logging
import os
import shutil
import tempfile
import threading
import traceback
from collections.abc import Iterable, Mapping
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass, field, replace
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
    DatasetTask,
    FastqSet,
    GenomeRef,
    ProcessingResult,
    ProcessingUnit,
    ResourceSpec,
    StagedFastq,
    WorkspaceJob,
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
from .workspace import WorkspaceStore

_METADATA_BUNDLE_CACHE_VERSION = 2
LOGGER = logging.getLogger("ncbi_dataset_builder.workflow")


@dataclass(frozen=True)
class BuilderConfig:
    """Configure a dataset builder.

    Args:
        workspace: Root directory for caches, jobs, state, and outputs.
        email: Contact email required for NCBI requests.
        ncbi_api_key: Optional key for the higher NCBI request rate.
        max_workers: Maximum simultaneous local tasks.
        total_threads: Optional thread budget shared by local tasks.
        total_memory_gb: Optional memory budget shared by local tasks.
        genome_policy: Policy used to choose NCBI assemblies.
        pipeline_policy: Bounded staging, storage, cleanup, and logging policy.
        group_by: Stable entity level represented by one workspace unit.
        description_profile: Compact metadata profile published for training.
        prefetch_max_size: SRA Toolkit maximum archive size, or ``u`` for unlimited.
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
    group_by: GroupLevel = "experiment"
    description_profile: str = "training"
    prefetch_max_size: str = "u"
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
        if self.group_by not in {"run", "experiment", "sra_sample", "biosample"}:
            raise ValueError(f"Unknown workspace grouping level: {self.group_by!r}")
        if self.description_profile not in {"training", "full"}:
            raise ValueError(f"Unknown description profile: {self.description_profile!r}")
        if not self.prefetch_max_size.strip():
            raise ValueError("prefetch_max_size cannot be empty")


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
    """Collect ordered outcomes and the automatic *job_id* for a local build.

    Args:
        outcomes: Ordered per-unit build outcomes.
        job_id: Automatic workspace job identifier.
    """

    outcomes: tuple[TaskOutcome, ...]
    job_id: str

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
        task: Workspace dataset task.
        state_key: Durable task-state key.
        log_path: The unit's only log file.
        genome: Resolved reference, when staging succeeded.
        staged: Downloaded input description, when staging succeeded.
        outcome: Terminal staging or skip outcome, when processing is unnecessary.
        reset_outputs: Remove package-owned prior work before processing.
    """

    task: DatasetTask
    state_key: str
    log_path: Path
    genome: GenomeRef | None = None
    staged: StagedFastq | None = None
    outcome: TaskOutcome | None = None
    reset_outputs: bool = False


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
        genome_root = config.workspace / "work" / "genome_cache"
        if genome_manager is None:
            legacy_lock = config.workspace / "genomes" / "genomes.lock.json"
            current_lock = genome_root / "genomes.lock.json"
            if legacy_lock.is_file() and not current_lock.exists():
                atomic_write_json(current_lock, read_json(legacy_lock))
                self.progress.message("Adopted prior genome lock into the workspace cache")
        self.genomes = genome_manager or GenomeManager(
            genome_root,
            policy=config.genome_policy,
            progress=self.progress,
        )
        self.state = TaskStateStore(config.workspace / "state" / "units")
        self.batch_state = BatchStateStore(config.workspace / "state" / "jobs")

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

    @staticmethod
    def _task_fingerprint(
        unit: ProcessingUnit,
        *,
        group_by: GroupLevel,
        genome_pin: str | None,
        processor_identity: str,
        fastq_identity: str,
    ) -> str:
        """Hash semantic unit, grouping, genome, processor, and *fastq_identity* inputs.

        *unit*, *group_by*, *genome_pin*, and *processor_identity* identify the
        remaining semantic inputs.
        """

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
            "fastq_identity": fastq_identity,
        }
        return hashlib.sha256(
            json.dumps(identity, sort_keys=True, separators=(",", ":")).encode("utf-8")
        ).hexdigest()

    def _fastq_identity(self, unit: ProcessingUnit) -> str:
        """Return the semantic FASTQ-provider identity for *unit*."""

        provider = self.fastq_provider
        explicit = getattr(provider, "cache_identity", None)
        if callable(explicit):
            return str(explicit(unit))
        cls = provider.__class__
        identity: dict[str, Any] = {"provider": f"{cls.__module__}:{cls.__qualname__}"}
        config = getattr(provider, "config", None)
        if config is not None:
            identity["config"] = repr(config)
        urls = getattr(provider, "urls", None)
        if isinstance(urls, Mapping):
            identity["urls"] = list(urls.get(unit.unit_id, ()))
        return json.dumps(identity, sort_keys=True, separators=(",", ":"))

    @staticmethod
    def _job_identifier(created_at: str, tasks: Iterable[DatasetTask]) -> str:
        """Return a readable timestamp and content hash for *created_at* and *tasks*."""

        digest = hashlib.sha256(
            "\n".join(task.fingerprint for task in tasks).encode("utf-8")
        ).hexdigest()[:10]
        stamp = (
            created_at.split("+", 1)[0]
            .replace("-", "")
            .replace(":", "")
            .replace(".", "")
            + "Z"
        )
        return f"job-{stamp}-{digest}"

    def _state_artifacts_valid(self, state: dict[str, Any]) -> bool:
        """Return whether all persisted processor and genome artifacts in *state* remain valid."""

        result = state.get("result")
        if not isinstance(result, dict):
            return False
        processing = result.get("processing")
        if not isinstance(processing, dict):
            return False
        outputs = [Path(str(value)) for value in processing.get("outputs", ())]
        if not outputs or any(not path.is_file() or path.stat().st_size == 0 for path in outputs):
            return False
        expected = result.get("output_sha256", {})
        facts = result.get("output_files", {})
        for path in outputs:
            fact = facts.get(str(path), {}) if isinstance(facts, dict) else {}
            current_size = path.stat().st_size / 1_000_000_000
            if (
                isinstance(fact, dict)
                and fact.get("size_gb") == current_size
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
        if not fasta.is_file() or fasta.stat().st_size == 0:
            return False
        genome_fact = result.get("genome_file", {})
        current_genome_size = fasta.stat().st_size / 1_000_000_000
        if (
            isinstance(genome_fact, dict)
            and genome_fact.get("size_gb") == current_genome_size
            and genome_fact.get("modified_ns") == fasta.stat().st_mtime_ns
        ):
            return True
        checksum = genome.get("sha256")
        return bool(checksum) and sha256_file(fasta, progress=self.progress) == checksum

    def _migrate_legacy_state(
        self,
        task: DatasetTask,
        *,
        processor_identity: str,
    ) -> dict[str, Any] | None:
        """Adopt compatible legacy state for *task* and *processor_identity* once."""

        legacy_root = self.config.workspace / "state" / "tasks"
        if not legacy_root.is_dir():
            return None
        suffix = f"__{sanitize_identifier(task.task_id)}.json"
        candidates = sorted(
            (path for path in legacy_root.glob(f"*{suffix}") if path.is_file()),
            key=lambda path: path.stat().st_mtime,
            reverse=True,
        )
        for path in candidates:
            try:
                record = read_json(path)
                if record.get("status") not in {"succeeded", "failed"}:
                    continue
                legacy_key = str(record.get("task_id") or path.stem)
                submission_id = legacy_key.removesuffix(f"__{task.task_id}")
                registration = (
                    self.config.workspace
                    / "state"
                    / "plans"
                    / f"{sanitize_identifier(submission_id)}.json"
                )
                registered_identity = (
                    read_json(registration).get("processor_identity")
                    if registration.is_file()
                    else None
                )
                compatible_identity = registered_identity == processor_identity or (
                    isinstance(registered_identity, str)
                    and registered_identity.startswith(processor_identity + ":source_sha256=")
                )
                if not compatible_identity:
                    continue
                result = record.get("result")
                fastq = result.get("fastq", {}) if isinstance(result, dict) else {}
                if tuple(sorted(fastq.get("run_accessions", ()))) != tuple(
                    sorted(task.unit.run_accessions)
                ):
                    continue
                copied = {
                    **record,
                    "fingerprint": task.fingerprint,
                    "job_id": None,
                    "task": task.to_dict(),
                    "migrated_from": str(path),
                }
                if isinstance(result, dict):
                    outputs = [
                        Path(str(value))
                        for value in result.get("processing", {}).get("outputs", ())
                    ]
                    if record.get("status") == "succeeded" and (
                        not outputs
                        or any(not output.is_file() or output.stat().st_size == 0 for output in outputs)
                    ):
                        continue
                    copied_result = dict(result)
                    copied_result["output_files"] = {
                        str(output): {
                            "size_gb": output.stat().st_size / 1_000_000_000,
                            "modified_ns": output.stat().st_mtime_ns,
                        }
                        for output in outputs
                    }
                    genome = result.get("genome", {})
                    fasta = Path(str(genome.get("fasta", ""))) if isinstance(genome, dict) else None
                    if fasta is not None and fasta.is_file() and fasta.stat().st_size > 0:
                        copied_result["genome_file"] = {
                            "size_gb": fasta.stat().st_size / 1_000_000_000,
                            "modified_ns": fasta.stat().st_mtime_ns,
                        }
                    copied["result"] = copied_result
                old_log = Path(str(record.get("log_path", "")))
                new_log = self._unit_log_path(task)
                if old_log.is_file() and not new_log.exists():
                    new_log.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copy2(old_log, new_log)
                copied["log_path"] = str(new_log)
                if self.state.import_record(task.task_id, copied):
                    self.progress.message(f"Adopted compatible prior state for {task.task_id}")
                return self.state.get(task.task_id)
            except (OSError, TypeError, ValueError):
                continue
        return None

    def reconcile(
        self,
        catalog: RunCatalog,
        processor: Processor | str,
        *,
        group_by: GroupLevel | None = None,
        resources: ResourceSpec | None = None,
        max_batch_gb: float | None = None,
        max_batch_units: int | None = None,
        genome_pins: dict[int, str] | None = None,
        query: str | None = None,
        processor_id: str | None = None,
    ) -> WorkspaceJob:
        """Compare *catalog* and *processor* with workspace state and save a job.

        *group_by* defines processing units, *resources* apply to every job task,
        *max_batch_gb* and *max_batch_units* form batches, *genome_pins* fix
        species assemblies, *query* records the search, and *processor_id* can
        explicitly version a dynamic processor. Resource and batch changes do
        not invalidate successful units; semantic unit or processor changes do.
        """

        self.progress.message(f"Reconcile {catalog.frame.height:,} catalog rows with workspace")
        clean = catalog.deduplicate_runs()
        selected_group = group_by or self.config.group_by
        resources = resources or ResourceSpec()
        resolved = None if isinstance(processor, str) else processor
        processor_identity = self._processor_identity(processor, processor_id, resolved)
        self.workspace.configure(
            group_by=selected_group,
            description_profile=self.config.description_profile,
            genome_policy=asdict(self.config.genome_policy),
        )
        units = [
            replace(
                unit,
                run_accessions=tuple(sorted(unit.run_accessions)),
                experiment_accessions=tuple(sorted(unit.experiment_accessions)),
                sra_sample_accessions=tuple(sorted(unit.sra_sample_accessions)),
                biosample_accessions=tuple(sorted(unit.biosample_accessions)),
            )
            for unit in clean.processing_units(by=selected_group)
        ]
        batches = RunCatalog.batch_units(units, max_gb=max_batch_gb, max_units=max_batch_units)
        pins = genome_pins or {}
        tasks: list[DatasetTask] = []
        for batch_id, batch in enumerate(batches):
            for unit in batch:
                task_id = sanitize_identifier(unit.unit_id)
                genome_pin = pins.get(unit.taxid) if unit.taxid is not None else None
                tasks.append(
                    DatasetTask(
                        task_id=task_id,
                        unit=unit,
                        batch_id=batch_id,
                        resources=resources,
                        genome_pin=genome_pin,
                        fingerprint=self._task_fingerprint(
                            unit,
                            group_by=selected_group,
                            genome_pin=genome_pin,
                            processor_identity=processor_identity,
                            fastq_identity=self._fastq_identity(unit),
                        ),
                    )
                )
        created_at = utc_timestamp()
        counts = {
            "new": 0,
            "changed": 0,
            "repair": 0,
            "succeeded": 0,
            "failed": 0,
            "running": 0,
        }
        manifest_states: dict[str, dict[str, Any] | None] = {}
        for task in tasks:
            state = self.state.get(task.task_id) or self._migrate_legacy_state(
                task, processor_identity=processor_identity
            )
            manifest_states[task.task_id] = state
            if state is None:
                counts["new"] += 1
            elif state.get("fingerprint") != task.fingerprint:
                counts["changed"] += 1
            elif state.get("status") == "succeeded":
                if self._state_artifacts_valid(state):
                    counts["succeeded"] += 1
                else:
                    counts["repair"] += 1
                    manifest_states[task.task_id] = {
                        **state,
                        "status": "pending",
                        "result": None,
                        "error": "Persisted output requires repair",
                    }
            else:
                status = str(state.get("status", "failed"))
                counts[status if status in counts else "failed"] += 1
        job = WorkspaceJob(
            job_id=self._job_identifier(created_at, tasks),
            created_at=created_at,
            query=query,
            group_by=selected_group,
            tasks=tuple(tasks),
            processor_identity=processor_identity,
            catalog_audit=clean.audit,
            metadata={
                "batch_count": len(batches),
                "max_batch_gb": max_batch_gb,
                "max_batch_units": max_batch_units,
                "reconciliation": counts,
                "fastq_provider": (
                    f"{self.fastq_provider.__class__.__module__}:"
                    f"{self.fastq_provider.__class__.__qualname__}"
                ),
            },
        )
        self.workspace.save_job(job)
        self.workspace.sync_manifest(job, manifest_states)
        self.progress.message(
            f"Workspace job {job.job_id}: {len(job.tasks):,} units in {len(batches):,} "
            f"batches; {counts['succeeded']:,} reusable, "
            f"{counts['new'] + counts['changed'] + counts['repair']:,} require work"
        )
        return job

    def load_job(self, path_or_id: str | Path) -> WorkspaceJob:
        """Load an internal job snapshot identified by *path_or_id*."""

        return self.workspace.load_job(path_or_id)

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

    def _unit_log_path(self, task: DatasetTask) -> Path:
        """Return the one workspace log path permanently assigned to *task*."""

        species = sanitize_identifier(task.unit.scientific_name or "unknown_species")
        return (
            self.config.workspace
            / "logs"
            / species
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

    def _task_requires_work(self, task: DatasetTask, *, retry_failed: bool) -> bool:
        """Return whether *task* needs staging under *retry_failed* policy."""

        state = self.state.get(task.task_id)
        if state is None or state.get("fingerprint") != task.fingerprint:
            return True
        status = state.get("status")
        if status == "succeeded":
            return not self._state_artifacts_valid(state)
        if status == "failed":
            return retry_failed
        return True

    def _claim_and_stage_task(
        self,
        task: DatasetTask,
        *,
        job_id: str,
        retry_failed: bool,
        policy: PipelinePolicy,
        staging_started_event: threading.Event | None = None,
        reclaim_running: bool = False,
    ) -> _PreparedTask:
        """Claim and stage *task* in *job_id* under retry and logging policy.

        *retry_failed* controls retries, *policy* controls logging, and optional
        *staging_started_event* signals after this provider stage completes.
        *reclaim_running* recovers state while holding the workspace lock.
        """

        state_key = task.task_id
        log_path = self._unit_log_path(task)
        previous = self.state.get(state_key)
        force = bool(
            previous
            and previous.get("status") == "succeeded"
            and previous.get("fingerprint") == task.fingerprint
            and not self._state_artifacts_valid(previous)
        )
        reset_outputs = previous is None or force or previous.get("fingerprint") != task.fingerprint
        try:
            claimed = self.state.start(
                state_key,
                retry_failed=retry_failed,
                log_path=log_path,
                reclaim_running=reclaim_running,
                fingerprint=task.fingerprint,
                job_id=job_id,
                task=task.to_dict(),
                force=force,
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
                        "pin or add TaxID before reconciliation"
                    )
                self.progress.message(f"Resolve genome for taxid {task.unit.taxid} and stage FASTQ")
                genome = self.genomes.resolve(
                    taxid=task.unit.taxid,
                    scientific_name=task.unit.scientific_name,
                    pin=task.genome_pin,
                )
                if staging_started_event is not None:
                    staging_started_event.set()
                staged = self._stage_fastq(task)
                self.state.set_phase(state_key, "ready")
            return _PreparedTask(
                task,
                state_key,
                log_path,
                genome=genome,
                staged=staged,
                reset_outputs=reset_outputs,
            )
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
        job: WorkspaceJob,
        tasks: list[DatasetTask],
        *,
        retry_failed: bool,
        policy: PipelinePolicy,
        occupied_size_gb: float,
        started_event: threading.Event | None = None,
    ) -> _PreparedBatch:
        """Stage one *tasks* batch from *job* under *retry_failed* and *policy*.

        *occupied_size_gb* accounts for the batch currently being processed;
        optional *started_event* is set after the staging transition is durable.
        """

        batch_id = tasks[0].batch_id
        estimated = sum(
            task.unit.total_size_gb
            for task in tasks
            if self._task_requires_work(task, retry_failed=retry_failed)
        )
        manifest = BatchManifest(
            job_id=job.job_id,
            batch_id=batch_id,
            status="staging",
            unit_ids=tuple(task.task_id for task in tasks),
            estimated_size_gb=estimated,
            task_statuses={task.task_id: "pending" for task in tasks},
            log_paths={
                task.task_id: str(self._unit_log_path(task)) for task in tasks
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
        prepared_by_index: dict[int, _PreparedTask] = {}
        worker_count = min(policy.download_workers, len(tasks))
        with ThreadPoolExecutor(
            max_workers=worker_count, thread_name_prefix="batch-download"
        ) as downloads:
            futures = {
                downloads.submit(
                    self._claim_and_stage_task,
                    task,
                    job_id=job.job_id,
                    retry_failed=retry_failed,
                    policy=policy,
                    staging_started_event=started_event,
                    reclaim_running=True,
                ): index
                for index, task in enumerate(tasks)
            }
            with self.progress.task(
                f"Stage batch {batch_id}", total=len(futures), unit="units"
            ) as progress:
                for future in as_completed(futures):
                    prepared_by_index[futures[future]] = future.result()
                    progress.update()
        prepared_entries = [prepared_by_index[index] for index in range(len(tasks))]
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
        job: WorkspaceJob,
        batch_id: int,
        *,
        retry_failed: bool = False,
        policy: PipelinePolicy | None = None,
        occupied_size_gb: float = 0.0,
    ) -> BatchManifest:
        """Stage *batch_id* from *job* for distributed workers.

        *retry_failed* includes previously failed tasks, *policy* controls storage
        and logging, and *occupied_size_gb* accounts for a currently running batch.
        This method does not claim task state; Slurm workers claim their own units.
        """

        active_policy = policy or self.config.pipeline_policy
        tasks = [task for task in job.tasks if task.batch_id == batch_id]
        if not tasks:
            raise ValueError(f"Job {job.job_id} has no batch {batch_id}")
        eligible: list[DatasetTask] = []
        statuses: dict[str, str] = {}
        for task in tasks:
            state = self.state.get(task.task_id)
            status = (state or {}).get("status", "pending")
            statuses[task.task_id] = status
            same_work = (state or {}).get("fingerprint") == task.fingerprint
            if same_work and (
                status == "succeeded" and self._state_artifacts_valid(state or {})
                or status == "failed" and not retry_failed
            ):
                continue
            eligible.append(task)
        estimated = sum(task.unit.total_size_gb for task in eligible)
        self._validate_staging_capacity(
            estimated_size_gb=estimated,
            occupied_size_gb=occupied_size_gb,
            policy=active_policy,
        )
        manifest = BatchManifest(
            job_id=job.job_id,
            batch_id=batch_id,
            status="staging",
            unit_ids=tuple(task.task_id for task in tasks),
            estimated_size_gb=estimated,
            task_statuses=statuses,
            log_paths={task.task_id: str(self._unit_log_path(task)) for task in tasks},
        )
        self.batch_state.save(manifest)
        cleanup_roots: dict[str, list[str]] = {}
        staging_failed = False

        def stage_one(task: DatasetTask) -> tuple[DatasetTask, StagedFastq | None, str | None]:
            """Stage one distributed *task* and return its result or traceback."""

            log_path = self._unit_log_path(task)
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
                    return task, staged, None
            except Exception:  # noqa: BLE001 - worker receives a later independent retry
                error = traceback.format_exc()
                with unit_log(
                    log_path,
                    phase="distributed-prefetch-error",
                    unit_id=task.task_id,
                    fsync=active_policy.fsync_logs,
                ):
                    LOGGER.error("Distributed prefetch failed:\n%s", error)
                return task, None, error

        worker_count = min(active_policy.download_workers, len(eligible)) if eligible else 1
        with ThreadPoolExecutor(
            max_workers=worker_count, thread_name_prefix="distributed-download"
        ) as downloads:
            futures = [downloads.submit(stage_one, task) for task in eligible]
            with self.progress.task(
                f"Prefetch batch {batch_id}", total=len(futures), unit="units"
            ) as progress:
                for future in as_completed(futures):
                    task, staged, error = future.result()
                    if error is None and staged is not None:
                        cleanup_roots[task.task_id] = [
                            str(path) for path in staged.cleanup_roots
                        ]
                        statuses[task.task_id] = "ready"
                    else:
                        staging_failed = True
                        statuses[task.task_id] = "staging_failed"
                        self.progress.message(
                            f"Prefetch failed for {task.task_id}; worker staging may retry it",
                            level=logging.WARNING,
                        )
                    progress.update()
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

    def finalize_distributed_batch(self, job: WorkspaceJob, batch_id: int) -> BatchManifest:
        """Finalize distributed *batch_id* state for *job* after its Slurm array exits."""

        tasks = [task for task in job.tasks if task.batch_id == batch_id]
        if not tasks:
            raise ValueError(f"Job {job.job_id} has no batch {batch_id}")
        prior = self.batch_state.get(job.job_id, batch_id)
        if prior is None:
            raise ValueError(f"Batch {batch_id} has no staging manifest")
        statuses = {
            task.task_id: (
                self.state.get(task.task_id) or {"status": "pending"}
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
        policy: PipelinePolicy,
    ) -> TaskOutcome:
        """Materialize and process one staged *prepared* task with *processor*.

        *policy* controls durable logging and cleanup behavior.
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
                task_root = self.config.workspace / "work" / "units" / task.task_id
                output_root = self.config.workspace / "outputs" / task.task_id
                if prepared.reset_outputs:
                    for owned in (task_root, output_root):
                        if owned.is_dir():
                            shutil.rmtree(owned)
                        elif owned.exists():
                            owned.unlink()
                fastq = replace(
                    fastq,
                    work_dir=task_root,
                    output_dir=output_root,
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
                    "output_files": {
                        str(path): {
                            "size_gb": path.stat().st_size / 1_000_000_000,
                            "modified_ns": path.stat().st_mtime_ns,
                        }
                        for path in result.outputs
                    },
                    "genome": prepared.genome.to_dict(),
                    "genome_file": {
                        "size_gb": prepared.genome.fasta.stat().st_size / 1_000_000_000,
                        "modified_ns": prepared.genome.fasta.stat().st_mtime_ns,
                    },
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
        policy: PipelinePolicy,
    ) -> tuple[TaskOutcome, ...]:
        """Process and safely clean one *prepared_batch* with *processor*.

        *policy* controls resources and cleanup.
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
            lambda entry: self._process_prepared_task(entry, processor, policy=policy),
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
        job_id: str,
        retry_failed: bool,
    ) -> TaskOutcome:
        """Execute *task* with *processor* in *job_id* under *retry_failed*."""

        policy = self.config.pipeline_policy
        prepared = self._claim_and_stage_task(
            task,
            job_id=job_id,
            retry_failed=retry_failed,
            policy=policy,
        )
        outcome = self._process_prepared_task(prepared, processor, policy=policy)
        self._cleanup_inputs(prepared, outcome, policy=policy)
        return outcome

    def build(
        self,
        catalog: RunCatalog,
        processor: Processor | str,
        *,
        group_by: GroupLevel | None = None,
        resources: ResourceSpec | None = None,
        max_batch_gb: float | None = None,
        max_batch_units: int | None = None,
        genome_pins: dict[int, str] | None = None,
        query: str | None = None,
        retry_failed: bool = False,
        batch_ids: set[int] | None = None,
        processor_id: str | None = None,
        policy: PipelinePolicy | None = None,
    ) -> BuildReport:
        """Reconcile *catalog* and execute required units with *processor*.

        *group_by*, *resources*, *max_batch_gb*, *max_batch_units*,
        *genome_pins*, and *query* describe the automatic job. *retry_failed*
        enables failed units, *batch_ids* limits execution, *processor_id*
        versions dynamic callables, and *policy* overrides pipeline behavior.
        """

        job = self.reconcile(
            catalog,
            processor,
            group_by=group_by,
            resources=resources,
            max_batch_gb=max_batch_gb,
            max_batch_units=max_batch_units,
            genome_pins=genome_pins,
            query=query,
            processor_id=processor_id,
        )
        lock = self.config.workspace / "state" / "pipeline-coordinator.lock"
        with exclusive_file_lock(
            lock,
            timeout_seconds=5,
            stale_after_seconds=5 * 60,
            heartbeat_seconds=30,
        ):
            report = self._run_job(
                job,
                processor,
                retry_failed=retry_failed,
                batch_ids=batch_ids,
                processor_id=processor_id,
                policy=policy,
            )
        self.workspace.sync_manifest(
            job, {task.task_id: self.state.get(task.task_id) for task in job.tasks}
        )
        return report

    def run_job(
        self,
        job: WorkspaceJob,
        processor: Processor | str,
        *,
        retry_failed: bool = False,
        batch_ids: set[int] | None = None,
        processor_id: str | None = None,
        policy: PipelinePolicy | None = None,
    ) -> BuildReport:
        """Execute internal *job* with *processor* under the workspace lock.

        *retry_failed*, *batch_ids*, *processor_id*, and *policy* control the
        same behavior as :meth:`build` without reconciling a new catalog.
        """

        lock = self.config.workspace / "state" / "pipeline-coordinator.lock"
        with exclusive_file_lock(
            lock,
            timeout_seconds=5,
            stale_after_seconds=5 * 60,
            heartbeat_seconds=30,
        ):
            return self._run_job(
                job,
                processor,
                retry_failed=retry_failed,
                batch_ids=batch_ids,
                processor_id=processor_id,
                policy=policy,
            )

    def _run_job(
        self,
        job: WorkspaceJob,
        processor: Processor | str,
        *,
        retry_failed: bool = False,
        batch_ids: set[int] | None = None,
        processor_id: str | None = None,
        policy: PipelinePolicy | None = None,
    ) -> BuildReport:
        """Run internal *job* with *processor* using bounded batch staging.

        *retry_failed* enables failed units, *batch_ids* limits execution,
        *processor_id* identifies dynamic callables, and optional *policy*
        overrides staging and cleanup configuration.
        """

        callable_processor = load_processor(processor) if isinstance(processor, str) else processor
        identity = self._processor_identity(processor, processor_id, callable_processor)
        if identity != job.processor_identity:
            raise ValueError(
                f"Job {job.job_id} requires processor {job.processor_identity!r}, got {identity!r}"
            )
        tasks = [task for task in job.tasks if batch_ids is None or task.batch_id in batch_ids]
        if batch_ids is not None and not tasks:
            raise ValueError(f"No tasks belong to requested batches: {sorted(batch_ids)}")
        if not tasks:
            return BuildReport((), job.job_id)
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
            job,
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
                        job,
                        batches[index + 1],
                        retry_failed=retry_failed,
                        policy=active_policy,
                        occupied_size_gb=current.manifest.staged_size_gb,
                        started_event=started_event,
                    )
                    started_event.wait()
                outcomes.extend(
                    self._process_batch(current, callable_processor, policy=active_policy)
                )
                if index + 1 >= len(batches):
                    continue
                if next_future is not None:
                    current = next_future.result()
                    next_future = None
                else:
                    current = self._stage_batch(
                        job,
                        batches[index + 1],
                        retry_failed=retry_failed,
                        policy=active_policy,
                        occupied_size_gb=0.0,
                    )
        order = {task.task_id: index for index, task in enumerate(tasks)}
        outcomes.sort(key=lambda outcome: order[outcome.task_id])
        report = BuildReport(tuple(outcomes), job.job_id)
        self.workspace.sync_manifest(
            job, {task.task_id: self.state.get(task.task_id) for task in job.tasks}
        )
        return report

    def run_task(
        self,
        job: WorkspaceJob,
        task_index: int,
        processor: Processor | str,
        *,
        retry_failed: bool = False,
        processor_id: str | None = None,
    ) -> TaskOutcome:
        """Execute one *task_index* from internal *job* with *processor*.

        *retry_failed* enables a failed task and *processor_id* explicitly
        identifies dynamic callables when automatic identity is insufficient.
        """

        if task_index < 0 or task_index >= len(job.tasks):
            raise IndexError(f"Task index {task_index} is outside 0..{len(job.tasks) - 1}")
        callable_processor = load_processor(processor) if isinstance(processor, str) else processor
        identity = self._processor_identity(processor, processor_id, callable_processor)
        if identity != job.processor_identity:
            raise ValueError(
                f"Job {job.job_id} requires processor {job.processor_identity!r}, got {identity!r}"
            )
        outcome = self._execute_task(
            job.tasks[task_index],
            callable_processor,
            job_id=job.job_id,
            retry_failed=retry_failed,
        )
        self.workspace.sync_manifest(
            job, {task.task_id: self.state.get(task.task_id) for task in job.tasks}
        )
        return outcome

    def submit_slurm(
        self,
        catalog: RunCatalog,
        *,
        processor_reference: str,
        options: SlurmOptions,
        group_by: GroupLevel | None = None,
        max_batch_gb: float | None = None,
        max_batch_units: int | None = None,
        genome_pins: dict[int, str] | None = None,
        query: str | None = None,
        script_path: Path | None = None,
        submit: bool = True,
        retry_failed: bool = False,
        batch_ids: set[int] | None = None,
        policy: PipelinePolicy | None = None,
    ) -> tuple[Path, str | None]:
        """Reconcile *catalog* and create or submit its Slurm coordinator.

        *processor_reference* must be importable, *options* configures Slurm,
        *group_by*, *max_batch_gb*, *max_batch_units*, *genome_pins*, and
        *query* describe the automatic job. *script_path* controls the script,
        *submit* chooses dry-run behavior, *retry_failed* enables retries,
        *batch_ids* limits tasks, and *policy* overrides pipeline behavior.
        """

        job = self.reconcile(
            catalog,
            processor_reference,
            group_by=group_by,
            resources=options.resources,
            max_batch_gb=max_batch_gb,
            max_batch_units=max_batch_units,
            genome_pins=genome_pins,
            query=query,
        )
        saved_job = self.workspace.jobs / f"{job.job_id}.json"
        target_script = script_path or self.config.workspace / "slurm" / f"{job.job_id}.sbatch"
        executor = SlurmExecutor(progress=self.progress)
        (self.config.workspace / "logs" / "slurm").mkdir(parents=True, exist_ok=True)
        selected_tasks = [
            task for task in job.tasks if batch_ids is None or task.batch_id in batch_ids
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
                job_path=saved_job,
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
                prefetch_max_size=self.config.prefetch_max_size,
                download_workers=active_policy.download_workers,
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
            job_path=saved_job,
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
            prefetch_max_size=self.config.prefetch_max_size,
            download_workers=active_policy.download_workers,
        )
        return script, executor.submit(script) if submit else None

    def status(
        self,
        job_id: str | None = None,
        *,
        batch_ids: set[int] | None = None,
    ) -> dict[str, Any]:
        """Return workspace status for optional *job_id* and *batch_ids*."""

        job = self.workspace.load_job(job_id) if job_id is not None else self.workspace.latest_job()
        tasks = [task for task in job.tasks if batch_ids is None or task.batch_id in batch_ids]
        counts = {"pending": 0, "running": 0, "succeeded": 0, "failed": 0}
        records: list[dict[str, Any]] = []
        states: dict[str, str] = {}
        for task in tasks:
            saved = self.state.get(task.task_id)
            status = "pending"
            if saved is not None and saved.get("fingerprint") == task.fingerprint:
                status = str(saved.get("status", "pending"))
                if status == "succeeded" and not self._state_artifacts_valid(saved):
                    status = "pending"
                records.append({**saved, "effective_status": status})
            counts[status] = counts.get(status, 0) + 1
            states[task.task_id] = status
        summary: dict[str, Any] = {"counts": counts, "tasks": records}
        batches: dict[int, dict[str, int]] = {}
        for task in tasks:
            counts = batches.setdefault(
                task.batch_id,
                {"pending": 0, "running": 0, "succeeded": 0, "failed": 0},
            )
            status = states.get(task.task_id, "pending")
            counts[status] = counts.get(status, 0) + 1
        summary["batches"] = [
            {"batch_id": batch_id, "counts": counts} for batch_id, counts in sorted(batches.items())
        ]
        summary["batch_manifests"] = [
            saved.to_dict()
            for batch_id in sorted(batches)
            if (saved := self.batch_state.get(job.job_id, batch_id)) is not None
        ]
        summary["job_id"] = job.job_id
        return summary

    def publish_dataset(
        self,
        destination: Path | None = None,
        *,
        job_id: str | None = None,
        mode: PublishMode = "auto",
        overwrite: bool = False,
    ) -> DatasetExport:
        """Publish optional *job_id* into compact *destination* using *mode*.

        *overwrite* permits atomic replacement of an existing published dataset.
        """

        job = self.workspace.load_job(job_id) if job_id is not None else self.workspace.latest_job()
        return DatasetPublisher(self.config.workspace, progress=self.progress).publish(
            job,
            destination,
            mode=mode,
            overwrite=overwrite,
        )
