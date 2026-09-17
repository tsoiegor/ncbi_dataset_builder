"""High-level workspace API and sample-streaming scheduler."""

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
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from dataclasses import asdict, dataclass, field, replace
from pathlib import Path
from typing import Any

from .acquisition.fastq import (
    AtomicDownloader,
    FastqProvider,
    GeoFastqProvider,
    SraToolkitProvider,
)
from .acquisition.genomes import GenomeManager, GenomeSelectionPolicy
from .acquisition.geo import GeoClient
from .catalog import GroupLevel, RunCatalog, validate_polars_runtime
from .errors import StaleUnitClaim, UnitAlreadyRunning
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
from .models import (
    FastqSet,
    GenomeRef,
    ProcessingContext,
    ProcessingResult,
    ProcessingUnit,
    StagedFastq,
)
from .processing.base import Processor, load_processor
from .support.progress import ProgressReporter
from .support.unit_logging import install_unit_logging, unit_log
from .support.util import (
    atomic_write_json,
    exclusive_file_lock,
    sanitize_identifier,
    sha256_file,
    utc_timestamp,
)
from .workspace import WorkspaceStore

LOGGER = logging.getLogger("ncbi_dataset_builder.api")


@dataclass(frozen=True)
class BuilderConfig:
    """Configure stable NCBI and workspace behavior.

    Args:
        workspace: Root directory for the manifest, runtime data, and default output.
        output_dir: Processor-owned output root; defaults to ``workspace/output``.
        email: Contact email required by NCBI Entrez.
        ncbi_api_key: Optional NCBI key for a higher request rate.
        genome_policy: Policy used to choose NCBI assemblies.
        group_by: Catalog entity represented by one processing unit.
        prefetch_max_size: SRA Toolkit archive-size limit, or ``u`` for unlimited.
        show_progress: Display long-running operation progress.
        progress_bars: Use tqdm bars when available.

    CPU, memory, storage, and concurrency belong to an execution-system object,
    not to this stable builder configuration.
    """

    workspace: Path
    output_dir: Path | None = None
    email: str | None = None
    ncbi_api_key: str | None = None
    genome_policy: GenomeSelectionPolicy = field(default_factory=GenomeSelectionPolicy)
    group_by: GroupLevel = "experiment"
    prefetch_max_size: str = "u"
    show_progress: bool = True
    progress_bars: bool = True

    def __post_init__(self) -> None:
        """Normalize the workspace and validate stable settings."""

        object.__setattr__(self, "workspace", Path(self.workspace))
        if self.output_dir is not None:
            object.__setattr__(self, "output_dir", Path(self.output_dir))
        if self.group_by not in {"run", "experiment", "sra_sample", "biosample"}:
            raise ValueError(f"Unknown workspace grouping level: {self.group_by!r}")
        if not self.prefetch_max_size.strip():
            raise ValueError("prefetch_max_size cannot be empty")

    def to_worker_dict(self, *, output_dir: Path) -> dict[str, Any]:
        """Serialize non-secret settings with resolved processor *output_dir*."""

        return {
            "output_dir": str(output_dir.resolve()),
            "genome_policy": asdict(self.genome_policy),
            "group_by": self.group_by,
            "prefetch_max_size": self.prefetch_max_size,
            "show_progress": self.show_progress,
            "progress_bars": self.progress_bars,
        }

    @classmethod
    def from_worker_dict(
        cls,
        value: Mapping[str, Any],
        *,
        workspace: Path,
        email: str | None,
        ncbi_api_key: str | None,
    ) -> BuilderConfig:
        """Restore *value* for *workspace* using runtime *email* and *ncbi_api_key*."""

        return cls(
            workspace=workspace,
            output_dir=Path(str(value["output_dir"])) if value.get("output_dir") else None,
            email=email,
            ncbi_api_key=ncbi_api_key,
            genome_policy=GenomeSelectionPolicy(**dict(value.get("genome_policy", {}))),
            group_by=str(value.get("group_by", "experiment")),
            prefetch_max_size=str(value.get("prefetch_max_size", "u")),
            show_progress=bool(value.get("show_progress", True)),
            progress_bars=bool(value.get("progress_bars", True)),
        )


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
    claim_id: str | None = None


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
        self.workspace = WorkspaceStore(config.workspace, output_dir=config.output_dir)
        if config.email:
            entrez = EntrezClient(
                email=config.email,
                api_key=config.ncbi_api_key,
                cache_dir=self.workspace.path("metadata_cache"),
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
            self.workspace.path("genomes"),
            policy=config.genome_policy,
            progress=self.progress,
        )
        self.state = UnitStateStore(self.workspace.path("state") / "units")
        self._description_cache_lock = threading.Lock()
        self._description_cache_signature: tuple[str, str] | None = None
        self._description_cache: dict[str, dict[str, Any]] = {}

    def _serialize_fastq_provider(self, *, required: bool) -> dict[str, Any] | None:
        """Serialize the configured provider, rejecting unsupported Slurm providers."""

        provider = self.fastq_provider
        if type(provider) is SraToolkitProvider:
            return {
                "kind": "sra_toolkit",
                "retries": provider.retries,
                "prefetch_max_size": provider.prefetch_max_size,
                "prefetch_reset_after_failures": provider.prefetch_reset_after_failures,
                "prefetch_retry_max_delay_seconds": provider.prefetch_retry_max_delay_seconds,
            }
        if type(provider) is GeoFastqProvider:
            downloader = provider.downloader
            return {
                "kind": "geo_fastq",
                "urls": provider.urls,
                "downloader": {
                    "user_agent": downloader.user_agent,
                    "retries": downloader.retries,
                    "timeout_seconds": downloader.timeout_seconds,
                },
            }
        if required:
            raise TypeError(
                "Slurm execution requires a serializable SraToolkitProvider or "
                "GeoFastqProvider; custom providers cannot be reconstructed safely"
            )
        return None

    @staticmethod
    def _restore_fastq_provider(value: Mapping[str, Any] | None) -> FastqProvider | None:
        """Restore a built-in FASTQ provider from serialized *value*."""

        if not value:
            return None
        kind = value.get("kind")
        if kind == "sra_toolkit":
            return SraToolkitProvider(
                retries=int(value.get("retries", 3)),
                prefetch_max_size=str(value.get("prefetch_max_size", "u")),
                prefetch_reset_after_failures=int(
                    value.get("prefetch_reset_after_failures", 4)
                ),
                prefetch_retry_max_delay_seconds=float(
                    value.get("prefetch_retry_max_delay_seconds", 300.0)
                ),
            )
        if kind == "geo_fastq":
            downloader_value = dict(value.get("downloader", {}))
            downloader = AtomicDownloader(
                user_agent=str(downloader_value["user_agent"]),
                retries=int(downloader_value.get("retries", 5)),
                timeout_seconds=float(downloader_value.get("timeout_seconds", 120.0)),
            )
            return GeoFastqProvider(
                {
                    str(unit_id): [str(url) for url in urls]
                    for unit_id, urls in dict(value.get("urls", {})).items()
                },
                downloader=downloader,
            )
        raise ValueError(f"Unknown serialized FASTQ provider: {kind!r}")

    @classmethod
    def from_execution_record(
        cls,
        *,
        workspace: Path,
        record: ExecutionRecord,
        email: str | None,
        ncbi_api_key: str | None,
    ) -> DatasetBuilder:
        """Reconstruct *record* in *workspace* using runtime *email* and *ncbi_api_key*."""

        serialized = record.metadata.get("builder_config")
        if not isinstance(serialized, Mapping):
            raise TypeError(
                "Execution record lacks complete builder_config; recreate the Slurm execution"
            )
        config = BuilderConfig.from_worker_dict(
            serialized,
            workspace=workspace,
            email=email,
            ncbi_api_key=ncbi_api_key,
        )
        provider_value = record.metadata.get("fastq_provider_config")
        if provider_value is not None and not isinstance(provider_value, Mapping):
            raise TypeError("Execution fastq_provider_config must be a mapping")
        return cls(config, fastq_provider=cls._restore_fastq_provider(provider_value))

    def _processor_description(
        self,
        processor: Processor,
        unit: ProcessingUnit,
    ) -> tuple[str, dict[str, Any]] | None:
        """Return the profile and description requested by *processor* for *unit*."""

        profile = getattr(processor, "description_profile", None)
        if profile is None:
            return None
        if not isinstance(profile, str) or not profile:
            raise TypeError("Processor description_profile must be a non-empty string")
        experiments = unit.experiment_accessions
        if len(experiments) != 1:
            raise ValueError(
                f"Processor {processor!r} requires exactly one experiment per unit; "
                f"{unit.unit_id} contains {list(experiments)}"
            )
        metadata_path = self.workspace.path("metadata") / "metadata.json"
        if not metadata_path.is_file():
            raise FileNotFoundError(
                f"Processor {processor!r} requires materialized {profile!r} metadata, "
                f"but normalized metadata is missing at {metadata_path}. "
                "Call builder.enrich_metadata(catalog) before build()."
            )
        signature = (profile, sha256_file(metadata_path, progress=self.progress))
        with self._description_cache_lock:
            if self._description_cache_signature != signature:
                bundle = MetadataBundle.load(metadata_path, progress=self.progress)
                self._description_cache = bundle.descriptions_by_experiment(
                    profile=profile,
                    progress=self.progress,
                )
                self._description_cache_signature = signature
            descriptions = self._description_cache
        experiment = experiments[0]
        description = descriptions.get(experiment)
        if description is None:
            raise KeyError(
                f"No {profile!r} description is available for experiment {experiment}"
            )
        return profile, description

    def _materialize_processor_description(
        self,
        processor: Processor,
        item: QueueItem,
        output_dir: Path,
    ) -> Path | None:
        """Materialize a requested experiment description beside processor outputs."""

        requested = self._processor_description(processor, item.unit)
        if requested is None:
            return None
        _profile, description = requested
        destination = output_dir / f"{item.item_id}.json"
        atomic_write_json(destination, description)
        return destination

    def _description_identity(
        self,
        processor: Processor,
        unit: ProcessingUnit,
    ) -> dict[str, str] | None:
        """Return the profile and canonical content hash affecting *unit*."""

        requested = self._processor_description(processor, unit)
        if requested is None:
            return None
        profile, description = requested
        payload = json.dumps(
            description,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        return {"profile": profile, "sha256": hashlib.sha256(payload).hexdigest()}

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
        cache = self.workspace.path("catalogs") / f"sra.{digest}.csv"
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
    ) -> MetadataBundle:
        """Fetch normalized metadata for SRA *accessions* and save it.

        Args:
            accessions: SRA accessions such as ``SRP...``, ``SRX...``, or ``SRR...``.
            destination: Output directory; defaults to ``workspace/runtime/metadata``.
            include_raw: Retain parsed raw XML trees in the bundle.
            refresh: Bypass reusable NCBI response caches.
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
            destination or self.workspace.path("metadata"),
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
    ) -> MetadataBundle:
        """Fetch and save normalized metadata for runs in *catalog*.

        Args:
            catalog: Run catalog whose linked accessions are fetched.
            destination: Output directory; defaults to ``workspace/runtime/metadata``.
            include_raw: Retain parsed raw XML trees in the bundle.
            refresh: Bypass reusable NCBI response caches.
        """

        if self.sra is None or self.biosample is None:
            raise ValueError("enrich_metadata requires an email in BuilderConfig")
        target = destination or self.workspace.path("metadata")
        target.mkdir(parents=True, exist_ok=True)
        with exclusive_file_lock(target / ".metadata.lock", timeout_seconds=120):
            metadata_path = target / "metadata.json"
            index_path = target / "metadata_index.json"
            cumulative = (
                MetadataBundle.load(metadata_path, progress=self.progress)
                if metadata_path.is_file()
                else MetadataBundle()
            )
            indexed_complete: set[str] | None = None
            if index_path.is_file():
                try:
                    index_value = json.loads(index_path.read_text(encoding="utf-8"))
                    indexed_complete = {
                        str(accession)
                        for accession, entry in index_value.get("experiments", {}).items()
                        if isinstance(entry, dict)
                        and entry.get("complete") is True
                        and (not include_raw or entry.get("raw_available") is True)
                    }
                except (OSError, json.JSONDecodeError, AttributeError):
                    indexed_complete = None
            if "Experiment" in catalog.frame.columns:
                requested = sorted(
                    str(value)
                    for value in catalog.frame.get_column("Experiment")
                    .drop_nulls()
                    .unique()
                    .to_list()
                    if str(value)
                )
            else:
                requested = []
            if requested:
                complete = cumulative.complete_experiment_accessions(include_raw=include_raw)
                if indexed_complete is not None:
                    # A stale index must never hide records absent from metadata.json.
                    complete &= indexed_complete
                missing = requested if refresh else sorted(set(requested) - complete)
                if missing:
                    fetched = fetch_metadata_for_accessions(
                        missing,
                        sra=self.sra,
                        biosample=self.biosample,
                        include_raw=include_raw,
                        refresh=refresh,
                    )
                    cumulative.merge_from(fetched)
                scoped = cumulative.subset_experiments(requested)
            else:
                scoped = fetch_metadata_for_catalog(
                    catalog,
                    sra=self.sra,
                    biosample=self.biosample,
                    include_raw=include_raw,
                    refresh=refresh,
                )
                cumulative.merge_from(scoped)
            cumulative.save(
                target,
                progress=self.progress,
            )
            sample_by_accession = {
                row.get("accession"): row for row in cumulative.sra_samples
            }
            normalized_complete = cumulative.complete_experiment_accessions(
                include_raw=False
            )
            raw_complete = cumulative.complete_experiment_accessions(include_raw=True)
            index = {}
            for relation in cumulative.packages:
                experiment = relation.get("experiment_accession")
                sample = relation.get("sra_sample_accession")
                if not experiment or not sample:
                    continue
                sample_record = sample_by_accession.get(sample, {})
                index[str(experiment)] = {
                    "sra_sample": sample,
                    "biosample": sample_record.get("biosample"),
                    "complete": experiment in normalized_complete,
                    "raw_available": experiment in raw_complete,
                }
            atomic_write_json(
                index_path,
                {
                    "schema_version": 1,
                    "updated_at": utc_timestamp(),
                    "experiments": index,
                },
            )
            return scoped

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
        description_identity: dict[str, str] | None,
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
            "description_identity": description_identity,
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
        resolved = load_processor(processor) if isinstance(processor, str) else processor
        processor_identity = self._processor_identity(processor, processor_id, resolved)
        self.workspace.configure(
            group_by=selected_group,
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
                        description_identity=self._description_identity(resolved, unit),
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
                "builder_config": self.config.to_worker_dict(
                    output_dir=self.workspace.output
                ),
                "fastq_provider_config": self._serialize_fastq_provider(
                    required=isinstance(
                        execution, (SlurmSingleNodeExecution, SlurmDistributedExecution)
                    )
                ),
                "output_dir": str(self.workspace.output.resolve()),
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
        serialized_outputs = processing.get("outputs", {})
        if isinstance(serialized_outputs, dict):
            outputs = {
                str(role): Path(str(value.get("path") if isinstance(value, dict) else value))
                for role, value in serialized_outputs.items()
            }
        elif isinstance(serialized_outputs, list):
            outputs = {str(path): Path(str(path)) for path in serialized_outputs}
        else:
            return False
        if not outputs or any(
            not path.is_file() or path.stat().st_size == 0 for path in outputs.values()
        ):
            return False
        expected = result.get("output_sha256", {})
        facts = result.get("output_files", {})
        for role, path in outputs.items():
            fact = facts.get(role, facts.get(str(path), {})) if isinstance(facts, dict) else {}
            if (
                isinstance(fact, dict)
                and fact.get("size") == path.stat().st_size
                and fact.get("modified_ns") == path.stat().st_mtime_ns
            ):
                continue
            checksum = fact.get("sha256") if isinstance(fact, dict) else None
            if not checksum and isinstance(expected, dict):
                checksum = expected.get(role, expected.get(str(path)))
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
        return self.workspace.path("logs") / species / f"{item.item_id}.log"

    def _stage_fastq(self, item: QueueItem) -> StagedFastq:
        """Stage input for *item*, adapting providers that expose only ``fetch``."""

        destination = self.workspace.path("fastq")
        stage = getattr(self.fastq_provider, "stage", None)
        if callable(stage):
            return stage(item.unit, destination, threads=item.resources.cpus)
        ready = self.fastq_provider.fetch(item.unit, destination, threads=item.resources.cpus)
        unit_root = destination / sanitize_identifier(item.unit.unit_id)
        return StagedFastq(
            unit_id=item.unit.unit_id,
            source=ready.source,
            size_gb=paths_size_gb([unit_root]),
            cleanup_roots=(unit_root,),
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
                self.workspace.path("fastq"),
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
            claim_id = self.state.start(
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
        if not claim_id:
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
                self.state.set_phase(item.item_id, "ready", claim_id=str(claim_id))
            return _PreparedUnit(
                item=item,
                log_path=log_path,
                genome=genome,
                staged=staged,
                reset_outputs=reset_outputs,
                claim_id=str(claim_id),
            )
        except Exception:  # noqa: BLE001 - sample boundary persists operational failures
            error = traceback.format_exc()
            self.state.fail(item.item_id, error, claim_id=str(claim_id))
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
        if prepared.claim_id is None:
            raise ValueError(f"Prepared sample {item.item_id} has no state claim")
        try:
            self.state.set_phase(
                item.item_id,
                "processing",
                claim_id=prepared.claim_id,
            )
            self.state.set_runtime_resources(
                item.item_id,
                cpus=cpus,
                memory_gb=item.resources.memory_gb,
                claim_id=prepared.claim_id,
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
                output_root = self.workspace.output / item.item_id
                if prepared.reset_outputs:
                    if output_root.is_dir():
                        shutil.rmtree(output_root)
                    elif output_root.exists():
                        output_root.unlink()
                output_root.mkdir(parents=True, exist_ok=True)
                self._materialize_processor_description(processor, item, output_root)
                current_state = self.state.get(item.item_id) or {}
                context = ProcessingContext(
                    unit_id=item.item_id,
                    threads=cpus,
                    output_dir=output_root,
                    log_path=prepared.log_path,
                    execution_id=str(current_state.get("execution_id") or "unknown"),
                )
                result = processor(fastq, prepared.genome, context)
                if not isinstance(result, ProcessingResult):
                    raise TypeError(
                        "Processor must return ProcessingResult, got "
                        f"{type(result).__name__} for {item.item_id}"
                    )
                result.validate(output_dir=output_root)
                resolved_outputs = result.resolved_outputs(output_root)
                payload = {
                    "processing": {
                        **result.to_dict(),
                        "outputs": {
                            role: str(path) for role, path in resolved_outputs.items()
                        },
                    },
                    "output_files": {
                        role: {
                            "path": str(path),
                            "sha256": sha256_file(path, progress=self.progress),
                            "size": path.stat().st_size,
                            "modified_ns": path.stat().st_mtime_ns,
                        }
                        for role, path in resolved_outputs.items()
                    },
                    "genome": prepared.genome.to_dict(),
                    "fastq": fastq.to_dict(),
                    "context": context.to_dict(),
                    "log_path": str(prepared.log_path),
                }
                self.state.succeed(item.item_id, payload, claim_id=prepared.claim_id)
            return UnitOutcome(item.item_id, "succeeded", result=payload)
        except Exception:  # noqa: BLE001 - sample boundary persists operational failures
            error = traceback.format_exc()
            try:
                self.state.fail(item.item_id, error, claim_id=prepared.claim_id)
            except StaleUnitClaim:
                LOGGER.exception("Discard stale processing result for %s", item.item_id)
                raise
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
        boundary = self.workspace.path("fastq").resolve()
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
                        reclaim_running=False,
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
                ``(fastq, genome, context)``.
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
        lock = self.workspace.path("state") / "queue-coordinator.lock"
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
        target = script_path or self.workspace.path("slurm") / f"{record.execution_id}.sbatch"
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
        """Return a rich status report for optional *execution_id*."""

        if execution_id is not None:
            record = self.workspace.load_execution(execution_id)
            items = list(record.items)
            scope = "execution"
        else:
            records = self.workspace.all_executions()
            if not records:
                raise FileNotFoundError("Workspace has no execution records")
            record = records[-1]
            indexed_items: dict[str, QueueItem] = {}
            for saved_record in records:
                for item in saved_record.items:
                    indexed_items[item.item_id] = item
            workspace_states = self.state.summary()["units"]
            for state in workspace_states:
                serialized_item = state.get("item")
                if not isinstance(serialized_item, dict):
                    continue
                item = QueueItem.from_dict(serialized_item)
                indexed_items[item.item_id] = item
            items = list(indexed_items.values())
            scope = "workspace"
        summary = self.state.summary([item.item_id for item in items])
        states = {state.get("unit_id"): state for state in summary["units"]}
        experiments = []
        execution_genomes: dict[str, dict[str, Any]] = {}
        for item in items:
            state = states.get(item.item_id) or {}
            result = state.get("result") if isinstance(state.get("result"), dict) else {}
            processing = (
                result.get("processing") if isinstance(result.get("processing"), dict) else {}
            )
            serialized_outputs = processing.get("outputs", {})
            if isinstance(serialized_outputs, dict):
                output_roles = list(serialized_outputs)
            elif isinstance(serialized_outputs, list):
                output_roles = [Path(str(path)).name for path in serialized_outputs]
            else:
                output_roles = []
            genome = result.get("genome") if isinstance(result.get("genome"), dict) else {}
            if genome.get("accession"):
                execution_genomes[str(genome["accession"])] = genome
            error_lines = [line.strip() for line in str(state.get("error") or "").splitlines() if line.strip()]
            experiments.append(
                {
                    "experiment_id": item.unit.unit_id,
                    "status": state.get("status", "pending"),
                    "phase": state.get("phase", "waiting"),
                    "species": item.unit.scientific_name,
                    "taxid": item.unit.taxid,
                    "runs": list(item.unit.run_accessions),
                    "genome_accession": genome.get("accession"),
                    "genome_available": bool(
                        genome.get("fasta")
                        and Path(str(genome["fasta"])).is_file()
                        and Path(str(genome["fasta"])).stat().st_size > 0
                    ),
                    "outputs": output_roles,
                    "attempts": int(state.get("attempts", 0)),
                    "allocated_cpus": state.get("allocated_cpus"),
                    "slurm_job_id": state.get("slurm_job_id"),
                    "started_at": state.get("started_at"),
                    "finished_at": state.get("finished_at"),
                    "log_path": state.get("log_path"),
                    "error": error_lines[-1] if error_lines else None,
                }
            )

        genome_inventory: dict[str, dict[str, Any]] = {}
        lockfile = getattr(self.genomes, "lockfile", None)
        if isinstance(lockfile, Path) and lockfile.is_file():
            try:
                inventory = json.loads(lockfile.read_text(encoding="utf-8")).get("genomes", {})
            except (OSError, json.JSONDecodeError):
                inventory = {}
            for taxid, genome in sorted(inventory.items()):
                fasta = Path(str(genome.get("fasta", "")))
                accession = str(genome.get("accession") or f"taxid:{taxid}")
                genome_inventory[accession] = {
                    "taxid": int(taxid),
                    "scientific_name": genome.get("scientific_name"),
                    "accession": genome.get("accession"),
                    "available": fasta.is_file() and fasta.stat().st_size > 0,
                    "fasta": str(fasta),
                }
        for accession, genome in execution_genomes.items():
            fasta = Path(str(genome.get("fasta", "")))
            genome_inventory.setdefault(
                accession,
                {
                    "taxid": genome.get("taxid"),
                    "scientific_name": genome.get("scientific_name"),
                    "accession": accession,
                    "available": fasta.is_file() and fasta.stat().st_size > 0,
                    "fasta": str(fasta),
                },
            )
        genomes = list(genome_inventory.values())

        metadata_index = self.workspace.path("metadata") / "metadata_index.json"
        cached_experiments = 0
        if metadata_index.is_file():
            try:
                metadata_value = json.loads(metadata_index.read_text(encoding="utf-8"))
                cached_experiments = len(metadata_value.get("experiments", {}))
            except (OSError, json.JSONDecodeError):
                pass
        return {
            "execution_id": record.execution_id,
            "scope": scope,
            "execution": {
                "created_at": record.created_at,
                "type": record.execution_type,
                "group_by": record.group_by,
                "processor": record.processor_identity,
                "query": record.query,
                "total_experiments": len(items),
            },
            "counts": summary["counts"],
            "experiments": experiments,
            "genomes": {
                "registered": len(genomes),
                "downloaded": sum(genome["available"] for genome in genomes),
                "items": genomes,
            },
            "metadata": {"cached_experiments": cached_experiments},
            "units": summary["units"],
        }
