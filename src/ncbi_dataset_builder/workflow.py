from __future__ import annotations

import hashlib
import inspect
import os
import tempfile
import traceback
import uuid
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any

from .catalog import GroupLevel, RunCatalog
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
from .models import DatasetPlan, DatasetTask, ProcessingResult, ResourceSpec
from .processing.base import Processor, load_processor
from .state import TaskStateStore
from .util import (
    atomic_write_json,
    exclusive_file_lock,
    read_json,
    sanitize_identifier,
    sha256_file,
    utc_timestamp,
)


@dataclass(frozen=True)
class BuilderConfig:
    workspace: Path
    email: str | None = None
    ncbi_api_key: str | None = None
    max_workers: int = 1
    total_threads: int | None = None
    genome_policy: GenomeSelectionPolicy = field(default_factory=GenomeSelectionPolicy)

    def __post_init__(self) -> None:
        object.__setattr__(self, "workspace", Path(self.workspace))
        if self.max_workers < 1:
            raise ValueError("max_workers must be positive")
        if self.total_threads is not None and self.total_threads < 1:
            raise ValueError("total_threads must be positive")


@dataclass(frozen=True)
class TaskOutcome:
    task_id: str
    status: str
    result: dict[str, Any] | None = None
    error: str | None = None


@dataclass(frozen=True)
class BuildReport:
    outcomes: tuple[TaskOutcome, ...]

    @property
    def succeeded(self) -> int:
        return sum(item.status == "succeeded" for item in self.outcomes)

    @property
    def failed(self) -> int:
        return sum(item.status == "failed" for item in self.outcomes)

    @property
    def skipped(self) -> int:
        return sum(item.status == "skipped" for item in self.outcomes)


class DatasetBuilder:
    """High-level API for catalog, metadata, planning, local work, and Slurm work."""

    def __init__(
        self,
        config: BuilderConfig,
        *,
        fastq_provider: FastqProvider | None = None,
        genome_manager: GenomeManager | None = None,
    ) -> None:
        self.config = config
        config.workspace.mkdir(parents=True, exist_ok=True)
        if config.email:
            entrez = EntrezClient(
                email=config.email,
                api_key=config.ncbi_api_key,
                cache_dir=config.workspace / "metadata_cache",
            )
            self.sra: SraClient | None = SraClient(entrez)
            self.biosample: BioSampleClient | None = BioSampleClient(entrez)
            self.geo: GeoClient | None = GeoClient(entrez, self.sra)
        else:
            self.sra = None
            self.biosample = None
            self.geo = None
        self.fastq_provider = fastq_provider or SraToolkitProvider()
        self.genomes = genome_manager or GenomeManager(
            config.workspace / "genomes", policy=config.genome_policy
        )
        self.state = TaskStateStore(config.workspace / "state" / "tasks")

    def fetch_runs(self, query: str, *, refresh: bool = False) -> RunCatalog:
        if self.sra is None:
            raise ValueError("fetch_runs requires a contact email in BuilderConfig")
        digest = hashlib.sha256(query.encode("utf-8")).hexdigest()[:16]
        cache = self.config.workspace / "catalogs" / f"sra.{digest}.csv"
        if cache.is_file() and not refresh:
            return RunCatalog.from_csv(cache).deduplicate_runs()
        catalog = self.sra.fetch_runinfo(query)
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
        return catalog.replace_frame(catalog.frame, event=f"cached SRA query {query!r} at {cache}")

    @staticmethod
    def load_runs(path: str | Path) -> RunCatalog:
        return RunCatalog.from_csv(path).deduplicate_runs()

    def fetch_geo_runs(self, accessions: list[str]) -> RunCatalog:
        if self.geo is None:
            raise ValueError("fetch_geo_runs requires a contact email in BuilderConfig")
        return self.geo.resolve_to_sra(accessions)

    def fetch_metadata(
        self,
        accessions: list[str],
        *,
        destination: Path | None = None,
        include_raw: bool = False,
        description_profile: str = "training",
        description_policy: DescriptionPolicy | None = None,
        legacy_descriptions: dict[str, dict[str, Any]] | None = None,
    ) -> MetadataBundle:
        """Fetch complete SRA-package and linked BioSample metadata by accession."""

        if self.sra is None or self.biosample is None:
            raise ValueError("fetch_metadata requires a contact email in BuilderConfig")
        bundle = fetch_metadata_for_accessions(
            accessions,
            sra=self.sra,
            biosample=self.biosample,
            include_raw=include_raw,
        )
        bundle.save(
            destination or self.config.workspace / "metadata",
            description_profile=description_profile,
            policy=description_policy,
            legacy_descriptions=legacy_descriptions,
        )
        return bundle

    def enrich_metadata(
        self,
        catalog: RunCatalog,
        *,
        destination: Path | None = None,
        include_raw: bool = False,
        description_profile: str = "training",
        description_policy: DescriptionPolicy | None = None,
        legacy_descriptions: dict[str, dict[str, Any]] | None = None,
    ) -> MetadataBundle:
        if self.sra is None or self.biosample is None:
            raise ValueError("enrich_metadata requires a contact email in BuilderConfig")
        bundle = fetch_metadata_for_catalog(
            catalog,
            sra=self.sra,
            biosample=self.biosample,
            include_raw=include_raw,
        )
        bundle.save(
            destination or self.config.workspace / "metadata",
            description_profile=description_profile,
            policy=description_policy,
            legacy_descriptions=legacy_descriptions,
        )
        return bundle

    def plan(
        self,
        catalog: RunCatalog,
        *,
        group_by: GroupLevel = "experiment",
        resources: ResourceSpec | None = None,
        max_batch_bytes: int | None = None,
        max_batch_units: int | None = None,
        genome_pins: dict[int, str] | None = None,
        query: str | None = None,
    ) -> DatasetPlan:
        clean = catalog.deduplicate_runs()
        resources = resources or ResourceSpec()
        units = clean.processing_units(by=group_by)
        batches = RunCatalog.batch_units(
            units, max_bytes=max_batch_bytes, max_units=max_batch_units
        )
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
        return DatasetPlan(
            plan_id=str(uuid.uuid4()),
            created_at=utc_timestamp(),
            query=query,
            group_by=group_by,
            tasks=tuple(tasks),
            catalog_audit=clean.audit,
            metadata={"batch_count": len(batches)},
        )

    def save_plan(self, plan: DatasetPlan, path: Path | None = None) -> Path:
        target = path or self.config.workspace / "plans" / f"{plan.plan_id}.json"
        atomic_write_json(target, plan.to_dict())
        return target

    @staticmethod
    def load_plan(path: str | Path) -> DatasetPlan:
        return DatasetPlan.from_dict(read_json(Path(path)))

    @staticmethod
    def _processor_identity(
        processor: Processor | str,
        processor_id: str | None,
        resolved: Processor | None = None,
    ) -> str:
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

    def _execute_task(
        self,
        task: DatasetTask,
        processor: Processor,
        *,
        plan_id: str,
        retry_failed: bool,
    ) -> TaskOutcome:
        state_key = f"{plan_id}__{task.task_id}"
        try:
            claimed = self.state.start(state_key, retry_failed=retry_failed)
            if not claimed:
                previous = self.state.get(state_key) or {}
                return TaskOutcome(
                    task.task_id,
                    "skipped",
                    result=previous.get("result"),
                    error=previous.get("error"),
                )
            if task.unit.taxid is None:
                raise ValueError(
                    f"Task {task.task_id} has no species taxid; pin or add TaxID before planning"
                )
            genome = self.genomes.resolve(
                taxid=task.unit.taxid,
                scientific_name=task.unit.scientific_name,
                pin=task.genome_pin,
            )
            fastq = self.fastq_provider.fetch(
                task.unit,
                self.config.workspace / "fastq",
                threads=task.resources.threads,
            )
            task_root = self.config.workspace / "work" / sanitize_identifier(plan_id) / task.task_id
            fastq = replace(
                fastq,
                work_dir=task_root,
                output_dir=self.config.workspace
                / "results"
                / sanitize_identifier(plan_id)
                / task.task_id,
            )
            result = processor(fastq, genome, task.resources.threads)
            if not isinstance(result, ProcessingResult):
                raise TypeError(
                    f"Processor must return ProcessingResult, got {type(result).__name__} for {task.task_id}"
                )
            result.validate()
            payload = {
                "processing": result.to_dict(),
                "output_sha256": {str(path): sha256_file(path) for path in result.outputs},
                "genome": genome.to_dict(),
                "fastq": fastq.to_dict(),
            }
            self.state.succeed(state_key, payload)
            return TaskOutcome(task.task_id, "succeeded", result=payload)
        except TaskAlreadyRunning as exc:
            return TaskOutcome(task.task_id, "skipped", error=str(exc))
        except Exception:  # noqa: BLE001 - a task boundary must persist every operational failure
            error = traceback.format_exc()
            self.state.fail(state_key, error)
            return TaskOutcome(task.task_id, "failed", error=error)

    def build(
        self,
        plan: DatasetPlan,
        processor: Processor | str,
        *,
        retry_failed: bool = False,
        batch_ids: set[int] | None = None,
        processor_id: str | None = None,
    ) -> BuildReport:
        callable_processor = load_processor(processor) if isinstance(processor, str) else processor
        identity = self._processor_identity(processor, processor_id, callable_processor)
        self._register_processor(plan, identity)
        tasks = [task for task in plan.tasks if batch_ids is None or task.batch_id in batch_ids]
        if batch_ids is not None and not tasks:
            raise ValueError(f"No tasks belong to requested batches: {sorted(batch_ids)}")
        threads = max((task.resources.threads for task in tasks), default=1)
        executor: LocalExecutor[DatasetTask, TaskOutcome] = LocalExecutor(
            max_workers=self.config.max_workers,
            total_threads=self.config.total_threads,
        )
        outcomes = executor.map(
            tasks,
            lambda task: self._execute_task(
                task,
                callable_processor,
                plan_id=plan.plan_id,
                retry_failed=retry_failed,
            ),
            threads_per_task=threads,
        )
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
    ) -> tuple[Path, str | None]:
        saved_plan = self.save_plan(plan, plan_path)
        target_script = script_path or self.config.workspace / "slurm" / f"{plan.plan_id}.sbatch"
        executor = SlurmExecutor()
        (self.config.workspace / "logs" / "slurm").mkdir(parents=True, exist_ok=True)
        task_indices = [
            index
            for index, task in enumerate(plan.tasks)
            if batch_ids is None or task.batch_id in batch_ids
        ]
        if not task_indices:
            raise ValueError(f"No tasks belong to requested batches: {sorted(batch_ids or set())}")
        script = executor.create_script(
            plan_path=saved_plan,
            task_count=len(plan.tasks),
            processor_reference=processor_reference,
            workspace=self.config.workspace,
            email=self.config.email,
            output_path=target_script,
            options=options,
            retry_failed=retry_failed,
            task_indices=task_indices,
        )
        return script, executor.submit(script) if submit else None

    def status(self, plan: DatasetPlan, *, batch_ids: set[int] | None = None) -> dict[str, Any]:
        keys = [
            f"{plan.plan_id}__{task.task_id}"
            for task in plan.tasks
            if batch_ids is None or task.batch_id in batch_ids
        ]
        return self.state.summary(keys)
