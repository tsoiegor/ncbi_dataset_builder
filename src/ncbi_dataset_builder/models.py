from __future__ import annotations

import re
from dataclasses import asdict, dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any


class FastqLayout(str, Enum):
    """Describe whether a FASTQ set contains single, paired, or mixed reads."""

    SINGLE = "single"
    PAIRED = "paired"
    MIXED = "mixed"


@dataclass(frozen=True)
class ProcessingUnit:
    """Describe runs processed together.

    Args:
        unit_id: Stable grouping identifier.
        run_accessions: Ordered SRA runs to combine.
        experiment_accessions: Linked experiment accessions.
        sra_sample_accessions: Linked SRA Sample accessions.
        biosample_accessions: Linked BioSample accessions.
        scientific_name: Species name shared by the unit.
        taxid: Species taxonomy ID shared by the unit.
        total_bases: Estimated total sequenced bases.
        total_size_gb: Estimated download size in decimal gigabytes.
        metadata: Additional grouping metadata.
    """

    unit_id: str
    run_accessions: tuple[str, ...]
    experiment_accessions: tuple[str, ...] = ()
    sra_sample_accessions: tuple[str, ...] = ()
    biosample_accessions: tuple[str, ...] = ()
    scientific_name: str | None = None
    taxid: int | None = None
    total_bases: int = 0
    total_size_gb: float = 0.0
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Serialize this processing unit to JSON-compatible values."""

        return asdict(self)

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> ProcessingUnit:
        """Restore a processing unit from serialized mapping *value*."""

        copied = dict(value)
        legacy_bytes = copied.pop("total_bytes", None)
        if "total_size_gb" not in copied and legacy_bytes is not None:
            copied["total_size_gb"] = float(legacy_bytes) / 1_000_000_000
        for key in (
            "run_accessions",
            "experiment_accessions",
            "sra_sample_accessions",
            "biosample_accessions",
        ):
            copied[key] = tuple(copied.get(key, ()))
        return cls(**copied)


@dataclass(frozen=True)
class FastqSet:
    """Hold local FASTQ files and provenance for one processing unit.

    Args:
        unit_id: Processing-unit identifier.
        layout: Single, paired, or mixed read layout.
        run_accessions: Source SRA runs in merge order.
        read1: First-mate FASTQ paths.
        read2: Second-mate FASTQ paths matching ``read1``.
        single: Single-end or orphan FASTQ paths.
        source: Provider label such as ``sra`` or ``geo``.
        work_dir: Directory for processor intermediates.
        output_dir: Directory for final processor outputs.
        checksums: SHA-256 values keyed by file path.
        metadata: Provider-specific provenance.
    """

    unit_id: str
    layout: FastqLayout
    run_accessions: tuple[str, ...]
    read1: tuple[Path, ...] = ()
    read2: tuple[Path, ...] = ()
    single: tuple[Path, ...] = ()
    source: str = "sra"
    work_dir: Path = Path(".")
    output_dir: Path = Path(".")
    checksums: dict[str, str] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)

    def validate(self) -> None:
        """Validate layout-specific file counts and require non-empty FASTQ files."""

        if self.layout in {FastqLayout.PAIRED, FastqLayout.MIXED} and (
            not self.read1 or len(self.read1) != len(self.read2)
        ):
            raise ValueError("Paired FASTQ input requires matching read1/read2 files")
        if self.layout == FastqLayout.SINGLE and not self.single:
            raise ValueError("Single-end FASTQ input has no files")
        if self.layout == FastqLayout.MIXED and not self.single:
            raise ValueError("Mixed FASTQ input has no single/orphan files")
        for path in (*self.read1, *self.read2, *self.single):
            if not path.is_file() or path.stat().st_size == 0:
                raise ValueError(f"FASTQ file is missing or empty: {path}")

    def to_dict(self) -> dict[str, Any]:
        """Serialize the FASTQ set, converting enums and paths to strings."""

        return {
            **asdict(self),
            "layout": self.layout.value,
            "read1": [str(path) for path in self.read1],
            "read2": [str(path) for path in self.read2],
            "single": [str(path) for path in self.single],
            "work_dir": str(self.work_dir),
            "output_dir": str(self.output_dir),
        }

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> FastqSet:
        """Restore a FASTQ set from serialized mapping *value*."""

        return cls(
            unit_id=value["unit_id"],
            layout=FastqLayout(value["layout"]),
            run_accessions=tuple(value.get("run_accessions", ())),
            read1=tuple(Path(item) for item in value.get("read1", ())),
            read2=tuple(Path(item) for item in value.get("read2", ())),
            single=tuple(Path(item) for item in value.get("single", ())),
            source=value.get("source", "sra"),
            work_dir=Path(value.get("work_dir", ".")),
            output_dir=Path(value.get("output_dir", ".")),
            checksums=dict(value.get("checksums", {})),
            metadata=dict(value.get("metadata", {})),
        )


@dataclass(frozen=True)
class StagedFastq:
    """Describe downloaded inputs before CPU-heavy FASTQ materialization.

    Args:
        unit_id: Processing-unit identifier.
        source: Provider label such as ``sra`` or ``geo``.
        size_gb: Measured staged-input size in decimal gigabytes.
        cleanup_roots: Exact provider-owned roots eligible for post-success cleanup.
        ready_fastq: Optional already-materialized FASTQ set.
        metadata: Provider-specific staging information.
    """

    unit_id: str
    source: str
    size_gb: float
    cleanup_roots: tuple[Path, ...] = ()
    ready_fastq: FastqSet | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Serialize this staged input and all paths to JSON-compatible values."""

        return {
            "unit_id": self.unit_id,
            "source": self.source,
            "size_gb": self.size_gb,
            "cleanup_roots": [str(path) for path in self.cleanup_roots],
            "ready_fastq": self.ready_fastq.to_dict() if self.ready_fastq else None,
            "metadata": self.metadata,
        }


@dataclass(frozen=True)
class GenomeRef:
    """Describe one selected genome reference.

    Args:
        taxid: Species taxonomy ID.
        scientific_name: Species name.
        accession: Versioned assembly or custom reference ID.
        fasta: Local FASTA path.
        sha256: FASTA checksum.
        source_database: Reference source such as NCBI or custom.
        assembly_level: Reported assembly level.
        refseq_category: Reported RefSeq category.
        selection_rationale: Human-readable reasons for selection.
        indexes: Optional named index paths.
    """

    taxid: int
    scientific_name: str
    accession: str
    fasta: Path
    sha256: str
    source_database: str = "NCBI"
    assembly_level: str | None = None
    refseq_category: str | None = None
    selection_rationale: tuple[str, ...] = ()
    indexes: dict[str, Path] = field(default_factory=dict)

    def validate(self) -> None:
        """Require this genome reference to point to a non-empty FASTA file."""

        if not self.fasta.is_file() or self.fasta.stat().st_size == 0:
            raise ValueError(f"Genome FASTA is missing or empty: {self.fasta}")

    def to_dict(self) -> dict[str, Any]:
        """Serialize the genome reference, converting all paths to strings."""

        return {
            **asdict(self),
            "fasta": str(self.fasta),
            "selection_rationale": list(self.selection_rationale),
            "indexes": {key: str(path) for key, path in self.indexes.items()},
        }

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> GenomeRef:
        """Restore a genome reference from serialized mapping *value*."""

        copied = dict(value)
        copied["fasta"] = Path(copied["fasta"])
        copied["selection_rationale"] = tuple(copied.get("selection_rationale", ()))
        copied["indexes"] = {key: Path(path) for key, path in copied.get("indexes", {}).items()}
        return cls(**copied)


@dataclass(frozen=True)
class ProcessingResult:
    """Report the result of one assay processor call.

    Args:
        success: Whether processing completed successfully.
        outputs: Declared final output paths.
        metrics: Processor-defined quality or summary metrics.
        tool_versions: External tool versions used.
        message: Optional status or failure explanation.
    """

    success: bool
    outputs: tuple[Path, ...] = ()
    metrics: dict[str, Any] = field(default_factory=dict)
    tool_versions: dict[str, str] = field(default_factory=dict)
    message: str | None = None

    def validate(self) -> None:
        """Require success and at least one declared, non-empty output file."""

        if not self.success:
            raise ValueError(self.message or "Processor reported failure")
        if not self.outputs:
            raise ValueError("Processor reported success without outputs")
        for path in self.outputs:
            if not path.is_file() or path.stat().st_size == 0:
                raise ValueError(f"Processor output is missing or empty: {path}")

    def to_dict(self) -> dict[str, Any]:
        """Serialize the processing result, converting output paths to strings."""

        return {**asdict(self), "outputs": [str(path) for path in self.outputs]}


@dataclass(frozen=True)
class ResourceSpec:
    """Specify task resources.

    Args:
        threads: CPU threads available to the task.
        memory_gb: Requested memory in gigabytes.
        time_limit: Slurm-compatible wall-time limit.
    """

    threads: int = 4
    memory_gb: int = 16
    time_limit: str = "24:00:00"

    def __post_init__(self) -> None:
        """Validate positive resource values and a shell-safe time string."""

        if self.threads < 1 or self.memory_gb < 1:
            raise ValueError("threads and memory_gb must be positive")
        if not re.fullmatch(r"[0-9:-]+", self.time_limit):
            raise ValueError(f"Unsafe or invalid Slurm time limit: {self.time_limit!r}")


@dataclass(frozen=True)
class DatasetTask:
    """Bind one processing unit to execution settings.

    Args:
        task_id: Stable task identifier.
        unit: Processing unit handled by this task.
        batch_id: Deterministic batch assignment.
        resources: CPU, memory, and time settings.
        genome_pin: Optional exact assembly accession.
    """

    task_id: str
    unit: ProcessingUnit
    batch_id: int
    resources: ResourceSpec
    genome_pin: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Serialize this task and its nested unit and resources."""

        return {
            "task_id": self.task_id,
            "unit": self.unit.to_dict(),
            "batch_id": self.batch_id,
            "resources": asdict(self.resources),
            "genome_pin": self.genome_pin,
        }

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> DatasetTask:
        """Restore a dataset task from serialized mapping *value*."""

        return cls(
            task_id=value["task_id"],
            unit=ProcessingUnit.from_dict(value["unit"]),
            batch_id=int(value["batch_id"]),
            resources=ResourceSpec(**value["resources"]),
            genome_pin=value.get("genome_pin"),
        )


@dataclass(frozen=True)
class DatasetPlan:
    """Store a reproducible collection of dataset tasks.

    Args:
        plan_id: Unique plan identifier.
        created_at: UTC creation timestamp.
        query: Optional source NCBI query.
        group_by: Entity level used to form processing units.
        tasks: Ordered immutable tasks.
        catalog_audit: Catalog operations preceding the plan.
        metadata: Additional plan-level provenance.
    """

    plan_id: str
    created_at: str
    query: str | None
    group_by: str
    tasks: tuple[DatasetTask, ...]
    catalog_audit: tuple[str, ...] = ()
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Serialize the plan and all nested tasks to JSON-compatible values."""

        return {
            "plan_id": self.plan_id,
            "created_at": self.created_at,
            "query": self.query,
            "group_by": self.group_by,
            "tasks": [task.to_dict() for task in self.tasks],
            "catalog_audit": list(self.catalog_audit),
            "metadata": self.metadata,
        }

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> DatasetPlan:
        """Restore a complete dataset plan from serialized mapping *value*."""

        return cls(
            plan_id=value["plan_id"],
            created_at=value["created_at"],
            query=value.get("query"),
            group_by=value["group_by"],
            tasks=tuple(DatasetTask.from_dict(item) for item in value["tasks"]),
            catalog_audit=tuple(value.get("catalog_audit", ())),
            metadata=value.get("metadata", {}),
        )
