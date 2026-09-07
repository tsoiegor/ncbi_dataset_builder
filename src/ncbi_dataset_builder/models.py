from __future__ import annotations

import re
from dataclasses import asdict, dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any


class FastqLayout(str, Enum):
    SINGLE = "single"
    PAIRED = "paired"
    MIXED = "mixed"


@dataclass(frozen=True)
class ProcessingUnit:
    unit_id: str
    run_accessions: tuple[str, ...]
    experiment_accessions: tuple[str, ...] = ()
    sra_sample_accessions: tuple[str, ...] = ()
    biosample_accessions: tuple[str, ...] = ()
    scientific_name: str | None = None
    taxid: int | None = None
    total_bases: int = 0
    total_bytes: int = 0
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> ProcessingUnit:
        copied = dict(value)
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
        return {
            **asdict(self),
            "layout": self.layout.value,
            "read1": [str(path) for path in self.read1],
            "read2": [str(path) for path in self.read2],
            "single": [str(path) for path in self.single],
            "work_dir": str(self.work_dir),
            "output_dir": str(self.output_dir),
        }


@dataclass(frozen=True)
class GenomeRef:
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
        if not self.fasta.is_file() or self.fasta.stat().st_size == 0:
            raise ValueError(f"Genome FASTA is missing or empty: {self.fasta}")

    def to_dict(self) -> dict[str, Any]:
        return {
            **asdict(self),
            "fasta": str(self.fasta),
            "selection_rationale": list(self.selection_rationale),
            "indexes": {key: str(path) for key, path in self.indexes.items()},
        }

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> GenomeRef:
        copied = dict(value)
        copied["fasta"] = Path(copied["fasta"])
        copied["selection_rationale"] = tuple(copied.get("selection_rationale", ()))
        copied["indexes"] = {key: Path(path) for key, path in copied.get("indexes", {}).items()}
        return cls(**copied)


@dataclass(frozen=True)
class ProcessingResult:
    success: bool
    outputs: tuple[Path, ...] = ()
    metrics: dict[str, Any] = field(default_factory=dict)
    tool_versions: dict[str, str] = field(default_factory=dict)
    message: str | None = None

    def validate(self) -> None:
        if not self.success:
            raise ValueError(self.message or "Processor reported failure")
        if not self.outputs:
            raise ValueError("Processor reported success without outputs")
        for path in self.outputs:
            if not path.is_file() or path.stat().st_size == 0:
                raise ValueError(f"Processor output is missing or empty: {path}")

    def to_dict(self) -> dict[str, Any]:
        return {**asdict(self), "outputs": [str(path) for path in self.outputs]}


@dataclass(frozen=True)
class ResourceSpec:
    threads: int = 4
    memory_gb: int = 16
    time_limit: str = "24:00:00"

    def __post_init__(self) -> None:
        if self.threads < 1 or self.memory_gb < 1:
            raise ValueError("threads and memory_gb must be positive")
        if not re.fullmatch(r"[0-9:-]+", self.time_limit):
            raise ValueError(f"Unsafe or invalid Slurm time limit: {self.time_limit!r}")


@dataclass(frozen=True)
class DatasetTask:
    task_id: str
    unit: ProcessingUnit
    batch_id: int
    resources: ResourceSpec
    genome_pin: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "task_id": self.task_id,
            "unit": self.unit.to_dict(),
            "batch_id": self.batch_id,
            "resources": asdict(self.resources),
            "genome_pin": self.genome_pin,
        }

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> DatasetTask:
        return cls(
            task_id=value["task_id"],
            unit=ProcessingUnit.from_dict(value["unit"]),
            batch_id=int(value["batch_id"]),
            resources=ResourceSpec(**value["resources"]),
            genome_pin=value.get("genome_pin"),
        )


@dataclass(frozen=True)
class DatasetPlan:
    plan_id: str
    created_at: str
    query: str | None
    group_by: str
    tasks: tuple[DatasetTask, ...]
    catalog_audit: tuple[str, ...] = ()
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
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
        return cls(
            plan_id=value["plan_id"],
            created_at=value["created_at"],
            query=value.get("query"),
            group_by=value["group_by"],
            tasks=tuple(DatasetTask.from_dict(item) for item in value["tasks"]),
            catalog_audit=tuple(value.get("catalog_audit", ())),
            metadata=value.get("metadata", {}),
        )
