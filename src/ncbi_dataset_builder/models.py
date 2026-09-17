"""Small public data models shared across package subsystems."""

from __future__ import annotations

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
    """Describe runs processed as one sample.

    Args:
        unit_id: Stable grouping identifier.
        run_accessions: Ordered SRA runs combined for this sample.
        experiment_accessions: Linked experiment accessions.
        sra_sample_accessions: Linked SRA Sample accessions.
        biosample_accessions: Linked BioSample accessions.
        scientific_name: Species name shared by the sample.
        taxid: Species taxonomy ID shared by the sample.
        total_bases: Estimated total sequenced bases.
        total_size_gb: Estimated download size in decimal GB.
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
    """Hold local FASTQ files and their acquisition provenance.

    Args:
        layout: Single, paired, or mixed read layout.
        run_accessions: Source SRA runs in merge order.
        read1: First-mate FASTQ paths.
        read2: Second-mate FASTQ paths matching :attr:`read1`.
        single: Single-end or orphan FASTQ paths.
        source: Provider label such as ``sra`` or ``geo``.
        checksums: SHA-256 values keyed by file path.
        provider_metadata: Provider-specific FASTQ provenance.
    """

    layout: FastqLayout
    run_accessions: tuple[str, ...]
    read1: tuple[Path, ...] = ()
    read2: tuple[Path, ...] = ()
    single: tuple[Path, ...] = ()
    source: str = "sra"
    checksums: dict[str, str] = field(default_factory=dict)
    provider_metadata: dict[str, Any] = field(default_factory=dict)

    def validate(self) -> None:
        """Validate layout-specific file counts and require non-empty FASTQs."""

        has_pair = bool(self.read1 or self.read2)
        if has_pair and (not self.read1 or len(self.read1) != len(self.read2)):
            raise ValueError("Paired FASTQ input requires matching read1/read2 files")
        if self.layout == FastqLayout.SINGLE and (not self.single or has_pair):
            raise ValueError("Single-end FASTQ input requires only single/orphan files")
        if self.layout == FastqLayout.PAIRED and (not self.read1 or self.single):
            raise ValueError("Paired FASTQ input requires only matching read1/read2 files")
        if self.layout == FastqLayout.MIXED and (not self.read1 or not self.single):
            raise ValueError("Mixed FASTQ input requires paired and single/orphan files")
        for path in (*self.read1, *self.read2, *self.single):
            if not path.is_file() or path.stat().st_size == 0:
                raise ValueError(f"FASTQ file is missing or empty: {path}")

    def to_dict(self) -> dict[str, Any]:
        """Serialize this FASTQ set, converting enums and paths to strings."""

        return {
            **asdict(self),
            "layout": self.layout.value,
            "read1": [str(path) for path in self.read1],
            "read2": [str(path) for path in self.read2],
            "single": [str(path) for path in self.single],
        }

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> FastqSet:
        """Restore a FASTQ set from serialized mapping *value*."""

        return cls(
            layout=FastqLayout(value["layout"]),
            run_accessions=tuple(value.get("run_accessions", ())),
            read1=tuple(Path(item) for item in value.get("read1", ())),
            read2=tuple(Path(item) for item in value.get("read2", ())),
            single=tuple(Path(item) for item in value.get("single", ())),
            source=value.get("source", "sra"),
            checksums=dict(value.get("checksums", {})),
            provider_metadata=dict(
                value.get("provider_metadata", value.get("metadata", {}))
            ),
        )


@dataclass(frozen=True)
class StagedFastq:
    """Describe downloaded input before FASTQ materialization.

    Args:
        unit_id: Processing-unit identifier.
        source: Provider label such as ``sra`` or ``geo``.
        size_gb: Measured staged-input size in decimal GB.
        cleanup_roots: Exact provider-owned roots eligible for cleanup.
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
        """Serialize this staged input and all paths."""

        return {
            "unit_id": self.unit_id,
            "source": self.source,
            "size_gb": self.size_gb,
            "cleanup_roots": [str(path) for path in self.cleanup_roots],
            "ready_fastq": self.ready_fastq.to_dict() if self.ready_fastq else None,
            "metadata": self.metadata,
        }

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> StagedFastq:
        """Restore staged FASTQ information from serialized *value*."""

        ready = value.get("ready_fastq")
        return cls(
            unit_id=str(value["unit_id"]),
            source=str(value["source"]),
            size_gb=float(value["size_gb"]),
            cleanup_roots=tuple(Path(path) for path in value.get("cleanup_roots", ())),
            ready_fastq=FastqSet.from_dict(ready) if isinstance(ready, dict) else None,
            metadata=dict(value.get("metadata", {})),
        )


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
        """Require a non-empty FASTA file."""

        if not self.fasta.is_file() or self.fasta.stat().st_size == 0:
            raise ValueError(f"Genome FASTA is missing or empty: {self.fasta}")

    def to_dict(self) -> dict[str, Any]:
        """Serialize this reference, converting paths to strings."""

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
class ProcessingContext:
    """Describe the pipeline-owned environment for one processor invocation.

    Args:
        unit_id: Stable processing-unit identifier.
        threads: Maximum CPUs assigned to the processor.
        output_dir: Processor-owned directory for all artifacts and intermediates.
        log_path: Permanent unit log path.
        execution_id: Execution snapshot that requested this invocation.
    """

    unit_id: str
    threads: int
    output_dir: Path
    log_path: Path
    execution_id: str

    def __post_init__(self) -> None:
        """Validate required identity and resource values."""

        if not self.unit_id:
            raise ValueError("Processing context requires a unit ID")
        if self.threads < 1:
            raise ValueError("Processing context threads must be positive")
        if not self.execution_id:
            raise ValueError("Processing context requires an execution ID")

    def to_dict(self) -> dict[str, Any]:
        """Serialize this processing context, converting paths to strings."""

        return {
            **asdict(self),
            "output_dir": str(self.output_dir),
            "log_path": str(self.log_path),
        }


@dataclass(frozen=True)
class ProcessingResult:
    """Report the result of one processor call.

    Args:
        success: Whether processing completed successfully.
        outputs: Declared final output paths keyed by stable artifact role.
        metrics: Processor-defined quality or summary metrics.
        tool_versions: External tool versions used.
        message: Optional status or failure explanation.
    """

    success: bool
    outputs: dict[str, Path] = field(default_factory=dict)
    metrics: dict[str, Any] = field(default_factory=dict)
    tool_versions: dict[str, str] = field(default_factory=dict)
    message: str | None = None

    def resolved_outputs(self, output_dir: Path) -> dict[str, Path]:
        """Resolve declared paths below the processor-owned *output_dir*."""

        root = output_dir.resolve()
        resolved: dict[str, Path] = {}
        for role, declared in self.outputs.items():
            if not isinstance(declared, Path):
                raise TypeError(f"Processor output {role!r} must be a pathlib.Path")
            path = declared if declared.is_absolute() else output_dir / declared
            path = path.resolve()
            if path == root or not path.is_relative_to(root):
                raise ValueError(
                    f"Processor output {role!r} is outside its output directory: {path}"
                )
            resolved[role] = path
        return resolved

    def validate(self, *, output_dir: Path | None = None) -> None:
        """Require outputs to be valid and, when given, inside *output_dir*."""

        if not self.success:
            raise ValueError(self.message or "Processor reported failure")
        if not self.outputs:
            raise ValueError("Processor reported success without outputs")
        if any(not isinstance(role, str) or not role.strip() for role in self.outputs):
            raise ValueError("Processor output roles must be non-empty strings")
        resolved = (
            self.resolved_outputs(output_dir)
            if output_dir is not None
            else dict(self.outputs)
        )
        paths = list(resolved.values())
        if len({str(path) for path in paths}) != len(paths):
            raise ValueError("Processor output paths must be unique")
        for role, path in resolved.items():
            if not isinstance(path, Path):
                raise TypeError(f"Processor output {role!r} must be a pathlib.Path")
            if not path.is_file() or path.stat().st_size == 0:
                raise ValueError(f"Processor output {role!r} is missing or empty: {path}")

    def to_dict(self) -> dict[str, Any]:
        """Serialize this processing result, converting paths to strings."""

        return {
            **asdict(self),
            "outputs": {role: str(path) for role, path in self.outputs.items()},
        }
