from __future__ import annotations

import gzip
import json
import logging
import os
import shutil
import threading
import zipfile
from collections.abc import Iterable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, ClassVar

from .commands import CommandRunner
from .errors import DownloadError, GenomeSelectionError
from .models import GenomeRef
from .progress import ProgressReporter, get_progress
from .util import (
    atomic_write_json,
    bytes_to_gb,
    exclusive_file_lock,
    existing_nonempty,
    read_json,
    sha256_file,
)

LOGGER = logging.getLogger(__name__)


def _nested(value: dict[str, Any], *paths: str, default=None):
    """Return the first non-empty dotted *paths* found in *value*, else *default*."""

    for path in paths:
        current: Any = value
        for part in path.split("."):
            if not isinstance(current, dict) or part not in current:
                current = None
                break
            current = current[part]
        if current not in (None, ""):
            return current
    return default


def _integer(value: Any) -> int | None:
    """Convert *value* to an integer, returning ``None`` when invalid."""

    try:
        return int(value)
    except (TypeError, ValueError):
        return None


@dataclass(frozen=True)
class GenomeCandidate:
    """Represent one assembly returned by NCBI Datasets.

    Args:
        accession: Versioned assembly accession.
        taxid: Assembly organism taxonomy ID.
        scientific_name: Assembly organism name.
        source_database: RefSeq, GenBank, or another reported source.
        assembly_status: Current, replaced, suppressed, or related status.
        refseq_category: Reference or representative-genome category.
        assembly_level: Contig, scaffold, chromosome, or complete genome.
        release_date: Date used as a late ranking tie-breaker.
        contig_n50: Reported contig N50.
        scaffold_n50: Reported scaffold N50.
        total_length: Reported assembly sequence length.
        atypical: Whether NCBI marks the assembly atypical.
        warnings: Status or quality warnings.
        raw: Original NCBI report for provenance.
    """

    accession: str
    taxid: int | None
    scientific_name: str | None
    source_database: str | None
    assembly_status: str | None
    refseq_category: str | None
    assembly_level: str | None
    release_date: str | None
    contig_n50: int | None
    scaffold_n50: int | None
    total_length: int | None
    atypical: bool = False
    warnings: tuple[str, ...] = ()
    raw: dict[str, Any] = field(default_factory=dict, compare=False)

    @classmethod
    def from_report(cls, report: dict[str, Any]) -> GenomeCandidate:
        """Normalize one NCBI Datasets assembly *report* into a candidate."""

        warnings_value = _nested(
            report,
            "assembly_info.assembly_status_notes",
            "assembly_info.warnings",
            "warnings",
            default=[],
        )
        if isinstance(warnings_value, str):
            warnings = (warnings_value,)
        elif isinstance(warnings_value, list):
            warnings = tuple(str(item) for item in warnings_value)
        else:
            warnings = ()
        atypical = bool(
            _nested(
                report,
                "assembly_info.is_atypical",
                "assembly_info.atypical",
                "is_atypical",
                default=False,
            )
        ) or any("atypical" in value.lower() for value in warnings)
        return cls(
            accession=str(_nested(report, "accession", "current_accession", default="")),
            taxid=_integer(_nested(report, "organism.tax_id", "organism.taxid", "tax_id")),
            scientific_name=_nested(report, "organism.organism_name", "organism_name"),
            source_database=_nested(report, "source_database", "assembly_info.source_database"),
            assembly_status=_nested(report, "assembly_info.assembly_status", "assembly_status"),
            refseq_category=_nested(report, "assembly_info.refseq_category", "refseq_category"),
            assembly_level=_nested(report, "assembly_info.assembly_level", "assembly_level"),
            release_date=_nested(report, "assembly_info.release_date", "release_date"),
            contig_n50=_integer(_nested(report, "assembly_stats.contig_n50", "contig_n50")),
            scaffold_n50=_integer(_nested(report, "assembly_stats.scaffold_n50", "scaffold_n50")),
            total_length=_integer(
                _nested(
                    report,
                    "assembly_stats.total_sequence_length",
                    "total_sequence_length",
                    "total_length",
                )
            ),
            atypical=atypical,
            warnings=warnings,
            raw=report,
        )


@dataclass(frozen=True)
class GenomeSelectionPolicy:
    """Filter and rank assemblies with deterministic tie-breakers.

    Args:
        allow_atypical: Permit assemblies marked atypical.
        minimum_assembly_level: Optional minimum accepted assembly level.
        prefer_reference: Rank reference/representative genomes first.
        prefer_refseq: Rank RefSeq assemblies before otherwise equal candidates.
    """

    allow_atypical: bool = False
    minimum_assembly_level: str | None = None
    prefer_reference: bool = True
    prefer_refseq: bool = True

    LEVELS: ClassVar[dict[str, int]] = {
        "contig": 1,
        "scaffold": 2,
        "chromosome": 3,
        "complete genome": 4,
    }

    def _allowed(self, candidate: GenomeCandidate, taxid: int) -> bool:
        """Return whether *candidate* is acceptable for requested *taxid*."""

        if not candidate.accession:
            return False
        if candidate.taxid is not None and candidate.taxid != taxid:
            return False
        status = (candidate.assembly_status or "current").lower()
        if any(word in status for word in ("suppressed", "replaced", "withdrawn", "anomalous")):
            return False
        if candidate.atypical and not self.allow_atypical:
            return False
        if self.minimum_assembly_level:
            current = self.LEVELS.get((candidate.assembly_level or "").lower(), 0)
            minimum = self.LEVELS.get(self.minimum_assembly_level.lower())
            if minimum is None:
                raise ValueError(f"Unknown assembly level: {self.minimum_assembly_level}")
            if current < minimum:
                return False
        return True

    def _rank(self, candidate: GenomeCandidate) -> tuple[Any, ...]:
        """Return the deterministic quality-ranking tuple for *candidate*."""

        category = (candidate.refseq_category or "").lower()
        reference_score = 2 if "reference" in category else 1 if "representative" in category else 0
        refseq_score = 1 if "refseq" in (candidate.source_database or "").lower() else 0
        level = self.LEVELS.get((candidate.assembly_level or "").lower(), 0)
        n50 = candidate.scaffold_n50 or candidate.contig_n50 or 0
        return (
            reference_score if self.prefer_reference else 0,
            refseq_score if self.prefer_refseq else 0,
            level,
            n50,
            candidate.total_length or 0,
            candidate.release_date or "",
            candidate.accession,
        )

    def select(
        self,
        candidates: Iterable[GenomeCandidate],
        *,
        taxid: int,
        pin: str | None = None,
    ) -> GenomeCandidate:
        """Select the best of *candidates* for *taxid*, or require exact *pin*."""

        materialized = list(candidates)
        if pin:
            matched = [item for item in materialized if item.accession == pin]
            if not matched:
                raise GenomeSelectionError(
                    f"Pinned genome {pin} was not returned for taxid {taxid}"
                )
            if matched[0].taxid is not None and matched[0].taxid != taxid:
                raise GenomeSelectionError(
                    f"Pinned genome {pin} belongs to taxid {matched[0].taxid}, not {taxid}"
                )
            return matched[0]
        allowed = [item for item in materialized if self._allowed(item, taxid)]
        if not allowed:
            raise GenomeSelectionError(
                f"No current genome assembly satisfies the policy for taxid {taxid}"
            )
        return max(allowed, key=self._rank)

    def rationale(self, candidate: GenomeCandidate) -> tuple[str, ...]:
        """Describe the ranking properties of selected *candidate*."""

        return (
            f"exact species taxid: {candidate.taxid}",
            f"assembly status: {candidate.assembly_status or 'current/unspecified'}",
            f"RefSeq category: {candidate.refseq_category or 'none'}",
            f"source database: {candidate.source_database or 'unspecified'}",
            f"assembly level: {candidate.assembly_level or 'unspecified'}",
            f"scaffold/contig N50: {candidate.scaffold_n50 or candidate.contig_n50 or 'unknown'}",
            f"release date tie-breaker: {candidate.release_date or 'unknown'}",
        )


def _report_dicts(value: Any) -> Iterable[dict[str, Any]]:
    """Yield assembly report dictionaries recursively from JSON-like *value*."""

    if isinstance(value, dict):
        if value.get("accession") and (value.get("organism") or value.get("assembly_info")):
            yield value
            return
        for child in value.values():
            yield from _report_dicts(child)
    elif isinstance(value, list):
        for child in value:
            yield from _report_dicts(child)


class GenomeManager:
    """Discover, select, download, checksum, and cache genomes.

    Args:
        root: Directory for genomes and their lockfile.
        runner: Optional external-command implementation.
        policy: Optional assembly selection policy.
        progress: Optional progress and logging reporter.
    """

    def __init__(
        self,
        root: Path,
        *,
        runner: CommandRunner | None = None,
        policy: GenomeSelectionPolicy | None = None,
        progress: ProgressReporter | None = None,
    ) -> None:
        """Initialize *root* with optional *runner*, *policy*, and *progress*."""

        self.root = root
        self.runner = runner or CommandRunner()
        self.policy = policy or GenomeSelectionPolicy()
        self.progress = get_progress(progress)
        self.lockfile = root / "genomes.lock.json"
        self._resolved: dict[int, GenomeRef] = {}
        self._resolved_lock = threading.Lock()

    def preflight(self) -> dict[str, str]:
        """Require the NCBI Datasets CLI and return its version."""

        self.runner.require("datasets")
        return {"datasets": self.runner.version("datasets", "version")}

    def candidates(self, taxid: int) -> list[GenomeCandidate]:
        """Return unique NCBI assembly candidates reported for *taxid*."""

        self.runner.require("datasets")
        self.progress.message(f"Discover NCBI genome candidates for taxid {taxid}")
        completed = self.runner.run(
            ["datasets", "summary", "genome", "taxon", str(taxid), "--as-json-lines"]
        )
        text = completed.stdout or ""
        parsed_values: list[Any] = []
        try:
            parsed_values.append(json.loads(text))
        except json.JSONDecodeError:
            for line in text.splitlines():
                if line.strip():
                    parsed_values.append(json.loads(line))
        reports: list[dict[str, Any]] = []
        for value in parsed_values:
            reports.extend(_report_dicts(value))
        candidates = [GenomeCandidate.from_report(report) for report in reports]
        unique: dict[str, GenomeCandidate] = {
            item.accession: item for item in candidates if item.accession
        }
        if not unique:
            raise GenomeSelectionError(
                f"NCBI Datasets returned no genome reports for taxid {taxid}"
            )
        self.progress.message(
            f"NCBI returned {len(unique):,} unique genome candidates for taxid {taxid}"
        )
        return list(unique.values())

    def _load_lockfile(self) -> dict[str, Any]:
        """Read the genome lockfile or return an empty versioned structure."""

        if self.lockfile.is_file():
            return read_json(self.lockfile)
        return {"schema_version": 1, "genomes": {}}

    def _memory_cached(self, taxid: int, pin: str | None) -> GenomeRef | None:
        """Return a process-local reference for *taxid* matching optional *pin*."""

        with self._resolved_lock:
            reference = self._resolved.get(taxid)
        if reference is not None and (pin is None or reference.accession == pin):
            return reference
        return None

    def _remember(self, reference: GenomeRef) -> GenomeRef:
        """Store and return *reference* for process-local reuse."""

        with self._resolved_lock:
            self._resolved[reference.taxid] = reference
        return reference

    def _cached(self, taxid: int, pin: str | None) -> GenomeRef | None:
        """Return a checksum-valid cached genome for *taxid* and optional *pin*."""

        remembered = self._memory_cached(taxid, pin)
        if remembered is not None:
            return remembered
        data = self._load_lockfile().get("genomes", {}).get(str(taxid))
        if not data or (pin and data.get("accession") != pin):
            return None
        reference = GenomeRef.from_dict(data)
        try:
            reference.validate()
        except ValueError:
            return None
        if sha256_file(reference.fasta, progress=self.progress) != reference.sha256:
            return None
        return self._remember(reference)

    def cache_inventory(
        self,
        requirements: Iterable[tuple[int, str | None]],
        *,
        description: str = "Genome references",
    ) -> dict[tuple[int, str | None], GenomeRef]:
        """Report cache state for unique *requirements* under *description*.

        Each requirement is a ``(taxid, pin)`` pair. The returned mapping
        contains only checksum-valid cached references and seeds process-local
        reuse so later task resolution does not revalidate the same FASTA.
        """

        unique = list(dict.fromkeys((int(taxid), pin) for taxid, pin in requirements))
        cached: dict[tuple[int, str | None], GenomeRef] = {}
        for taxid, pin in unique:
            reference = self._cached(taxid, pin)
            if reference is not None:
                cached[(taxid, pin)] = reference
        self.progress.cache_summary(
            description,
            cached=len(cached),
            missing=len(unique) - len(cached),
            unit="genomes",
        )
        return cached

    def resolve(
        self,
        *,
        taxid: int,
        scientific_name: str | None = None,
        pin: str | None = None,
    ) -> GenomeRef:
        """Resolve a cached or selected genome for one species.

        *taxid* identifies the species, *scientific_name* supplies a display
        label when needed, and *pin* requires an exact assembly accession.
        """

        cached = self._cached(taxid, pin)
        if cached is not None:
            LOGGER.debug("Reuse genome %s for taxid %s", cached.accession, taxid)
            return cached
        self.root.mkdir(parents=True, exist_ok=True)
        with exclusive_file_lock(self.root / f".{taxid}.resolve.lock", timeout_seconds=3600):
            cached = self._cached(taxid, pin)
            if cached is not None:
                LOGGER.info(
                    "Genome %s for taxid %s became available while waiting for the lock",
                    cached.accession,
                    taxid,
                )
                return cached
            self.progress.message(
                f"Genome cache miss for taxid {taxid}; this worker will prepare it"
            )
            candidate = self.policy.select(self.candidates(taxid), taxid=taxid, pin=pin)
            self.progress.message(
                f"Selected genome {candidate.accession} for taxid {taxid}; downloading if needed"
            )
            reference = self._download(candidate, taxid=taxid, scientific_name=scientific_name)
            with exclusive_file_lock(self.root / ".lockfile.lock"):
                lock = self._load_lockfile()
                lock.setdefault("genomes", {})[str(taxid)] = reference.to_dict()
                atomic_write_json(self.lockfile, lock)
            return self._remember(reference)

    def _download(
        self, candidate: GenomeCandidate, *, taxid: int, scientific_name: str | None
    ) -> GenomeRef:
        """Download and extract *candidate* for *taxid* into a checked genome reference.

        *scientific_name* overrides the assembly report's species label when supplied.
        """

        self.root.mkdir(parents=True, exist_ok=True)
        download_dir = self.root / ".downloads" / candidate.accession
        download_dir.mkdir(parents=True, exist_ok=True)
        archive = download_dir / f"{candidate.accession}.zip"
        fasta = self.root / f"{candidate.accession}.fasta.gz"
        if existing_nonempty(fasta):
            try:
                self._validate_downloaded_fasta(fasta)
            except DownloadError:
                self.progress.message(f"Discard invalid cached genome FASTA: {fasta}")
                fasta.unlink(missing_ok=True)
        if not existing_nonempty(fasta):
            if not zipfile.is_zipfile(archive):
                self.progress.message(f"Download genome archive {candidate.accession}")
                partial_archive = archive.with_name(archive.name + ".part")
                partial_archive.unlink(missing_ok=True)
                self.runner.run(
                    [
                        "datasets",
                        "download",
                        "genome",
                        "accession",
                        candidate.accession,
                        "--include",
                        "genome",
                        "--filename",
                        str(partial_archive),
                        "--no-progressbar",
                    ],
                    timeout=6 * 60 * 60,
                )
                if not zipfile.is_zipfile(partial_archive):
                    raise DownloadError(
                        f"NCBI Datasets did not create a valid ZIP: {partial_archive}"
                    )
                os.replace(partial_archive, archive)
            else:
                self.progress.message(f"Genome archive cache hit: {archive}")
            self.progress.message(f"Extract and compress genome FASTA {candidate.accession}")
            with zipfile.ZipFile(archive) as bundle:
                members = [
                    item
                    for item in bundle.infolist()
                    if item.filename.endswith("_genomic.fna") and item.file_size > 0
                ]
                if not members:
                    members = [
                        item
                        for item in bundle.infolist()
                        if item.filename.endswith(".fna") and item.file_size > 0
                    ]
                if not members:
                    raise DownloadError(f"Genome archive has no genomic FASTA: {archive}")
                member = max(members, key=lambda item: item.file_size)
                partial_fasta = fasta.with_name(fasta.name + ".part")
                with (
                    bundle.open(member) as source,
                    gzip.open(partial_fasta, "wb", compresslevel=6) as output,
                    self.progress.task(
                        f"Extract {candidate.accession}",
                        total=bytes_to_gb(member.file_size),
                        unit="GB",
                    ) as progress,
                ):
                    while chunk := source.read(8 * 1024 * 1024):
                        output.write(chunk)
                        progress.update(bytes_to_gb(len(chunk)))
                os.replace(partial_fasta, fasta)
        self._validate_downloaded_fasta(fasta)
        if download_dir.is_dir():
            shutil.rmtree(download_dir)
            downloads_root = download_dir.parent
            try:
                downloads_root.rmdir()
            except OSError:
                pass
        self.progress.message(f"Checksum genome FASTA {fasta}")
        return GenomeRef(
            taxid=taxid,
            scientific_name=scientific_name or candidate.scientific_name or str(taxid),
            accession=candidate.accession,
            fasta=fasta,
            sha256=sha256_file(fasta, progress=self.progress),
            source_database=candidate.source_database or "NCBI",
            assembly_level=candidate.assembly_level,
            refseq_category=candidate.refseq_category,
            selection_rationale=self.policy.rationale(candidate),
        )

    @staticmethod
    def _validate_downloaded_fasta(fasta: Path) -> None:
        """Fully validate gzip-compressed downloaded FASTA *fasta*."""

        if not existing_nonempty(fasta):
            raise DownloadError(f"Genome FASTA is missing after extraction: {fasta}")
        try:
            with gzip.open(fasta, "rb") as handle:
                first = handle.readline()
                if not first.startswith(b">"):
                    raise DownloadError(f"Genome FASTA has no header: {fasta}")
                while handle.read(8 * 1024 * 1024):
                    pass
        except (OSError, EOFError) as exc:
            raise DownloadError(f"Genome FASTA failed gzip validation: {fasta}") from exc

    def register_custom(
        self,
        *,
        taxid: int,
        scientific_name: str,
        accession: str,
        fasta: Path,
    ) -> GenomeRef:
        """Register *fasta* under *taxid*, *scientific_name*, and *accession*."""

        if not existing_nonempty(fasta):
            raise FileNotFoundError(fasta)
        reference = GenomeRef(
            taxid=taxid,
            scientific_name=scientific_name,
            accession=accession,
            fasta=fasta.resolve(),
            sha256=sha256_file(fasta, progress=self.progress),
            source_database="custom",
            selection_rationale=("user-supplied genome",),
        )
        self.root.mkdir(parents=True, exist_ok=True)
        with exclusive_file_lock(self.root / ".lockfile.lock"):
            lock = self._load_lockfile()
            lock.setdefault("genomes", {})[str(taxid)] = reference.to_dict()
            atomic_write_json(self.lockfile, lock)
        return self._remember(reference)
