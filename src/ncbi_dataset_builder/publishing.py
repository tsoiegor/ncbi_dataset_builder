from __future__ import annotations

import gzip
import os
import shutil
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

from .descriptions import training_fields_by_experiment
from .metadata import MetadataBundle
from .models import WorkspaceJob
from .progress import ProgressReporter, get_progress
from .state import TaskStateStore
from .util import (
    atomic_write_json,
    bytes_to_gb,
    exclusive_file_lock,
    read_json,
    sanitize_identifier,
    sha256_file,
)

PublishMode = Literal["auto", "hardlink", "copy"]


@dataclass(frozen=True)
class DatasetExport:
    """Describe a completed compact dataset export.

    Args:
        destination: Root containing ``bigWig``, ``genomes``, and ``descriptions``.
        manifest: Path to the exported provenance manifest.
        experiments: Number of exported experiment records.
        genomes: Number of distinct exported species genomes.
    """

    destination: Path
    manifest: Path
    experiments: int
    genomes: int


class DatasetPublisher:
    """Publish validated workspace outputs as a compact model-ready dataset."""

    def __init__(
        self,
        workspace: Path,
        *,
        progress: ProgressReporter | None = None,
    ) -> None:
        """Read build artifacts from *workspace* and report work with *progress*."""

        self.workspace = Path(workspace)
        self.progress = get_progress(progress)
        self.state = TaskStateStore(self.workspace / "state" / "units")
        self._metadata: MetadataBundle | None = None
        self._generated_training: dict[str, dict[str, Any]] | None = None
        self._experiment_fields: dict[str, dict[str, Any]] | None = None

    @staticmethod
    def _remove_path(path: Path) -> None:
        """Remove package-owned staging or backup *path* without following symlinks."""

        if path.is_symlink() or path.is_file():
            path.unlink(missing_ok=True)
        elif path.is_dir():
            shutil.rmtree(path)

    @staticmethod
    def _publish_file(source: Path, destination: Path, mode: PublishMode) -> str:
        """Publish *source* at *destination* using *mode* and return the method used."""

        if not source.is_file() or source.stat().st_size == 0:
            raise FileNotFoundError(f"Cannot publish missing or empty file: {source}")
        destination.parent.mkdir(parents=True, exist_ok=True)
        if mode in {"auto", "hardlink"}:
            try:
                os.link(source, destination)
                return "hardlink"
            except OSError:
                if mode == "hardlink":
                    raise
        shutil.copy2(source, destination)
        return "copy"

    @staticmethod
    def _publish_genome(source: Path, destination: Path, mode: PublishMode) -> str:
        """Publish *source* as gzip-compressed FASTA at *destination* using *mode*."""

        if source.suffix == ".gz":
            return DatasetPublisher._publish_file(source, destination, mode)
        destination.parent.mkdir(parents=True, exist_ok=True)
        with source.open("rb") as input_handle, gzip.open(
            destination, "wb", compresslevel=6
        ) as output_handle:
            shutil.copyfileobj(input_handle, output_handle, length=8 * 1024 * 1024)
        return "gzip"

    @staticmethod
    def _validate_genome(path: Path) -> None:
        """Fully read gzip FASTA *path* and require a leading sequence header."""

        try:
            with gzip.open(path, "rb") as handle:
                if not handle.readline().startswith(b">"):
                    raise ValueError(f"Published genome FASTA has no header: {path}")
                while handle.read(8 * 1024 * 1024):
                    pass
        except (OSError, EOFError) as exc:
            raise ValueError(f"Published genome failed gzip validation: {path}") from exc

    @staticmethod
    def _processing_bigwig(state: dict[str, Any], experiment_id: str) -> Path:
        """Return the unique BigWig output in *state* for *experiment_id*."""

        outputs = state.get("result", {}).get("processing", {}).get("outputs", [])
        bigwigs = [Path(item) for item in outputs if str(item).lower().endswith((".bw", ".bigwig"))]
        if len(bigwigs) != 1:
            raise ValueError(
                f"Experiment {experiment_id} must have exactly one BigWig output; "
                f"found {len(bigwigs)}"
            )
        return bigwigs[0]

    def _load_metadata(self) -> MetadataBundle:
        """Load and process-cache the workspace's normalized metadata bundle."""

        if self._metadata is None:
            metadata_path = self.workspace / "metadata" / "metadata.json"
            if not metadata_path.is_file():
                raise FileNotFoundError(
                    f"Experiment-specific publishing requires normalized metadata: {metadata_path}"
                )
            self._metadata = MetadataBundle.load(metadata_path, progress=self.progress)
        return self._metadata

    def _description(self, sample_id: str, experiment_id: str) -> dict[str, Any]:
        """Load *sample_id* training metadata and bind it to *experiment_id*."""

        source = self.workspace / "metadata" / "sample_descriptions" / f"{sample_id}.json"
        if not source.is_file():
            raise FileNotFoundError(f"Sample description is missing: {source}")
        description = read_json(source)
        if not isinstance(description, dict):
            raise TypeError(f"Sample description is not a JSON object: {source}")
        if "sra_sample" in description and "package_relations" in description:
            if self._generated_training is None:
                self._generated_training = self._load_metadata().descriptions_by_sample(
                    profile="training",
                    progress=self.progress,
                )
            generated = self._generated_training.get(sample_id)
            if generated is None:
                raise ValueError(f"Normalized metadata has no SRA Sample {sample_id}")
            description = dict(generated)
        existing_id = description.get("ID")
        if existing_id not in (None, sample_id):
            raise ValueError(
                f"Description {source} has ID {existing_id!r}, expected {sample_id!r}"
            )
        if "Experiments" in description:
            if self._experiment_fields is None:
                self._experiment_fields = training_fields_by_experiment(self._load_metadata())
            fields = self._experiment_fields.get(experiment_id)
            if fields is None:
                raise ValueError(
                    f"Normalized metadata has no package relationship for {experiment_id}"
                )
            description.pop("Experiments")
            description.update(fields)
        description["ID"] = sample_id
        description["Experiment ID"] = experiment_id
        return description

    def publish(
        self,
        job: WorkspaceJob,
        destination: Path | None = None,
        *,
        mode: PublishMode = "auto",
        overwrite: bool = False,
    ) -> DatasetExport:
        """Publish completed experiment *job* into *destination* using *mode*.

        *overwrite* atomically replaces an existing destination after the new
        dataset has been fully materialized and validated.
        """

        if job.group_by != "experiment":
            raise ValueError("Compact publishing requires a workspace grouped by experiment")
        if mode not in {"auto", "hardlink", "copy"}:
            raise ValueError(f"Unknown publish mode: {mode!r}")
        target = Path(destination) if destination is not None else self.workspace
        in_place = target.resolve() == self.workspace.resolve()
        target_exists = target.exists() or target.is_symlink()
        if target_exists and not in_place and not overwrite:
            raise FileExistsError(f"Dataset destination already exists: {target}")
        staging = (
            self.workspace / "work" / "publishing" / job.job_id
            if in_place
            else target.parent / f"{target.name}.staging-{uuid.uuid4().hex}"
        )
        backup = target.parent / f"{target.name}.backup-{uuid.uuid4().hex}"
        self._remove_path(staging)
        records: dict[str, dict[str, Any]] = {}
        genome_records: dict[str, dict[str, Any]] = {}
        try:
            for task in self.progress.track(job.tasks, "Publish dataset", unit="experiments"):
                experiments = task.unit.experiment_accessions
                samples = task.unit.sra_sample_accessions
                if len(experiments) != 1 or experiments[0] != task.unit.unit_id:
                    raise ValueError(
                        f"Task {task.task_id} is not one unambiguous experiment: {experiments}"
                    )
                if len(samples) != 1:
                    raise ValueError(
                        f"Experiment {task.task_id} must link exactly one SRA sample; found {samples}"
                    )
                experiment_id = sanitize_identifier(experiments[0])
                if experiment_id in records:
                    raise ValueError(f"Duplicate published Experiment ID: {experiment_id}")
                state_key = task.task_id
                state = self.state.get(state_key)
                if state is None or state.get("status") != "succeeded":
                    raise ValueError(f"Experiment {experiment_id} has not completed successfully")
                bigwig_source = self._processing_bigwig(state, experiment_id)
                bigwig_target = staging / "bigWig" / f"{experiment_id}.bw"
                bigwig_method = self._publish_file(bigwig_source, bigwig_target, mode)

                sample_id = samples[0]
                description_target = staging / "descriptions" / f"{experiment_id}.json"
                atomic_write_json(description_target, self._description(sample_id, experiment_id))

                genome = state.get("result", {}).get("genome")
                if not isinstance(genome, dict):
                    raise TypeError(f"Experiment {experiment_id} has no persisted genome reference")
                if genome.get("taxid") != task.unit.taxid:
                    raise ValueError(
                        f"Experiment {experiment_id} task taxid {task.unit.taxid} does not match "
                        f"its persisted genome taxid {genome.get('taxid')}"
                    )
                species = str(genome.get("scientific_name") or task.unit.scientific_name or "")
                species_filename = f"{sanitize_identifier(species)}.fasta.gz"
                genome_source = Path(str(genome["fasta"]))
                genome_target = staging / "genomes" / species_filename
                prior = genome_records.get(species_filename)
                if prior is not None and (
                    prior["accession"] != genome.get("accession")
                    or prior["taxid"] != genome.get("taxid")
                    or prior["scientific_name"] != species
                ):
                    raise ValueError(
                        f"Species filename collision for {species_filename}: "
                        f"{prior['accession']} and {genome.get('accession')}"
                    )
                if prior is None:
                    genome_method = self._publish_genome(genome_source, genome_target, mode)
                    self._validate_genome(genome_target)
                    genome_records[species_filename] = {
                        "taxid": genome.get("taxid"),
                        "scientific_name": species,
                        "accession": genome.get("accession"),
                        "source_database": genome.get("source_database"),
                        "assembly_level": genome.get("assembly_level"),
                        "refseq_category": genome.get("refseq_category"),
                        "selection_rationale": list(genome.get("selection_rationale") or ()),
                        "source": str(genome_source),
                        "sha256": sha256_file(genome_target, progress=self.progress),
                        "size_gb": bytes_to_gb(genome_target.stat().st_size),
                        "publish_method": genome_method,
                    }
                records[experiment_id] = {
                    "sample_id": sample_id,
                    "run_accessions": list(task.unit.run_accessions),
                    "species": species,
                    "taxid": task.unit.taxid,
                    "assembly_accession": genome.get("accession"),
                    "bigwig": f"bigWig/{bigwig_target.name}",
                    "description": f"descriptions/{description_target.name}",
                    "genome": f"genomes/{species_filename}",
                    "bigwig_sha256": sha256_file(bigwig_target, progress=self.progress),
                    "description_sha256": sha256_file(
                        description_target, progress=self.progress
                    ),
                    "bigwig_size_gb": bytes_to_gb(bigwig_target.stat().st_size),
                    "description_size_gb": bytes_to_gb(description_target.stat().st_size),
                    "publish_method": bigwig_method,
                }
            manifest = {
                "schema_version": 1,
                "job_id": job.job_id,
                "group_by": job.group_by,
                "experiments": records,
                "genomes": genome_records,
            }
            atomic_write_json(staging / "manifest.json", manifest)
            if in_place:
                for directory in ("bigWig", "descriptions", "genomes"):
                    destination_root = target / directory
                    destination_root.mkdir(parents=True, exist_ok=True)
                    for source in (staging / directory).iterdir():
                        os.replace(source, destination_root / source.name)
                with exclusive_file_lock(target / "state" / "workspace-manifest.lock"):
                    root_manifest = (
                        read_json(target / "manifest.json")
                        if (target / "manifest.json").is_file()
                        else {"schema_version": 1, "units": {}}
                    )
                    previous_dataset = root_manifest.get("dataset", {})
                    previous_records = previous_dataset.get("experiments", {})
                    if isinstance(previous_records, dict):
                        for record in previous_records.values():
                            if isinstance(record, dict):
                                record["requested_by_latest_job"] = False
                        for experiment_id, record in records.items():
                            record["requested_by_latest_job"] = True
                            previous_records[experiment_id] = record
                        manifest["experiments"] = previous_records
                    previous_genomes = previous_dataset.get("genomes", {})
                    if isinstance(previous_genomes, dict):
                        previous_genomes.update(genome_records)
                        manifest["genomes"] = previous_genomes
                    root_manifest["dataset"] = manifest
                    atomic_write_json(target / "manifest.json", root_manifest)
                self._remove_path(staging)
            else:
                if target_exists:
                    os.replace(target, backup)
                os.replace(staging, target)
                self._remove_path(backup)
        except BaseException:
            self._remove_path(staging)
            if (backup.exists() or backup.is_symlink()) and not target.exists():
                os.replace(backup, target)
            raise
        return DatasetExport(
            destination=target,
            manifest=target / "manifest.json",
            experiments=len(records),
            genomes=len(genome_records),
        )
