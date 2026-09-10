import gzip
import json

from ncbi_dataset_builder.models import DatasetTask, ProcessingUnit, ResourceSpec, WorkspaceJob
from ncbi_dataset_builder.publishing import DatasetPublisher
from ncbi_dataset_builder.state import TaskStateStore


def test_publish_creates_compact_experiment_dataset_and_preserves_sample_id(tmp_path):
    bigwig = tmp_path / "results" / "SRX1.coverage.bw"
    bigwig.parent.mkdir(parents=True)
    bigwig.write_bytes(b"bigwig")
    genome = tmp_path / "genomes" / "GCF_TEST.fasta.gz"
    genome.parent.mkdir(parents=True)
    with gzip.open(genome, "wb") as handle:
        handle.write(b">chr1\nACGT\n")
    descriptions = tmp_path / "metadata" / "sample_descriptions"
    descriptions.mkdir(parents=True)
    (descriptions / "SRS1.json").write_text(
        json.dumps({"ID": "SRS1", "Tissue": "liver"}), encoding="utf-8"
    )
    unit = ProcessingUnit(
        "SRX1",
        ("SRR1",),
        experiment_accessions=("SRX1",),
        sra_sample_accessions=("SRS1",),
        scientific_name="Homo sapiens",
        taxid=9606,
    )
    job = WorkspaceJob(
        "job-1",
        "now",
        None,
        "experiment",
        (DatasetTask("SRX1", unit, 0, ResourceSpec()),),
        "test:processor",
    )
    state = TaskStateStore(tmp_path / "state" / "units")
    state.start("SRX1")
    state.succeed(
        "SRX1",
        {
            "processing": {"outputs": [str(bigwig)]},
            "genome": {
                "taxid": 9606,
                "scientific_name": "Homo sapiens",
                "accession": "GCF_TEST",
                "fasta": str(genome),
            },
        },
    )

    exported = DatasetPublisher(tmp_path).publish(job, mode="copy")

    assert (exported.destination / "bigWig" / "SRX1.bw").read_bytes() == b"bigwig"
    assert (exported.destination / "genomes" / "Homo_sapiens.fasta.gz").is_file()
    description = json.loads(
        (exported.destination / "descriptions" / "SRX1.json").read_text(encoding="utf-8")
    )
    assert description["ID"] == "SRS1"
    assert description["Experiment ID"] == "SRX1"
    manifest = json.loads(exported.manifest.read_text(encoding="utf-8"))
    assert manifest["dataset"]["experiments"]["SRX1"]["assembly_accession"] == "GCF_TEST"


def test_publish_selects_only_the_target_experiment_from_a_shared_sample(tmp_path):
    """Published SRX metadata must not retain another SRX's variant fields."""

    bigwig = tmp_path / "results" / "SRX1.bw"
    bigwig.parent.mkdir(parents=True)
    bigwig.write_bytes(b"bigwig")
    genome = tmp_path / "genomes" / "GCF_TEST.fasta.gz"
    genome.parent.mkdir(parents=True)
    with gzip.open(genome, "wb") as handle:
        handle.write(b">chr1\nACGT\n")
    metadata_root = tmp_path / "metadata"
    descriptions = metadata_root / "sample_descriptions"
    descriptions.mkdir(parents=True)
    (descriptions / "SRS1.json").write_text(
        json.dumps(
            {
                "ID": "SRS1",
                "Tissue": "liver",
                "Experiments": [
                    {"Strategy": "ATAC-seq", "Study": "chromatin study"},
                    {"Strategy": "RNA-Seq", "Study": "expression study"},
                ],
            }
        ),
        encoding="utf-8",
    )
    (metadata_root / "metadata.json").write_text(
        json.dumps(
            {
                "packages": [
                    {
                        "experiment_accession": "SRX1",
                        "sra_sample_accession": "SRS1",
                        "study_accession": "SRP1",
                        "submission_accession": "SRA1",
                    },
                    {
                        "experiment_accession": "SRX2",
                        "sra_sample_accession": "SRS1",
                        "study_accession": "SRP2",
                        "submission_accession": "SRA1",
                    },
                ],
                "experiments": [
                    {"accession": "SRX1", "library": {"strategy": "ATAC-seq"}},
                    {"accession": "SRX2", "library": {"strategy": "RNA-Seq"}},
                ],
                "sra_samples": [{"accession": "SRS1"}],
                "studies": [
                    {"accession": "SRP1", "title": "chromatin study"},
                    {"accession": "SRP2", "title": "expression study"},
                ],
                "submissions": [{"accession": "SRA1"}],
                "runs": [],
                "biosamples": [],
                "raw_sra_packages": [],
            }
        ),
        encoding="utf-8",
    )
    unit = ProcessingUnit(
        "SRX1",
        ("SRR1",),
        experiment_accessions=("SRX1",),
        sra_sample_accessions=("SRS1",),
        scientific_name="Homo sapiens",
        taxid=9606,
    )
    job = WorkspaceJob(
        "job-1",
        "now",
        None,
        "experiment",
        (DatasetTask("SRX1", unit, 0, ResourceSpec()),),
        "test:processor",
    )
    state = TaskStateStore(tmp_path / "state" / "units")
    state.start("SRX1")
    state.succeed(
        "SRX1",
        {
            "processing": {"outputs": [str(bigwig)]},
            "genome": {
                "taxid": 9606,
                "scientific_name": "Homo sapiens",
                "accession": "GCF_TEST",
                "fasta": str(genome),
            },
        },
    )

    exported = DatasetPublisher(tmp_path).publish(job, mode="copy")
    description = json.loads(
        (exported.destination / "descriptions" / "SRX1.json").read_text(encoding="utf-8")
    )

    assert "Experiments" not in description
    assert description["ID"] == "SRS1"
    assert description["Experiment ID"] == "SRX1"
    assert description["Strategy"] == "ATAC-seq"
    assert description["Study"] == "chromatin study"
    assert "expression study" not in json.dumps(description)


def test_publisher_projects_a_full_profile_back_to_compact_training_metadata(tmp_path):
    """A saved full metadata profile must not leak into the compact dataset."""

    metadata_root = tmp_path / "metadata"
    descriptions = metadata_root / "sample_descriptions"
    descriptions.mkdir(parents=True)
    (descriptions / "SRS1.json").write_text(
        json.dumps(
            {
                "schema_version": "1.0",
                "sra_sample": {"accession": "SRS1"},
                "experiments": [{"accession": "SRX1"}],
                "package_relations": [{"experiment_accession": "SRX1"}],
            }
        ),
        encoding="utf-8",
    )
    (metadata_root / "metadata.json").write_text(
        json.dumps(
            {
                "packages": [
                    {
                        "experiment_accession": "SRX1",
                        "sra_sample_accession": "SRS1",
                        "study_accession": "SRP1",
                    }
                ],
                "experiments": [
                    {"accession": "SRX1", "library": {"strategy": "ATAC-seq"}}
                ],
                "sra_samples": [
                    {"accession": "SRS1", "organism": "Homo sapiens"}
                ],
                "studies": [{"accession": "SRP1", "title": "chromatin study"}],
                "submissions": [],
                "runs": [],
                "biosamples": [],
                "raw_sra_packages": [],
            }
        ),
        encoding="utf-8",
    )

    description = DatasetPublisher(tmp_path)._description("SRS1", "SRX1")

    assert description["ID"] == "SRS1"
    assert description["Experiment ID"] == "SRX1"
    assert description["Species"] == "Homo sapiens"
    assert description["Strategy"] == "ATAC-seq"
    assert "sra_sample" not in description
    assert "package_relations" not in description
