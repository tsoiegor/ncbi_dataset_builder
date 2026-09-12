import gzip
import hashlib
import json
import zipfile
from io import StringIO

import pytest

from ncbi_dataset_builder.acquisition.genomes import (
    GenomeCandidate,
    GenomeManager,
    GenomeSelectionPolicy,
)
from ncbi_dataset_builder.errors import GenomeSelectionError
from ncbi_dataset_builder.models import GenomeRef
from ncbi_dataset_builder.support.progress import ProgressReporter


class FakeDatasetsRunner:
    """Create a minimal NCBI Datasets archive at the requested output path."""

    def require(self, *executables):
        """Accept the requested fake *executables*."""

        return

    def run(self, command, **kwargs):
        """Write a genome ZIP named by *command* and ignore execution *kwargs*."""

        del kwargs
        target = command[command.index("--filename") + 1]
        with zipfile.ZipFile(target, "w") as archive:
            archive.writestr("ncbi_dataset/data/GCF_TEST/genomic.fna", ">chr1\nACGT\n")


def candidate(accession, **values):
    defaults = {
        "taxid": 9606,
        "scientific_name": "Homo sapiens",
        "source_database": "GenBank",
        "assembly_status": "current",
        "refseq_category": None,
        "assembly_level": "Chromosome",
        "release_date": "2025-01-01",
        "contig_n50": 10,
        "scaffold_n50": 10,
        "total_length": 100,
    }
    defaults.update(values)
    return GenomeCandidate(accession=accession, **defaults)


def test_reference_refseq_wins_and_decision_is_explainable():
    policy = GenomeSelectionPolicy()
    selected = policy.select(
        [
            candidate("GCA_NEW", scaffold_n50=1000),
            candidate(
                "GCF_REF",
                source_database="RefSeq",
                refseq_category="reference genome",
                scaffold_n50=500,
            ),
        ],
        taxid=9606,
    )
    assert selected.accession == "GCF_REF"
    assert any("reference genome" in line for line in policy.rationale(selected))


def test_wrong_taxon_suppressed_and_atypical_are_rejected():
    policy = GenomeSelectionPolicy()
    with pytest.raises(GenomeSelectionError):
        policy.select(
            [
                candidate("WRONG", taxid=10090),
                candidate("OLD", assembly_status="suppressed"),
                candidate("ODD", atypical=True),
            ],
            taxid=9606,
        )


def test_pin_is_explicit_override_but_must_exist():
    policy = GenomeSelectionPolicy()
    selected = policy.select([candidate("GCA_1", atypical=True)], taxid=9606, pin="GCA_1")
    assert selected.accession == "GCA_1"
    with pytest.raises(GenomeSelectionError):
        policy.select([candidate("GCA_1")], taxid=9606, pin="GCA_missing")
    with pytest.raises(GenomeSelectionError, match="belongs to taxid"):
        policy.select([candidate("GCA_mouse", taxid=10090)], taxid=9606, pin="GCA_mouse")


def test_ncbi_datasets_json_enums_are_parsed_for_ranking():
    report = {
        "accession": "GCF_000001405.40",
        "organism": {"tax_id": 9606, "organism_name": "Homo sapiens"},
        "source_database": "SOURCE_DATABASE_REFSEQ",
        "assembly_info": {
            "assembly_status": "current",
            "refseq_category": "reference genome",
            "assembly_level": "Chromosome",
            "release_date": "2022-02-03",
        },
        "assembly_stats": {"contig_n50": 57_879_411, "total_sequence_length": 3_099_734_149},
    }
    parsed = GenomeCandidate.from_report(report)
    assert parsed.taxid == 9606
    assert parsed.source_database == "SOURCE_DATABASE_REFSEQ"
    assert GenomeSelectionPolicy().select([parsed], taxid=9606) == parsed


def test_cache_inventory_deduplicates_requirements_and_seeds_memory(tmp_path):
    fasta = tmp_path / "9606" / "GCF_TEST" / "GCF_TEST.fna.gz"
    fasta.parent.mkdir(parents=True)
    fasta.write_bytes(b">chr1\nACGT\n")
    reference = GenomeRef(
        taxid=9606,
        scientific_name="Homo sapiens",
        accession="GCF_TEST",
        fasta=fasta,
        sha256=hashlib.sha256(fasta.read_bytes()).hexdigest(),
    )
    (tmp_path / "genomes.lock.json").write_text(
        json.dumps({"schema_version": 1, "genomes": {"9606": reference.to_dict()}}),
        encoding="utf-8",
    )
    stream = StringIO()
    manager = GenomeManager(
        tmp_path,
        progress=ProgressReporter(use_bars=False, stream=stream),
    )

    cached = manager.cache_inventory([(9606, None), (9606, None)])

    assert cached == {(9606, None): reference}
    assert "1 genomes loaded from cache; 0 genomes require work" in stream.getvalue()
    assert manager.resolve(taxid=9606).accession == "GCF_TEST"


def test_genome_cache_miss_is_reported_only_for_actual_preparation(tmp_path, monkeypatch):
    fasta = tmp_path / "prepared.fna.gz"
    fasta.write_bytes(b">chr1\nACGT\n")
    reference = GenomeRef(
        taxid=9606,
        scientific_name="Homo sapiens",
        accession="GCF_TEST",
        fasta=fasta,
        sha256=hashlib.sha256(fasta.read_bytes()).hexdigest(),
    )
    stream = StringIO()
    manager = GenomeManager(
        tmp_path / "genomes",
        progress=ProgressReporter(use_bars=False, stream=stream),
    )
    monkeypatch.setattr(manager, "candidates", lambda taxid: [candidate("GCF_TEST")])
    monkeypatch.setattr(
        manager,
        "_download",
        lambda selected, *, taxid, scientific_name: reference,
    )

    assert manager.resolve(taxid=9606).accession == "GCF_TEST"
    assert manager.resolve(taxid=9606).accession == "GCF_TEST"

    assert stream.getvalue().count("Genome cache miss for taxid 9606") == 1


def test_genome_download_is_flat_compressed_and_removes_archive(tmp_path):
    manager = GenomeManager(tmp_path, runner=FakeDatasetsRunner())

    reference = manager._download(
        candidate("GCF_TEST"), taxid=9606, scientific_name="Homo sapiens"
    )

    assert reference.fasta == tmp_path / "GCF_TEST.fasta.gz"
    with gzip.open(reference.fasta, "rt", encoding="utf-8") as handle:
        assert handle.read() == ">chr1\nACGT\n"
    assert not (tmp_path / "downloads").exists()

    reference.fasta.write_bytes(b"corrupt gzip")
    repaired = manager._download(
        candidate("GCF_TEST"), taxid=9606, scientific_name="Homo sapiens"
    )

    with gzip.open(repaired.fasta, "rt", encoding="utf-8") as handle:
        assert handle.read() == ">chr1\nACGT\n"
    assert not (tmp_path / "downloads").exists()
