import pytest

from ncbi_dataset_builder.errors import GenomeSelectionError
from ncbi_dataset_builder.genomes import GenomeCandidate, GenomeSelectionPolicy


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
