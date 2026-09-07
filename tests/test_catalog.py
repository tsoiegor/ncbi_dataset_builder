import polars as pl
import pytest

from ncbi_dataset_builder.catalog import RunCatalog
from ncbi_dataset_builder.errors import CatalogConflictError
from ncbi_dataset_builder.models import ProcessingUnit


def records():
    return [
        {
            "Run": "SRR1",
            "Experiment": "SRX1",
            "SRA Sample": "SRS1",
            "BioSample": "SAMN1",
            "TaxID": "9606",
            "ScientificName": "Homo sapiens",
            "LibraryStrategy": "ATAC-seq",
            "LibraryLayout": "PAIRED",
            "Platform": "ILLUMINA",
            "bases": "100",
            "size_MB": "2",
        },
        {
            "Run": "SRR2",
            "Experiment": "SRX1",
            "SRA Sample": "SRS1",
            "BioSample": "SAMN1",
            "TaxID": "9606",
            "ScientificName": "Homo sapiens",
            "LibraryStrategy": "ATAC-seq",
            "LibraryLayout": "PAIRED",
            "Platform": "ILLUMINA",
            "bases": "200",
            "size_MB": "3",
        },
    ]


def test_arbitrary_filters_and_entity_preserving_grouping():
    catalog = RunCatalog.from_records(records())
    filtered = catalog.filter(pl.col("LibraryStrategy") == "ATAC-seq").filter(
        lambda row: int(row["bases"]) >= 100, description="enough bases"
    )
    units = filtered.processing_units(by="experiment")
    assert len(units) == 1
    assert units[0].run_accessions == ("SRR1", "SRR2")
    assert units[0].experiment_accessions == ("SRX1",)
    assert units[0].biosample_accessions == ("SAMN1",)
    assert units[0].total_bytes == 5_000_000
    assert "2 -> 2 rows" in filtered.audit[-1]


def test_identical_duplicates_are_removed_but_conflicts_fail():
    rows = records()
    catalog = RunCatalog.from_records([rows[0], dict(rows[0]), rows[1]])
    assert catalog.deduplicate_runs().frame.height == 2
    conflicting = dict(rows[0], bases="999")
    with pytest.raises(CatalogConflictError, match="bases"):
        RunCatalog.from_records([rows[0], conflicting]).deduplicate_runs()


def test_batching_terminates_and_keeps_oversized_unit():
    units = [
        ProcessingUnit("large", ("SRR1",), total_bytes=200),
        ProcessingUnit("small-a", ("SRR2",), total_bytes=40),
        ProcessingUnit("small-b", ("SRR3",), total_bytes=50),
    ]
    batches = RunCatalog.batch_units(units, max_bytes=100, max_units=2)
    assert [[unit.unit_id for unit in batch] for batch in batches] == [
        ["large"],
        ["small-b", "small-a"],
    ]


def test_a_processing_unit_cannot_span_species():
    rows = records()
    rows[1] = dict(rows[1], TaxID="10090", ScientificName="Mus musculus")
    with pytest.raises(CatalogConflictError, match="spans multiple species"):
        RunCatalog.from_records(rows).processing_units(by="experiment")
