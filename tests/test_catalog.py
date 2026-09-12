import polars as pl
import pytest

from ncbi_dataset_builder.catalog import RunCatalog, validate_polars_runtime
from ncbi_dataset_builder.errors import CatalogConflictError, DependencyError


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
    assert units[0].total_size_gb == 0.005
    assert "2 -> 2 rows" in filtered.audit[-1]


def test_identical_duplicates_are_removed_but_conflicts_fail():
    rows = records()
    catalog = RunCatalog.from_records([rows[0], dict(rows[0]), rows[1]])
    assert catalog.deduplicate_runs().frame.height == 2
    conflicting = dict(rows[0], bases="999")
    with pytest.raises(CatalogConflictError, match="bases"):
        RunCatalog.from_records([rows[0], conflicting]).deduplicate_runs()


def test_transform_applies_polars_callable_and_records_audit():
    catalog = RunCatalog.from_records(records())

    transformed = catalog.transform(
        lambda frame: frame.filter(pl.col("Run") == "SRR2").with_columns(
            pl.lit("selected").alias("status")
        ),
        description="select SRR2",
    )

    assert transformed.frame.get_column("Run").to_list() == ["SRR2"]
    assert transformed.frame.get_column("status").to_list() == ["selected"]
    assert transformed.audit[-1] == "transform select SRR2: 2 -> 1 rows"


def test_transform_requires_a_dataframe_with_run_column():
    catalog = RunCatalog.from_records(records())

    with pytest.raises(TypeError, match="Polars DataFrame"):
        catalog.transform(lambda frame: frame.to_dicts())
    with pytest.raises(ValueError, match="Run"):
        catalog.transform(lambda frame: frame.drop("Run"))


def test_polars_runtime_validation_reports_mixed_install(monkeypatch):
    import polars._reexport as polars_reexport

    monkeypatch.delattr(polars_reexport, "Expr")
    with pytest.raises(DependencyError, match="polars-lts-cpu"):
        validate_polars_runtime()


def test_a_processing_unit_cannot_span_species():
    rows = records()
    rows[1] = dict(rows[1], TaxID="10090", ScientificName="Mus musculus")
    with pytest.raises(CatalogConflictError, match="spans multiple species"):
        RunCatalog.from_records(rows).processing_units(by="experiment")
