import copy
import json

import pytest
from test_metadata import BIOSAMPLE_XML, SRA_XML, FakeEntrez

from ncbi_dataset_builder import DescriptionPolicy, RunCatalog
from ncbi_dataset_builder.metadata import (
    BioSampleClient,
    MetadataBundle,
    SraClient,
    fetch_metadata_for_catalog,
)


def example_bundle():
    bundle = SraClient.parse_packages(SRA_XML)
    bundle.biosamples = BioSampleClient.parse(BIOSAMPLE_XML)
    return bundle


def test_default_export_is_compact_with_original_sample_id_and_full_text(tmp_path):
    bundle = example_bundle()
    text = "Protocol: " + "step with exact concentration 2.5 mM; " * 300
    bundle.experiments[0]["library"]["construction_protocol"] = text
    bundle.save(tmp_path)
    description = json.loads(
        (tmp_path / "sample_descriptions/SRS4739189.json").read_text(encoding="utf-8")
    )
    assert description["ID"] == "SRS4739189"
    assert description["Strategy"] == "ATAC-seq"
    assert description["Study"] == "A study"
    assert description["Abstract"] == "Text & design"
    assert description["Construction protocol"] == text
    assert description["Submitted by"] == "NCBI (GEO)"
    assert description["GEO Accession"] == "GSM3756614"
    assert (
        not {"runs", "raw", "biosample", "provenance", "packages", "identifiers", "studies"}
        & description.keys()
    )
    # Complete metadata is still available for auditing and downloads.
    assert json.loads((tmp_path / "metadata.json").read_text(encoding="utf-8"))["runs"][0]["files"]


def test_biology_aliases_placeholders_and_administrative_attributes():
    bundle = example_bundle()
    bundle.biosamples[0]["attribute_records"] = [
        {"name": name, "value": value}
        for name, value in [
            ("cell_type", "CD8+ T cell"),
            ("dev_stage", "embryo"),
            ("genotype/variation", "WT"),
            ("treatment", "NO"),
            ("age", "0"),
            ("phenotype", "not collected"),
            ("External Id", "SAMEA1"),
            ("collection_date", "2022-01-01"),
            ("unknown mechanism", "novel state"),
        ]
    ]
    description = bundle.descriptions_by_sample()["SRS4739189"]
    assert description["Cell type"] == "CD8+ T cell"
    assert description["Developmental stage"] == "embryo"
    assert description["Genotype"] == "WT"
    assert description["Treatment"] == "NO"
    assert description["Age"] == "0"
    assert "Phenotype" not in description
    assert "External Id" not in description
    assert "collection_date" not in description
    custom = bundle.descriptions_by_sample(
        policy=DescriptionPolicy(extra_attributes={"unknown mechanism": "Mechanism"})
    )["SRS4739189"]
    assert custom["Mechanism"] == "novel state"


def test_units_in_sra_xml_are_retained_in_compact_preparation_values():
    xml = SRA_XML.replace(
        b"</EXPERIMENT_ATTRIBUTES>",
        b"<EXPERIMENT_ATTRIBUTE><TAG>sampling to preparation interval</TAG><VALUE>4.0</VALUE><UNITS>months</UNITS></EXPERIMENT_ATTRIBUTE></EXPERIMENT_ATTRIBUTES>",
    )
    bundle = SraClient.parse_packages(xml)
    assert (
        bundle.descriptions_by_sample()["SRS4739189"]["sampling to preparation interval"]
        == "4.0 months"
    )


def two_assay_bundle():
    bundle = example_bundle()
    other = copy.deepcopy(bundle.experiments[0])
    other.update(accession="SRX2")
    other["library"].update(strategy="RNA-Seq", construction_protocol="RNA protocol")
    bundle.experiments.append(other)
    package = {
        **bundle.packages[0],
        "accession": "SRX2",
        "experiment_accession": "SRX2",
        "run_accessions": ["SRR2"],
    }
    bundle.packages.append(package)
    bundle.runs.append({"accession": "SRR2", "experiment_accession": "SRX2"})
    return bundle


def test_multiple_assays_preserve_protocol_relationship_and_shared_values_once():
    bundle = two_assay_bundle()
    description = bundle.descriptions_by_sample()["SRS4739189"]
    assert description["Study"] == "A study"
    assert "Strategy" not in description
    assert [
        (row["Strategy"], row["Construction protocol"]) for row in description["Experiments"]
    ] == [
        ("ATAC-seq", "ATAC protocol & cleanup"),
        ("RNA-Seq", "RNA protocol"),
    ]
    scoped = bundle.subset_experiments(["SRX5809925"])
    compact = scoped.descriptions_by_sample()["SRS4739189"]
    assert compact["Strategy"] == "ATAC-seq"
    assert "Experiments" not in compact
    assert len(scoped.runs) == 2


def test_catalog_enrichment_does_not_pull_in_other_assays():
    class FakeSra:
        def fetch_packages(self, accessions, **kwargs):
            return two_assay_bundle()

    catalog = RunCatalog.from_records([{"Run": "SRR9032674", "Experiment": "SRX5809925"}])
    bundle = fetch_metadata_for_catalog(
        catalog, sra=FakeSra(), biosample=BioSampleClient(FakeEntrez())
    )
    assert [row["accession"] for row in bundle.experiments] == ["SRX5809925"]
    assert bundle.descriptions_by_sample()["SRS4739189"]["Strategy"] == "ATAC-seq"


def test_unknown_profile_and_empty_bundle():
    assert MetadataBundle().descriptions_by_sample() == {}
    with pytest.raises(ValueError, match="profile"):
        MetadataBundle().descriptions_by_sample(profile="typo")
