from __future__ import annotations

import csv
import hashlib
import io
import json
import re
import threading
import xml.etree.ElementTree as ET
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from html import unescape
from html.parser import HTMLParser
from pathlib import Path
from typing import Any

import polars as pl

from .catalog import RunCatalog
from .descriptions import DescriptionPolicy, training_descriptions
from .errors import MetadataError
from .http import HttpClient
from .progress import ProgressReporter, get_progress
from .util import atomic_write_bytes, atomic_write_json, atomic_write_text, read_json

EUTILS = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils"
_SAFE_IDENTIFIER = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.:-]*\Z")
_HTML_TAG = re.compile(r"</?[A-Za-z][^>]*>")


def _client_progress(client: Any) -> ProgressReporter:
    """Return the configured reporter for *client* or a disabled fallback."""

    return get_progress(getattr(client, "progress", None))


def _response_cached(client: Any, endpoint: str, params: Mapping[str, Any]) -> bool:
    """Return whether *client* reports cached *endpoint* and *params*."""

    checker = getattr(client, "is_cached", None)
    return bool(checker and checker(endpoint, params))


def _tag(element: ET.Element) -> str:
    """Return local tag name for possibly namespaced XML *element*."""

    return element.tag.rsplit("}", 1)[-1]


def _text(element: ET.Element | None) -> str | None:
    """Return whitespace-normalized descendant text from *element*."""

    if element is None:
        return None
    value = " ".join("".join(element.itertext()).split())
    return value or None


def _child(element: ET.Element | None, tag: str) -> ET.Element | None:
    """Return the first direct child of *element* with local *tag*."""

    if element is None:
        return None
    return next((node for node in element if _tag(node) == tag), None)


def _descendant(element: ET.Element | None, tag: str) -> ET.Element | None:
    """Return the first descendant of *element* with local *tag*."""

    if element is None:
        return None
    return next((node for node in element.iter() if _tag(node) == tag), None)


def _descendant_text(element: ET.Element | None, tag: str) -> str | None:
    """Return normalized text from *element*'s first descendant named *tag*."""

    return _text(_descendant(element, tag))


def _integer(value: str | None) -> int | None:
    """Convert string *value* to an integer, returning ``None`` when invalid."""

    if value in (None, ""):
        return None
    try:
        return int(value)
    except ValueError:
        return None


def _gigabytes(value: str | None) -> float | None:
    """Convert an optional byte-count *value* from XML to decimal GB."""

    parsed = _integer(value)
    return parsed / 1_000_000_000 if parsed is not None else None


def xml_to_dict(element: ET.Element) -> dict[str, Any]:
    """Convert XML *element* to a dictionary without losing repeats or attributes."""

    value: dict[str, Any] = {}
    if element.attrib:
        value["@attributes"] = dict(element.attrib)
    direct_text = (element.text or "").strip()
    if direct_text:
        value["#text"] = direct_text
    for child in element:
        key = _tag(child)
        child_value = xml_to_dict(child)
        existing = value.get(key)
        if existing is None:
            value[key] = child_value
        elif isinstance(existing, list):
            existing.append(child_value)
        else:
            value[key] = [existing, child_value]
    return value


def _append_mapping_value(result: dict[str, str | list[str]], name: str, value: str) -> None:
    """Append *value* under *name* in *result*, promoting repeats to a list."""

    previous = result.get(name)
    if previous is None:
        result[name] = value
    elif isinstance(previous, list):
        previous.append(value)
    else:
        result[name] = [previous, value]


def _attribute_records(parent: ET.Element | None) -> list[dict[str, Any]]:
    """Extract named values, units, and properties below attribute *parent*."""

    records: list[dict[str, Any]] = []
    if parent is None:
        return records
    for element in parent.iter():
        element_tag = _tag(element)
        if element_tag != "Attribute" and not element_tag.endswith("_ATTRIBUTE"):
            continue
        name = (
            element.attrib.get("harmonized_name")
            or element.attrib.get("attribute_name")
            or element.attrib.get("display_name")
            or _text(_child(element, "TAG"))
        )
        value = _text(_child(element, "VALUE")) or _text(element)
        if not name or value is None:
            continue
        record: dict[str, Any] = {"name": name, "value": value}
        units = (
            _text(_child(element, "UNITS"))
            or element.attrib.get("unit")
            or element.attrib.get("units")
        )
        if units:
            record["units"] = units
        if element.attrib:
            record["properties"] = dict(element.attrib)
        records.append(record)
    return records


def _attributes(parent: ET.Element | None) -> dict[str, str | list[str]]:
    """Flatten attribute records below *parent* while preserving repeated names."""

    result: dict[str, str | list[str]] = {}
    for record in _attribute_records(parent):
        value = record["value"]
        if record.get("units"):
            value = f"{value} {record['units']}"
        _append_mapping_value(result, record["name"], value)
    return result


def _identifier_records(parent: ET.Element | None) -> list[dict[str, Any]]:
    """Extract typed and namespaced identifier records from XML *parent*."""

    container = _child(parent, "IDENTIFIERS")
    if container is None:
        container = _child(parent, "Ids")
    if container is None:
        return []
    records: list[dict[str, Any]] = []
    for element in container:
        value = _text(element)
        if value is None:
            continue
        element_tag = _tag(element)
        record: dict[str, Any] = {
            "kind": {
                "PRIMARY_ID": "primary",
                "EXTERNAL_ID": "external",
                "SUBMITTER_ID": "submitter",
                "Id": "id",
            }.get(element_tag, element_tag.lower()),
            "value": value,
        }
        namespace = (
            element.attrib.get("namespace")
            or element.attrib.get("db")
            or element.attrib.get("db_label")
        )
        if namespace:
            record["namespace"] = namespace
        for key in ("label", "is_primary"):
            if key in element.attrib:
                record[key] = element.attrib[key]
        records.append(record)
    return records


def _identifier_value(records: list[dict[str, Any]], namespace: str) -> str | None:
    """Return the first identifier in *records* matching *namespace*."""

    expected = namespace.casefold()
    return next(
        (
            str(record["value"])
            for record in records
            if str(record.get("namespace", "")).casefold() == expected
        ),
        None,
    )


def _links(parent: ET.Element | None) -> list[dict[str, Any]]:
    """Normalize cross-reference, URL, and Entrez links below *parent*."""

    if parent is None:
        return []
    records: list[dict[str, Any]] = []
    for element in parent.iter():
        element_tag = _tag(element)
        if element_tag == "XREF_LINK":
            records.append(
                {
                    "type": "xref",
                    "database": _descendant_text(element, "DB"),
                    "id": _descendant_text(element, "ID"),
                    "label": _descendant_text(element, "LABEL"),
                }
            )
        elif element_tag == "URL_LINK":
            records.append(
                {
                    "type": "url",
                    "label": _descendant_text(element, "LABEL"),
                    "url": _descendant_text(element, "URL"),
                }
            )
        elif element_tag == "ENTREZ_LINK":
            records.append(
                {
                    "type": "entrez",
                    "database": _descendant_text(element, "DB"),
                    "id": _descendant_text(element, "ID"),
                }
            )
        elif element_tag == "Link":
            record = {"type": element.attrib.get("type", "link"), "value": _text(element)}
            record.update(element.attrib)
            records.append(record)
    return records


def _library(experiment: ET.Element) -> dict[str, Any]:
    """Extract library strategy, source, layout, and protocol from *experiment*."""

    descriptor = _descendant(experiment, "LIBRARY_DESCRIPTOR")
    if descriptor is None:
        return {}
    layout_node = _child(descriptor, "LIBRARY_LAYOUT")
    layout = next(iter(layout_node), None) if layout_node is not None else None
    return {
        "name": _descendant_text(descriptor, "LIBRARY_NAME"),
        "strategy": _descendant_text(descriptor, "LIBRARY_STRATEGY"),
        "source": _descendant_text(descriptor, "LIBRARY_SOURCE"),
        "selection": _descendant_text(descriptor, "LIBRARY_SELECTION"),
        "layout": _tag(layout) if layout is not None else None,
        "layout_properties": dict(layout.attrib) if layout is not None else {},
        "construction_protocol": _descendant_text(descriptor, "LIBRARY_CONSTRUCTION_PROTOCOL"),
    }


def _platform(experiment: ET.Element) -> dict[str, Any]:
    """Extract sequencing platform and instrument model from *experiment*."""

    platform = _child(experiment, "PLATFORM")
    implementation = next(iter(platform), None) if platform is not None else None
    if implementation is None:
        return {}
    return {
        "name": _tag(implementation),
        "instrument_model": _descendant_text(implementation, "INSTRUMENT_MODEL"),
    }


def _organization(element: ET.Element | None) -> dict[str, Any] | None:
    """Extract submitting organization and contacts from XML *element*."""

    if element is None:
        return None
    name = _child(element, "Name")
    contacts: list[dict[str, Any]] = []
    for contact in (node for node in element.iter() if _tag(node) == "Contact"):
        contact_name = _child(contact, "Name")
        contacts.append(
            {
                "first_name": _descendant_text(contact_name, "First"),
                "last_name": _descendant_text(contact_name, "Last"),
                "email": contact.attrib.get("email"),
            }
        )
    return {
        "type": element.attrib.get("type"),
        "name": _text(name),
        "abbreviation": name.attrib.get("abbr") if name is not None else None,
        "contacts": contacts,
    }


def _members(parent: ET.Element | None) -> list[dict[str, Any]]:
    """Extract pooled member identifiers and BioSample/GEO links from *parent*."""

    if parent is None:
        return []
    records: list[dict[str, Any]] = []
    for element in (node for node in parent.iter() if _tag(node) == "Member"):
        identifiers = _identifier_records(element)
        record: dict[str, Any] = dict(element.attrib)
        record["identifiers"] = identifiers
        record["biosample"] = _identifier_value(identifiers, "BioSample")
        record["geo"] = _identifier_value(identifiers, "GEO")
        records.append(record)
    return records


def _run_files(run: ET.Element) -> list[dict[str, Any]]:
    """Extract SRA file locations and alternatives from *run*."""

    records: list[dict[str, Any]] = []
    for element in (node for node in run.iter() if _tag(node) == "SRAFile"):
        record: dict[str, Any] = dict(element.attrib)
        record["alternatives"] = [
            dict(alternative.attrib)
            for alternative in element
            if _tag(alternative) == "Alternatives"
        ]
        records.append(record)
    return records


def _study_record(study: ET.Element, *, include_raw: bool) -> dict[str, Any]:
    """Normalize SRA *study* XML; *include_raw* retains its complete tree."""

    identifiers = _identifier_records(study)
    study_type = _descendant(study, "STUDY_TYPE")
    links = _links(_child(study, "STUDY_LINKS"))
    record: dict[str, Any] = {
        "accession": study.attrib.get("accession") or study.attrib.get("alias"),
        "alias": study.attrib.get("alias"),
        "center_name": study.attrib.get("center_name"),
        "identifiers": identifiers,
        "bioproject": _identifier_value(identifiers, "BioProject"),
        "geo": _identifier_value(identifiers, "GEO"),
        "title": _descendant_text(study, "STUDY_TITLE"),
        "type": study_type.attrib.get("existing_study_type") if study_type is not None else None,
        "abstract": _descendant_text(study, "STUDY_ABSTRACT"),
        "center_project_name": _descendant_text(study, "CENTER_PROJECT_NAME"),
        "links": links,
        "pubmed_ids": [
            link["id"]
            for link in links
            if str(link.get("database", "")).casefold() == "pubmed" and link.get("id")
        ],
        "attributes": _attributes(study),
        "attribute_records": _attribute_records(study),
    }
    if include_raw:
        record["raw"] = xml_to_dict(study)
    return record


def _experiment_record(experiment: ET.Element, *, include_raw: bool) -> dict[str, Any]:
    """Normalize SRA *experiment* XML; *include_raw* retains its complete tree."""

    identifiers = _identifier_records(experiment)
    study_ref = _descendant(experiment, "STUDY_REF")
    sample_ref = _descendant(experiment, "SAMPLE_DESCRIPTOR")
    record: dict[str, Any] = {
        "accession": experiment.attrib.get("accession") or experiment.attrib.get("alias"),
        "alias": experiment.attrib.get("alias"),
        "identifiers": identifiers,
        "title": _descendant_text(experiment, "TITLE"),
        "study_accession": study_ref.attrib.get("accession") if study_ref is not None else None,
        "sample_accession": sample_ref.attrib.get("accession") if sample_ref is not None else None,
        "design_description": _descendant_text(experiment, "DESIGN_DESCRIPTION"),
        "library": _library(experiment),
        "platform": _platform(experiment),
        "links": _links(_child(experiment, "EXPERIMENT_LINKS")),
        "attributes": _attributes(_child(experiment, "EXPERIMENT_ATTRIBUTES")),
        "attribute_records": _attribute_records(_child(experiment, "EXPERIMENT_ATTRIBUTES")),
    }
    if include_raw:
        record["raw"] = xml_to_dict(experiment)
    return record


def _sample_record(sample: ET.Element, *, include_raw: bool) -> dict[str, Any]:
    """Normalize SRA *sample* XML; *include_raw* retains its complete tree."""

    identifiers = _identifier_records(sample)
    record: dict[str, Any] = {
        "accession": sample.attrib.get("accession") or sample.attrib.get("alias"),
        "alias": sample.attrib.get("alias"),
        "identifiers": identifiers,
        "biosample": _identifier_value(identifiers, "BioSample"),
        "geo": _identifier_value(identifiers, "GEO"),
        "title": _descendant_text(sample, "TITLE"),
        "organism": _descendant_text(sample, "SCIENTIFIC_NAME"),
        "taxid": _descendant_text(sample, "TAXON_ID"),
        "links": _links(_child(sample, "SAMPLE_LINKS")),
        "attributes": _attributes(_child(sample, "SAMPLE_ATTRIBUTES")),
        "attribute_records": _attribute_records(_child(sample, "SAMPLE_ATTRIBUTES")),
    }
    if include_raw:
        record["raw"] = xml_to_dict(sample)
    return record


def _submission_record(
    submission: ET.Element, organization: ET.Element | None, *, include_raw: bool
) -> dict[str, Any]:
    """Normalize SRA *submission* and *organization*; *include_raw* retains XML."""

    record: dict[str, Any] = {
        "accession": submission.attrib.get("accession") or submission.attrib.get("alias"),
        "alias": submission.attrib.get("alias"),
        "broker_name": submission.attrib.get("broker_name"),
        "center_name": submission.attrib.get("center_name"),
        "lab_name": submission.attrib.get("lab_name"),
        "comment": submission.attrib.get("submission_comment"),
        "identifiers": _identifier_records(submission),
        "organization": _organization(organization),
    }
    if include_raw:
        record["raw"] = xml_to_dict(submission)
    return record


def _run_record(run: ET.Element, *, include_raw: bool) -> dict[str, Any]:
    """Normalize SRA *run* XML; *include_raw* retains its complete tree."""

    experiment_ref = _child(run, "EXPERIMENT_REF")
    statistics = _child(run, "Statistics")
    title_node = _child(run, "TITLE")
    record: dict[str, Any] = {
        "accession": run.attrib.get("accession") or run.attrib.get("alias"),
        "alias": run.attrib.get("alias"),
        "identifiers": _identifier_records(run),
        "title": _text(title_node),
        "experiment_accession": experiment_ref.attrib.get("accession")
        if experiment_ref is not None
        else None,
        "experiment_refname": experiment_ref.attrib.get("refname")
        if experiment_ref is not None
        else None,
        "spots": _integer(run.attrib.get("total_spots")),
        "bases": _integer(run.attrib.get("total_bases")),
        "size_gb": _gigabytes(run.attrib.get("size")),
        "published_at": run.attrib.get("published"),
        "loaded": run.attrib.get("load_done"),
        "is_public": run.attrib.get("is_public"),
        "cluster_name": run.attrib.get("cluster_name"),
        "members": _members(_child(run, "Pool")),
        "files": _run_files(run),
        "statistics": {
            "reads": _integer(statistics.attrib.get("nreads")) if statistics is not None else None,
            "spots": _integer(statistics.attrib.get("nspots")) if statistics is not None else None,
        },
        "attributes": _attributes(run),
        "attribute_records": _attribute_records(run),
    }
    if include_raw:
        record["raw"] = xml_to_dict(run)
    return record


@dataclass
class MetadataBundle:
    """Hold normalized metadata collections and their relations.

    Args:
        packages: Links among experiments, samples, studies, submissions, and runs.
        runs: Normalized SRA run records.
        experiments: Normalized experiment and library records.
        sra_samples: Normalized SRA Sample records.
        studies: Normalized SRA Study records.
        submissions: Normalized submission records.
        biosamples: Linked NCBI BioSample records.
        raw_sra_packages: Optional complete Experiment Package trees.
    """

    packages: list[dict[str, Any]] = field(default_factory=list)
    runs: list[dict[str, Any]] = field(default_factory=list)
    experiments: list[dict[str, Any]] = field(default_factory=list)
    sra_samples: list[dict[str, Any]] = field(default_factory=list)
    studies: list[dict[str, Any]] = field(default_factory=list)
    submissions: list[dict[str, Any]] = field(default_factory=list)
    biosamples: list[dict[str, Any]] = field(default_factory=list)
    raw_sra_packages: list[dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        """Return all bundle collections as one JSON-compatible mapping."""

        return {
            "packages": self.packages,
            "runs": self.runs,
            "experiments": self.experiments,
            "sra_samples": self.sra_samples,
            "studies": self.studies,
            "submissions": self.submissions,
            "biosamples": self.biosamples,
            "raw_sra_packages": self.raw_sra_packages,
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> MetadataBundle:
        """Restore a metadata bundle from serialized mapping *value*."""

        if not isinstance(value, Mapping):
            raise TypeError("Metadata bundle must be a mapping")
        collections: dict[str, list[dict[str, Any]]] = {}
        for name in (
            "packages",
            "runs",
            "experiments",
            "sra_samples",
            "studies",
            "submissions",
            "biosamples",
            "raw_sra_packages",
        ):
            records = value.get(name, [])
            if not isinstance(records, list) or any(not isinstance(row, dict) for row in records):
                raise TypeError(f"Metadata collection {name!r} must be a list of mappings")
            collections[name] = records
        return cls(**collections)

    @classmethod
    def load(cls, path: str | Path, *, progress: ProgressReporter | None = None) -> MetadataBundle:
        """Load a bundle from JSON file or directory *path* with *progress*."""

        source = Path(path)
        if source.is_dir():
            source = source / "metadata.json"
        reporter = get_progress(progress)
        with reporter.task("Load normalized metadata", total=1, unit="bundle") as task:
            bundle = cls.from_dict(read_json(source))
            task.update()
        return bundle

    @staticmethod
    def _selected(
        records: list[dict[str, Any]], accessions: Iterable[str | None]
    ) -> list[dict[str, Any]]:
        """Select *records* whose accession appears in *accessions*."""

        wanted = {accession for accession in accessions if accession}
        return [record for record in records if record.get("accession") in wanted]

    def subset_experiments(self, accessions: Iterable[str]) -> MetadataBundle:
        """Keep experiment *accessions* and every entity linked to them."""
        wanted = set(accessions)
        packages = [row for row in self.packages if row.get("experiment_accession") in wanted]
        samples = self._selected(
            self.sra_samples, (row.get("sra_sample_accession") for row in packages)
        )
        return MetadataBundle(
            packages=packages,
            experiments=self._selected(self.experiments, wanted),
            sra_samples=samples,
            studies=self._selected(self.studies, (row.get("study_accession") for row in packages)),
            submissions=self._selected(
                self.submissions, (row.get("submission_accession") for row in packages)
            ),
            runs=self._selected(
                self.runs, (run for row in packages for run in row.get("run_accessions", []))
            ),
            biosamples=self._selected(self.biosamples, (row.get("biosample") for row in samples)),
            raw_sra_packages=[
                row
                for row in self.raw_sra_packages
                if row.get("EXPERIMENT", {}).get("@attributes", {}).get("accession") in wanted
            ],
        )

    def descriptions_by_sample(
        self,
        *,
        profile: str = "training",
        policy: DescriptionPolicy | None = None,
        legacy_descriptions: Mapping[str, dict[str, Any]] | None = None,
        progress: ProgressReporter | None = None,
    ) -> dict[str, dict[str, Any]]:
        """Build per-sample descriptions using *profile*, *policy*, and *progress*.

        ``training`` returns compact records; ``full`` returns nested provenance.
        *legacy_descriptions* optionally preserves fields from old final outputs.
        """
        if profile == "training":
            return training_descriptions(
                self,
                policy=policy,
                legacy_descriptions=legacy_descriptions,
                progress=progress,
            )
        if profile != "full":
            raise ValueError("description profile must be 'training' or 'full'")

        descriptions: dict[str, dict[str, Any]] = {}
        reporter = get_progress(progress)
        for sample in reporter.track(
            self.sra_samples, "Build full sample descriptions", unit="samples"
        ):
            accession = sample.get("accession")
            if not accession:
                continue
            relations = [
                package
                for package in self.packages
                if package.get("sra_sample_accession") == accession
            ]
            experiments = self._selected(
                self.experiments,
                (relation.get("experiment_accession") for relation in relations),
            )
            studies = self._selected(
                self.studies,
                (relation.get("study_accession") for relation in relations),
            )
            submissions = self._selected(
                self.submissions,
                (relation.get("submission_accession") for relation in relations),
            )
            runs = self._selected(
                self.runs,
                (
                    run_accession
                    for relation in relations
                    for run_accession in relation.get("run_accessions", [])
                ),
            )
            linked_biosample = next(
                (
                    record
                    for record in self.biosamples
                    if record.get("accession") == sample.get("biosample")
                ),
                None,
            )
            descriptions[str(accession)] = {
                "schema_version": "1.0",
                "sra_sample": sample,
                "biosample": linked_biosample,
                "experiments": experiments,
                "studies": studies,
                "submissions": submissions,
                "runs": runs,
                "package_relations": relations,
                "provenance": {
                    "sources": [
                        "NCBI SRA Experiment Package XML",
                        "NCBI BioSample XML",
                    ],
                    "presentation_html_parsed": False,
                },
            }
        return descriptions

    def save_sample_descriptions(
        self,
        directory: Path,
        *,
        profile: str = "training",
        policy: DescriptionPolicy | None = None,
        legacy_descriptions: Mapping[str, dict[str, Any]] | None = None,
        progress: ProgressReporter | None = None,
    ) -> None:
        """Write descriptions below *directory* using *profile*, *policy*, and *progress*.

        *legacy_descriptions* supplies optional compatibility records keyed by sample ID.
        """

        directory.mkdir(parents=True, exist_ok=True)
        reporter = get_progress(progress)
        descriptions = self.descriptions_by_sample(
            profile=profile,
            policy=policy,
            legacy_descriptions=legacy_descriptions,
            progress=reporter,
        )
        written = 0
        unchanged = 0
        for accession, description in reporter.track(
            descriptions.items(), "Save sample descriptions", unit="samples"
        ):
            if atomic_write_json(directory / f"{accession}.json", description):
                written += 1
            else:
                unchanged += 1
        reporter.message(f"Sample description files: {unchanged:,} unchanged; {written:,} written")

    def save(
        self,
        directory: Path,
        *,
        description_profile: str = "training",
        policy: DescriptionPolicy | None = None,
        legacy_descriptions: Mapping[str, dict[str, Any]] | None = None,
        progress: ProgressReporter | None = None,
    ) -> None:
        """Persist complete metadata and selected per-sample descriptions.

        *directory* receives normalized JSON/NDJSON. *description_profile* and
        *policy* control compact files; *legacy_descriptions* preserves old
        values; and *progress* reports generation and writes.
        """

        reporter = get_progress(progress)
        directory.mkdir(parents=True, exist_ok=True)
        reporter.message(f"Save complete normalized metadata to {directory}")
        with reporter.task("Save complete metadata JSON", total=1, unit="file") as task:
            atomic_write_json(directory / "metadata.json", self.to_dict())
            task.update()
        collections = (
            "packages",
            "runs",
            "experiments",
            "sra_samples",
            "studies",
            "submissions",
            "biosamples",
        )
        for name in reporter.track(collections, "Save normalized tables", unit="tables"):
            rows = getattr(self, name)
            if rows:
                text = "\n".join(
                    json.dumps(row, ensure_ascii=False, sort_keys=True) for row in rows
                )
                atomic_write_text(directory / f"{name}.ndjson", text + "\n")
        self.save_sample_descriptions(
            directory / "sample_descriptions",
            profile=description_profile,
            policy=policy,
            legacy_descriptions=legacy_descriptions,
            progress=reporter,
        )

    def attach_to_runs(self, catalog: RunCatalog) -> RunCatalog:
        """Join normalized metadata columns onto *catalog* by entity accession."""

        def text(value: Any) -> str | None:
            """Convert an optional NCBI value to stable nullable text."""

            return None if value in (None, "") else str(value)

        def integer(value: Any) -> int | None:
            """Convert an optional NCBI count to an integer when possible."""

            if value in (None, ""):
                return None
            try:
                return int(float(value))
            except (TypeError, ValueError):
                return None

        def real(value: Any) -> float | None:
            """Convert an optional NCBI size to a floating-point value."""

            if value in (None, ""):
                return None
            try:
                return float(value)
            except (TypeError, ValueError):
                return None

        frame = catalog.frame
        if self.runs:
            run_fields = [
                {
                    "Run": text(row.get("accession")),
                    "sra_run_title": text(row.get("title")),
                    "sra_run_spots": integer(row.get("spots")),
                    "sra_run_bases": integer(row.get("bases")),
                    "sra_run_size_gb": real(row.get("size_gb")),
                    "sra_run_published_at": text(row.get("published_at")),
                }
                for row in self.runs
                if row.get("accession")
            ]
            if run_fields:
                frame = frame.join(
                    pl.DataFrame(
                        run_fields,
                        schema={
                            "Run": pl.String,
                            "sra_run_title": pl.String,
                            "sra_run_spots": pl.Int64,
                            "sra_run_bases": pl.Int64,
                            "sra_run_size_gb": pl.Float64,
                            "sra_run_published_at": pl.String,
                        },
                    ),
                    on="Run",
                    how="left",
                )
        if self.experiments and "Experiment" in frame.columns:
            studies = {row.get("accession"): row for row in self.studies}
            experiment_fields = []
            for row in self.experiments:
                if not row.get("accession"):
                    continue
                library = row.get("library", {})
                platform = row.get("platform", {})
                study = studies.get(row.get("study_accession"), {})
                experiment_fields.append(
                    {
                        "Experiment": text(row["accession"]),
                        "sra_experiment_title": text(row.get("title")),
                        "sra_library_strategy": text(library.get("strategy")),
                        "sra_library_source": text(library.get("source")),
                        "sra_library_selection": text(library.get("selection")),
                        "sra_library_layout": text(library.get("layout")),
                        "sra_library_construction_protocol": text(library.get("construction_protocol")),
                        "sra_instrument_model": text(platform.get("instrument_model")),
                        "sra_study_accession": text(row.get("study_accession")),
                        "sra_study_title": text(study.get("title")),
                        "sra_study_abstract": text(study.get("abstract")),
                        "sra_bioproject": text(study.get("bioproject")),
                    }
                )
            if experiment_fields:
                frame = frame.join(
                    pl.DataFrame(
                        experiment_fields,
                        schema={name: pl.String for name in experiment_fields[0]},
                    ),
                    on="Experiment",
                    how="left",
                )
        if self.sra_samples and "Sample" in frame.columns:
            sample_fields = [
                {
                    "Sample": text(row.get("accession")),
                    "sra_sample_title": text(row.get("title")),
                    "sra_sample_attributes_json": json.dumps(
                        row.get("attributes", {}), sort_keys=True, ensure_ascii=False
                    ),
                }
                for row in self.sra_samples
                if row.get("accession")
            ]
            if sample_fields:
                frame = frame.join(
                    pl.DataFrame(
                        sample_fields,
                        schema={name: pl.String for name in sample_fields[0]},
                    ),
                    on="Sample",
                    how="left",
                )
        if self.biosamples and "BioSample" in frame.columns:
            sample_fields = [
                {
                    "BioSample": text(row["accession"]),
                    "biosample_title": text(row.get("title")),
                    "biosample_organism": text(row.get("organism")),
                    "biosample_attributes_json": json.dumps(
                        row.get("attributes", {}), sort_keys=True, ensure_ascii=False
                    ),
                }
                for row in self.biosamples
                if row.get("accession")
            ]
            if sample_fields:
                frame = frame.join(
                    pl.DataFrame(
                        sample_fields,
                        schema={name: pl.String for name in sample_fields[0]},
                    ),
                    on="BioSample",
                    how="left",
                )
        return catalog.replace_frame(frame, event="attached normalized SRA/BioSample metadata")


class EntrezClient:
    """Call NCBI E-utilities with identity, rate limiting, and optional raw caching."""

    def __init__(
        self,
        *,
        email: str,
        api_key: str | None = None,
        tool: str = "ncbi_dataset_builder",
        cache_dir: Path | None = None,
        http: HttpClient | None = None,
        progress: ProgressReporter | None = None,
    ) -> None:
        """Configure NCBI identity, credentials, cache, and HTTP transport.

        *email* is the required contact, *api_key* enables the higher NCBI rate,
        *tool* names this client, *cache_dir* stores raw responses, *http* can
        inject a custom transport, and *progress* reports cache/network work.
        """

        if not email:
            raise ValueError("NCBI requires a contact email")
        self.email = email
        self.api_key = api_key
        self.tool = tool
        self.cache_dir = cache_dir
        self.progress = get_progress(progress)
        self._statistics_lock = threading.Lock()
        self._cache_hits = 0
        self._network_requests = 0
        self.http = http or HttpClient(
            user_agent=f"{tool}/0.1 ({email})",
            requests_per_second=10.0 if api_key else 3.0,
        )

    def _params(self, values: dict[str, Any]) -> dict[str, Any]:
        """Add configured NCBI identity fields to request parameter *values*."""

        result = {**values, "email": self.email, "tool": self.tool}
        if self.api_key:
            result["api_key"] = self.api_key
        return result

    def _cache_path(self, endpoint: str, params: Mapping[str, Any]) -> Path | None:
        """Return the raw-cache path for *endpoint* and *params*, when configured."""

        if self.cache_dir is None:
            return None
        digest = hashlib.sha256(
            (endpoint + json.dumps(params, sort_keys=True)).encode()
        ).hexdigest()[:20]
        return self.cache_dir / "raw" / f"{endpoint}.{digest}.raw"

    def is_cached(self, endpoint: str, params: Mapping[str, Any]) -> bool:
        """Return whether *endpoint* and *params* have a non-empty raw response."""

        path = self._cache_path(endpoint, params)
        try:
            return path is not None and path.is_file() and path.stat().st_size > 0
        except FileNotFoundError:
            return False

    def statistics(self) -> dict[str, int]:
        """Return thread-safe cumulative raw cache and network request counts."""

        with self._statistics_lock:
            return {
                "raw_cache_hits": self._cache_hits,
                "network_requests": self._network_requests,
            }

    def get(
        self,
        endpoint: str,
        params: dict[str, Any],
        *,
        refresh: bool = False,
        read_cache: bool = True,
    ) -> bytes:
        """Return E-utilities *endpoint* bytes for *params*.

        A non-empty raw response is reused when *read_cache* is true. *refresh*
        bypasses and replaces it with a new successful HTTP response.
        """

        cache = self._cache_path(endpoint, params)
        if cache is not None and read_cache and not refresh:
            try:
                if cache.is_file() and cache.stat().st_size > 0:
                    with self._statistics_lock:
                        self._cache_hits += 1
                    return cache.read_bytes()
            except FileNotFoundError:
                pass
        response = self.http.request(f"{EUTILS}/{endpoint}", params=self._params(params))
        with self._statistics_lock:
            self._network_requests += 1
        if cache is not None:
            atomic_write_bytes(cache, response.body)
        return response.body

    def search_history(self, database: str, query: str, *, refresh: bool = False) -> dict[str, Any]:
        """Create Entrez history for *query* in *database*.

        *refresh* bypasses cached HTTP data. History responses are never read
        from cache because their WebEnv tokens expire.
        """

        payload = json.loads(
            self.get(
                "esearch.fcgi",
                {
                    "db": database,
                    "term": query,
                    "retmode": "json",
                    "retmax": 0,
                    "usehistory": "y",
                },
                refresh=refresh,
                read_cache=False,
            )
        )["esearchresult"]
        return {
            "count": int(payload["count"]),
            "query_key": payload["querykey"],
            "webenv": payload["webenv"],
        }

    def search_ids(
        self,
        database: str,
        query: str,
        *,
        limit: int = 100_000,
        refresh: bool = False,
    ) -> list[str]:
        """Return at most *limit* Entrez IDs for *query* in *database*.

        *refresh* bypasses a cached ESearch response.
        """

        payload = json.loads(
            self.get(
                "esearch.fcgi",
                {"db": database, "term": query, "retmode": "json", "retmax": limit},
                refresh=refresh,
            )
        )["esearchresult"]
        count = int(payload.get("count", 0))
        identifiers = [str(item) for item in payload.get("idlist", [])]
        if count > limit:
            raise MetadataError(
                f"Entrez query resolved to {count} records, above the configured {limit} limit"
            )
        return identifiers

    def resolve_uids(
        self,
        database: str,
        identifiers: Iterable[str],
        *,
        field: str = "Accession",
        batch_size: int = 50,
        refresh: bool = False,
    ) -> list[str]:
        """Resolve *identifiers* from *field* to UIDs in *database*.

        Requests contain at most *batch_size* accessions, and *refresh* bypasses
        cached ESearch responses.
        """

        if batch_size < 1:
            raise ValueError("batch_size must be positive")
        if not re.fullmatch(r"[A-Za-z][A-Za-z ]*", field):
            raise ValueError(f"Unsafe Entrez field: {field!r}")
        unique = list(
            dict.fromkeys(str(value).strip() for value in identifiers if str(value).strip())
        )
        resolved = [value for value in unique if value.isdigit()]
        accessions = [value for value in unique if not value.isdigit()]
        for accession in accessions:
            if not _SAFE_IDENTIFIER.fullmatch(accession):
                raise ValueError(f"Unsafe database identifier: {accession!r}")
        queries = [
            " OR ".join(
                f'"{accession}"[{field}]' for accession in accessions[start : start + batch_size]
            )
            for start in range(0, len(accessions), batch_size)
        ]
        reporter = _client_progress(self)
        cached = (
            sum(
                _response_cached(
                    self,
                    "esearch.fcgi",
                    {"db": database, "term": query, "retmode": "json", "retmax": 100_000},
                )
                for query in queries
            )
            if not refresh
            else 0
        )
        reporter.network_summary(
            f"Resolve {database} accessions",
            cached=cached,
            to_fetch=len(queries) - cached,
            unit="request batches",
        )
        for query in reporter.track(queries, f"Resolve {database} accessions", unit="batches"):
            resolved.extend(self.search_ids(database, query, refresh=refresh))
        return list(dict.fromkeys(resolved))


class SraClient:
    """Fetch and normalize SRA RunInfo and Experiment Package records."""

    def __init__(self, entrez: EntrezClient) -> None:
        """Use configured *entrez* for all SRA requests."""

        self.entrez = entrez

    @staticmethod
    def _parse_runinfo(payload: bytes) -> list[dict[str, Any]]:
        """Parse RunInfo CSV *payload* into row dictionaries."""

        text = payload.decode("utf-8-sig", errors="replace")
        reader = csv.DictReader(io.StringIO(text))
        if not reader.fieldnames or "Run" not in reader.fieldnames:
            raise MetadataError(f"Unexpected SRA RunInfo response header: {reader.fieldnames!r}")
        return [dict(row) for row in reader if row.get("Run")]

    def fetch_runinfo(
        self, query: str, *, page_size: int = 5000, refresh: bool = False
    ) -> RunCatalog:
        """Fetch complete RunInfo for Entrez *query* in pages of *page_size*.

        *refresh* bypasses cached Entrez responses.
        """

        history = self.entrez.search_history("sra", query, refresh=refresh)
        records: list[dict[str, Any]] = []
        starts = range(0, history["count"], page_size)
        for start in _client_progress(self.entrez).track(
            starts,
            "Fetch SRA RunInfo",
            total=(history["count"] + page_size - 1) // page_size,
            unit="pages",
        ):
            payload = self.entrez.get(
                "efetch.fcgi",
                {
                    "db": "sra",
                    "query_key": history["query_key"],
                    "WebEnv": history["webenv"],
                    "rettype": "runinfo",
                    "retmode": "text",
                    "retstart": start,
                    "retmax": page_size,
                },
                refresh=refresh,
            )
            records.extend(self._parse_runinfo(payload))
        if not records:
            raise MetadataError(f"SRA query returned no runs: {query!r}")
        return RunCatalog.from_records(records).deduplicate_runs()

    def fetch_runinfo_ids(
        self, ids: list[str], *, batch_size: int = 200, refresh: bool = False
    ) -> RunCatalog:
        """Fetch RunInfo for SRA *ids* in batches of *batch_size*.

        *refresh* bypasses cached Entrez responses.
        """

        if batch_size < 1:
            raise ValueError("batch_size must be positive")
        uids = self.entrez.resolve_uids("sra", ids, batch_size=min(batch_size, 50), refresh=refresh)
        records: list[dict[str, Any]] = []
        starts = range(0, len(uids), batch_size)
        for start in _client_progress(self.entrez).track(
            starts,
            "Fetch SRA RunInfo by IDs",
            total=(len(uids) + batch_size - 1) // batch_size,
            unit="batches",
        ):
            payload = self.entrez.get(
                "efetch.fcgi",
                {
                    "db": "sra",
                    "id": ",".join(uids[start : start + batch_size]),
                    "rettype": "runinfo",
                    "retmode": "text",
                },
                refresh=refresh,
            )
            records.extend(self._parse_runinfo(payload))
        if not records:
            raise MetadataError("The supplied SRA identifiers resolved to no runs")
        return RunCatalog.from_records(records).deduplicate_runs()

    @staticmethod
    def parse_packages(
        payload: bytes,
        *,
        include_raw: bool = False,
        progress: ProgressReporter | None = None,
    ) -> MetadataBundle:
        """Parse package XML *payload* with optional *progress*.

        *include_raw* retains complete trees.
        """

        try:
            root = ET.fromstring(payload)
        except ET.ParseError as exc:
            raise MetadataError("NCBI returned malformed SRA XML") from exc
        bundle = MetadataBundle()
        seen: dict[str, set[str]] = {
            "packages": set(),
            "runs": set(),
            "experiments": set(),
            "sra_samples": set(),
            "studies": set(),
            "submissions": set(),
        }

        def append_unique(collection: str, record: dict[str, Any]) -> None:
            """Append accession-unique *record* to named bundle *collection*."""

            accession = record.get("accession")
            if not accession or accession in seen[collection]:
                return
            seen[collection].add(str(accession))
            getattr(bundle, collection).append(record)

        packages = [node for node in root.iter() if _tag(node) == "EXPERIMENT_PACKAGE"]
        reporter = get_progress(progress)
        for package in reporter.track(packages, "Parse SRA packages", unit="packages"):
            if include_raw:
                bundle.raw_sra_packages.append(xml_to_dict(package))
            study = _child(package, "STUDY")
            experiment = _child(package, "EXPERIMENT")
            sample = _child(package, "SAMPLE")
            submission = _child(package, "SUBMISSION")
            organization = _child(package, "Organization")
            run_set = _child(package, "RUN_SET")
            runs = [node for node in run_set if _tag(node) == "RUN"] if run_set is not None else []

            if study is not None:
                append_unique("studies", _study_record(study, include_raw=include_raw))
            if experiment is not None:
                append_unique(
                    "experiments", _experiment_record(experiment, include_raw=include_raw)
                )
            if sample is not None:
                append_unique("sra_samples", _sample_record(sample, include_raw=include_raw))
            if submission is not None:
                append_unique(
                    "submissions",
                    _submission_record(submission, organization, include_raw=include_raw),
                )
            for run in runs:
                append_unique("runs", _run_record(run, include_raw=include_raw))

            experiment_accession = (
                experiment.attrib.get("accession") or experiment.attrib.get("alias")
                if experiment is not None
                else None
            )
            package_key = experiment_accession or f"package-{len(bundle.packages)}"
            if package_key in seen["packages"]:
                continue
            seen["packages"].add(package_key)
            bundle.packages.append(
                {
                    "accession": package_key,
                    "experiment_accession": experiment_accession,
                    "study_accession": (
                        study.attrib.get("accession") or study.attrib.get("alias")
                        if study is not None
                        else None
                    ),
                    "sra_sample_accession": (
                        sample.attrib.get("accession") or sample.attrib.get("alias")
                        if sample is not None
                        else None
                    ),
                    "submission_accession": (
                        submission.attrib.get("accession") or submission.attrib.get("alias")
                        if submission is not None
                        else None
                    ),
                    "run_accessions": [
                        accession
                        for run in runs
                        if (accession := run.attrib.get("accession") or run.attrib.get("alias"))
                    ],
                    "members": _members(_child(package, "Pool")),
                    "totals": {
                        "runs": _integer(run_set.attrib.get("runs"))
                        if run_set is not None
                        else None,
                        "spots": _integer(run_set.attrib.get("spots"))
                        if run_set is not None
                        else None,
                        "bases": _integer(run_set.attrib.get("bases"))
                        if run_set is not None
                        else None,
                        "size_gb": _gigabytes(run_set.attrib.get("bytes"))
                        if run_set is not None
                        else None,
                    },
                }
            )
        return bundle

    @staticmethod
    def _merge(target: MetadataBundle, source: MetadataBundle) -> None:
        """Merge accession-unique entity records from *source* into *target*."""

        for name in (
            "packages",
            "runs",
            "experiments",
            "sra_samples",
            "studies",
            "submissions",
        ):
            records = getattr(target, name)
            seen = {record.get("accession") for record in records}
            records.extend(
                record for record in getattr(source, name) if record.get("accession") not in seen
            )
        target.raw_sra_packages.extend(source.raw_sra_packages)

    def fetch_packages(
        self,
        accessions: list[str],
        *,
        batch_size: int = 100,
        include_raw: bool = False,
        refresh: bool = False,
    ) -> MetadataBundle:
        """Fetch package XML for SRA *accessions* by *batch_size*.

        *include_raw* retains complete XML trees alongside normalized records,
        and *refresh* bypasses cached Entrez responses.
        """

        if not accessions:
            return MetadataBundle()
        if batch_size < 1:
            raise ValueError("batch_size must be positive")
        uids = self.entrez.resolve_uids(
            "sra", accessions, batch_size=min(batch_size, 50), refresh=refresh
        )
        if not uids:
            raise MetadataError("The supplied SRA accessions resolved to no Entrez records")
        bundle = MetadataBundle()
        parameters = [
            {
                "db": "sra",
                "id": ",".join(uids[start : start + batch_size]),
                "retmode": "xml",
            }
            for start in range(0, len(uids), batch_size)
        ]
        reporter = _client_progress(self.entrez)
        cached = (
            sum(_response_cached(self.entrez, "efetch.fcgi", params) for params in parameters)
            if not refresh
            else 0
        )
        reporter.network_summary(
            "SRA package XML",
            cached=cached,
            to_fetch=len(parameters) - cached,
            unit="request batches",
        )
        for params in reporter.track(parameters, "Fetch and parse SRA package XML", unit="batches"):
            payload = self.entrez.get(
                "efetch.fcgi",
                params,
                refresh=refresh,
            )
            self._merge(
                bundle,
                self.parse_packages(payload, include_raw=include_raw),
            )
        requested = {accession for accession in accessions if not accession.isdigit()}
        known = {
            str(value)
            for name in (
                "runs",
                "experiments",
                "sra_samples",
                "studies",
                "submissions",
            )
            for record in getattr(bundle, name)
            for value in (
                record.get("accession"),
                record.get("alias"),
                *(identifier.get("value") for identifier in record.get("identifiers", [])),
            )
            if value
        }
        missing = sorted(requested - known)
        if missing:
            raise MetadataError(
                "NCBI did not return metadata for requested SRA accessions: " + ", ".join(missing)
            )
        return bundle


class BioSampleClient:
    """Fetch and normalize BioSample XML records through Entrez."""

    def __init__(self, entrez: EntrezClient) -> None:
        """Use configured *entrez* for all BioSample requests."""

        self.entrez = entrez

    @staticmethod
    def parse(
        payload: bytes,
        *,
        include_raw: bool = False,
        progress: ProgressReporter | None = None,
    ) -> list[dict[str, Any]]:
        """Parse BioSample XML *payload* with optional *progress*.

        *include_raw* retains complete trees.
        """

        try:
            root = ET.fromstring(payload)
        except ET.ParseError as exc:
            raise MetadataError("NCBI returned malformed BioSample XML") from exc
        records: list[dict[str, Any]] = []
        seen: set[str] = set()
        samples = [node for node in root.iter() if _tag(node) == "BioSample"]
        reporter = get_progress(progress)
        for sample in reporter.track(samples, "Parse BioSamples", unit="samples"):
            accession = sample.attrib.get("accession")
            if not accession or accession in seen:
                continue
            seen.add(accession)
            description = _child(sample, "Description")
            organism = _descendant(description, "Organism")
            identifiers = _identifier_records(sample)
            owner = _child(sample, "Owner")
            package = _child(sample, "Package")
            status = _child(sample, "Status")
            owner_name = _child(owner, "Name")
            record: dict[str, Any] = {
                "accession": accession,
                "id": sample.attrib.get("id"),
                "access": sample.attrib.get("access"),
                "publication_date": sample.attrib.get("publication_date"),
                "last_update": sample.attrib.get("last_update"),
                "submission_date": sample.attrib.get("submission_date"),
                "identifiers": identifiers,
                "sra_sample": _identifier_value(identifiers, "SRA"),
                "geo": _identifier_value(identifiers, "GEO"),
                "title": _text(_child(description, "Title")),
                "comment": _text(_child(description, "Comment")),
                "organism": organism.attrib.get("taxonomy_name") if organism is not None else None,
                "taxid": organism.attrib.get("taxonomy_id") if organism is not None else None,
                "owner": {
                    "name": _text(owner_name),
                    "contacts": [
                        {
                            "first_name": _descendant_text(contact, "First"),
                            "last_name": _descendant_text(contact, "Last"),
                            "email": contact.attrib.get("email"),
                        }
                        for contact in (owner.iter() if owner is not None else [])
                        if _tag(contact) == "Contact"
                    ],
                }
                if owner is not None
                else None,
                "models": [
                    value
                    for model in (node for node in sample.iter() if _tag(node) == "Model")
                    if (value := _text(model)) is not None
                ],
                "package": {
                    "name": _text(package),
                    "display_name": package.attrib.get("display_name"),
                }
                if package is not None
                else None,
                "attributes": _attributes(_child(sample, "Attributes")),
                "attribute_records": _attribute_records(_child(sample, "Attributes")),
                "links": _links(_child(sample, "Links")),
                "status": dict(status.attrib) if status is not None else None,
            }
            if include_raw:
                record["raw"] = xml_to_dict(sample)
            records.append(record)
        return records

    def fetch(
        self,
        accessions: list[str],
        *,
        batch_size: int = 100,
        include_raw: bool = False,
        refresh: bool = False,
    ) -> list[dict[str, Any]]:
        """Fetch BioSample *accessions* in batches of *batch_size*.

        *include_raw* retains XML trees, and *refresh* bypasses cached Entrez
        responses.
        """

        if not accessions:
            return []
        if batch_size < 1:
            raise ValueError("batch_size must be positive")
        uids = self.entrez.resolve_uids(
            "biosample", accessions, batch_size=min(batch_size, 50), refresh=refresh
        )
        if not uids:
            raise MetadataError("The supplied BioSample accessions resolved to no Entrez records")
        records: list[dict[str, Any]] = []
        seen: set[str] = set()
        parameters = [
            {
                "db": "biosample",
                "id": ",".join(uids[start : start + batch_size]),
                "retmode": "xml",
            }
            for start in range(0, len(uids), batch_size)
        ]
        reporter = _client_progress(self.entrez)
        cached = (
            sum(_response_cached(self.entrez, "efetch.fcgi", params) for params in parameters)
            if not refresh
            else 0
        )
        reporter.network_summary(
            "BioSample XML",
            cached=cached,
            to_fetch=len(parameters) - cached,
            unit="request batches",
        )
        for params in reporter.track(parameters, "Fetch and parse BioSample XML", unit="batches"):
            payload = self.entrez.get(
                "efetch.fcgi",
                params,
                refresh=refresh,
            )
            for record in self.parse(payload, include_raw=include_raw):
                if record["accession"] not in seen:
                    seen.add(record["accession"])
                    records.append(record)
        requested = {accession for accession in accessions if not accession.isdigit()}
        known = {
            str(value)
            for record in records
            for value in (
                record.get("accession"),
                *(identifier.get("value") for identifier in record.get("identifiers", [])),
            )
            if value
        }
        missing = sorted(requested - known)
        if missing:
            raise MetadataError(
                "NCBI did not return metadata for requested BioSample accessions: "
                + ", ".join(missing)
            )
        return records


def fetch_metadata_for_accessions(
    accessions: list[str],
    *,
    sra: SraClient,
    biosample: BioSampleClient,
    include_raw: bool = False,
    refresh: bool = False,
) -> MetadataBundle:
    """Fetch SRA *accessions* with *sra* and linked samples with *biosample*.

    *include_raw* retains complete XML trees in the resulting bundle, and
    *refresh* bypasses cached Entrez responses.
    """

    bundle = sra.fetch_packages(accessions, include_raw=include_raw, refresh=refresh)
    biosample_accessions = list(
        dict.fromkeys(
            str(record["biosample"]) for record in bundle.sra_samples if record.get("biosample")
        )
    )
    bundle.biosamples = biosample.fetch(
        biosample_accessions, include_raw=include_raw, refresh=refresh
    )
    return bundle


def fetch_metadata_for_catalog(
    catalog: RunCatalog,
    *,
    sra: SraClient,
    biosample: BioSampleClient,
    include_raw: bool = False,
    refresh: bool = False,
) -> MetadataBundle:
    """Enrich run *catalog* using *sra* and *biosample* clients.

    *include_raw* retains complete XML trees in the scoped result, and *refresh*
    bypasses cached Entrez responses.
    """

    runs = [str(value) for value in catalog.frame.get_column("Run").drop_nulls().unique().to_list()]
    bundle = sra.fetch_packages(runs, include_raw=include_raw, refresh=refresh)
    # Entrez returns whole experiments; keep only those containing requested runs.
    wanted_runs = set(runs)
    selected_experiments = {
        row["experiment_accession"]
        for row in bundle.packages
        if wanted_runs.intersection(row.get("run_accessions", []))
    }
    bundle = bundle.subset_experiments(selected_experiments)
    accessions = [
        str(record["biosample"]) for record in bundle.sra_samples if record.get("biosample")
    ]
    if "BioSample" in catalog.frame.columns:
        accessions.extend(
            str(value)
            for value in catalog.frame.get_column("BioSample").drop_nulls().unique().to_list()
        )
    bundle.biosamples = biosample.fetch(
        list(dict.fromkeys(accessions)), include_raw=include_raw, refresh=refresh
    )
    return bundle


class _MarkupTextExtractor(HTMLParser):
    """Collect visible text while discarding HTML presentation tags."""

    def __init__(self) -> None:
        """Initialize an empty text-part buffer with entity conversion enabled."""

        super().__init__(convert_charrefs=True)
        self.parts: list[str] = []

    def handle_data(self, data: str) -> None:
        """Append visible text fragment *data* encountered by the parser."""

        self.parts.append(data)


def sanitize_presentation_markup(value: Any) -> Any:
    """Recursively remove HTML presentation markup from JSON-like *value*."""

    if isinstance(value, dict):
        return {key: sanitize_presentation_markup(item) for key, item in value.items()}
    if isinstance(value, list):
        return [sanitize_presentation_markup(item) for item in value]
    if not isinstance(value, str):
        return value
    decoded = value
    for _ in range(5):
        previous = decoded
        decoded = unescape(decoded)
        if decoded == previous:
            break
    if not _HTML_TAG.search(decoded):
        return decoded
    parser = _MarkupTextExtractor()
    try:
        parser.feed(decoded)
        parser.close()
    except (ValueError, TypeError):
        return decoded
    return " ".join("".join(parser.parts).split())


def sanitize_legacy_metadata(paths: Iterable[Path]) -> list[Path]:
    """Clean legacy JSON files or directories in *paths* and return changed files."""

    files: list[Path] = []
    for path in paths:
        path = Path(path)
        if path.is_dir():
            files.extend(sorted(path.rglob("*.json")))
        elif path.suffix.casefold() == ".json":
            files.append(path)
        else:
            raise ValueError(f"Expected a JSON file or directory, got {path}")
    changed: list[Path] = []
    for path in dict.fromkeys(files):
        try:
            original = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise MetadataError(f"Could not read legacy metadata JSON {path}") from exc
        cleaned = sanitize_presentation_markup(original)
        if cleaned != original:
            atomic_write_json(path, cleaned)
            changed.append(path)
    return changed
