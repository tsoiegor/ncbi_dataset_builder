"""Project structured metadata into compact biological descriptions.

Full structured metadata remains the source of truth. This module only projects
it into model input; it never summarizes or truncates protocols or abstracts.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from ..support.progress import ProgressReporter
    from .core import MetadataBundle


def normalized_name(name: str) -> str:
    """Normalize attribute *name* for case-insensitive alias matching."""

    return " ".join(re.sub(r"[_/\-]+", " ", name).casefold().split())


# Aliases select and consolidate field names, never rewrite biological values.
_BIOLOGICAL_GROUPS = {
    "Species": ("organism", "scientific name"),
    "Tissue": (
        "tissue",
        "tissue type",
        "organism part",
        "organ",
        "body site",
        "anatomical site",
        "host tissue sampled",
    ),
    "Source name": ("source name", "isolation source"),
    "Cell type": ("cell type", "celltype"),
    "Cell subtype": ("cell subtype", "cell sub type"),
    "Cell line": ("cell line", "cellline"),
    "Cell markers": (
        "cell population of facs sorting",
        "cell markers",
        "enrichment",
        "sorting markers",
    ),
    "Developmental stage": (
        "developmental stage",
        "development stage",
        "dev stage",
        "stage",
        "differentiation stage",
    ),
    "Life stage": ("life stage", "lifestage"),
    "Age": ("age",),
    "Age at collection": ("animal age at collection", "age at collection"),
    "Age units": ("age units", "age unit"),
    "Sex": ("sex", "gender"),
    "Genotype": ("genotype", "genotype variation", "mutation", "mutant", "genetic modification"),
    "Genetic background": ("genetic background",),
    "Strain": ("strain", "strain background", "line"),
    "Breed": ("breed",),
    "Cultivar": ("cultivar",),
    "Ecotype": ("ecotype",),
    "Isolate": ("isolate",),
    "Disease": ("disease", "disease state"),
    "Disease stage": ("disease stage", "tumor stage"),
    "Health state": ("health state", "health status", "health status at collection"),
    "Phenotype": ("phenotype",),
    "Condition": ("condition", "class", "experimental condition"),
    "Treatment": ("treatment", "intervention", "perturbation", "stimulus", "stimulation"),
    "Compound": ("compound", "drug"),
    "Dose": ("dose", "dosage", "concentration"),
    "Vaccination": ("vaccination",),
    "Exercise group": ("acute exercise group", "exercise group"),
    "Exercise performance (meters)": ("bestdist (meters)",),
    "Feeding state": ("fasted status", "fasting", "feeding state", "diet"),
    "Time": ("time", "time point", "timepoint", "week", "time of acquisition"),
    "Culture conditions": (
        "culture conditions",
        "culture condition",
        "growth condition",
        "growth conditions",
    ),
    "Culture type": ("culture type",),
    "Growth protocol": ("growth protocol",),
    "Cell culture protocol": ("cell culture protocol",),
    "Passage": ("number of passages", "passage", "passage number"),
    "Material": ("material", "sample type"),
    "Sample description": ("sample description", "sample info", "submission description"),
    "Collection protocol": ("specimen collection protocol", "sample collection device or method"),
    "Pool protocol": ("pool creation protocol",),
    "Storage": (
        "store cond",
        "storage condition",
        "storage conditions",
        "specimen with known storage state",
        "sample storage",
    ),
    "Storage processing": ("sample storage processing",),
    "Protocol": ("protocol",),
}
BIOLOGICAL_ATTRIBUTES = {
    normalized_name(alias): output
    for output, aliases in _BIOLOGICAL_GROUPS.items()
    for alias in aliases
}
_MISSING = {
    "",
    "missing",
    "not collected",
    "not provided",
    "not applicable",
    "not available",
    "unknown",
    "n/a",
    "na",
    "null",
    "none provided",
}

@dataclass(frozen=True)
class DescriptionPolicy:
    """Select compact description fields with optional exact custom aliases.

    Experiment attributes are retained because they describe assay conditions.
    Null and placeholder filtering applies to selected sample attributes.
    *extra_attributes* maps normalized source names to output names.
    """

    extra_attributes: Mapping[str, str] = field(default_factory=dict)

    def select_attribute(self, name: str, source: str) -> tuple[str | None, str]:
        """Return the output name and rationale for attribute *name* from *source*."""

        if source == "experiment":
            output = name.removeprefix("Experimental Factor: ").strip()
            if output in {"ID", "Experiments"}:
                output = f"Experiment attribute: {output}"
            return output, "keep: experiment attributes retained by original parser"
        if source not in {"sra_sample", "biosample"}:
            return None, "omit: archive/study bookkeeping; study text is retained separately"
        key = normalized_name(name)
        extras = {normalized_name(k): v for k, v in self.extra_attributes.items()}
        output = extras.get(key, BIOLOGICAL_ATTRIBUTES.get(key))
        if output:
            return output, "keep: biological identity, state, perturbation, or preparation"
        return None, "omit: administrative identifier/date/location or unselected attribute"


def _comparison(value: Any) -> str:
    """Convert *value* to whitespace-normalized text for comparisons."""

    return " ".join(str(value).split())


def _add(target: dict[str, Any], key: str, value: Any, *, keep_missing: bool = False) -> None:
    """Add unique *value* under *key* in *target*; *keep_missing* keeps placeholders."""

    from .core import sanitize_presentation_markup

    if value is None:
        return
    if isinstance(value, (list, tuple)):
        for item in value:
            _add(target, key, item, keep_missing=keep_missing)
        return
    value = sanitize_presentation_markup(value)
    if not keep_missing and _comparison(value).casefold() in _MISSING:
        return
    if key not in target:
        target[key] = value
        return
    previous = target[key] if isinstance(target[key], list) else [target[key]]
    if key == "Tissue":
        # Compare optional bracketed source labels without duplicating the value.
        def tissue_text(item):
            """Remove a bracketed tissue label from *item* for comparison."""

            return re.sub(r"^\[[^]]+\]\s*", "", _comparison(item)).casefold()

        if any(tissue_text(item) == tissue_text(value) for item in previous):
            return
    if all(_comparison(item) != _comparison(value) for item in previous):
        target[key] = [*previous, value]


def _sample_attributes(
    record: dict[str, Any], target: dict[str, Any], policy: DescriptionPolicy
) -> None:
    """Add policy-selected attributes from sample *record* to *target*."""

    records = record.get("attribute_records") or [
        {"name": key, "value": value} for key, value in record.get("attributes", {}).items()
    ]
    for attribute in records:
        output, _ = policy.select_attribute(attribute["name"], "biosample")
        if output is None:
            properties = attribute.get("properties", {})
            output, _ = policy.select_attribute(properties.get("attribute_name", ""), "biosample")
        if output:
            value = attribute["value"]
            properties = attribute.get("properties", {})
            unit = attribute.get("units") or properties.get("unit") or properties.get("units")
            if unit and value is not None:
                value = f"{value} {unit}"
            _add(target, output, value)


def _experiment_description(experiment, study, submission, policy):
    """Combine *experiment*, *study*, and *submission* fields selected by *policy*."""

    row: dict[str, Any] = {}
    _add(row, "Design", experiment.get("design_description"))
    library = experiment.get("library", {})
    for source, destination in (
        ("name", "Name"),
        ("strategy", "Strategy"),
        ("source", "Source"),
        ("selection", "Selection"),
        ("layout", "Layout"),
        ("construction_protocol", "Construction protocol"),
    ):
        _add(row, destination, library.get(source), keep_missing=True)
    _add(row, "Instrument", experiment.get("platform", {}).get("instrument_model"))
    for key, value in library.get("layout_properties", {}).items():
        _add(
            row,
            {"NOMINAL_LENGTH": "Insert size", "NOMINAL_SDEV": "Insert size SD"}.get(
                key.upper(), key
            ),
            value,
        )
    _add(row, "Study", study.get("title"))
    _add(row, "Abstract", study.get("abstract"))
    organization = submission.get("organization") or {}
    name, abbreviation = organization.get("name"), organization.get("abbreviation")
    submitted_by = (
        f"{name} ({abbreviation})"
        if name and abbreviation and name != abbreviation
        else name or submission.get("center_name")
    )
    _add(row, "Submitted by", submitted_by)
    for key, value in experiment.get("attributes", {}).items():
        output, _ = policy.select_attribute(key, "experiment")
        _add(row, output, value, keep_missing=True)
    return row


def training_fields_by_experiment(
    bundle: MetadataBundle,
    *,
    policy: DescriptionPolicy | None = None,
) -> dict[str, dict[str, Any]]:
    """Return compact library and study fields keyed by Experiment accession.

    Args:
        bundle: Normalized SRA metadata and package relationships.
        policy: Optional attribute-selection and renaming policy.

    Raises:
        ValueError: If repeated package relationships describe one Experiment
            inconsistently.
    """

    policy = policy or DescriptionPolicy()
    indexes = {
        name: {row["accession"]: row for row in getattr(bundle, name)}
        for name in ("experiments", "studies", "submissions")
    }
    descriptions: dict[str, dict[str, Any]] = {}
    for relation in bundle.packages:
        accession = relation.get("experiment_accession")
        if not accession:
            continue
        row = _experiment_description(
            indexes["experiments"].get(accession, {}),
            indexes["studies"].get(relation.get("study_accession"), {}),
            indexes["submissions"].get(relation.get("submission_accession"), {}),
            policy,
        )
        previous = descriptions.get(accession)
        if previous is not None and previous != row:
            raise ValueError(f"Conflicting compact metadata for Experiment {accession}")
        descriptions[accession] = row
    return descriptions


def training_descriptions(
    bundle: MetadataBundle,
    *,
    policy: DescriptionPolicy | None = None,
    progress: ProgressReporter | None = None,
) -> dict[str, dict[str, Any]]:
    """Build compact descriptions from *bundle* using *policy* and *progress*.

    Repeated experiments with identical retained descriptions collapse. Shared
    fields appear once at sample level; varying fields stay in Experiments so
    strategy, library protocol and study relationships cannot be mixed.
    """

    from ..support.progress import get_progress

    policy = policy or DescriptionPolicy()
    reporter = get_progress(progress)
    indexes = {
        name: {row["accession"]: row for row in getattr(bundle, name)}
        for name in ("experiments", "studies", "submissions", "biosamples")
    }
    relations: dict[str, list[dict[str, Any]]] = {}
    for package in bundle.packages:
        relations.setdefault(package.get("sra_sample_accession"), []).append(package)
    descriptions = {}
    for sample in reporter.track(bundle.sra_samples, "Build training descriptions", unit="samples"):
        accession = sample["accession"]
        output: dict[str, Any] = {"ID": accession}
        _add(output, "Species", sample.get("organism"))
        biosample = indexes["biosamples"].get(sample.get("biosample"), {})
        _sample_attributes(biosample, output, policy)
        _sample_attributes(sample, output, policy)
        _add(output, "Sample description", biosample.get("comment"))

        experiment_rows = []
        for relation in relations.get(accession, []):
            experiment = indexes["experiments"].get(relation.get("experiment_accession"), {})
            study = indexes["studies"].get(relation.get("study_accession"), {})
            submission = indexes["submissions"].get(relation.get("submission_accession"), {})
            row = _experiment_description(experiment, study, submission, policy)
            if row not in experiment_rows:
                experiment_rows.append(row)
        if experiment_rows:
            shared = {
                key: value
                for key, value in experiment_rows[0].items()
                if all(
                    key in row and _comparison(row[key]) == _comparison(value)
                    for row in experiment_rows[1:]
                )
            }
            for key, value in shared.items():
                _add(output, key, value, keep_missing=True)
            variants = [
                {key: value for key, value in row.items() if key not in shared}
                for row in experiment_rows
            ]
            if len(variants) > 1:
                output["Experiments"] = variants

        # Remove a redundant source label already represented by Tissue.
        source = output.get("Source name")
        tissue_values = output.get("Tissue", [])
        if not isinstance(tissue_values, list):
            tissue_values = [tissue_values]
        if (
            source is not None
            and any(
                _comparison(source).casefold()
                == re.sub(r"^\[[^]]+\]\s*", "", _comparison(value)).casefold()
                for value in tissue_values
            )
        ):
            del output["Source name"]
        descriptions[accession] = output
    return descriptions
