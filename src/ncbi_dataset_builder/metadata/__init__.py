"""NCBI metadata clients and normalized metadata records."""

from .core import (
    BioSampleClient,
    EntrezClient,
    MetadataBundle,
    SraClient,
    fetch_metadata_for_accessions,
    fetch_metadata_for_catalog,
    sanitize_presentation_markup,
)
from .descriptions import DescriptionPolicy, training_fields_by_experiment

__all__ = [
    "BioSampleClient",
    "DescriptionPolicy",
    "EntrezClient",
    "MetadataBundle",
    "SraClient",
    "fetch_metadata_for_accessions",
    "fetch_metadata_for_catalog",
    "sanitize_presentation_markup",
    "training_fields_by_experiment",
]
