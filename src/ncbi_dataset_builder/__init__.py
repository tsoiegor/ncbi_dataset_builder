"""Public API for building reproducible datasets from NCBI resources."""

from .catalog import RunCatalog
from .descriptions import DescriptionPolicy
from .execution import LocalExecutor, SlurmExecutor, SlurmOptions
from .fastq import AtomicDownloader, GeoFastqProvider, SraToolkitProvider
from .genomes import GenomeManager, GenomeSelectionPolicy
from .geo import GeoClient, GeoSupplementaryFile
from .metadata import (
    BioSampleClient,
    EntrezClient,
    MetadataBundle,
    SraClient,
    sanitize_legacy_metadata,
    sanitize_presentation_markup,
)
from .models import (
    DatasetPlan,
    DatasetTask,
    FastqLayout,
    FastqSet,
    GenomeRef,
    ProcessingResult,
    ProcessingUnit,
    ResourceSpec,
)
from .processing import AtacSeqConfig, AtacSeqProcessor, Processor, process_atac
from .workflow import BuilderConfig, DatasetBuilder

__all__ = [
    "AtacSeqConfig",
    "AtacSeqProcessor",
    "AtomicDownloader",
    "BioSampleClient",
    "BuilderConfig",
    "DatasetBuilder",
    "DatasetPlan",
    "DatasetTask",
    "DescriptionPolicy",
    "EntrezClient",
    "FastqLayout",
    "FastqSet",
    "GenomeManager",
    "GenomeRef",
    "GenomeSelectionPolicy",
    "GeoClient",
    "GeoFastqProvider",
    "GeoSupplementaryFile",
    "LocalExecutor",
    "MetadataBundle",
    "ProcessingResult",
    "ProcessingUnit",
    "Processor",
    "ResourceSpec",
    "RunCatalog",
    "SlurmExecutor",
    "SlurmOptions",
    "SraClient",
    "SraToolkitProvider",
    "process_atac",
    "sanitize_legacy_metadata",
    "sanitize_presentation_markup",
]

__version__ = "0.1.2"
