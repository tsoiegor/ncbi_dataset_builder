"""Public API for building reproducible datasets from NCBI resources."""

from .catalog import RunCatalog, validate_polars_runtime
from .descriptions import DescriptionPolicy
from .execution import LocalExecutor, SlurmExecutor, SlurmOptions
from .fastq import AtomicDownloader, GeoFastqProvider, SraToolkitProvider, StagedFastqProvider
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
    StagedFastq,
)
from .pipeline import BatchManifest, BatchStateStore, PipelinePolicy
from .processing import AtacSeqConfig, AtacSeqProcessor, Processor, process_atac
from .progress import ProgressReporter, ProgressTask
from .unit_logging import current_unit_log_handle, current_unit_log_path
from .workflow import BuilderConfig, DatasetBuilder

__all__ = [
    "AtacSeqConfig",
    "AtacSeqProcessor",
    "AtomicDownloader",
    "BatchManifest",
    "BatchStateStore",
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
    "PipelinePolicy",
    "ProcessingResult",
    "ProcessingUnit",
    "Processor",
    "ProgressReporter",
    "ProgressTask",
    "ResourceSpec",
    "RunCatalog",
    "SlurmExecutor",
    "SlurmOptions",
    "SraClient",
    "SraToolkitProvider",
    "StagedFastq",
    "StagedFastqProvider",
    "current_unit_log_handle",
    "current_unit_log_path",
    "process_atac",
    "sanitize_legacy_metadata",
    "sanitize_presentation_markup",
    "validate_polars_runtime",
]

__version__ = "0.2.0"
