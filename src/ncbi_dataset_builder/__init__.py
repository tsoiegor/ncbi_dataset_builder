"""Public API for building reproducible datasets from NCBI resources."""

from .catalog import RunCatalog, validate_polars_runtime
from .descriptions import DescriptionPolicy, training_fields_by_experiment
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
    DatasetTask,
    FastqLayout,
    FastqSet,
    GenomeRef,
    ProcessingResult,
    ProcessingUnit,
    ResourceSpec,
    StagedFastq,
    WorkspaceJob,
)
from .pipeline import BatchManifest, BatchStateStore, PipelinePolicy
from .processing import AtacSeqConfig, AtacSeqProcessor, Processor, process_atac
from .progress import ProgressReporter, ProgressTask
from .publishing import DatasetExport, DatasetPublisher, PublishMode
from .unit_logging import current_unit_log_handle, current_unit_log_path
from .workflow import BuilderConfig, DatasetBuilder
from .workspace import WorkspaceConfig, WorkspaceStore

__all__ = [
    "AtacSeqConfig",
    "AtacSeqProcessor",
    "AtomicDownloader",
    "BatchManifest",
    "BatchStateStore",
    "BioSampleClient",
    "BuilderConfig",
    "DatasetBuilder",
    "DatasetExport",
    "DatasetPublisher",
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
    "PublishMode",
    "ResourceSpec",
    "RunCatalog",
    "SlurmExecutor",
    "SlurmOptions",
    "SraClient",
    "SraToolkitProvider",
    "StagedFastq",
    "StagedFastqProvider",
    "WorkspaceConfig",
    "WorkspaceJob",
    "WorkspaceStore",
    "current_unit_log_handle",
    "current_unit_log_path",
    "process_atac",
    "sanitize_legacy_metadata",
    "sanitize_presentation_markup",
    "training_fields_by_experiment",
    "validate_polars_runtime",
]

__version__ = "0.5.0"
