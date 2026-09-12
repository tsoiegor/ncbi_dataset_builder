"""Public API for workspace-centered NCBI dataset construction."""

from .acquisition.fastq import (
    AtomicDownloader,
    GeoFastqProvider,
    SraToolkitProvider,
    StagedFastqProvider,
)
from .acquisition.genomes import GenomeManager, GenomeSelectionPolicy
from .acquisition.geo import GeoClient, GeoSupplementaryFile
from .api import BuilderConfig, BuildReport, DatasetBuilder, UnitOutcome
from .catalog import RunCatalog, validate_polars_runtime
from .execution.config import (
    FilesystemStorage,
    LocalExecution,
    QueuePolicy,
    QuotaStorage,
    SlurmDistributedExecution,
    SlurmSingleNodeExecution,
)
from .metadata import BioSampleClient, EntrezClient, MetadataBundle, SraClient
from .metadata.descriptions import DescriptionPolicy, training_fields_by_experiment
from .models import (
    FastqLayout,
    FastqSet,
    GenomeRef,
    ProcessingResult,
    ProcessingUnit,
    StagedFastq,
)
from .processing import (
    AtacIntermediateFiles,
    AtacSeqConfig,
    AtacSeqProcessor,
    Processor,
    process_atac,
)
from .support.progress import ProgressReporter, ProgressTask
from .workspace import WorkspaceConfig, WorkspaceStore
from .workspace.publishing import DatasetExport, DatasetPublisher, PublishMode

__all__ = [
    "AtacIntermediateFiles",
    "AtacSeqConfig",
    "AtacSeqProcessor",
    "AtomicDownloader",
    "BioSampleClient",
    "BuildReport",
    "BuilderConfig",
    "DatasetBuilder",
    "DatasetExport",
    "DatasetPublisher",
    "DescriptionPolicy",
    "EntrezClient",
    "FastqLayout",
    "FastqSet",
    "FilesystemStorage",
    "GenomeManager",
    "GenomeRef",
    "GenomeSelectionPolicy",
    "GeoClient",
    "GeoFastqProvider",
    "GeoSupplementaryFile",
    "LocalExecution",
    "MetadataBundle",
    "ProcessingResult",
    "ProcessingUnit",
    "Processor",
    "ProgressReporter",
    "ProgressTask",
    "PublishMode",
    "QueuePolicy",
    "QuotaStorage",
    "RunCatalog",
    "SlurmDistributedExecution",
    "SlurmSingleNodeExecution",
    "SraClient",
    "SraToolkitProvider",
    "StagedFastq",
    "StagedFastqProvider",
    "UnitOutcome",
    "WorkspaceConfig",
    "WorkspaceStore",
    "process_atac",
    "training_fields_by_experiment",
    "validate_polars_runtime",
]

__version__ = "0.1.0"
