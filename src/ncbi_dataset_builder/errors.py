"""Package-specific exceptions with actionable error messages."""


class DatasetBuilderError(RuntimeError):
    """Base class for expected dataset construction errors."""


class DependencyError(DatasetBuilderError):
    """A required Python or external dependency is inconsistent or unusable."""


class CatalogConflictError(DatasetBuilderError):
    """The same accession has contradictory catalog records."""


class ExternalToolError(DatasetBuilderError):
    """An external command is missing or failed."""


class DownloadError(DatasetBuilderError):
    """A remote artifact could not be downloaded or validated."""


class MetadataError(DatasetBuilderError):
    """Remote metadata was malformed or incomplete."""


class GenomeSelectionError(DatasetBuilderError):
    """No genome meets the configured policy."""


class TaskAlreadyRunning(DatasetBuilderError):
    """A non-stale worker already owns a task."""


class ProcessingError(DatasetBuilderError):
    """A processor failed or produced invalid outputs."""
