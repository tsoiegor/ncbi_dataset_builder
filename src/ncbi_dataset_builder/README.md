# `ncbi_dataset_builder` package

This package is the public Python API for turning NCBI sequencing records into
durable, independently restartable processing units.

The [project README](../../README.md) explains why and when to use the package.
This page documents the modules stored directly in `ncbi_dataset_builder/`
and acts as an index to the focused subpackage references.

## Package map

| Path | Responsibility | API documentation |
| --- | --- | --- |
| `api.py` | High-level builder, streaming scheduler, resume decisions, and Slurm submission | [Builder API](#builder-api) |
| `models.py` | Values exchanged between catalog, acquisition, processing, and workspace layers | [Shared data models](#shared-data-models) |
| `errors.py` | Package-specific expected failures | [Exceptions](#exceptions) |
| [`acquisition/`](acquisition/README.md) | SRA/GEO FASTQ providers and genome selection | Providers, downloaders, GEO, assemblies, and genomes |
| [`catalog/`](catalog/README.md) | RunInfo table operations and processing-unit grouping | `RunCatalog` and Polars validation |
| [`execution/`](execution/README.md) | Queue, CPU, memory, storage, state, and Slurm scripts | All three execution configurations |
| [`metadata/`](metadata/README.md) | Entrez/SRA/BioSample retrieval and normalized records | Clients, bundles, descriptions, and HTTP support |
| [`processing/`](processing/README.md) | Assay-processor contract | `Processor` and `load_processor` |
| [`processing/atac/`](processing/atac/README.md) | Built-in ATAC-seq implementation | Every config and retention field |
| [`workspace/`](workspace/README.md) | Durable directories and compact dataset publication | Workspace and publisher classes |
| [`support/`](support/README.md) | Commands, progress, unit logging, and safe file helpers | Advanced support API |
| [`cli/`](cli/README.md) | Argument parsing and command mapping | Command-to-Python reference |
| `__init__.py` | Curated top-level import surface | [Public imports](#public-imports) |

## End-to-end object flow

1. `DatasetBuilder.fetch_runs()` or `load_runs()` returns a
   [`RunCatalog`](catalog/README.md).
2. `RunCatalog.processing_units()` returns `ProcessingUnit` values.
3. The selected [execution configuration](execution/README.md) turns each unit
   into a durable queue item with resource limits.
4. A [FASTQ provider](acquisition/README.md) returns `StagedFastq`, then
   `FastqSet`.
5. `GenomeManager` returns `GenomeRef`.
6. A [processor](processing/README.md) receives `FastqSet`, `GenomeRef`, and
   a CPU count, then returns `ProcessingResult`.
7. The builder validates and checksums outputs, writes unit state, and returns
   `UnitOutcome` values inside `BuildReport`.
8. [`DatasetPublisher`](workspace/README.md) can build a compact model-ready
   dataset from successful experiment units.

## Public imports

All names below are exported from `ncbi_dataset_builder`.

| Area | Top-level names |
| --- | --- |
| Builder | `BuilderConfig`, `DatasetBuilder`, `BuildReport`, `UnitOutcome` |
| Catalog | `RunCatalog`, `validate_polars_runtime` |
| Execution | `FilesystemStorage`, `QuotaStorage`, `QueuePolicy`, `LocalExecution`, `SlurmSingleNodeExecution`, `SlurmDistributedExecution` |
| Acquisition | `AtomicDownloader`, `StagedFastqProvider`, `SraToolkitProvider`, `GeoFastqProvider`, `GeoClient`, `GeoSupplementaryFile`, `GenomeManager`, `GenomeSelectionPolicy` |
| Metadata | `EntrezClient`, `SraClient`, `BioSampleClient`, `MetadataBundle`, `DescriptionPolicy`, `training_fields_by_experiment` |
| Processing | `Processor`, `AtacIntermediateFiles`, `AtacSeqConfig`, `AtacSeqProcessor`, `process_atac` |
| Shared models | `FastqLayout`, `ProcessingUnit`, `StagedFastq`, `FastqSet`, `GenomeRef`, `ProcessingResult` |
| Workspace | `WorkspaceConfig`, `WorkspaceStore`, `PublishMode`, `DatasetExport`, `DatasetPublisher` |
| Progress | `ProgressTask`, `ProgressReporter` |

`GenomeCandidate`, `FastqProvider`, `load_processor`, HTTP support types,
and low-level execution classes are available from their focused subpackages
but are intentionally not added to the top-level namespace.

# Builder API

## `BuilderConfig`

Stable workspace and NCBI configuration:

```python
BuilderConfig(
    workspace,
    email=None,
    ncbi_api_key=None,
    genome_policy=GenomeSelectionPolicy(),
    group_by="experiment",
    description_profile="training",
    prefetch_max_size="u",
    show_progress=True,
    progress_bars=True,
)
```

| Argument | Meaning |
| --- | --- |
| `workspace: Path` | Root for caches, inputs, work, outputs, state, executions, and logs. It is normalized to `Path`. |
| `email: str | None` | NCBI contact email. Direct Entrez methods require a non-empty value; loading an existing CSV does not. |
| `ncbi_api_key: str | None` | Optional NCBI API key passed to Entrez for the higher request rate. |
| `genome_policy: GenomeSelectionPolicy` | Deterministic assembly filtering and ranking used by `GenomeManager`. |
| `group_by` | Default processing-unit level: `"run"`, `"experiment"`, `"sra_sample"`, or `"biosample"`. |
| `description_profile` | `"training"` for compact descriptions or `"full"` for the full normalized projection. |
| `prefetch_max_size: str` | Value sent to SRA Toolkit `prefetch --max-size`, such as `"100G"` or unlimited `"u"`. |
| `show_progress: bool` | Enable direct progress display. Logging records remain separate. |
| `progress_bars: bool` | Use optional tqdm bars when installed; otherwise use throttled text. |

CPU, memory, concurrency, and storage do not belong here. They are specific to
the selected [execution system](execution/README.md).

Constructing `DatasetBuilder` creates the workspace directories and persists
or validates stable workspace semantics. Changing `group_by`,
`description_profile`, or `genome_policy` after unit state exists is
rejected.

## `DatasetBuilder`

```python
DatasetBuilder(
    config,
    *,
    fastq_provider=None,
    genome_manager=None,
    progress=None,
)
```

| Argument | Meaning |
| --- | --- |
| `config: BuilderConfig` | Stable workspace and NCBI configuration. |
| `fastq_provider: FastqProvider | None` | Custom provider; defaults to `SraToolkitProvider`. A staged provider enables download/materialization overlap. |
| `genome_manager: GenomeManager | None` | Custom manager; defaults to one rooted at `workspace/work/genome_cache/`. |
| `progress: ProgressReporter | None` | Custom progress sink; defaults are built from `BuilderConfig`. |

Construction also exposes `workspace`, `state`, `entrez`, `sra`,
`biosample`, and `geo` service attributes. Most users should call the
methods below rather than coordinating those services directly.

### Catalog methods

#### `fetch_runs(query, *, refresh=False) -> RunCatalog`

Fetch a complete SRA RunInfo catalog and cache it below
`workspace/catalogs/`.

| Argument | Meaning |
| --- | --- |
| `query: str` | Entrez SRA expression, for example `'"ATAC-seq"[Strategy] AND "Homo sapiens"[Organism]'`. |
| `refresh: bool` | Bypass the query CSV cache and replace it with a fresh response. |

The configured email is required. The result is deduplicated before return.

#### `load_runs(path) -> RunCatalog`

Static method that loads and deduplicates an existing RunInfo CSV.
`path: str | Path` is read immediately; no NCBI credentials are needed.

#### `fetch_geo_runs(accessions) -> RunCatalog`

`accessions: list[str]` accepts GSE or GSM accessions. GEO links are resolved
through Entrez and the linked SRA RunInfo rows are returned as one deduplicated
catalog.

### Metadata methods

#### `fetch_metadata(accessions, *, ...) -> MetadataBundle`

```python
bundle = builder.fetch_metadata(
    ["SRX123456", "SRR234567"],
    destination=Path("/data/ncbi-workspace/metadata"),
    include_raw=False,
    refresh=False,
    description_profile="training",
    description_policy=None,
)
```

| Argument | Meaning |
| --- | --- |
| `accessions: list[str]` | SRA study, experiment, sample, or run accessions to resolve. |
| `destination: Path | None` | Save directory; defaults to `workspace/metadata/`. |
| `include_raw: bool` | Retain parsed complete SRA/BioSample XML trees in addition to normalized records. |
| `refresh: bool` | Bypass reusable NCBI response caches. |
| `description_profile: str | None` | `"training"`, `"full"`, or `None` to use `BuilderConfig`. |
| `description_policy: DescriptionPolicy | None` | Optional attribute-selection and exact alias policy for compact descriptions. |

The method saves normalized metadata and sample-description files, then returns
the in-memory bundle.

#### `enrich_metadata(catalog, *, ...) -> MetadataBundle`

Uses the same keyword arguments as `fetch_metadata()`, but discovers linked
SRA and BioSample accessions from `catalog: RunCatalog`.

### `build(...)`

```python
build(
    catalog,
    processor,
    *,
    execution=None,
    queue=None,
    group_by=None,
    genome_pins=None,
    query=None,
    retry_failed=False,
    processor_id=None,
) -> BuildReport
```

Run the streaming scheduler in the current process. This is the entry point for
an ordinary server; the default execution is `LocalExecution()`.

| Argument | Meaning |
| --- | --- |
| `catalog: RunCatalog` | Source runs. The builder deduplicates and groups them. |
| `processor: Processor | str` | Callable or importable `"package.module:object"` accepting `(fastq, genome, cpus)`. |
| `execution: LocalExecution | None` | CPU, concurrency, and local free-space settings. |
| `queue: QueuePolicy | None` | Download concurrency, in-flight storage, cleanup, and log durability. |
| `group_by` | Optional override of the builder’s grouping level for this execution. |
| `genome_pins: dict[int, str] | None` | Exact versioned assembly accessions keyed by taxonomy ID. |
| `query: str | None` | Source query stored as provenance; it does not refetch the supplied catalog. |
| `retry_failed: bool` | Retry matching unit state currently marked failed. |
| `processor_id: str | None` | Explicit semantic identity for a dynamic callable when source inspection is not a sufficient version signal. |

The return order matches processing-unit order. Sample exceptions are recorded
as failed `UnitOutcome` values rather than aborting already completed units.

### `submit_slurm(...)`

```python
submit_slurm(
    catalog,
    *,
    processor_reference,
    execution,
    queue=None,
    group_by=None,
    genome_pins=None,
    query=None,
    retry_failed=False,
    script_path=None,
    submit=True,
) -> tuple[Path, str | None]
```

| Argument | Meaning |
| --- | --- |
| `catalog: RunCatalog` | Runs captured in the automatic execution snapshot. |
| `processor_reference: str` | Importable `"package.module:callable"` available on compute nodes. |
| `execution` | `SlurmSingleNodeExecution` or `SlurmDistributedExecution`. |
| `queue: QueuePolicy | None` | Shared sample-streaming controls. |
| `group_by` | Optional grouping override. |
| `genome_pins` | Exact assembly mapping by taxonomy ID. |
| `query` | Optional source-query provenance. |
| `retry_failed` | Forwarded to the coordinator/workers. |
| `script_path: Path | None` | Coordinator script destination; defaults below `workspace/slurm/`. |
| `submit: bool` | Call `sbatch` when true. False writes a dry-run script and returns no job ID. |

The returned tuple contains the coordinator script and scheduler job ID.
See [Choosing an execution system](../../docs/ExecutionSystems.md) for path
visibility and resource differences.

### Status and publication

| Method | Arguments and result |
| --- | --- |
| `status(execution_id=None)` | Load the named or latest execution and return its ID, status counts, and per-unit state. |
| `publish_dataset(destination=None, *, execution_id=None, mode="auto", overwrite=False)` | Publish verified experiment BigWigs, descriptions, and genomes; return `DatasetExport`. See the [workspace API](workspace/README.md). |

## `UnitOutcome`

```python
UnitOutcome(
    unit_id,
    status,
    result=None,
    error=None,
)
```

| Field | Meaning |
| --- | --- |
| `unit_id: str` | Stable processing-unit identifier. |
| `status: str` | `"succeeded"`, `"failed"`, or `"skipped"`. |
| `result: dict | None` | Serialized processing, output, genome, FASTQ, and log data for a success. |
| `error: str | None` | Traceback or skip explanation. |

## `BuildReport`

```python
BuildReport(outcomes, execution_id)
```

| Field/property | Meaning |
| --- | --- |
| `outcomes: tuple[UnitOutcome, ...]` | Ordered results for every requested processing unit. |
| `execution_id: str` | Automatic timestamp/hash identifier. |
| `succeeded` | Computed success count. |
| `failed` | Computed failure count. |
| `skipped` | Count reused from valid workspace state. |

## Internal scheduler carrier

`_PreparedUnit(item, log_path, genome=None, staged=None, outcome=None,
reset_outputs=False)` is a private frozen dataclass used only while the local
streaming scheduler moves a queue item from download to processing. Its fields
hold the source `QueueItem`, unit log path, optional resolved `GenomeRef`,
optional `StagedFastq`, optional terminal `UnitOutcome`, and whether stale
outputs must be reset. It is documented to make the implementation map
complete, but it is not public compatibility API.

# Shared data models

All model classes are frozen dataclasses except `FastqLayout`, which is a
string enum. Serialization methods return JSON-compatible dictionaries.

## `FastqLayout`

| Value | Required `FastqSet` paths |
| --- | --- |
| `FastqLayout.SINGLE` | One or more `single` files, no paired files |
| `FastqLayout.PAIRED` | Equal non-zero `read1` and `read2` counts, no `single` files |
| `FastqLayout.MIXED` | A valid paired component plus one or more `single` files |

## `ProcessingUnit`

```python
ProcessingUnit(
    unit_id,
    run_accessions,
    experiment_accessions=(),
    sra_sample_accessions=(),
    biosample_accessions=(),
    scientific_name=None,
    taxid=None,
    total_bases=0,
    total_size_gb=0.0,
    metadata={},
)
```

| Argument | Meaning |
| --- | --- |
| `unit_id` | Grouping key and user-visible sample identity. |
| `run_accessions` | Ordered SRA runs combined into this unit. |
| `experiment_accessions` | Linked SRX accessions. |
| `sra_sample_accessions` | Linked SRS accessions. |
| `biosample_accessions` | Linked SAMN accessions. |
| `scientific_name`, `taxid` | Single validated species identity for genome selection. |
| `total_bases` | Sum of RunInfo `bases`; used as provenance. |
| `total_size_gb` | Sum of `size_MB / 1000`; used for queue admission estimates. |
| `metadata` | Library strategy/layout/platform and per-run size information. |

`RunCatalog.processing_units()` creates these values; execution records
persist them. `to_dict()` and `from_dict(value)` round-trip them.

## `StagedFastq`

```python
StagedFastq(
    unit_id,
    source,
    size_gb,
    cleanup_roots=(),
    ready_fastq=None,
    metadata={},
)
```

| Argument | Meaning |
| --- | --- |
| `unit_id` | Processing-unit identity. |
| `source` | Provider label such as `"sra"` or `"geo"`. |
| `size_gb` | Measured staged size in decimal GB. |
| `cleanup_roots` | Exact provider-owned roots eligible for queue cleanup. The builder additionally requires them below `workspace/fastq/`. |
| `ready_fastq` | Optional already materialized `FastqSet`, used by GEO and cache hits. |
| `metadata` | Provider-specific staging provenance. |

`to_dict()` serializes paths and an optional ready FASTQ set.

## `FastqSet`

```python
FastqSet(
    unit_id,
    layout,
    run_accessions,
    read1=(),
    read2=(),
    single=(),
    source="sra",
    work_dir=Path("."),
    output_dir=Path("."),
    checksums={},
    metadata={},
)
```

| Argument | Meaning |
| --- | --- |
| `unit_id` | Unit being processed. |
| `layout: FastqLayout` | Single, paired, or mixed structure. |
| `run_accessions` | Source runs in merge order. |
| `read1`, `read2` | Ordered mate files; counts must match. |
| `single` | Single-end or orphan files. |
| `source` | Provider label. |
| `work_dir` | Processor-owned intermediate root assigned by the builder. |
| `output_dir` | Processor final-output root assigned by the builder. |
| `checksums` | SHA-256 values keyed by file path. |
| `metadata` | Provider provenance plus the unit log path. |

| Method | Behavior |
| --- | --- |
| `validate()` | Enforce layout-specific file counts and require every referenced FASTQ to exist and be non-empty. |
| `to_dict()` | Serialize the enum and paths. |
| `from_dict(value)` | Reconstruct paths and `FastqLayout`. |

Processors should call `validate()` before expensive work.

## `GenomeRef`

```python
GenomeRef(
    taxid,
    scientific_name,
    accession,
    fasta,
    sha256,
    source_database="NCBI",
    assembly_level=None,
    refseq_category=None,
    selection_rationale=(),
    indexes={},
)
```

| Argument | Meaning |
| --- | --- |
| `taxid`, `scientific_name` | Organism identity. |
| `accession` | Versioned NCBI assembly or custom reference ID. |
| `fasta: Path` | Local genome FASTA, compressed or uncompressed. |
| `sha256` | Recorded checksum used for cache validation. |
| `source_database` | Usually `"NCBI"` or `"custom"`. |
| `assembly_level`, `refseq_category` | Optional NCBI assembly properties. |
| `selection_rationale` | Human-readable ranking reasons. |
| `indexes` | Optional named index paths. The built-in ATAC processor manages its Bowtie2 index beside the FASTA. |

`validate()` requires a non-empty FASTA. `to_dict()` and
`from_dict(value)` handle path serialization.

## `ProcessingResult`

```python
ProcessingResult(
    success,
    outputs=(),
    metrics={},
    tool_versions={},
    message=None,
)
```

| Argument | Meaning |
| --- | --- |
| `success: bool` | Whether the processor considers the unit complete. |
| `outputs: tuple[Path, ...]` | Every final artifact that defines successful reusable work. |
| `metrics: dict` | JSON-compatible QC or scientific summary data. |
| `tool_versions: dict[str, str]` | External software provenance. |
| `message: str | None` | Optional status/failure explanation. |

`validate()` requires `success=True`, at least one output, and every output
to exist as a non-empty file. The builder calls it, computes output SHA-256
values, and stores `to_dict()` in state.

# Exceptions

Import these from `ncbi_dataset_builder.errors`.

| Class | Meaning |
| --- | --- |
| `DatasetBuilderError` | Base class for expected package runtime errors. |
| `DependencyError` | Python or external dependency is inconsistent or unusable. |
| `CatalogConflictError` | Repeated accession records contradict each other. |
| `ExternalToolError` | Required executable is missing or a command failed. |
| `DownloadError` | Remote input could not be downloaded or validated. |
| `MetadataError` | NCBI metadata is malformed or incomplete. |
| `GenomeSelectionError` | No assembly meets the policy or requested pin. |
| `UnitAlreadyRunning` | A non-stale worker already owns the unit. |
| `ProcessingError` | Processor pipeline or output validation failed. |

Sample-processing exceptions are normally captured into unit state and
`UnitOutcome.error`. Configuration and coordinator-level errors can still
propagate directly.

# Minimal composition example

```python
from pathlib import Path

from ncbi_dataset_builder import (
    BuilderConfig,
    DatasetBuilder,
    LocalExecution,
    QueuePolicy,
)
from ncbi_dataset_builder.processing.atac import process_atac

# Stable API and workspace configuration.
builder = DatasetBuilder(
    BuilderConfig(
        workspace=Path("/data/ncbi-workspace"),
        email="researcher@example.org",
    )
)

# Live NCBI alternative:
# catalog = builder.fetch_runs('"ATAC-seq"[Strategy]')
catalog = builder.load_runs(Path("runinfo.csv"))

# Resource and queue behavior are selected per execution.
report = builder.build(
    catalog,
    process_atac,
    execution=LocalExecution(
        total_cpus=32,
        max_running_jobs=4,
    ),
    queue=QueuePolicy(
        download_workers=2,
        cleanup="after_success",
    ),
)

if report.failed:
    for outcome in report.outcomes:
        if outcome.status == "failed":
            print(outcome.unit_id, outcome.error)
```

Continue with the [execution API](execution/README.md) for resource fields or
the [ATAC API](processing/atac/README.md) for processor configuration.
