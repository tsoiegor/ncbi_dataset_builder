# NCBI Dataset Builder

`ncbi-dataset-builder` turns NCBI SRA or GEO sequencing records into a
restartable, auditable dataset on your own server. It manages the repetitive
parts of the job—catalog retrieval, metadata, reference genomes, FASTQ
materialization, scheduling, state, logs, and publication—while letting you use
the built-in ATAC-seq workflow or supply your own processor.

The same Python API runs on:

- one ordinary server;
- one large Slurm allocation; or
- many independent Slurm worker jobs.

Start with [Choosing an execution system](docs/ExecutionSystems.md) if you
already know what server or cluster you have.

## Why use this package

### Work starts as soon as it is ready

Samples are independent processing units. The queue overlaps downloads with
processing instead of waiting for the complete study to download first. On a
local server and inside a single Slurm allocation, CPUs are redistributed at
launch time among the samples that are actually ready to run.

### Completed work is reusable

The workspace records a semantic fingerprint for each unit and verifies every
declared output before reusing a success. Catalog responses, NCBI responses,
SRA archives, converted FASTQs, genomes, Bowtie2 indexes, and several ATAC
intermediates also have their own validation-aware caches.

### Storage is bounded deliberately

The queue can limit estimated in-flight data and account for processing
expansion. Local execution checks real filesystem free space; Slurm execution
uses an explicit user or project quota so a large shared filesystem does not
look unlimited.

### NCBI provenance stays attached

RunInfo operations have an immutable audit trail. Processing units retain run,
experiment, SRA Sample, BioSample, species, and taxonomy relationships.
Assembly selection is deterministic, can be pinned to an exact accession, and
is recorded with the output.

### Failures do not erase the whole run

State is atomic per processing unit. One failed sample does not invalidate
successful samples. Logs are also per unit, so retries append to the same
diagnostic history.

### ATAC-seq has a production-oriented default

The built-in processor runs fastp, Bowtie2, samtools, and deepTools; validates
each final file; reuses genome indexes; and exposes explicit retention controls.
For mixed paired/single input, a default-on strict defense drops the complete
single-end branch when its fastp statistics look suspicious or cannot be
validated, then continues with paired-end reads.

## What should I read?

| Your goal | Read this |
| --- | --- |
| Decide how to run on your server or cluster | [Choosing an execution system](docs/ExecutionSystems.md) |
| Install Python and external bioinformatics tools | [Installation](docs/Installation.md) |
| Fetch or filter SRA RunInfo | [Catalogs](docs/Catalogs.md), then the [catalog API](src/ncbi_dataset_builder/catalog/README.md) |
| Run the built-in ATAC-seq pipeline | [ATAC-seq workflow](docs/AtacSeqProcessing.md), then the [ATAC API](src/ncbi_dataset_builder/processing/atac/README.md) |
| Write a different assay processor | [Writing a processor](docs/Processors.md), then the [processing API](src/ncbi_dataset_builder/processing/README.md) |
| Understand files, resume behavior, and cleanup | [Architecture](docs/Architecture.md), [storage](docs/Storage.md), and the [workspace API](src/ncbi_dataset_builder/workspace/README.md) |
| Use shell commands instead of Python | [Command-line interface](docs/CommandLineInterface.md) |
| Look up a class, method, or argument | [Python API index](src/ncbi_dataset_builder/README.md) |

The [documentation index](docs/README.md) also provides short reading paths for
first runs, ATAC-seq users, cluster administrators, and API extenders.

## Installation

Python 3.10 or newer is required. The Python package itself depends on Polars.
The built-in acquisition and ATAC components also call external programs.

```bash
# Create an isolated environment.
conda create -n ncbi-builder python=3.12 -y
conda activate ncbi-builder

# Install tools used by SRA acquisition and the built-in ATAC processor.
conda install -c conda-forge -c bioconda \
  sra-tools ncbi-datasets-cli fastp bowtie2 samtools deeptools -y

# Install this checkout plus progress bars.
python -m pip install -e ".[progress]"
```

Use `".[dev,progress]"` when you also need pytest, coverage, and Ruff. See the
[installation guide](docs/Installation.md) for tool-by-tool requirements and
verification commands.

## Five-minute local example

This example loads an existing RunInfo CSV and processes it on one 32-CPU
server. Replace the paths and resource values with limits that are safe on your
machine.

```python
from pathlib import Path

from ncbi_dataset_builder import (
    BuilderConfig,
    DatasetBuilder,
    FilesystemStorage,
    LocalExecution,
    QueuePolicy,
)
from ncbi_dataset_builder.processing.atac import process_atac

# The workspace is durable state, not a temporary output directory.
builder = DatasetBuilder(
    BuilderConfig(
        workspace=Path("/data/ncbi-workspace"),
        # These are required only for live NCBI requests.
        email="researcher@example.org",
        ncbi_api_key=None,
    )
)

# Loading a CSV is offline. Use builder.fetch_runs(...) for a live SRA query.
catalog = builder.load_runs(Path("runinfo.csv"))

report = builder.build(
    catalog,
    process_atac,
    execution=LocalExecution(
        total_cpus=32,                 # Total processing CPU budget.
        min_cpus_per_job=4,            # Never start a sample with fewer.
        max_cpus_per_job=16,           # Prevent one sample taking all CPUs.
        max_running_jobs=4,            # At most four processors at once.
        storage=FilesystemStorage(
            reserve_free_gb=200,       # Keep 200 GB free on this filesystem.
        ),
    ),
    queue=QueuePolicy(
        download_workers=2,            # Two sample downloads may overlap.
        max_inflight_gb=800,           # Bound estimated active storage.
        processing_storage_multiplier=2,
    ),
)

print(report.execution_id)
print(report.succeeded, report.failed, report.skipped)
```

What happens:

1. The catalog is deduplicated and grouped into processing units.
2. Required metadata and genomes are resolved and cached.
3. Downloads begin within the configured storage window.
4. A ready unit receives CPUs and is passed to `process_atac`.
5. Final outputs are validated, checksummed, and written to durable state.
6. Provider-owned input is removed after success by the default queue policy.
7. Running the same semantic work again reuses valid successful units.

For a custom callable, see [Writing a processor](docs/Processors.md). For Slurm,
use `DatasetBuilder.submit_slurm()` as shown in the
[execution-system guide](docs/ExecutionSystems.md).

## The three execution systems

| System | Where scheduling happens | Processor placement | Path requirement |
| --- | --- | --- | --- |
| `LocalExecution` | Current Python process | Threads in one server process | Workspace and inputs must exist on that server |
| `SlurmSingleNodeExecution` | Inside one submitted allocation | Several sample processors share one node/allocation | Absolute workspace and Python paths must resolve on the compute node |
| `SlurmDistributedExecution` | A small coordinator job | One Slurm job per active sample | The same absolute workspace must be visible to coordinator and every worker |

CPU, memory, concurrency, and storage are intentionally not fields of
`BuilderConfig`. They belong to the chosen execution object, where their
meaning can be enforced consistently. The complete comparison—including path
examples, configuration tables, and selection advice—is in
[Choosing an execution system](docs/ExecutionSystems.md).

## End-to-end data flow

1. `DatasetBuilder.fetch_runs()` or `load_runs()` creates a
   [`RunCatalog`](src/ncbi_dataset_builder/catalog/README.md).
2. Catalog transformations return new audited catalogs.
3. `RunCatalog.processing_units()` groups runs by experiment, run, SRA Sample,
   or BioSample.
4. The builder creates an immutable execution snapshot.
5. The queue resolves a genome and stages SRA/GEO input for each unit.
6. A processor receives `FastqSet`, `GenomeRef`, and the assigned CPU count.
7. The processor returns a validated `ProcessingResult`.
8. Unit state stores output paths, hashes, file metadata, genome provenance,
   FASTQ provenance, and the unit log path.
9. `publish_dataset()` can assemble experiment BigWigs, descriptions, genomes,
   and a manifest into a compact dataset.

## Workspace layout

```text
ncbi-workspace/
├── workspace.json       # Stable grouping and genome-selection semantics.
├── manifest.json        # Latest unit state and optional published dataset.
├── catalogs/            # Cached RunInfo query results.
├── metadata/            # Normalized records and sample descriptions.
├── metadata_cache/      # Reusable raw NCBI responses.
├── genomes/             # Optional in-place published genome FASTAs.
├── fastq/               # Provider-owned staged inputs; cleanup may remove these.
├── work/genome_cache/   # Downloaded references, lockfile, and indexes.
├── work/units/          # Processor intermediates by processing unit.
├── outputs/             # Processor-declared outputs by processing unit.
├── state/units/         # Atomic per-unit status records.
├── executions/          # Immutable execution snapshots.
├── slurm/               # Generated coordinator and sample scripts.
└── logs/                # Per-unit and Slurm logs.
```

These roles are identical in all three execution systems. What changes is
which machine must be able to resolve the absolute workspace path.

## Optimization and performance model

| Optimization | Effect |
| --- | --- |
| Streaming queue | Downloads and processing overlap; the full study need not be staged first. |
| Two-phase providers | Raw SRA can be staged within a bounded window and materialized only when a processor can run. |
| Dynamic launch-time CPU sharing | Ready local/single-node units receive a fair share between configured minimum and maximum limits. |
| Response and artifact caches | Repeated catalog, metadata, FASTQ, genome, index, and processor steps can be reused after validation. |
| Per-unit state and fingerprints | Valid completed work survives restarts; changed semantics invalidate only affected reuse. |
| Atomic writes and partial files | Interrupted downloads and writes do not masquerade as complete artifacts. |
| Checksums and non-empty-file validation | Speed from caching does not rely only on file names. |
| Storage admission | Large samples wait instead of filling the server or exceeding a configured Slurm quota. |
| Independent distributed workers | Multi-node Slurm scales by sample while preserving one shared workspace and coordinator quota. |

The scheduler cannot resize an already submitted distributed Slurm worker.
Each worker keeps the CPU request assigned at launch; later workers can receive
different allocations as capacity changes.

## Built-in ATAC-seq outputs

The default processor always creates one or more BigWigs. With default
retention it also declares the final BAM, CSI index, and fastp JSON/HTML
reports as outputs. Staged/cleaned FASTQs and component BAMs created by the
processor are removed after validation. Bowtie2 indexes and their uncompressed
FASTA are retained for reuse.

Queue cleanup is a separate layer: `QueuePolicy(cleanup="after_success")`
removes only provider-declared roots below `workspace/fastq/` after successful
processing. It does not remove `workspace/work/units/` or
`workspace/outputs/`.

See the [ATAC API reference](src/ncbi_dataset_builder/processing/atac/README.md)
for every command option, output name, mixed-layout rule, and retention flag.

## Documentation map

### User guides

| Page | Purpose |
| --- | --- |
| [Documentation index](docs/README.md) | Reading order by user goal |
| [Installation](docs/Installation.md) | Python, package extras, and external tools |
| [Choosing an execution system](docs/ExecutionSystems.md) | Local, single-node Slurm, and distributed Slurm comparison |
| [Local execution](docs/LocalExecution.md) | Complete ordinary-server workflow |
| [Single-node Slurm](docs/SlurmSingleNodeExecution.md) | One allocation running several samples |
| [Distributed Slurm](docs/SlurmDistributedExecution.md) | Coordinator plus independent sample jobs |
| [Catalogs](docs/Catalogs.md) | CSV/NCBI sources, filtering, and grouping |
| [ATAC-seq processing](docs/AtacSeqProcessing.md) | Built-in assay workflow |
| [Writing a processor](docs/Processors.md) | Custom callable contract |
| [Architecture](docs/Architecture.md) | Lifecycle, state, and directories |
| [Storage](docs/Storage.md) | Free-space and quota policies |
| [CLI](docs/CommandLineInterface.md) | Commands and flags |

### Python API

| Page | Public area |
| --- | --- |
| [Package API](src/ncbi_dataset_builder/README.md) | `BuilderConfig`, `DatasetBuilder`, shared models, errors, and import map |
| [Acquisition API](src/ncbi_dataset_builder/acquisition/README.md) | SRA/GEO FASTQ providers and genome selection |
| [Catalog API](src/ncbi_dataset_builder/catalog/README.md) | `RunCatalog` |
| [Execution API](src/ncbi_dataset_builder/execution/README.md) | Queue, storage, and three execution configs |
| [Metadata API](src/ncbi_dataset_builder/metadata/README.md) | Entrez, SRA, BioSample, bundles, and description projection |
| [Processing API](src/ncbi_dataset_builder/processing/README.md) | Processor contract and loading |
| [ATAC API](src/ncbi_dataset_builder/processing/atac/README.md) | Built-in processor configuration and files |
| [Workspace API](src/ncbi_dataset_builder/workspace/README.md) | Durable layout and publication |
| [Support API](src/ncbi_dataset_builder/support/README.md) | Commands, progress, logging, and filesystem helpers |
| [CLI implementation](src/ncbi_dataset_builder/cli/README.md) | Python-to-command mapping |

Runnable files are indexed in [examples](examples/README.md). Test organization
is described in [tests](tests/README.md).
