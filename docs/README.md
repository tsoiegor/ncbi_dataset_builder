# Documentation guide

These guides explain how to turn an NCBI run catalog into validated outputs on
your own server or Slurm cluster. They are arranged by decisions a user must
make, not by Python source-file order.

If this is your first run, do not begin by copying the largest template.
Identify the machine, storage limit, sample size, and processor requirements
first. The execution guides then show how those facts map to parameters.

Return to the [project README](../README.md) for the short introduction and API
map. Use the [Python API READMEs](../src/ncbi_dataset_builder/README.md) when
you need class-by-class reference documentation.

## Start here

| Your situation | First page | Then read |
| --- | --- | --- |
| One ordinary server, no Slurm | [Local execution](LocalExecution.md) | [Storage](Storage.md), then your processor guide |
| Slurm; one node is large enough | [Single-node Slurm](SlurmSingleNodeExecution.md) | [Storage](Storage.md) and [execution comparison](ExecutionSystems.md) |
| Slurm; samples should be separate jobs | [Distributed Slurm](SlurmDistributedExecution.md) | [Storage](Storage.md) and [execution comparison](ExecutionSystems.md) |
| You do not yet know which system fits | [Choosing an execution system](ExecutionSystems.md) | The selected mode’s complete guide |
| You have a RunInfo CSV | [Catalogs](Catalogs.md) | The selected execution guide |
| You need NCBI to find the runs | [Installation](Installation.md) | [Catalogs](Catalogs.md) |
| You want the built-in ATAC-seq workflow | [ATAC-seq processing](AtacSeqProcessing.md) | The selected execution guide |
| You have another assay or toolchain | [Writing a processor](Processors.md) | [Processing API](../src/ncbi_dataset_builder/processing/README.md) |
| You prefer shell commands | [Command-line interface](CommandLineInterface.md) | The matching execution guide |
| You need to understand restart or files | [Architecture](Architecture.md) | [Storage](Storage.md) |

## Recommended first-run order

1. Read [Installation](Installation.md) and run the preflight checks.
2. Read [Choosing an execution system](ExecutionSystems.md).
3. Prepare the sizing worksheet in that page: available CPUs, memory, storage
   quota, typical sample size, and desired concurrency.
4. Read the complete guide for
   [local](LocalExecution.md),
   [single-node Slurm](SlurmSingleNodeExecution.md), or
   [distributed Slurm](SlurmDistributedExecution.md).
5. Build and inspect a dry-run configuration with one or two samples.
6. Read [Catalogs](Catalogs.md) and the guide for the selected processor.
7. Start the full catalog only after the small run’s outputs, logs, and storage
   growth look correct.

## Where parameters belong

The package deliberately separates stable biological choices from runtime
resource choices.

| Object or call | What it controls | Detailed guide |
| --- | --- | --- |
| `BuilderConfig` | Workspace, NCBI credentials, grouping, metadata profile, SRA archive limit, progress display | [Architecture](Architecture.md) and each execution guide |
| `LocalExecution` | CPUs, local concurrency, and filesystem free-space reserve | [Local execution](LocalExecution.md) |
| `SlurmSingleNodeExecution` | One allocation plus in-allocation CPU, memory, and concurrency limits | [Single-node Slurm](SlurmSingleNodeExecution.md) |
| `SlurmDistributedExecution` | Coordinator resources, worker resources, CPU quota, and job concurrency | [Distributed Slurm](SlurmDistributedExecution.md) |
| `FilesystemStorage` | Usable free space on an ordinary server | [Storage](Storage.md#filesystemstorage) |
| `QuotaStorage` | User/project quota on shared storage | [Storage](Storage.md#quotastorage) |
| `QueuePolicy` | Download concurrency, estimated in-flight storage, cleanup, log flushing, and polling | [Storage](Storage.md#queuepolicy) |
| `DatasetBuilder.build()` | Local processor, execution object, queue, grouping override, genome pins, retry, and processor identity | [Local execution](LocalExecution.md#build-call-parameters) |
| `DatasetBuilder.submit_slurm()` | Slurm processor reference, execution object, queue, retry, script location, and dry run | [Execution comparison](ExecutionSystems.md#submission-parameters) |
| `AtacSeqConfig` | Tool paths and scientific/technical ATAC processing choices | [ATAC-seq processing](AtacSeqProcessing.md) |
| `AtacIntermediateFiles` | Which ATAC-created intermediates remain after success | [ATAC-seq processing](AtacSeqProcessing.md#retention-parameters) |

## Guide map

| Page | Scope |
| --- | --- |
| [Installation](Installation.md) | Python, external tools, cluster prerequisites, and verification |
| [Choosing an execution system](ExecutionSystems.md) | Decision process, path visibility, shared parameters, and comparison |
| [Local execution](LocalExecution.md) | Complete ordinary-server template and parameter sizing |
| [Single-node Slurm](SlurmSingleNodeExecution.md) | Complete one-allocation template and resource admission |
| [Distributed Slurm](SlurmDistributedExecution.md) | Coordinator/worker topology, quotas, job submission, and recovery |
| [Storage](Storage.md) | `FilesystemStorage`, `QuotaStorage`, and every `QueuePolicy` field |
| [Catalogs](Catalogs.md) | Loading, fetching, filtering, grouping, and audit history |
| [ATAC-seq processing](AtacSeqProcessing.md) | Pipeline stages, all configuration fields, mixed-layout defense, and outputs |
| [Writing a processor](Processors.md) | Callable contract, paths, resources, validation, and Slurm imports |
| [Architecture](Architecture.md) | End-to-end flow, workspace tree, state, fingerprints, and cleanup boundaries |
| [Command-line interface](CommandLineInterface.md) | Every CLI option, default, restriction, and Python equivalent |

## Guides versus API reference

These guides answer “what should I choose?” and “what happens next?” The API
READMEs answer “what does this class or method accept?”:

| API area | Reference |
| --- | --- |
| High-level builder and shared models | [Package API](../src/ncbi_dataset_builder/README.md) |
| Catalog manipulation | [Catalog API](../src/ncbi_dataset_builder/catalog/README.md) |
| FASTQ and genome acquisition | [Acquisition API](../src/ncbi_dataset_builder/acquisition/README.md) |
| Execution, storage, queue, and state | [Execution API](../src/ncbi_dataset_builder/execution/README.md) |
| NCBI metadata | [Metadata API](../src/ncbi_dataset_builder/metadata/README.md) |
| Custom processors | [Processing API](../src/ncbi_dataset_builder/processing/README.md) |
| Built-in ATAC processor | [ATAC API](../src/ncbi_dataset_builder/processing/atac/README.md) |
| Workspace and publication | [Workspace API](../src/ncbi_dataset_builder/workspace/README.md) |

## Units used in these guides

| Quantity | Interpretation |
| --- | --- |
| CPU values | Integer logical CPUs or Slurm CPUs |
| Storage and memory fields ending in `_gb` | Decimal GB as used by the package |
| Slurm time strings | Values containing digits, colons, and hyphens, such as `12:00:00` or `2-00:00:00` |
| Processing unit | One run, experiment, SRA Sample, or BioSample, selected by `group_by` |

The examples are starting points, not universal recommendations. Measure a
representative sample before scaling the catalog.
