# `ncbi_dataset_builder` package

This is the public package facade and high-level workspace API.

Install the unreleased package from its checkout with `python -m pip install
.`, or create a Conda Python environment and install the checkout with pip.
The full [installation guide](../../README.md#installation) covers editable
installs, wheels, extras, and external tools.

## Main classes

### `BuilderConfig`

Stable settings shared by every execution: `workspace`, optional NCBI `email`
and `ncbi_api_key`, `genome_policy`, catalog `group_by`, metadata
`description_profile`, SRA Toolkit `prefetch_max_size`, and progress-display
switches. It deliberately contains no CPU, memory, storage, or concurrency
fields. `DatasetBuilder` consumes it and passes its stable values to metadata,
genome, FASTQ, workspace, and Slurm-worker components.

`workspace` accepts `Path`; `group_by` is `run`, `experiment`, `sra_sample`, or
`biosample`; `description_profile` is `training` or `full`;
`prefetch_max_size` accepts values such as `100G` and `u`; progress fields are
Booleans. The email may be omitted for an existing local catalog, while direct
NCBI methods require it.

### `DatasetBuilder`

The main user-facing object. Construct it as `DatasetBuilder(config,
fastq_provider=None, genome_manager=None, progress=None)`. The optional objects
are interfaces from [`acquisition`](acquisition/README.md) and
[`support`](support/README.md); they make non-SRA inputs, custom genomes, and
test transports possible without changing execution code.

- `fetch_runs(query, refresh=False)` fetches a complete SRA RunInfo catalog;
  `query` is an Entrez expression and `refresh=True` bypasses the catalog cache.
- `load_runs(path)` loads a RunInfo CSV from `str` or `Path`.
- `fetch_geo_runs(accessions)` resolves GSE/GSM accessions to SRA runs.
- `fetch_metadata(accessions, destination=None, include_raw=False,
  refresh=False, description_profile=None, description_policy=None)` fetches
  normalized SRA and BioSample records for explicit accessions.
- `enrich_metadata(catalog, ...)` accepts a [`RunCatalog`](catalog/README.md)
  and the same output/refresh/description controls.
- `build(catalog, processor, execution=None, queue=None, group_by=None,
  genome_pins=None, query=None, retry_failed=False, processor_id=None)` streams
  samples locally. `processor` is a callable or `module:object`; `execution` is
  [`LocalExecution`](execution/README.md); `queue` is `QueuePolicy`; and
  `genome_pins` maps taxonomy IDs to exact assembly accessions.
- `submit_slurm(catalog, processor_reference, execution, queue=None, ...,
  script_path=None, submit=True)` writes and optionally submits either a
  single-node or distributed Slurm workflow. The processor must be importable
  on compute nodes.
- `status(execution_id=None)` returns counts and per-sample durable state for an
  explicit or latest execution.
- `publish_dataset(destination=None, execution_id=None, mode="auto",
  overwrite=False)` publishes verified experiment BigWigs, descriptions, and
  genomes. `mode` is `auto`, `hardlink`, or `copy`.

### `BuildReport` and `UnitOutcome`

`BuildReport` contains ordered `outcomes`, its automatic `execution_id`, and
the computed `succeeded`, `failed`, and `skipped` counts. Each `UnitOutcome`
contains `unit_id`, `status`, optional serialized `result`, and optional
`error`. They are returned by `DatasetBuilder.build()` and are not required as
inputs elsewhere.

## Subpackages

- [`acquisition`](acquisition/README.md): FASTQ, GEO, and genome acquisition.
- [`catalog`](catalog/README.md): typed run tables and sample grouping.
- [`cli`](cli/README.md): command-line mapping onto `DatasetBuilder`.
- [`execution`](execution/README.md): system configs, queue records, state, and
  Slurm scripts/workers.
- [`metadata`](metadata/README.md): Entrez/SRA/BioSample clients and normalized
  records.
- [`processing`](processing/README.md): processor protocol and implementations.
- [`support`](support/README.md): low-level commands, progress, logging, and
  safe filesystem helpers.
- [`workspace`](workspace/README.md): durable layout and dataset publication.

## Shared model classes

`models.py` contains immutable values used across subpackages:

- `ProcessingUnit(unit_id, run_accessions, experiment_accessions=(),
  sra_sample_accessions=(), biosample_accessions=(), scientific_name=None,
  taxid=None, total_bases=0, total_size_gb=0, metadata={})` is produced by
  `RunCatalog.processing_units()` and stored in execution records. `to_dict()`
  and `from_dict(value)` provide durable serialization.
- `FastqSet(unit_id, layout, run_accessions, read1=(), read2=(), single=(),
  source="sra", work_dir=Path("."), output_dir=Path("."), checksums={},
  metadata={})` is returned by FASTQ providers and consumed by processors.
  `layout` is `FastqLayout.SINGLE`, `PAIRED`, or `MIXED`; `validate()` checks
  layout/counts and non-empty files; `to_dict()`/`from_dict(value)` serialize
  paths and the enum.
- `StagedFastq(unit_id, source, size_gb, cleanup_roots=(), ready_fastq=None,
  metadata={})` represents downloaded input before processing.
  `cleanup_roots` are the only provider-owned paths the queue may remove;
  `to_dict()` persists it.
- `GenomeRef(taxid, scientific_name, accession, fasta, sha256,
  source_database="NCBI", assembly_level=None, refseq_category=None,
  selection_rationale=(), indexes={})` is returned by `GenomeManager` and
  consumed by processors/publishing. `validate()` requires a non-empty FASTA;
  `to_dict()`/`from_dict(value)` serialize it.
- `ProcessingResult(success, outputs=(), metrics={}, tool_versions={},
  message=None)` is the mandatory processor return. `validate()` requires a
  successful result with non-empty declared files; `to_dict()` persists it.

Acquisition creates these values, execution stores them, processors consume
them, and the workspace persists their serialized forms.
