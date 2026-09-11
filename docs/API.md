# API and extension points

## `DatasetBuilder`

```python
DatasetBuilder(
    BuilderConfig(...),
    fastq_provider=None,
    genome_manager=None,
    progress=None,
)
```

Public workflow methods:

- `fetch_runs(query, refresh=False) -> RunCatalog`
- `load_runs(path) -> RunCatalog`
- `fetch_geo_runs(accessions) -> RunCatalog`
- `fetch_metadata(accessions, destination=None, include_raw=False, refresh=False) -> MetadataBundle`
- `enrich_metadata(catalog, destination=None, include_raw=False, refresh=False) -> MetadataBundle`
- `plan(catalog, group_by=..., resources=..., max_batch_gb=..., max_batch_units=..., genome_pins=...) -> DatasetPlan`
- `save_plan(plan, path=None) -> Path`
- `load_plan(path) -> DatasetPlan`
- `build(plan, processor, retry_failed=False, batch_ids=None, processor_id=None, policy=None) -> BuildReport`
- `run_task(plan, task_index, processor, retry_failed=False) -> TaskOutcome`
- `submit_slurm(plan, processor_reference=..., options=..., submit=True, batch_ids=None, policy=None) -> (script, job_id)`
- `status(plan, batch_ids=None) -> dict`
- `publish_dataset(plan, destination=None, mode="auto", overwrite=False) -> DatasetExport`

`prefetch_batch()` and `finalize_distributed_batch()` are public low-level hooks used by the
distributed Slurm dispatcher. Normal callers should use `build()` or `submit_slurm()`.

`fetch_runs`, `fetch_geo_runs`, `fetch_metadata`, and `enrich_metadata` require `BuilderConfig.email`. `fetch_metadata` accepts Study, Sample, Experiment, or Run accessions and resolves them to numeric Entrez UIDs before EFetch. Offline catalog filtering/planning and execution from an existing plan do not. Metadata methods reuse catalog/accession-scoped normalized bundles and completed raw Entrez batches by default; pass `refresh=True` to bypass and replace both cache layers.

## Progress reporting

`DatasetBuilder` creates a `ProgressReporter` from `BuilderConfig.show_progress` and
`BuilderConfig.progress_bars`, or accepts a custom reporter through `progress`. Time-consuming
metadata, download, checksum, archive, planning, execution, and processing operations report
their current phase. Iterable work and GB transfers have progress bars when the optional
`tqdm` dependency is installed.

Metadata messages separately identify normalized bundle-cache hits, raw Entrez-cache hits,
request batches that still require NCBI, and actual network requests made during the operation.
Opaque external tools such as `fastp` and Bowtie are reported at phase start and completion
because they do not provide a stable package-level percentage.

`MetadataBundle.descriptions_by_sample()` combines each SRA Sample with every linked experiment/library, study, submission, run, and BioSample record without flattening collisions. `save()` also writes these documents under `sample_descriptions/`. Use `sanitize_legacy_metadata(paths)` only for old JSON produced by the retired HTML parser; it recursively removes leaked markup and decodes HTML character references.

## `RunCatalog`

The underlying Polars `DataFrame` is available as `catalog.frame` without conversion to pandas.

```python
selected = catalog.filter(
    pl.col("LibraryStrategy").is_in(["ATAC-seq", "ChIP-Seq"])
    & pl.col("ScientificName").str.starts_with("Mus ")
)

selected = selected.filter(
    lambda row: custom_quality_rule(row),
    description="custom quality rule v2",
)
```

Every operation appends a human-readable entry to `catalog.audit`. Python callables are flexible but run row by row; prefer a Polars expression for large catalogs.

`processing_units(by=...)` accepts `run`, `experiment`, `sra_sample`, or `biosample`. `batch_units` uses deterministic first-fit-decreasing packing. An item larger than `max_gb` is put in a one-item batch rather than dropped or retried forever.

## Resources, batches, and execution

`ResourceSpec(threads, memory_gb, time_limit)` is copied to every task created by `plan()`.
`threads` is passed to the FASTQ provider and processor. For local builds, `max_workers` limits
simultaneous tasks, `total_threads` limits workers to
`floor(total_threads / maximum_task_threads)`, and `total_memory_gb` similarly limits workers by
the maximum unit `memory_gb`. Wall time is descriptive locally.

`SlurmOptions(mode="single_node")` requests aggregate CPU and memory for workers inside one
allocation. `mode="distributed"` creates a lightweight coordinator plus one array element per
unit. Each element requests its unit's `ResourceSpec`; `total_cpu_quota` and
`max_running_jobs` throttle array concurrency after reserving `coordinator_cpus` and one job for
the coordinator. `cpus_per_node` rejects impossible unit requests. A comma-separated partition
list is supported.

Batch creation first sorts processing units by decreasing RunInfo `size_MB` estimate, then by
unit ID. Each unit is placed in the first existing batch that satisfies `max_batch_gb` and
`max_batch_units`; otherwise a new batch is created. Oversized units form a one-unit batch.
Batch IDs are zero-based.

`build()` first stages all genomes and raw inputs for the current batch. With the default
`PipelinePolicy(prefetch_batches=1)`, it starts staging the next batch and then processes the
current batch. At most the current and one future batch are resident. Every transition is stored
under `state/batches/<plan>/`; `status()` returns both counts and `batch_manifests`.

`PipelinePolicy` also provides `max_staged_gb`, `minimum_free_gb`, `cleanup`,
`keep_failed_inputs`, and `fsync_logs`. Actual recursive sizes and free space are measured in
decimal GB. Cleanup targets only roots recorded by the provider and refuses paths outside the
workspace FASTQ cache.

## FASTQ model and providers

`FastqSet` contains:

- ordered source Run accessions;
- `read1`, `read2`, and `single` path tuples;
- `SINGLE`, `PAIRED`, or `MIXED` layout;
- source, work/output directories, checksums, and provider metadata.

`SraToolkitProvider.stage()` runs resumable `prefetch` plus `vdb-validate` and publishes a raw
completion marker. `materialize()` later runs `fasterq-dump --split-3`, compression, and merging.
`fetch()` remains as a compatibility composition of both phases. Multi-run gzip streams are
concatenated in RunInfo order. Raw SRA lives transiently under `fastq/<unit>/.raw/<run>/`; each
archive is deleted after its compressed per-run FASTQ validates. Per-run conversion files are
removed after validated unit-level merging. A retained completed unit contains only
`<unit>.fastq.gz` and/or mate files plus `fastq.manifest.json`.

To implement another source:

```python
class MyProvider:
    def fetch(self, unit: ProcessingUnit, destination: Path, *, threads: int) -> FastqSet:
        ...
```

Pass it as `DatasetBuilder(config, fastq_provider=MyProvider())`.

Providers implementing only `fetch()` remain compatible, but their complete FASTQ is downloaded
during the staging slot. A staged provider can additionally implement `stage()` and
`materialize()` with the `StagedFastq` contract.

## Processor

A processor is any callable compatible with:

```python
(FastqSet, GenomeRef, int) -> ProcessingResult
```

It should raise on failure. Returning `success=False`, returning another type, declaring no outputs, or declaring missing/empty outputs also fails the task. Use argument-list subprocess calls and check return codes; do not depend on shell pipelines unless your processor handles every pipeline status.

Local execution accepts a callable object directly or `module:function`. Slurm requires
`module:function` so the coordinator can import it. Each unit gets one append-only log containing
staging, materialization, processor stdout/stderr, package messages, captured command output, and
cleanup. Retry phases append to the same file.

Python `print()` calls are routed automatically. `CommandRunner` captures its command output
automatically. A custom processor that starts a subprocess directly can pass the public
`current_unit_log_handle()` as `stdout` and `stderr` without closing it; the same path is also
available as `fastq.metadata["unit_log_path"]`. OS-level child processes that bypass Python's
streams cannot be captured unless the processor redirects them to that handle.

The first execution binds a saved plan ID to one processor identity: import reference, optional `config` representation, and source-file SHA-256 when source is available. Reusing that plan with a different processor fails before work starts. For dynamically configured local callables, pass an explicit versioned `processor_id` when automatic identity is not sufficient.

## Built-in ATAC processor

```python
from ncbi_dataset_builder.processing import AtacSeqConfig, AtacSeqProcessor

processor = AtacSeqProcessor(
    AtacSeqConfig(
        maximum_insert_size=2000,
        bin_size=1,
        coverage_strands=(),  # one unstranded BigWig
        fastp_deduplicate=True,
        fastp_max_threads=16,
        coverage_ignore_duplicates=True,
        keep_intermediates=True,
    )
)
```

It supports paired, single, and mixed input; mixed input is aligned in separate checked pipelines and merged afterward. Bowtie2 indexes are considered complete only when all six standard or all six large-index files exist. Every subprocess return code is checked. Final BAM, CSI, BigWig, fastp reports, metrics, and tool versions are returned. No input cleanup happens before validation.

The two deepTools `--filterRNAstrand` tracks are opt-in via `coverage_strands=("forward", "reverse")`; their biological meaning must be chosen by the processor author.

## Genome customization

```python
builder.genomes.register_custom(
    taxid=9606,
    scientific_name="Homo sapiens",
    accession="my_reference_v1",
    fasta=Path("references/human.fa.gz"),
)
```

For NCBI genomes, pass `genome_pins={9606: "GCF_...version"}` to `plan`. Always pin the accession version for frozen benchmark datasets.

Downloaded references are stored as `genomes/<accession>.fasta.gz`. NCBI ZIP files are
transient and removed after full gzip/FASTA validation. Bowtie2 indexes are created lazily under
`genomes/indexes/<accession>/`.

## Compact dataset publishing

`publish_dataset()` requires an Experiment-grouped plan whose tasks all succeeded and each
declared exactly one BigWig. It creates `bigWig/<experiment>.bw`,
`descriptions/<experiment>.json`, `genomes/<species>.fasta.gz`, and `manifest.json`. Description
`ID` remains the SRA Sample accession; `Experiment ID` names the output experiment. Metadata for
a sample shared by several experiments is projected back onto the exact experiment before
export.

`mode="auto"` attempts hard links and falls back to copies, `hardlink` requires links, and
`copy` always duplicates files. Publishing builds a private staging tree and atomically swaps it
into place. Existing output requires `overwrite=True`.
