# API and extension points

## `DatasetBuilder`

```python
DatasetBuilder(BuilderConfig(...), fastq_provider=None, genome_manager=None)
```

Public workflow methods:

- `fetch_runs(query, refresh=False) -> RunCatalog`
- `load_runs(path) -> RunCatalog`
- `fetch_geo_runs(accessions) -> RunCatalog`
- `fetch_metadata(accessions, destination=None, include_raw=False) -> MetadataBundle`
- `enrich_metadata(catalog, destination=None, include_raw=False) -> MetadataBundle`
- `plan(catalog, group_by=..., resources=..., max_batch_bytes=..., max_batch_units=..., genome_pins=...) -> DatasetPlan`
- `save_plan(plan, path=None) -> Path`
- `load_plan(path) -> DatasetPlan`
- `build(plan, processor, retry_failed=False, batch_ids=None, processor_id=None) -> BuildReport`
- `run_task(plan, task_index, processor, retry_failed=False) -> TaskOutcome`
- `submit_slurm(plan, processor_reference=..., options=..., submit=True, batch_ids=None) -> (script, job_id)`
- `status(plan, batch_ids=None) -> dict`

`fetch_runs`, `fetch_geo_runs`, `fetch_metadata`, and `enrich_metadata` require `BuilderConfig.email`. `fetch_metadata` accepts Study, Sample, Experiment, or Run accessions and resolves them to numeric Entrez UIDs before EFetch. Offline catalog filtering/planning and execution from an existing plan do not.

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

`processing_units(by=...)` accepts `run`, `experiment`, `sra_sample`, or `biosample`. `batch_units` uses deterministic first-fit-decreasing packing. An item larger than the byte target is put in a one-item batch rather than dropped or retried forever.

## FASTQ model and providers

`FastqSet` contains:

- ordered source Run accessions;
- `read1`, `read2`, and `single` path tuples;
- `SINGLE`, `PAIRED`, or `MIXED` layout;
- source, work/output directories, checksums, and provider metadata.

`SraToolkitProvider` uses SRA Toolkit's resumable cache path, validates it with `vdb-validate`, converts with `fasterq-dump --split-3`, compresses with `pigz` when available, and publishes a manifest only after all outputs exist. Multi-run gzip streams are concatenated in RunInfo order; concatenated gzip members are standards-compliant and avoid decompress/recompress cost.

To implement another source:

```python
class MyProvider:
    def fetch(self, unit: ProcessingUnit, destination: Path, *, threads: int) -> FastqSet:
        ...
```

Pass it as `DatasetBuilder(config, fastq_provider=MyProvider())`.

## Processor

A processor is any callable compatible with:

```python
(FastqSet, GenomeRef, int) -> ProcessingResult
```

It should raise on failure. Returning `success=False`, returning another type, declaring no outputs, or declaring missing/empty outputs also fails the task. Use argument-list subprocess calls and check return codes; do not depend on shell pipelines unless your processor handles every pipeline status.

Local execution accepts a callable object directly or `module:function`. Slurm requires `module:function` so each independent worker can import it.

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
