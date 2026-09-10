# NCBI Dataset Builder

`ncbi-dataset-builder` creates resumable biological datasets from NCBI SRA, GEO,
BioSample, and Genome data. The package is assay-independent: a custom processor receives a
`FastqSet`, a `GenomeRef`, and a CPU-thread count, so the same download and scheduling layer can
support ATAC-seq, RNA-seq, ChIP-seq, or another assay.

The current workflow is workspace-centered. You submit a catalog directly; the builder compares
each unit with durable workspace state, automatically records an execution snapshot under
`jobs/`, and performs only new, changed, failed-with-retry, or damaged work. Batch boundaries and
resource requests are operational details and do not invalidate successful samples.

## Main features

- Paginated SRA RunInfo retrieval and unrestricted Polars filtering.
- Structured SRA Experiment Package and BioSample XML metadata with raw-response and normalized
  bundle caches; no HTML scraping.
- GEO-to-SRA resolution and GEO supplementary FASTQ discovery.
- Resumable SRA Toolkit download, validation, FASTQ conversion, compression, and deterministic
  multi-run merging.
- Deterministic NCBI genome selection, exact accession pins, checksums, and a workspace lockfile.
- Local execution and Slurm execution with per-unit CPU/memory, total CPU quota, running-job
  quota, and node-capacity checks.
- A bounded two-batch pipeline: download the next batch while processing the current batch.
- Visible staging directories, storage accounting in decimal GB, atomic state, progress bars,
  and one append-only log per unit.
- Compact checkout as `bigWig/<experiment>.bw`, `descriptions/<experiment>.json`, and
  `genomes/<species>.fasta.gz` directly in the workspace or in another destination.

The tested legacy implementation remains under `src/multispeciesATACseq_processing/` as reference
material; the package does not import it.

## Install and external tools

Python 3.10 or newer is required.

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install -e '.[dev,progress]'
```

Metadata requests require `NCBI_EMAIL`; an optional `NCBI_API_KEY` raises the E-utilities request
rate. Data and processing require the relevant external programs:

```text
SRA Toolkit: prefetch, vdb-validate, fasterq-dump
Genome retrieval: datasets
Optional FASTQ compression: pigz
Built-in ATAC processor: fastp, bowtie2, bowtie2-build, samtools, bamCoverage
Slurm submission: sbatch
```

Run `ncbi-dataset --workspace workspace preflight --processor
ncbi_dataset_builder.processing.atac:default_atac_processor` before a large job.

Do not install both `polars` and `polars-lts-cpu` in one environment. Restart notebook kernels
after changing Polars or this package.

## Python workflow

```python
from pathlib import Path

import polars as pl

from ncbi_dataset_builder import (
    BuilderConfig,
    DatasetBuilder,
    PipelinePolicy,
    ResourceSpec,
)

builder = DatasetBuilder(
    BuilderConfig(
        workspace=Path("workspace"),
        email="you@institute.org",
        max_workers=4,
        total_threads=32,
        total_memory_gb=128,
        prefetch_max_size="u",  # unlimited; avoids silent skips of runs above 100 GB
        pipeline_policy=PipelinePolicy(
            prefetch_batches=1,
            download_workers=2,
            max_staged_gb=500,
            minimum_free_gb=50,
            cleanup="after_success",
        ),
    )
)

catalog = builder.fetch_runs('ATAC-seq[Strategy] AND "Homo sapiens"[Organism]')
catalog = catalog.filter(
    (pl.col("LibraryLayout") == "PAIRED")
    & (pl.col("spots").cast(pl.Int64) >= 50_000_000)
)

metadata = builder.enrich_metadata(catalog, description_profile="training")
catalog = metadata.attach_to_runs(catalog)

report = builder.build(
    catalog,
    "my_pipeline:process",
    group_by="experiment",
    resources=ResourceSpec(threads=8, memory_gb=32, time_limit="24:00:00"),
    max_batch_gb=100,
    max_batch_units=100,
)
assert report.failed == 0

# Creates/updates workspace/bigWig, workspace/descriptions, and workspace/genomes.
dataset = builder.publish_dataset()
print(dataset.manifest)
```

Calling `build` again with a reordered catalog or different CPU, memory, or batch sizes reuses
successful units. Changing a unit's run accessions, genome pin, grouping semantics, or processor
identity rebuilds that unit. For a dynamically configured callable, pass a versioned
`processor_id`; an import string itself is its identity, so change/version the string when its
behavior changes.

## Processor contract

```python
from ncbi_dataset_builder import FastqSet, GenomeRef, ProcessingResult


def process(fastq: FastqSet, genome: GenomeRef, threads: int) -> ProcessingResult:
    fastq.validate()
    genome.validate()
    output = fastq.output_dir / "coverage.bw"
    output.parent.mkdir(parents=True, exist_ok=True)
    run_pipeline(fastq, genome.fasta, output, threads)
    return ProcessingResult(success=True, outputs=(output,))
```

Every declared output must exist and be non-empty. Processor prints and external commands using
the package command runner are appended to the unit's single log. Direct subprocesses can use
`current_unit_log_handle()` for stdout and stderr.

SRA files are downloaded into `fastq/<unit>/sra/<run>/data.sra`. Per-run FASTQ is written under
visible `runs/`, compressed, validated, and merged at unit level. A validated run's SRA archive is
deleted immediately; successful unit input is deleted after processing by default. Failed input
is retained unless `keep_failed_inputs=False`.

## Slurm

```python
from ncbi_dataset_builder import ResourceSpec, SlurmOptions

script, slurm_job_id = builder.submit_slurm(
    catalog,
    processor_reference="my_pipeline:process",
    max_batch_gb=100,
    options=SlurmOptions(
        resources=ResourceSpec(threads=16, memory_gb=64, time_limit="24:00:00"),
        mode="distributed",
        total_cpu_quota=500,
        max_running_jobs=50,
        coordinator_cpus=1,
        cpus_per_node=128,
        partition="amd_256M,amd_1Tb,amd_2Tb",
    ),
)
```

The coordinator stages batch 0, launches a quota-throttled worker array, starts staging batch 1,
and waits for batch 0 before advancing. With the example above, one coordinator CPU leaves room
for 31 simultaneous 16-CPU units: 497 CPUs and 32 jobs, both below the supplied limits.

## CLI outline

```bash
ncbi-dataset --workspace workspace --email you@institute.org fetch-runs \
  'ATAC-seq[Strategy] AND "Homo sapiens"[Organism]' --output selected.csv

ncbi-dataset --workspace workspace build --catalog selected.csv \
  --processor my_pipeline:process --group-by experiment \
  --threads 8 --memory-gb 32 --max-batch-gb 100 \
  --download-workers 2 --max-staged-gb 500 --minimum-free-gb 50

ncbi-dataset --workspace workspace submit-slurm --catalog selected.csv \
  --processor my_pipeline:process --threads 16 --memory-gb 64 \
  --slurm-mode distributed --total-cpu-quota 500 --max-running-jobs 50 \
  --coordinator-cpus 1 --cpus-per-node 128 \
  --partition amd_256M,amd_1Tb,amd_2Tb

ncbi-dataset --workspace workspace status
ncbi-dataset --workspace workspace publish
```

## Workspace layout

```text
workspace/
  workspace.json       stable workspace semantics and directory roles
  manifest.json        durable unit inventory plus published-dataset manifest
  bigWig/               final <experiment>.bw files
  descriptions/        final <experiment>.json files
  genomes/             final <species>.fasta.gz files
  catalogs/             cached RunInfo tables
  metadata/             normalized metadata and sample descriptions
  metadata_cache/       reusable NCBI response and bundle cache
  fastq/                bounded staged inputs; normally empty after success
  outputs/              generic processor outputs, one directory per unit
  work/                 genome cache and processor/download intermediates
  state/units/          one durable state file per unit
  state/jobs/           per-job batch lifecycle files
  jobs/                 automatic immutable job snapshots and latest.json
  slurm/                generated scheduler scripts
  logs/                 one unit log plus scheduler logs
```

No heavy hidden directories are created. Lock files and atomic temporary files can appear briefly,
but all storage-bearing directories are visible.

See [API.md](docs/API.md), [ARCHITECTURE.md](docs/ARCHITECTURE.md),
[OPERATIONS.md](docs/OPERATIONS.md), and [MIGRATION.md](docs/MIGRATION.md).
