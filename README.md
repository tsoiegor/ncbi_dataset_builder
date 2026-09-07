# NCBI Dataset Builder

This repository is now an installable Python API for constructing reproducible biological datasets from NCBI SRA, GEO, BioSample, and Genome data. The original multispecies ATAC-seq scripts and data remain in the repository as legacy reference material; new work should use `ncbi_dataset_builder`.

The package is assay-independent at its boundary. A processor receives exactly:

```python
def process(fastq: FastqSet, genome: GenomeRef, threads: int) -> ProcessingResult:
    ...
```

That function can build ATAC-seq, RNA-seq, ChIP-seq, or another dataset. A safer built-in ATAC example is included, but it is not coupled to downloading or scheduling.

## What is implemented

- Complete paginated SRA RunInfo retrieval through NCBI E-utilities.
- Arbitrary filtering with Polars expressions or Python callables, plus safe CLI filters.
- Structured SRA Experiment Package and BioSample XML metadata, including study, submission, library, platform, sample, run/file, and identifier fields; normalized tables; page-complete per-sample JSON; and cached full raw responses. No HTML scraping.
- GEO GSE/GSM-to-SRA resolution and GEO MINiML supplementary-file discovery.
- Resumable `prefetch` + `vdb-validate` + `fasterq-dump` conversion, deterministic multi-run merging, and explicit preservation of split-3 orphan reads.
- NCBI Datasets genome discovery, deterministic quality ranking, user pins, checksums, and a genome lockfile.
- Local bounded parallelism and real Slurm `sbatch` job arrays.
- Atomic task state, explicit retry of failed work, cache manifests, checksums, and provenance.
- A configurable ATAC processor using `fastp`, Bowtie2, samtools, and deepTools with checked pipeline exit codes.
- Unit/integration-style tests that mock external services and binaries.

## Install

Python 3.10 or newer is required.

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install -e '.[dev]'
```

On Windows, activate with `.venv\Scripts\activate`. NCBI metadata calls require a contact email; set `NCBI_EMAIL`. An `NCBI_API_KEY` is optional and raises the configured E-utilities rate from 3 to 10 requests per second.

Data download and built-in ATAC processing use external executables:

```text
NCBI SRA Toolkit: prefetch, vdb-validate, fasterq-dump
NCBI Datasets CLI: datasets
Optional compression: pigz
Built-in ATAC processor: fastp, bowtie2, bowtie2-build, samtools, bamCoverage
Slurm submission: sbatch
```

Run `ncbi-dataset --workspace workspace preflight --processor ncbi_dataset_builder.processing.atac:default_atac_processor` before a large ATAC job.

## Python workflow

```python
from pathlib import Path

import polars as pl

from ncbi_dataset_builder import BuilderConfig, DatasetBuilder, ResourceSpec

builder = DatasetBuilder(
    BuilderConfig(
        workspace=Path("workspace"),
        email="you@institute.org",
        max_workers=4,
        total_threads=32,
    )
)

catalog = builder.fetch_runs('ATAC-seq[Strategy] AND "Homo sapiens"[Organism]')

# Polars expressions are unrestricted.
catalog = catalog.filter(
    (pl.col("LibraryLayout") == "PAIRED")
    & (pl.col("spots").cast(pl.Int64) >= 50_000_000)
)

# A callable is also accepted when the condition is easier in Python.
catalog = catalog.filter(
    lambda row: "tumor" not in (row.get("SampleName") or "").lower(),
    description="exclude tumor labels",
)

metadata = builder.enrich_metadata(catalog)
catalog_with_metadata = metadata.attach_to_runs(catalog)

# Any SRA accession level is accepted directly (SRP/SRS/SRX/SRR and ENA/DDBJ equivalents).
one_sample = builder.fetch_metadata(["SRS4739189"])
description = one_sample.descriptions_by_sample()["SRS4739189"]

plan = builder.plan(
    catalog_with_metadata,
    group_by="experiment",
    resources=ResourceSpec(threads=8, memory_gb=32, time_limit="24:00:00"),
    max_batch_bytes=100_000_000_000,
    max_batch_units=100,
)
builder.save_plan(plan, Path("workspace/plans/atac.json"))

report = builder.build(plan, "my_pipeline:process")
assert report.failed == 0
```

The default grouping level is Experiment, the SRA entity that defines a library and references one sample. Grouping by BioSample is available but explicit because it can combine multiple experiments and protocols.

## Custom processor contract

```python
from ncbi_dataset_builder import FastqSet, GenomeRef, ProcessingResult


def process(fastq: FastqSet, genome: GenomeRef, threads: int) -> ProcessingResult:
    # fastq.layout is SINGLE, PAIRED, or MIXED.
    # Mixed means paired reads plus split-3 orphan/single reads are both present.
    fastq.validate()
    genome.validate()

    output = fastq.output_dir / "model-dataset.bin"
    output.parent.mkdir(parents=True, exist_ok=True)
    run_your_pipeline(fastq, genome.fasta, output, threads)

    return ProcessingResult(
        success=True,
        outputs=(output,),
        metrics={"records": count_records(output)},
        tool_versions={"my-tool": get_version()},
    )
```

Success is committed only after every declared output exists and is non-empty. Inputs are retained by default after both success and failure. For Slurm, the function must be importable on compute nodes as `module:function`; a notebook closure is local-only.

The built-in example is `ncbi_dataset_builder.processing.atac:process_atac`. Its default BigWig is unstranded because ATAC-seq is not an RNA stranded-library protocol. To intentionally reproduce the old two-track deepTools behavior, construct `AtacSeqProcessor(AtacSeqConfig(coverage_strands=("forward", "reverse")))` in your own importable wrapper.

## GEO

For raw sequencing deposited through GEO, resolve GEO records to their linked SRA runs and use the normal SRA provider:

```python
catalog = builder.fetch_geo_runs(["GSE12345", "GSM123456"])
```

GEO supplementary FASTQ files can also be discovered from a GSE MINiML package:

```python
files = builder.geo.discover_supplementary("GSE12345")
fastq_urls = [item.url for item in files if item.filename.endswith((".fastq.gz", ".fq.gz"))]
```

Supplementary file names are depositor-controlled. `GeoFastqProvider` recognizes common `R1`/`R2` and `_1`/`_2` names and refuses unequal mate counts; ambiguous naming should be mapped explicitly by a custom provider.

## Slurm

```python
from ncbi_dataset_builder.execution import SlurmOptions

script, job_id = builder.submit_slurm(
    plan,
    processor_reference="my_pipeline:process",
    options=SlurmOptions(
        resources=ResourceSpec(threads=8, memory_gb=32, time_limit="24:00:00"),
        max_parallel=20,
        partition="compute",
    ),
)
```

This writes an `sbatch` script with `set -euo pipefail` and submits one plan task per array index. The workspace and installed Python environment must be visible on every node. Failed tasks stay failed until `retry_failed=True` or `--retry-failed` is supplied; successful tasks are never repeated within the same saved plan.

## CLI outline

```bash
ncbi-dataset --workspace workspace --email you@institute.org fetch-runs \
  'ATAC-seq[Strategy] AND "Homo sapiens"[Organism]' --output runinfo.csv

ncbi-dataset --workspace workspace --email you@institute.org fetch-metadata \
  SRS4739189 --output metadata/SRS4739189

ncbi-dataset --workspace workspace filter --catalog runinfo.csv \
  --where 'LibraryLayout==PAIRED' --where 'spots>=50000000' --output selected.csv

ncbi-dataset --workspace workspace plan --catalog selected.csv \
  --group-by experiment --threads 8 --memory-gb 32 --output plan.json

ncbi-dataset --workspace workspace build --plan plan.json \
  --processor my_pipeline:process

ncbi-dataset --workspace workspace submit-slurm --plan plan.json \
  --processor my_pipeline:process --max-parallel 20

ncbi-dataset --workspace workspace status --plan plan.json

# One-time cleanup for JSON created by the retired HTML scraper.
ncbi-dataset sanitize-legacy-metadata data/multispeciesATACseq/bioSampleDescriptions
```

## Documentation

- [API and extension points](docs/API.md)
- [Architecture and data model](docs/ARCHITECTURE.md)
- [Operations, recovery, and cluster behavior](docs/OPERATIONS.md)
- [Migration from the original scripts and resolved quirks](docs/MIGRATION.md)
- [Original README](docs/legacy/README.original.md)

The original implementation is retained under `src/multispeciesATACseq_processing/` and the original data snapshot remains under `data/`. It is not imported by the new package.

## Current validation boundary

The test suite covers catalog conflicts and filters, batching, XML parsing, repeated BioSample attributes, genome selection, durable state, Slurm script generation, and an end-to-end mocked build. The repository's 34,440-row RunInfo snapshot is also used for a local scale check. Live NCBI calls and bioinformatics binaries depend on your network, credentials, toolkit versions, filesystem, and Slurm cluster; run `preflight` and a one-accession smoke test before launching a full dataset.
