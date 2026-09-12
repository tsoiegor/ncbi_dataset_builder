# NCBI Dataset Builder

`ncbi-dataset-builder` creates restartable datasets from NCBI SRA, GEO, and
Genome data. A workspace is the durable source of truth: catalogs, normalized
metadata, sample state, logs, downloaded inputs, processor work, outputs, and
execution snapshots all live below one directory. Samples move independently
through a streaming queue.

The same builder API supports an ordinary server, one Slurm allocation, or a
distributed Slurm cluster. CPU, memory, concurrency, and storage belong to the
selected execution system rather than `BuilderConfig`.

## Installation

Python 3.10 or newer is required.

### Conda environment


Conda can install Python and the executables needed by the built-in ATAC-seq
processor:

```bash
conda create -n ncbi-builder python=3.12 -y
conda activate ncbi-builder
conda install -c conda-forge -c bioconda \
  sra-tools fastp bowtie2 samtools deeptools -y
git clone https://github.com/tsoiegor/ncbi_dataset_builder.git
cd ncbi_dataset_builder
python -m pip install -e ".[dev,progress]"
```

The `dev` extra installs pytest, pytest-cov, and Ruff; `progress` installs tqdm. A basic
runtime install needs only Polars.

The built-in ATAC-seq processor also expects `sra-tools`, `fastp`, `bowtie2`,
`samtools`, and deepTools (`bamCoverage`) on `PATH`. A custom processor may use
any tools it needs. See [Installation](docs/Installation.md) for offline/local
wheel installation, optional dependencies, and external-tool checks.

## Catalogs from NCBI or CSV

NCBI asks clients to identify themselves with an email. An API key is optional.
The values below are examples; the key is deliberately fake.

```python
from pathlib import Path

from ncbi_dataset_builder import BuilderConfig, DatasetBuilder

builder = DatasetBuilder(
    BuilderConfig(
        workspace=Path("/data/atac-workspace"),
        email="researcher@example.org",
        ncbi_api_key="0123456789abcdef0123456789abcdef01234567",
    )
)

catalog_from_ncbi = builder.fetch_runs(
    '"ATAC-seq"[Strategy] AND "Homo sapiens"[Organism]'
)
catalog_from_file = builder.load_runs(Path("runinfo.csv"))
```

`RunCatalog.transform()` accepts a function from a Polars `DataFrame` to a
`DataFrame`, so project-specific catalog edits remain explicit and audited:

```python
import polars as pl

selected = catalog_from_file.transform(
    lambda frame: frame.filter(pl.col("size_MB") < 100_000),
    description="exclude runs above 100 GB",
)
```

## Local execution

```python
from ncbi_dataset_builder import (
    FilesystemStorage,
    LocalExecution,
    QueuePolicy,
)
from my_processors import process_sample

report = builder.build(
    selected,
    process_sample,
    execution=LocalExecution(
        total_cpus=100,
        min_cpus_per_job=4,
        max_cpus_per_job=20,
        max_running_jobs=10,
        storage=FilesystemStorage(reserve_free_gb=500),
    ),
    queue=QueuePolicy(
        download_workers=6,
        max_inflight_gb=1_200,
        processing_storage_multiplier=2,
    ),
)
```

Local execution intentionally has no memory setting or memory enforcement. It
can enforce a free-filesystem reserve before admitting another sample.

## Command line

The installed CLI uses the same three execution configurations:

```bash
ncbi-dataset fetch-catalog --workspace /data/ws \
  --email researcher@example.org \
  --query '"ATAC-seq"[Strategy]' --output runinfo.csv

ncbi-dataset build-local --workspace /data/ws --catalog runinfo.csv \
  --processor my_processors:process_sample --total-cpus 100 \
  --max-running-jobs 10 --reserve-free-gb 500
```

Slurm has separate `submit-single-node` and `submit-distributed` commands so
the accepted resource flags match the actual system. See the complete
[command-line reference](docs/CommandLineInterface.md).

## Resume and outputs

Every call creates an automatic execution snapshot under
`workspace/executions/`; users do not construct a separate orchestration
object. Each sample has independent atomic state. A repeated call reuses a
successful sample only when its semantic fingerprint and declared outputs are
still valid. Use `builder.status(execution_id)` or the `status` CLI command to
inspect progress, and `retry_failed=True`/`--retry-failed` to retry failures.

## Documentation map

- [Local execution](docs/LocalExecution.md): an ordinary 100-CPU server and a
  complete custom-processor example.
- [Single-node Slurm](docs/SlurmSingleNodeExecution.md): several samples inside
  one allocation with hard CPU and memory bounds.
- [Distributed Slurm](docs/SlurmDistributedExecution.md): a coordinator and
  independent sample jobs across many nodes.
- [Architecture](docs/Architecture.md): workspace layout and sample lifecycle.
- [Catalogs](docs/Catalogs.md): CSV/NCBI sources, filtering, transformation, and
  grouping.
- [Processors](docs/Processors.md): processor contract and output validation.
- [ATAC-seq processor](docs/AtacSeqProcessing.md): tools, outputs, and every
  intermediate-retention switch.
- [Storage](docs/Storage.md): local free space versus Slurm quota accounting.
- [Installation](docs/Installation.md): package, optional, and external-tool setup.
- [CLI reference](docs/CommandLineInterface.md): every command and flag.
- [Source package guide](src/ncbi_dataset_builder/README.md): links to every
  implementation subpackage.
- [Examples](examples/README.md) and [tests](tests/README.md).

The command-line interface mirrors these workflows; run
`ncbi-dataset --help` after installation.
