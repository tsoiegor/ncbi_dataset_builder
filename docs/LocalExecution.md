# Local execution

Example system: one server with 100 logical CPUs and 2 TB of workspace storage.
No scheduler enforces memory. The queue leaves 500 GB free, runs at most ten
processors, and gives each active sample between 4 and 20 CPUs.

## Processor function

Save this as `my_processors.py` somewhere importable:

```python
from hashlib import sha256

from ncbi_dataset_builder import FastqSet, GenomeRef, ProcessingResult


def process_sample(
    fastq: FastqSet,
    genome: GenomeRef,
    threads: int,
) -> ProcessingResult:
    """Write a small reproducible manifest for one sample.

    Args:
        fastq: Validated local FASTQ paths and workspace directories.
        genome: Selected local genome reference.
        threads: CPUs assigned to this invocation.
    """

    fastq.validate()
    genome.validate()
    fastq.output_dir.mkdir(parents=True, exist_ok=True)
    output = fastq.output_dir / f"{fastq.unit_id}.txt"
    digest = sha256()
    for path in (*fastq.read1, *fastq.read2, *fastq.single):
        with path.open("rb") as handle:
            while chunk := handle.read(1024 * 1024):
                digest.update(chunk)
    output.write_text(
        f"sample={fastq.unit_id}\n"
        f"assembly={genome.accession}\n"
        f"threads={threads}\n"
        f"fastq_sha256={digest.hexdigest()}\n",
        encoding="utf-8",
    )
    return ProcessingResult(
        success=True,
        outputs=(output,),
        metrics={"input_files": len(fastq.read1) + len(fastq.read2) + len(fastq.single)},
    )
```

A real processor can invoke tools with `subprocess.run([...], check=True)` and
must declare all final outputs in `ProcessingResult`. Put temporary work below
`fastq.work_dir` and final files below `fastq.output_dir`.

## Complete workflow

```python
from pathlib import Path

from my_processors import process_sample
from ncbi_dataset_builder import (
    BuilderConfig,
    DatasetBuilder,
    FilesystemStorage,
    LocalExecution,
    QueuePolicy,
)

builder = DatasetBuilder(
    BuilderConfig(
        workspace=Path("/data/ncbi-workspace"),
        email="researcher@example.org",
        ncbi_api_key="0123456789abcdef0123456789abcdef01234567",  # fake
    )
)

# Either source produces the same RunCatalog interface.
catalog = builder.load_runs(Path("runinfo.csv"))
# catalog = builder.fetch_runs('"ATAC-seq"[Strategy] AND "Homo sapiens"[Organism]')

report = builder.build(
    catalog,
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
        cleanup="after_success",
        keep_failed_inputs=True,
    ),
)
print(report.execution_id, report.succeeded, report.failed, report.skipped)
```

Memory is intentionally absent from `LocalExecution`. If a processor needs a
memory-aware concurrency policy, enforce it inside that processor or use a
Slurm execution where requested memory is a hard scheduler resource.

