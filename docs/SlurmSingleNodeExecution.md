# Single-node Slurm execution

Example system: one Slurm allocation with 128 CPUs, 1 TB RAM, a two-day wall
time, and a 5 TB user storage quota on a much larger shared filesystem. Up to
eight samples run inside the allocation; each reserves 100 GB RAM and receives
between 8 and 32 CPUs.

```python
from pathlib import Path

from ncbi_dataset_builder import (
    BuilderConfig,
    DatasetBuilder,
    QueuePolicy,
    QuotaStorage,
    SlurmSingleNodeExecution,
)

builder = DatasetBuilder(
    BuilderConfig(
        workspace=Path("/scratch/project-owner/ncbi-workspace"),
        email="researcher@example.org",
        ncbi_api_key="0123456789abcdef0123456789abcdef01234567",  # fake
    )
)
catalog = builder.load_runs(Path("runinfo.csv"))
# Or: catalog = builder.fetch_runs('"ATAC-seq"[Strategy]')

script, job_id = builder.submit_slurm(
    catalog,
    processor_reference="ncbi_dataset_builder.processing.atac:process_atac",
    execution=SlurmSingleNodeExecution(
        allocation_cpus=128,
        allocation_memory_gb=1_000,
        allocation_time_limit="2-00:00:00",
        min_cpus_per_job=8,
        max_cpus_per_job=32,
        memory_gb_per_job=100,
        max_running_jobs=8,
        storage=QuotaStorage(
            quota_gb=5_000,
            reserve_gb=250,
            usage_root=Path("/scratch/project-owner"),
        ),
        partition="amd_1Tb,amd_2Tb",
    ),
    queue=QueuePolicy(
        download_workers=8,
        max_inflight_gb=1_500,
        processing_storage_multiplier=2,
    ),
)
print(script, job_id)
```

The generated script requests one coordinator allocation. Inside it, the same
streaming scheduler used locally overlaps downloads and processing. Admission
is bounded by `allocation_cpus`, `allocation_memory_gb`,
`memory_gb_per_job`, `max_running_jobs`, and quota availability. Set
`submit=False` to inspect the generated script without calling `sbatch`.

`max_running_jobs` has the same meaning here and in the distributed API: the
maximum number of samples actively processing at once.

