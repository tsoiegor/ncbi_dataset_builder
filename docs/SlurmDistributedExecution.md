# Distributed Slurm execution

Example system: many Slurm nodes with at most 128 CPUs each, a total project
CPU quota of 500, up to 50 simultaneous sample jobs, 100 GB RAM per job, and a
5 TB storage quota. A one-CPU coordinator submits one job per ready sample.

```python
from pathlib import Path

from ncbi_dataset_builder import (
    BuilderConfig,
    DatasetBuilder,
    QueuePolicy,
    QuotaStorage,
    SlurmDistributedExecution,
)

builder = DatasetBuilder(
    BuilderConfig(
        workspace=Path("/scratch/project-owner/ncbi-workspace"),
        email="researcher@example.org",
        ncbi_api_key="0123456789abcdef0123456789abcdef01234567",  # fake
    )
)
catalog = builder.fetch_runs(
    '"ATAC-seq"[Strategy] AND "Mus musculus"[Organism]'
)
# An existing file works too: catalog = builder.load_runs(Path("runinfo.csv"))

script, coordinator_job_id = builder.submit_slurm(
    catalog,
    processor_reference="ncbi_dataset_builder.processing.atac:process_atac",
    execution=SlurmDistributedExecution(
        total_cpu_quota=500,
        max_running_jobs=50,
        cpus_per_node=128,
        min_cpus_per_job=8,
        max_cpus_per_job=64,
        memory_gb_per_job=100,
        worker_time_limit="3-00:00:00",
        coordinator_cpus=1,
        coordinator_memory_gb=4,
        coordinator_time_limit="7-00:00:00",
        storage=QuotaStorage(
            quota_gb=5_000,
            reserve_gb=250,
            usage_root=Path("/scratch/project-owner"),
        ),
        partition="amd_256M,amd_1Tb,amd_2Tb",
    ),
    queue=QueuePolicy(
        download_workers=10,
        max_inflight_gb=1_500,
        processing_storage_multiplier=2,
    ),
)
print(script, coordinator_job_id)
```

`total_cpu_quota` includes the coordinator and running sample jobs.
`cpus_per_node` is the hard ceiling for any one sample request;
`max_cpus_per_job` may be lower. Actual concurrency is the minimum of
`max_running_jobs`, available CPU quota, queue storage, and storage quota.

The coordinator first records a held job, persists its Slurm job ID and exact
resources in sample state, and then releases it. A worker reconstructs the
saved execution configuration and invokes the importable processor. This makes
submission restart-safe and keeps the Python API as the only configuration
surface.

