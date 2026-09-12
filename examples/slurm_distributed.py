"""Submit a quota-aware sample queue across many Slurm nodes."""

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
        ncbi_api_key="0123456789abcdef0123456789abcdef01234567",
    )
)
catalog = builder.fetch_runs('"ATAC-seq"[Strategy] AND "Mus musculus"[Organism]')

script, job_id = builder.submit_slurm(
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
        storage=QuotaStorage(
            quota_gb=5_000,
            reserve_gb=250,
            usage_root=Path("/scratch/project-owner"),
        ),
        partition="amd_256M,amd_1Tb,amd_2Tb",
    ),
    queue=QueuePolicy(download_workers=10, processing_storage_multiplier=2),
)
print(script, job_id)

