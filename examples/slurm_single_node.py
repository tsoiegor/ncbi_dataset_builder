"""Submit a streaming queue inside one Slurm allocation."""

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
        ncbi_api_key="0123456789abcdef0123456789abcdef01234567",
    )
)
catalog = builder.load_runs(Path("runinfo.csv"))

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
    queue=QueuePolicy(download_workers=8, processing_storage_multiplier=2),
)
print(script, job_id)

