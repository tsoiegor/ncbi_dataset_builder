"""Run a sample-streaming workflow on one ordinary server."""

from pathlib import Path

from custom_processor import process_sample

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
        ncbi_api_key="0123456789abcdef0123456789abcdef01234567",
    )
)
catalog = builder.load_runs(Path("runinfo.csv"))
# Direct NCBI alternative:
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
    ),
)
print(report)

