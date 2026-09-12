from pathlib import Path

import pytest

from ncbi_dataset_builder import (
    BuilderConfig,
    FilesystemStorage,
    LocalExecution,
    QueuePolicy,
    QuotaStorage,
    SlurmDistributedExecution,
    SlurmSingleNodeExecution,
)
from ncbi_dataset_builder.execution.config import execution_from_dict, execution_to_dict
from ncbi_dataset_builder.execution.slurm import SlurmExecutor


def single_node(storage: QuotaStorage) -> SlurmSingleNodeExecution:
    return SlurmSingleNodeExecution(
        allocation_cpus=64,
        allocation_memory_gb=256,
        allocation_time_limit="2-00:00:00",
        storage=storage,
        min_cpus_per_job=4,
        max_cpus_per_job=16,
        memory_gb_per_job=24,
        max_running_jobs=8,
        partition="cpu,large",
    )


def distributed(storage: QuotaStorage) -> SlurmDistributedExecution:
    return SlurmDistributedExecution(
        total_cpu_quota=500,
        max_running_jobs=50,
        cpus_per_node=128,
        min_cpus_per_job=8,
        max_cpus_per_job=64,
        memory_gb_per_job=100,
        worker_time_limit="3-00:00:00",
        storage=storage,
        coordinator_cpus=1,
        partition="amd_256M,amd_1Tb",
    )


def test_local_execution_has_cpu_and_free_storage_but_no_memory_setting(tmp_path, monkeypatch):
    class Usage:
        free = 15_000_000_000

    monkeypatch.setattr("ncbi_dataset_builder.execution.config.shutil.disk_usage", lambda _: Usage())
    execution = LocalExecution(
        total_cpus=100,
        min_cpus_per_job=4,
        max_cpus_per_job=20,
        max_running_jobs=10,
        storage=FilesystemStorage(reserve_free_gb=2),
    )
    assert not hasattr(execution, "memory_gb")
    assert execution.storage.available_gb(tmp_path) == pytest.approx(13)


def test_quota_storage_uses_owned_data_instead_of_filesystem_capacity(tmp_path):
    (tmp_path / "owned.bin").write_bytes(b"x" * 1_000_000)
    storage = QuotaStorage(quota_gb=5, reserve_gb=1, usage_root=tmp_path)
    assert storage.available_gb(Path("ignored")) == pytest.approx(3.999)


def test_execution_configuration_round_trip_and_capacity(tmp_path):
    execution = distributed(QuotaStorage(5_000, reserve_gb=100, usage_root=tmp_path))
    restored = execution_from_dict(execution_to_dict(execution))
    assert restored == execution
    assert execution.worker_capacity(10) == 49
    assert QueuePolicy(download_workers=10, max_inflight_gb=800)


def test_invalid_resource_combinations_are_rejected():
    with pytest.raises(ValueError, match="max_cpus_per_job"):
        LocalExecution(total_cpus=8, max_cpus_per_job=9)
    with pytest.raises(ValueError, match="memory_gb_per_job"):
        SlurmSingleNodeExecution(
            allocation_cpus=8,
            allocation_memory_gb=16,
            allocation_time_limit="01:00:00",
            storage=QuotaStorage(100),
            memory_gb_per_job=32,
        )
    with pytest.raises(ValueError, match="cpus_per_node"):
        distributed(QuotaStorage(5_000)).__class__(
            total_cpu_quota=500,
            max_running_jobs=50,
            cpus_per_node=16,
            min_cpus_per_job=8,
            max_cpus_per_job=32,
            memory_gb_per_job=100,
            worker_time_limit="3-00:00:00",
            storage=QuotaStorage(5_000),
        )


def test_slurm_scripts_target_new_worker_packages(tmp_path):
    builder_config = BuilderConfig(tmp_path / "workspace", email="researcher@example.org")
    executor = SlurmExecutor(python_executable="python")
    record = tmp_path / "execution.json"
    record.write_text("{}", encoding="utf-8")
    one = executor.create_single_node_script(
        record_path=record,
        processor_reference="example_processor:process",
        builder_config=builder_config,
        output_path=tmp_path / "one.sbatch",
        execution=single_node(QuotaStorage(5_000)),
    )
    many = executor.create_distributed_script(
        record_path=record,
        processor_reference="example_processor:process",
        builder_config=builder_config,
        output_path=tmp_path / "many.sbatch",
        execution=distributed(QuotaStorage(5_000)),
    )
    assert "ncbi_dataset_builder.execution.single_node_worker" in one.read_text()
    assert "#SBATCH --cpus-per-task=64" in one.read_text()
    assert "ncbi_dataset_builder.execution.distributed_worker" in many.read_text()

