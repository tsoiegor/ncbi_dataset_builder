import subprocess

import pytest

from ncbi_dataset_builder.execution import LocalExecutor, SlurmExecutor, SlurmOptions
from ncbi_dataset_builder.models import DatasetTask, ProcessingUnit, ResourceSpec
from ncbi_dataset_builder.slurm_dispatcher import (
    _fits_window,
    _planned_processing_count,
    _slurm_job_statuses,
)
from ncbi_dataset_builder.state import TaskStateStore


def test_state_is_resumable_and_failed_tasks_require_explicit_retry(tmp_path):
    state = TaskStateStore(tmp_path / "state")
    assert state.start("SRX1") is True
    state.fail("SRX1", "broken")
    assert state.start("SRX1") is False
    assert state.start("SRX1", retry_failed=True) is True
    state.succeed("SRX1", {"output": "ok"})
    assert state.start("SRX1", retry_failed=True) is False
    assert state.summary(["SRX1", "SRX2"])["counts"] == {
        "pending": 1,
        "running": 0,
        "succeeded": 1,
        "failed": 0,
    }


def test_coordinator_can_reclaim_an_interrupted_running_state(tmp_path):
    state = TaskStateStore(tmp_path / "state")
    assert state.start("SRX1") is True

    assert state.start("SRX1", reclaim_running=True) is True
    assert state.get("SRX1")["attempts"] == 2


def test_slurm_submission_state_is_claimed_without_losing_allocation(tmp_path):
    state = TaskStateStore(tmp_path / "state")
    state.record_submission(
        "SRX1",
        slurm_job_id="12345",
        threads=128,
        memory_gb=64,
        fingerprint="work",
        job_id="job-1",
        task={"task_id": "SRX1"},
        log_path=tmp_path / "logs" / "SRX1.log",
    )

    assert state.start("SRX1", fingerprint="work", job_id="job-1") is True
    saved = state.get("SRX1")
    assert saved["status"] == "running"
    assert saved["slurm_job_id"] == "12345"
    assert saved["allocated_threads"] == 128


def test_local_executor_reports_results_as_items_complete():
    seen = []
    executor = LocalExecutor[int, int](max_workers=2)

    results = executor.map(
        [1, 2, 3],
        lambda value: value * 10,
        threads_per_task=1,
        on_result=lambda item, result: seen.append((item, result)),
    )

    assert results == [10, 20, 30]
    assert sorted(seen) == [(1, 10), (2, 20), (3, 30)]


def test_local_executor_rejects_unsatisfied_cpu_and_memory_budgets():
    with pytest.raises(ValueError, match="cannot satisfy a 4-thread task"):
        LocalExecutor[int, int](max_workers=2, total_threads=2).map(
            [1], lambda value: value, threads_per_task=4
        )
    with pytest.raises(ValueError, match="cannot satisfy a 8 GB task"):
        LocalExecutor[int, int](max_workers=2, total_memory_gb=4).map(
            [1], lambda value: value, threads_per_task=1, memory_gb_per_task=8
        )


def test_slurm_uses_job_array_strict_shell_and_worker_index(tmp_path):
    script = SlurmExecutor(python_executable="python3").create_script(
        job_path=tmp_path / "job.json",
        task_count=11,
        processor_reference="my_pipeline:process",
        workspace=tmp_path / "workspace",
        email="test@example.org",
        output_path=tmp_path / "jobs.sbatch",
        options=SlurmOptions(resources=ResourceSpec(8, 32, "12:00:00"), max_parallel=3),
    )
    text = script.read_text(encoding="utf-8")
    assert "#SBATCH --array=0-10%3" in text
    assert "#SBATCH --cpus-per-task=8" in text
    assert "#SBATCH --open-mode=append" in text
    assert "set -euo pipefail" in text
    assert "${SLURM_ARRAY_TASK_ID}" in text
    assert "ncbi_dataset_builder.worker" in text
    assert "--job" in text
    assert "--prefetch-max-size u" in text
    assert "--plan" not in text


def test_slurm_can_submit_selected_noncontiguous_job_indices(tmp_path):
    script = SlurmExecutor(python_executable="python3").create_script(
        job_path=tmp_path / "job.json",
        task_count=10,
        processor_reference="my_pipeline:process",
        workspace=tmp_path / "workspace",
        email=None,
        output_path=tmp_path / "batch.sbatch",
        options=SlurmOptions(max_parallel=2),
        task_indices=[1, 2, 5, 7, 8, 9],
    )
    assert "#SBATCH --array=1-2,5,7-9%2" in script.read_text(encoding="utf-8")


def test_distributed_slurm_respects_cpu_and_job_quotas():
    options = SlurmOptions(
        mode="distributed",
        total_cpu_quota=500,
        max_running_jobs=50,
        coordinator_cpus=1,
        cpus_per_node=128,
    )

    assert options.worker_parallelism(16) == 31
    assert options.worker_parallelism(8) == 49
    with pytest.raises(ValueError, match="exceeds cpus_per_node"):
        options.worker_parallelism(129)
    with pytest.raises(ValueError, match="Coordinator request exceeds"):
        SlurmOptions(
            mode="distributed",
            total_cpu_quota=500,
            max_running_jobs=50,
            coordinator_cpus=129,
            cpus_per_node=128,
        )


def test_distributed_slurm_creates_lightweight_dispatcher(tmp_path):
    script = SlurmExecutor(python_executable="python3").create_dispatcher_script(
        job_path=tmp_path / "job.json",
        processor_reference="pipeline:process",
        workspace=tmp_path / "workspace",
        email="test@example.org",
        output_path=tmp_path / "dispatcher.sbatch",
        options=SlurmOptions(
            resources=ResourceSpec(16, 48, "12:00:00"),
            mode="distributed",
            total_cpu_quota=500,
            max_running_jobs=50,
            coordinator_cpus=1,
            coordinator_memory_gb=4,
            coordinator_time_limit="7-00:00:00",
            cpus_per_node=128,
            partition="amd_256M,amd_1Tb,amd_2Tb",
        ),
        prefetch_batches=1,
        max_staged_gb=500,
        minimum_free_gb=50,
        processing_storage_multiplier=4,
        max_threads_per_unit=128,
        scheduler_poll_seconds=3,
        cleanup="after_success",
        keep_failed_inputs=True,
        fsync_logs=True,
    )
    text = script.read_text(encoding="utf-8")
    assert "#SBATCH --cpus-per-task=1" in text
    assert "#SBATCH --array" not in text
    assert "ncbi_dataset_builder.slurm_dispatcher" in text
    assert "--total-cpu-quota 500" in text
    assert "--max-running-jobs 50" in text
    assert "--download-workers 2" in text
    assert "--processing-storage-multiplier 4" in text
    assert "--max-threads-per-unit 128" in text
    assert "--scheduler-poll-seconds 3" in text
    assert "#SBATCH --open-mode=append" in text


def test_slurm_submission_can_be_recorded_while_worker_is_held(tmp_path):
    class RecordingRunner:
        def __init__(self):
            self.calls = []

        def require(self, *commands):
            self.calls.append(("require", *commands))

        def run(self, command):
            self.calls.append(tuple(command))
            return subprocess.CompletedProcess(command, 0, stdout="12345;cluster\n", stderr="")

    runner = RecordingRunner()
    executor = SlurmExecutor(runner=runner)
    script = tmp_path / "worker.sbatch"
    script.write_text("#!/usr/bin/env bash\n", encoding="utf-8")

    assert executor.submit(script, hold=True) == "12345"
    executor.release("12345")
    executor.cancel("12345")

    assert ("sbatch", "--parsable", "--hold", str(script)) in runner.calls
    assert ("scontrol", "release", "12345") in runner.calls
    assert ("scancel", "12345") in runner.calls


def test_distributed_planning_gives_an_oversized_unit_a_single_unit_cohort():
    huge = DatasetTask(
        "SRX_BIG",
        ProcessingUnit(("SRX_BIG"), ("SRR_BIG",), total_size_gb=350),
        0,
        ResourceSpec(16, 64),
    )
    small = DatasetTask(
        "SRX_SMALL",
        ProcessingUnit(("SRX_SMALL"), ("SRR_SMALL",), total_size_gb=10),
        1,
        ResourceSpec(16, 64),
    )

    assert _fits_window(0, 350, 100, 0)
    assert not _fits_window(10, 350, 100, 1)
    assert _planned_processing_count(
        huge,
        active=[],
        waiting=[small],
        processing_window_gb=100,
        processing_unit_limit=None,
        worker_job_limit=49,
        worker_cpu_budget=499,
    ) == 1


def test_slurm_status_query_parses_held_jobs(monkeypatch):
    monkeypatch.setattr("ncbi_dataset_builder.slurm_dispatcher.shutil.which", lambda name: name)
    monkeypatch.setattr(
        "ncbi_dataset_builder.slurm_dispatcher.subprocess.run",
        lambda *args, **kwargs: subprocess.CompletedProcess(
            args[0],
            0,
            stdout="123|PENDING|JobHeldUser\n124|RUNNING|node01\n",
            stderr="",
        ),
    )

    assert _slurm_job_statuses({"124", "123"}) == {
        "123": ("PENDING", "JobHeldUser"),
        "124": ("RUNNING", "node01"),
    }
