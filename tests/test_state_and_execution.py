from ncbi_dataset_builder.execution import SlurmExecutor, SlurmOptions
from ncbi_dataset_builder.models import ResourceSpec
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


def test_slurm_uses_job_array_strict_shell_and_worker_index(tmp_path):
    script = SlurmExecutor(python_executable="python3").create_script(
        plan_path=tmp_path / "plan.json",
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
    assert "set -euo pipefail" in text
    assert "${SLURM_ARRAY_TASK_ID}" in text
    assert "ncbi_dataset_builder.worker" in text


def test_slurm_can_submit_selected_noncontiguous_plan_indices(tmp_path):
    script = SlurmExecutor(python_executable="python3").create_script(
        plan_path=tmp_path / "plan.json",
        task_count=10,
        processor_reference="my_pipeline:process",
        workspace=tmp_path / "workspace",
        email=None,
        output_path=tmp_path / "batch.sbatch",
        options=SlurmOptions(max_parallel=2),
        task_indices=[1, 2, 5, 7, 8, 9],
    )
    assert "#SBATCH --array=1-2,5,7-9%2" in script.read_text(encoding="utf-8")
