import pytest

from ncbi_dataset_builder.errors import UnitAlreadyRunning
from ncbi_dataset_builder.execution.records import ExecutionRecord, QueueItem, UnitResources
from ncbi_dataset_builder.execution.state import UnitStateStore
from ncbi_dataset_builder.models import ProcessingUnit
from ncbi_dataset_builder.workspace import WorkspaceStore


def queue_item() -> QueueItem:
    return QueueItem(
        item_id="SRX1",
        unit=ProcessingUnit("SRX1", ("SRR1",), taxid=9606),
        resources=UnitResources(cpus=4),
        fingerprint="fingerprint",
    )


def execution_record() -> ExecutionRecord:
    return ExecutionRecord(
        execution_id="execution-test",
        created_at="2026-09-12T00:00:00Z",
        query=None,
        group_by="experiment",
        items=(queue_item(),),
        processor_identity="example:process",
        execution_type="LocalExecution",
        execution_config={"kind": "LocalExecution", "config": {}},
        queue_config={},
    )


def test_state_claim_success_resume_and_repair(tmp_path):
    store = UnitStateStore(tmp_path / "state" / "units")
    item = queue_item()
    arguments = {
        "fingerprint": item.fingerprint,
        "execution_id": "execution-test",
        "item": item.to_dict(),
        "log_path": tmp_path / "sample.log",
    }
    assert store.start(item.item_id, **arguments)
    with pytest.raises(UnitAlreadyRunning):
        store.start(item.item_id, **arguments)
    store.set_phase(item.item_id, "processing")
    store.set_runtime_resources(item.item_id, cpus=8, memory_gb=None)
    store.succeed(item.item_id, {"processing": {"outputs": ["result.bw"]}})
    assert store.start(item.item_id, **arguments) is False
    assert store.start(item.item_id, **arguments, force=True)
    assert list((tmp_path / "state" / "history" / item.item_id).glob("*.json"))


def test_submitted_state_and_summary(tmp_path):
    store = UnitStateStore(tmp_path / "state" / "units")
    item = queue_item()
    store.record_submission(
        item.item_id,
        slurm_job_id="12345",
        cpus=16,
        memory_gb=100,
        fingerprint=item.fingerprint,
        execution_id="execution-test",
        item=item.to_dict(),
        log_path=tmp_path / "sample.log",
    )
    summary = store.summary([item.item_id, "SRX2"])
    assert summary["counts"]["submitted"] == 1
    assert summary["counts"]["pending"] == 1


def test_workspace_persists_only_execution_records(tmp_path):
    workspace = WorkspaceStore(tmp_path)
    record = execution_record()
    path = workspace.save_execution(record)
    assert path.parent.name == "executions"
    assert workspace.load_execution(record.execution_id) == record
    assert workspace.latest_execution() == record
    workspace.sync_manifest(record, {"SRX1": None})
