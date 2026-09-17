import pytest

from ncbi_dataset_builder.errors import StaleUnitClaim, UnitAlreadyRunning
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
    claim_id = store.start(item.item_id, **arguments)
    assert isinstance(claim_id, str)
    assert store.get(item.item_id)["status"] == "downloading"
    assert store.get(item.item_id)["phase"] == "resolving-genome"
    with pytest.raises(UnitAlreadyRunning):
        store.start(item.item_id, **arguments)
    store.set_phase(item.item_id, "processing", claim_id=claim_id)
    store.set_runtime_resources(item.item_id, cpus=8, memory_gb=None, claim_id=claim_id)
    store.succeed(
        item.item_id,
        {"processing": {"outputs": ["result.bw"]}},
        claim_id=claim_id,
    )
    assert store.start(item.item_id, **arguments) is False
    assert store.start(item.item_id, **arguments, force=True)
    assert list((tmp_path / "state" / "history" / item.item_id).glob("*.json"))


def test_replaced_claim_cannot_commit_old_result(tmp_path):
    store = UnitStateStore(tmp_path / "state" / "units")
    item = queue_item()
    arguments = {
        "execution_id": "execution-test",
        "item": item.to_dict(),
        "log_path": tmp_path / "sample.log",
    }
    old_claim = store.start(item.item_id, fingerprint="old", **arguments)
    new_claim = store.start(
        item.item_id,
        fingerprint="new",
        reclaim_running=True,
        **arguments,
    )
    assert isinstance(old_claim, str)
    assert isinstance(new_claim, str)
    with pytest.raises(StaleUnitClaim):
        store.succeed(item.item_id, {"producer": "old"}, claim_id=old_claim)
    store.succeed(item.item_id, {"producer": "new"}, claim_id=new_claim)
    assert store.get(item.item_id)["result"] == {"producer": "new"}


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


def test_interrupted_state_is_requeued_and_old_claim_is_invalidated(tmp_path):
    store = UnitStateStore(tmp_path / "state" / "units")
    item = queue_item()
    old_claim = store.start(
        item.item_id,
        fingerprint=item.fingerprint,
        execution_id="old-execution",
        item=item.to_dict(),
        log_path=tmp_path / "sample.log",
    )
    assert isinstance(old_claim, str)
    store.set_phase(item.item_id, "processing", claim_id=old_claim)

    state = store.requeue_interrupted(
        item.item_id,
        reason="Slurm job 1582667 is no longer active",
    )

    assert state["status"] == "pending"
    assert state["phase"] == "interrupted"
    assert state["allocated_cpus"] is None
    assert state["slurm_job_id"] is None
    with pytest.raises(StaleUnitClaim):
        store.succeed(item.item_id, {}, claim_id=old_claim)


def test_workspace_persists_only_execution_records(tmp_path):
    workspace = WorkspaceStore(tmp_path)
    record = execution_record()
    path = workspace.save_execution(record)
    assert path.parent.name == "executions"
    assert workspace.load_execution(record.execution_id) == record
    assert workspace.latest_execution() == record
    workspace.sync_manifest(record, {"SRX1": None})
