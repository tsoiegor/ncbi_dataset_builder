from dataclasses import replace
from threading import Event

from ncbi_dataset_builder import (
    BuilderConfig,
    DatasetBuilder,
    FilesystemStorage,
    LocalExecution,
    QueuePolicy,
    QuotaStorage,
    RunCatalog,
    SlurmDistributedExecution,
)
from ncbi_dataset_builder.execution import distributed_worker, sample_worker
from ncbi_dataset_builder.execution.config import execution_to_dict, queue_policy_to_dict
from ncbi_dataset_builder.models import (
    FastqLayout,
    FastqSet,
    GenomeRef,
    ProcessingResult,
)


class TrackingFastqProvider:
    def __init__(self, second_gate=None):
        self.stage_threads = []
        self.second_gate = second_gate
        self.second_started = Event()

    def fetch(self, unit, destination, *, threads):
        self.stage_threads.append((unit.unit_id, threads))
        if unit.unit_id == "SRX2" and self.second_gate is not None:
            self.second_started.set()
            if not self.second_gate.wait(timeout=5):
                raise TimeoutError("Second distributed stage was not released")
        root = destination / unit.unit_id
        root.mkdir(parents=True, exist_ok=True)
        read = root / "reads.fastq.gz"
        read.write_bytes(b"reads")
        return FastqSet(
            layout=FastqLayout.SINGLE,
            run_accessions=unit.run_accessions,
            single=(read,),
        )


class TrackingGenomeManager:
    def __init__(self, root):
        self.root = root

    def resolve(self, *, taxid, scientific_name, pin=None):
        self.root.mkdir(parents=True, exist_ok=True)
        fasta = self.root / f"{taxid}.fna"
        fasta.write_text(">chr1\nACGT\n", encoding="utf-8")
        return GenomeRef(
            taxid,
            scientific_name or "unknown",
            pin or "GCF_TEST",
            fasta,
            "sha",
        )


def _catalog():
    return RunCatalog.from_records(
        [
            {
                "Run": f"SRR{index}",
                "Experiment": f"SRX{index}",
                "SRA Sample": f"SRS{index}",
                "BioSample": f"SAMN{index}",
                "TaxID": 9606,
                "ScientificName": "Homo sapiens",
                "size_MB": 1,
            }
            for index in (1, 2)
        ]
    )


def _processor(fastq, genome, context):
    raise AssertionError("Processor must not run in the coordinator test")


def test_distributed_scheduler_stages_before_allocating_worker_cpus(tmp_path, monkeypatch):
    second_gate = Event()
    provider = TrackingFastqProvider(second_gate)
    builder = DatasetBuilder(
        BuilderConfig(tmp_path, show_progress=False),
        fastq_provider=provider,
        genome_manager=TrackingGenomeManager(tmp_path / "genome-cache"),
    )
    queue = QueuePolicy(download_workers=2, scheduler_poll_seconds=0.01)
    initial = builder._create_execution(
        _catalog(),
        _processor,
        execution=LocalExecution(
            total_cpus=2,
            storage=FilesystemStorage(reserve_free_gb=0),
        ),
        queue=queue,
        group_by=None,
        genome_pins=None,
        query=None,
        processor_id="processor-v1",
    )
    execution = SlurmDistributedExecution(
        total_cpu_quota=10,
        coordinator_cpus=2,
        max_running_jobs=2,
        cpus_per_node=8,
        min_cpus_per_job=2,
        max_cpus_per_job=8,
        memory_gb_per_job=4,
        worker_time_limit="01:00:00",
        storage=QuotaStorage(quota_gb=100),
    )
    record = replace(
        initial,
        execution_type=execution.__class__.__name__,
        execution_config=execution_to_dict(execution),
        queue_config=queue_policy_to_dict(queue),
    )
    builder.workspace.save_execution(record)

    submissions = []

    class FakeExecutor:
        def create_sample_script(self, *, item_index, cpus, output_path, **kwargs):
            del kwargs
            submissions.append({"index": item_index, "cpus": cpus})
            return output_path

        def submit(self, script, *, hold=False):
            assert hold
            submission = submissions[-1]
            item = record.items[submission["index"]]
            state = builder.state.get(item.item_id) or {}
            assert state["status"] == "running"
            assert state["phase"] == "ready"
            assert state["allocated_cpus"] is None
            assert isinstance(state.get("prepared"), dict)
            if submission["index"] == 0:
                assert provider.second_started.wait(timeout=2)
                assert not second_gate.is_set()
                second_gate.set()
            return f"job-{submission['index']}"

        def release(self, job_id):
            state = next(
                state
                for item in record.items
                if (state := builder.state.get(item.item_id))
                and state.get("slurm_job_id") == job_id
            )
            builder.state.succeed(
                str(state["unit_id"]),
                {},
                claim_id=str(state["claim_id"]),
            )

        def cancel(self, job_id):
            raise AssertionError(f"Unexpected cancellation: {job_id}")

    monkeypatch.setattr(
        distributed_worker.DatasetBuilder,
        "from_execution_record",
        classmethod(lambda cls, **kwargs: builder),
    )
    monkeypatch.setattr(
        distributed_worker,
        "SlurmExecutor",
        lambda **kwargs: FakeExecutor(),
    )
    monkeypatch.setattr(distributed_worker, "_slurm_states", lambda job_ids: {})

    result = distributed_worker.main(
        [
            "--execution",
            str(builder.workspace.executions / f"{record.execution_id}.json"),
            "--processor",
            "tests.fake:processor",
            "--workspace",
            str(tmp_path),
        ]
    )

    assert result == 0
    assert sorted(provider.stage_threads) == [("SRX1", 1), ("SRX2", 1)]
    assert [(item["index"], item["cpus"]) for item in submissions] == [(0, 8), (1, 8)]


def test_distributed_sample_worker_reuses_persisted_ready_input(tmp_path, monkeypatch):
    provider = TrackingFastqProvider()
    builder = DatasetBuilder(
        BuilderConfig(tmp_path, show_progress=False),
        fastq_provider=provider,
        genome_manager=TrackingGenomeManager(tmp_path / "genome-cache"),
    )
    queue = QueuePolicy(download_workers=1, cleanup="never")
    initial = builder._create_execution(
        _catalog(),
        _processor,
        execution=LocalExecution(
            total_cpus=2,
            storage=FilesystemStorage(reserve_free_gb=0),
        ),
        queue=queue,
        group_by=None,
        genome_pins=None,
        query=None,
        processor_id="processor-v1",
    )
    execution = SlurmDistributedExecution(
        total_cpu_quota=9,
        coordinator_cpus=1,
        max_running_jobs=2,
        cpus_per_node=8,
        min_cpus_per_job=2,
        max_cpus_per_job=8,
        memory_gb_per_job=4,
        worker_time_limit="01:00:00",
        storage=QuotaStorage(quota_gb=100),
    )
    record = replace(
        initial,
        execution_type=execution.__class__.__name__,
        execution_config=execution_to_dict(execution),
        queue_config=queue_policy_to_dict(queue),
    )
    builder.workspace.save_execution(record)
    item = record.items[0]
    prepared = builder._claim_and_stage(
        item,
        execution_id=record.execution_id,
        retry_failed=False,
        queue=queue,
        reclaim_running=False,
        stage_threads=1,
    )
    assert prepared.claim_id is not None
    builder.state.record_ready_submission(
        item.item_id,
        slurm_job_id="job-0",
        cpus=6,
        memory_gb=4,
        claim_id=prepared.claim_id,
    )
    observed_threads = []

    def processor(fastq, genome, context):
        del fastq, genome
        observed_threads.append(context.threads)
        context.output_dir.mkdir(parents=True, exist_ok=True)
        output = context.output_dir / "coverage.bw"
        output.write_bytes(b"coverage")
        return ProcessingResult(True, outputs={"coverage": output})

    monkeypatch.setattr(
        sample_worker.DatasetBuilder,
        "from_execution_record",
        classmethod(lambda cls, **kwargs: builder),
    )
    monkeypatch.setattr(sample_worker, "load_processor", lambda reference: processor)

    result = sample_worker.main(
        [
            "--execution",
            str(builder.workspace.executions / f"{record.execution_id}.json"),
            "--item-index",
            "0",
            "--processor",
            "tests.fake:processor",
            "--workspace",
            str(tmp_path),
            "--cpus",
            "6",
        ]
    )

    assert result == 0
    assert provider.stage_threads == [("SRX1", 1)]
    assert observed_threads == [6]
    assert (builder.state.get("SRX1") or {})["status"] == "succeeded"
