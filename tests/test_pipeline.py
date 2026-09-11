import sys
import threading

import pytest

from ncbi_dataset_builder.catalog import RunCatalog
from ncbi_dataset_builder.commands import CommandRunner
from ncbi_dataset_builder.execution import SlurmExecutor, SlurmOptions
from ncbi_dataset_builder.models import (
    FastqLayout,
    FastqSet,
    GenomeRef,
    ProcessingResult,
    ResourceSpec,
    StagedFastq,
)
from ncbi_dataset_builder.pipeline import PipelinePolicy
from ncbi_dataset_builder.workflow import BuilderConfig, DatasetBuilder


class PipelineGenomeManager:
    def __init__(self, root):
        self.root = root

    def cache_inventory(self, requirements, *, description):
        del requirements, description
        return {}

    def resolve(self, *, taxid, scientific_name, pin=None):
        self.root.mkdir(parents=True, exist_ok=True)
        fasta = self.root / f"{taxid}.fna"
        fasta.write_text(">chr1\nACGT\n", encoding="utf-8")
        return GenomeRef(taxid, scientific_name or str(taxid), pin or "GCF_TEST", fasta, "hash")


class StagedProvider:
    def __init__(self, events):
        self.events = events
        self.lock = threading.Lock()

    def _event(self, value):
        with self.lock:
            self.events.append(value)

    def stage(self, unit, destination, *, threads):
        del threads
        self._event(f"stage:{unit.unit_id}")
        root = destination / unit.unit_id
        root.mkdir(parents=True, exist_ok=True)
        (root / "raw.sra").write_bytes(b"raw")
        return StagedFastq(unit.unit_id, "test", 0.000000003, (root,))

    def materialize(self, unit, staged, destination, *, threads):
        del destination, threads
        self._event(f"materialize:{unit.unit_id}")
        read = staged.cleanup_roots[0] / "reads.fastq"
        read.write_bytes(b"reads")
        return FastqSet(
            unit.unit_id,
            FastqLayout.SINGLE,
            unit.run_accessions,
            single=(read,),
            source="test",
            work_dir=staged.cleanup_roots[0],
        )

    def fetch(self, unit, destination, *, threads):
        staged = self.stage(unit, destination, threads=threads)
        return self.materialize(unit, staged, destination, threads=threads)


def two_batch_catalog():
    return RunCatalog.from_records(
        [
            {
                "Run": "SRR1",
                "Experiment": "SRX1",
                "TaxID": 9606,
                "ScientificName": "Homo sapiens",
                "size_MB": 1,
            },
            {
                "Run": "SRR2",
                "Experiment": "SRX2",
                "TaxID": 9606,
                "ScientificName": "Homo sapiens",
                "size_MB": 1,
            },
        ]
    )


def test_units_from_different_batches_stream_without_a_batch_barrier(tmp_path):
    events = []
    provider = StagedProvider(events)
    builder = DatasetBuilder(
        BuilderConfig(
            tmp_path,
            max_workers=1,
            pipeline_policy=PipelinePolicy(cleanup="never"),
        ),
        fastq_provider=provider,
        genome_manager=PipelineGenomeManager(tmp_path / "genomes"),
    )
    def processor(fastq, genome, threads):
        del genome, threads
        events.append(f"process:{fastq.unit_id}")
        fastq.output_dir.mkdir(parents=True, exist_ok=True)
        output = fastq.output_dir / "result.bin"
        output.write_bytes(b"ok")
        return ProcessingResult(True, (output,))

    report = builder.build(two_batch_catalog(), processor, max_batch_units=1)

    assert report.succeeded == 2
    assert {value for value in events if value.startswith("stage:")} == {
        "stage:SRX1",
        "stage:SRX2",
    }
    assert {value for value in events if value.startswith("process:")} == {
        "process:SRX1",
        "process:SRX2",
    }


def test_processing_starts_without_waiting_for_every_download(tmp_path):
    processing_started = threading.Event()

    class BlockingSecondProvider(StagedProvider):
        """Keep the second download open until the first unit starts processing."""

        def stage(self, unit, destination, *, threads):
            if unit.unit_id == "SRX2" and not processing_started.wait(timeout=10):
                raise RuntimeError("processing did not start while another download was active")
            return super().stage(unit, destination, threads=threads)

    provider = BlockingSecondProvider([])
    builder = DatasetBuilder(
        BuilderConfig(
            tmp_path,
            max_workers=2,
            total_threads=8,
            pipeline_policy=PipelinePolicy(cleanup="never", download_workers=2),
        ),
        fastq_provider=provider,
        genome_manager=PipelineGenomeManager(tmp_path / "genomes"),
    )

    def processor(fastq, genome, threads):
        del genome, threads
        processing_started.set()
        fastq.output_dir.mkdir(parents=True, exist_ok=True)
        output = fastq.output_dir / "result.bin"
        output.write_bytes(b"ok")
        return ProcessingResult(True, (output,))

    report = builder.build(two_batch_catalog(), processor, max_batch_units=2)

    assert report.succeeded == 2


def test_one_oversized_unit_receives_all_available_cpus(tmp_path):
    catalog = RunCatalog.from_records(
        [
            {
                "Run": "SRR_BIG",
                "Experiment": "SRX_BIG",
                "TaxID": 9606,
                "ScientificName": "Homo sapiens",
                "size_MB": 1,
            }
        ]
    )
    allocated = []
    builder = DatasetBuilder(
        BuilderConfig(tmp_path, max_workers=8, total_threads=128),
        fastq_provider=StagedProvider([]),
        genome_manager=PipelineGenomeManager(tmp_path / "genomes"),
    )

    def processor(fastq, genome, threads):
        del genome
        allocated.append(threads)
        fastq.output_dir.mkdir(parents=True, exist_ok=True)
        output = fastq.output_dir / "result.bin"
        output.write_bytes(b"ok")
        return ProcessingResult(True, (output,))

    report = builder.build(
        catalog,
        processor,
        resources=ResourceSpec(16, 16),
        max_batch_gb=0.0005,
        policy=PipelinePolicy(cleanup="never", max_threads_per_unit=128),
    )

    assert report.succeeded == 1
    assert allocated == [128]
    assert builder.state.get("SRX_BIG")["allocated_threads"] == 128


def test_slurm_worker_restart_reuses_state_and_clears_partial_processor_output(tmp_path):
    allocated = []
    builder = DatasetBuilder(
        BuilderConfig(tmp_path),
        fastq_provider=StagedProvider([]),
        genome_manager=PipelineGenomeManager(tmp_path / "genomes"),
    )

    def processor(fastq, genome, threads):
        del genome
        allocated.append(threads)
        fastq.output_dir.mkdir(parents=True, exist_ok=True)
        output = fastq.output_dir / "result.bin"
        output.write_bytes(b"ok")
        return ProcessingResult(True, (output,))

    catalog = two_batch_catalog().filter(lambda row: row["Run"] == "SRR1")
    job = builder.reconcile(catalog, processor, resources=ResourceSpec(2, 4))
    task = job.tasks[0]
    builder.state.start(
        task.task_id,
        fingerprint=task.fingerprint,
        job_id=job.job_id,
        task=task.to_dict(),
    )
    builder.state.set_phase(task.task_id, "processing")
    partial = tmp_path / "outputs" / task.task_id / "partial.bin"
    partial.parent.mkdir(parents=True, exist_ok=True)
    partial.write_bytes(b"partial")
    builder.state.record_submission(
        task.task_id,
        slurm_job_id="12345",
        threads=8,
        memory_gb=4,
        fingerprint=task.fingerprint,
        job_id=job.job_id,
        task=task.to_dict(),
    )

    outcome = builder.run_task(job, 0, processor, retry_failed=True, threads_override=8)

    assert outcome.status == "succeeded"
    assert allocated == [8]
    assert not partial.exists()


def test_one_unit_log_contains_processor_and_command_output_and_inputs_are_cleaned(tmp_path):
    provider = StagedProvider([])
    builder = DatasetBuilder(
        BuilderConfig(tmp_path, max_workers=1),
        fastq_provider=provider,
        genome_manager=PipelineGenomeManager(tmp_path / "genomes"),
    )
    catalog = two_batch_catalog().filter(lambda row: row["Run"] == "SRR1")

    def processor(fastq, genome, threads):
        del genome, threads
        print("processor says hello")
        CommandRunner().run(
            [
                sys.executable,
                "-c",
                "import sys; print('command stdout'); print('command stderr', file=sys.stderr)",
            ]
        )
        fastq.output_dir.mkdir(parents=True, exist_ok=True)
        output = fastq.output_dir / "result.bin"
        output.write_bytes(b"ok")
        return ProcessingResult(True, (output,))

    report = builder.build(catalog, processor)

    assert report.succeeded == 1
    logs = list((tmp_path / "logs" / "Homo_sapiens").glob("*.log"))
    assert len(logs) == 1
    log = logs[0].read_text(encoding="utf-8")
    assert "processor says hello" in log
    assert "command stdout" in log
    assert "command stderr" in log
    assert "Run external command" in log
    assert not (tmp_path / "fastq" / "SRX1").exists()
    state = builder.state.get("SRX1")
    assert state["log_path"] == str(logs[0])
    assert builder.batch_state.get(report.job_id, 0).status == "cleaned"


def test_failed_unit_inputs_are_retained_by_default(tmp_path):
    provider = StagedProvider([])
    builder = DatasetBuilder(
        BuilderConfig(tmp_path),
        fastq_provider=provider,
        genome_manager=PipelineGenomeManager(tmp_path / "genomes"),
    )
    catalog = two_batch_catalog().filter(lambda row: row["Run"] == "SRR1")

    def processor(fastq, genome, threads):
        del fastq, genome, threads
        raise RuntimeError("intentional failure")

    report = builder.build(catalog, processor)

    assert report.failed == 1
    assert (tmp_path / "fastq" / "SRX1" / "raw.sra").is_file()
    assert builder.batch_state.get(report.job_id, 0).status == "partially_failed"


def test_storage_limit_is_expressed_and_enforced_in_gb(tmp_path):
    builder = DatasetBuilder(
        BuilderConfig(tmp_path),
        fastq_provider=StagedProvider([]),
        genome_manager=PipelineGenomeManager(tmp_path / "genomes"),
    )
    with pytest.raises(RuntimeError, match="max_staged_gb"):
        builder.build(
            two_batch_catalog(),
            lambda fastq, genome, threads: None,
            max_batch_units=1,
            policy=PipelinePolicy(max_staged_gb=0.0005),
        )


def test_slurm_coordinator_requests_aggregate_resources(tmp_path):
    script = SlurmExecutor(python_executable="python3").create_coordinator_script(
        job_path=tmp_path / "job.json",
        processor_reference="pipeline:processor",
        workspace=tmp_path / "workspace",
        email="test@example.org",
        output_path=tmp_path / "coordinator.sbatch",
        options=SlurmOptions(resources=ResourceSpec(4, 8, "12:00:00")),
        max_workers=3,
        total_threads=12,
        total_memory_gb=24,
        prefetch_batches=1,
        max_staged_gb=100,
        minimum_free_gb=10,
        processing_storage_multiplier=4,
        max_threads_per_unit=12,
        scheduler_poll_seconds=2,
        cleanup="after_success",
        keep_failed_inputs=True,
        fsync_logs=True,
    )
    text = script.read_text(encoding="utf-8")
    assert "#SBATCH --array" not in text
    assert "#SBATCH --cpus-per-task=12" in text
    assert "#SBATCH --mem=24G" in text
    assert "ncbi_dataset_builder.pipeline_worker" in text
    assert "--max-staged-gb 100" in text
    assert "--job" in text
    assert "--download-workers 2" in text
    assert "--processing-storage-multiplier 4" in text
    assert "--max-threads-per-unit 12" in text


def test_builder_generates_quota_aware_distributed_slurm_dispatcher(tmp_path):
    builder = DatasetBuilder(
        BuilderConfig(tmp_path),
        fastq_provider=StagedProvider([]),
        genome_manager=PipelineGenomeManager(tmp_path / "genomes"),
    )
    script, job_id = builder.submit_slurm(
        two_batch_catalog(),
        processor_reference="pipeline:processor",
        options=SlurmOptions(
            resources=ResourceSpec(16, 64, "24:00:00"),
            mode="distributed",
            total_cpu_quota=500,
            max_running_jobs=50,
            coordinator_cpus=1,
            cpus_per_node=128,
            partition="amd_256M,amd_1Tb,amd_2Tb",
        ),
        max_batch_units=1,
        policy=PipelinePolicy(
            processing_storage_multiplier=4,
            max_threads_per_unit=128,
        ),
        submit=False,
    )

    assert job_id is None
    text = script.read_text(encoding="utf-8")
    assert "ncbi_dataset_builder.slurm_dispatcher" in text
    assert "--total-cpu-quota 500" in text
    assert "--max-running-jobs 50" in text
    assert "--cpus-per-node 128" in text
    assert "--partition amd_256M,amd_1Tb,amd_2Tb" in text
    assert "--processing-storage-multiplier 4" in text
    assert "--max-threads-per-unit 128" in text
