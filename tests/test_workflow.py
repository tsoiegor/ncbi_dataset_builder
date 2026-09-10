import json
import os
from io import StringIO

from ncbi_dataset_builder.catalog import RunCatalog
from ncbi_dataset_builder.metadata import MetadataBundle
from ncbi_dataset_builder.models import (
    FastqLayout,
    FastqSet,
    GenomeRef,
    ProcessingResult,
    ResourceSpec,
)
from ncbi_dataset_builder.progress import ProgressReporter
from ncbi_dataset_builder.workflow import BuilderConfig, DatasetBuilder


class FakeFastqProvider:
    def fetch(self, unit, destination, *, threads):
        root = destination / unit.unit_id
        root.mkdir(parents=True, exist_ok=True)
        read = root / "reads.fastq.gz"
        read.write_bytes(b"reads")
        return FastqSet(
            unit_id=unit.unit_id,
            layout=FastqLayout.SINGLE,
            run_accessions=unit.run_accessions,
            single=(read,),
            work_dir=root,
            output_dir=destination.parent / "results" / unit.unit_id,
        )


class FakeGenomeManager:
    def __init__(self, root):
        self.root = root
        self.inventory_calls = []

    def cache_inventory(self, requirements, *, description):
        self.inventory_calls.append((list(requirements), description))
        return {}

    def resolve(self, *, taxid, scientific_name, pin=None):
        self.root.mkdir(parents=True, exist_ok=True)
        fasta = self.root / "genome.fna"
        fasta.write_text(">chr1\nACGT\n", encoding="utf-8")
        return GenomeRef(taxid, scientific_name, pin or "GCF_TEST", fasta, "not-used")


def processor(fastq, genome, threads):
    fastq.output_dir.mkdir(parents=True, exist_ok=True)
    output = fastq.output_dir / "dataset.bin"
    output.write_bytes(f"{genome.accession}:{threads}".encode())
    return ProcessingResult(True, outputs=(output,), metrics={"threads": threads})


def other_processor(fastq, genome, threads):
    return processor(fastq, genome, threads)


def test_end_to_end_orchestration_and_resume_without_external_tools(tmp_path):
    catalog = RunCatalog.from_records(
        [
            {
                "Run": "SRR1",
                "Experiment": "SRX1",
                "SRA Sample": "SRS1",
                "BioSample": "SAMN1",
                "TaxID": 9606,
                "ScientificName": "Homo sapiens",
                "bases": 100,
                "size_MB": 1,
            }
        ]
    )
    stream = StringIO()
    builder = DatasetBuilder(
        BuilderConfig(tmp_path, "test@example.org", max_workers=2, total_threads=4),
        fastq_provider=FakeFastqProvider(),
        genome_manager=FakeGenomeManager(tmp_path / "genomes"),
        progress=ProgressReporter(use_bars=False, stream=stream),
    )
    report = builder.build(
        catalog,
        processor,
        resources=ResourceSpec(2, 4, "01:00:00"),
    )
    assert report.succeeded == 1
    assert report.failed == 0
    assert builder.genomes.inventory_calls[0][0] == [(9606, None)]
    assert "1 selected tasks in 1 batch" in builder.genomes.inventory_calls[0][1]
    status = builder.status()
    assert status["counts"]["succeeded"] == 1
    assert status["batches"] == [
        {
            "batch_id": 0,
            "counts": {"pending": 0, "running": 0, "succeeded": 1, "failed": 0},
        }
    ]
    assert "Task SRX1:" not in stream.getvalue()
    assert "Batch 0 complete: 1 succeeded; 0 failed; 0 skipped" in stream.getvalue()
    resumed = builder.build(
        catalog,
        processor,
        resources=ResourceSpec(2, 4, "01:00:00"),
        retry_failed=True,
    )
    assert resumed.skipped == 1
    changed = builder.build(
        catalog,
        other_processor,
        resources=ResourceSpec(2, 4, "01:00:00"),
    )
    assert changed.succeeded == 1
    assert list((tmp_path / "state" / "history" / "SRX1").glob("*.json"))


def test_offline_builder_can_load_and_reconcile_without_ncbi_email(tmp_path):
    csv_path = tmp_path / "runs.csv"
    csv_path.write_text(
        "Run,Experiment,TaxID,ScientificName,size_MB\nSRR1,SRX1,9606,Homo sapiens,1.5\n",
        encoding="utf-8",
    )
    builder = DatasetBuilder(BuilderConfig(tmp_path / "workspace"))
    job = builder.reconcile(builder.load_runs(csv_path), processor)
    assert job.tasks[0].unit.total_size_gb == 0.0015


def test_metadata_enrichment_reuses_bundle_cache_and_refreshes(tmp_path):
    class FakeSra:
        def __init__(self):
            self.calls = 0

        def fetch_packages(self, accessions, **kwargs):
            self.calls += 1
            assert accessions == ["SRR1"]
            return MetadataBundle(
                packages=[
                    {
                        "accession": "SRX1",
                        "experiment_accession": "SRX1",
                        "sra_sample_accession": "SRS1",
                        "run_accessions": ["SRR1"],
                    }
                ],
                runs=[{"accession": "SRR1", "experiment_accession": "SRX1"}],
                experiments=[{"accession": "SRX1", "library": {"strategy": "ATAC-seq"}}],
                sra_samples=[
                    {"accession": "SRS1", "biosample": "SAMN1", "organism": "Test species"}
                ],
            )

    class FakeBioSample:
        def __init__(self):
            self.calls = 0

        def fetch(self, accessions, **kwargs):
            self.calls += 1
            assert accessions == ["SAMN1"]
            return [{"accession": "SAMN1", "attributes": {"tissue": "test tissue"}}]

    stream = StringIO()
    builder = DatasetBuilder(
        BuilderConfig(tmp_path, email="test@example.org"),
        progress=ProgressReporter(
            use_bars=False,
            stream=stream,
            text_interval_seconds=60,
        ),
    )
    sra = FakeSra()
    biosample = FakeBioSample()
    builder.sra = sra
    builder.biosample = biosample
    catalog = RunCatalog.from_records([{"Run": "SRR1", "Experiment": "SRX1", "BioSample": "SAMN1"}])

    first = builder.enrich_metadata(catalog)
    metadata_path = tmp_path / "metadata" / "metadata.json"
    os.utime(metadata_path, (1_000_000_000, 1_000_000_000))
    second = builder.enrich_metadata(catalog)
    assert first.to_dict() == second.to_dict()
    assert sra.calls == 1
    assert biosample.calls == 1
    assert metadata_path.stat().st_mtime == 1_000_000_000
    assert len(list((tmp_path / "metadata_cache" / "bundles").glob("*.json"))) == 1
    assert "Normalized metadata cache hit:" in stream.getvalue()
    assert "1 samples loaded from cache; 0 samples require work" in stream.getvalue()

    builder.enrich_metadata(catalog, refresh=True)
    assert sra.calls == 2
    assert biosample.calls == 2


def test_workspace_reuses_units_across_job_and_resource_changes(tmp_path):
    catalog = RunCatalog.from_records(
        [
            {
                "Run": "SRR1",
                "Experiment": "SRX1",
                "TaxID": 9606,
                "ScientificName": "Homo sapiens",
                "size_MB": 1,
            }
        ]
    )
    calls = []

    def counting_processor(fastq, genome, threads):
        calls.append((fastq.unit_id, threads))
        return processor(fastq, genome, threads)

    builder = DatasetBuilder(
        BuilderConfig(tmp_path, max_workers=1),
        fastq_provider=FakeFastqProvider(),
        genome_manager=FakeGenomeManager(tmp_path / "genome-cache"),
    )
    first = builder.build(catalog, counting_processor, resources=ResourceSpec(2, 4))
    second = builder.build(
        catalog,
        counting_processor,
        resources=ResourceSpec(8, 64),
        max_batch_units=10,
    )

    assert first.succeeded == 1
    assert second.skipped == 1
    assert calls == [("SRX1", 2)]
    assert (tmp_path / "workspace.json").is_file()
    assert (tmp_path / "jobs" / "latest.json").is_file()
    assert (tmp_path / "state" / "units" / "SRX1.json").is_file()
    assert not [path for path in tmp_path.iterdir() if path.is_dir() and path.name.startswith(".")]


def test_workspace_processes_new_and_changed_units_and_retains_removed_records(tmp_path):
    base = [
        {
            "Run": "SRR1",
            "Experiment": "SRX1",
            "TaxID": 9606,
            "ScientificName": "Homo sapiens",
            "size_MB": 1,
        }
    ]
    calls = []

    def counting_processor(fastq, genome, threads):
        calls.append(fastq.unit_id)
        return processor(fastq, genome, threads)

    builder = DatasetBuilder(
        BuilderConfig(tmp_path, max_workers=1),
        fastq_provider=FakeFastqProvider(),
        genome_manager=FakeGenomeManager(tmp_path / "genome-cache"),
    )
    builder.build(RunCatalog.from_records(base), counting_processor)
    expanded = RunCatalog.from_records(
        [
            *base,
            {
                "Run": "SRR2",
                "Experiment": "SRX2",
                "TaxID": 9606,
                "ScientificName": "Homo sapiens",
                "size_MB": 1,
            },
        ]
    )
    added = builder.build(expanded, counting_processor)
    changed = RunCatalog.from_records([*base, {**base[0], "Run": "SRR3"}])
    rebuilt = builder.build(changed, counting_processor)

    assert added.succeeded == 1 and added.skipped == 1
    assert rebuilt.succeeded == 1
    assert calls == ["SRX1", "SRX2", "SRX1"]
    manifest = json.loads((tmp_path / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["units"]["SRX2"]["requested_by_latest_job"] is False
    assert list((tmp_path / "state" / "history" / "SRX1").glob("*.json"))


def test_workspace_repairs_a_missing_successful_output(tmp_path):
    catalog = RunCatalog.from_records(
        [
            {
                "Run": "SRR1",
                "Experiment": "SRX1",
                "TaxID": 9606,
                "ScientificName": "Homo sapiens",
                "size_MB": 1,
            }
        ]
    )
    builder = DatasetBuilder(
        BuilderConfig(tmp_path),
        fastq_provider=FakeFastqProvider(),
        genome_manager=FakeGenomeManager(tmp_path / "genome-cache"),
    )
    builder.build(catalog, processor)
    output = tmp_path / "outputs" / "SRX1" / "dataset.bin"
    output.unlink()

    repaired = builder.build(catalog, processor)

    assert repaired.succeeded == 1
    assert output.is_file()


def test_workspace_adopts_compatible_legacy_success_state(tmp_path):
    catalog = RunCatalog.from_records(
        [
            {
                "Run": "SRR1",
                "Experiment": "SRX1",
                "TaxID": 9606,
                "ScientificName": "Homo sapiens",
                "size_MB": 1,
            }
        ]
    )
    output = tmp_path / "old-results" / "result.bw"
    output.parent.mkdir(parents=True)
    output.write_bytes(b"bigwig")
    genome = tmp_path / "old-genomes" / "genome.fna"
    genome.parent.mkdir(parents=True)
    genome.write_text(">chr1\nACGT\n", encoding="utf-8")
    old_state = tmp_path / "state" / "tasks" / "old-job__SRX1.json"
    old_state.parent.mkdir(parents=True)
    old_state.write_text(
        json.dumps(
            {
                "task_id": "old-job__SRX1",
                "status": "succeeded",
                "result": {
                    "processing": {"outputs": [str(output)]},
                    "output_sha256": {},
                    "genome": {"fasta": str(genome), "sha256": "old"},
                    "fastq": {"run_accessions": ["SRR1"]},
                },
            }
        ),
        encoding="utf-8",
    )
    registration = tmp_path / "state" / "plans" / "old-job.json"
    registration.parent.mkdir(parents=True)
    registration.write_text(
        json.dumps({"processor_identity": "pipeline:process:source_sha256=old"}),
        encoding="utf-8",
    )
    builder = DatasetBuilder(
        BuilderConfig(tmp_path),
        fastq_provider=FakeFastqProvider(),
        genome_manager=FakeGenomeManager(tmp_path / "genome-cache"),
    )

    job = builder.reconcile(catalog, "pipeline:process")

    state = builder.state.get("SRX1")
    assert state["status"] == "succeeded"
    assert state["fingerprint"] == job.tasks[0].fingerprint
    assert state["migrated_from"] == str(old_state)
