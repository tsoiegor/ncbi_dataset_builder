import json

from ncbi_dataset_builder import (
    BuilderConfig,
    DatasetBuilder,
    FilesystemStorage,
    LocalExecution,
    QueuePolicy,
    RunCatalog,
)
from ncbi_dataset_builder.models import FastqLayout, FastqSet, GenomeRef, ProcessingResult


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
            output_dir=destination.parent / "outputs" / unit.unit_id,
            metadata={"download_threads": threads},
        )


class FakeGenomeManager:
    def __init__(self, root):
        self.root = root

    def resolve(self, *, taxid, scientific_name, pin=None):
        self.root.mkdir(parents=True, exist_ok=True)
        fasta = self.root / f"{taxid}.fna"
        fasta.write_text(">chr1\nACGT\n", encoding="utf-8")
        return GenomeRef(taxid, scientific_name or "unknown", pin or "GCF_TEST", fasta, "sha")


def processor(fastq, genome, cpus):
    fastq.output_dir.mkdir(parents=True, exist_ok=True)
    output = fastq.output_dir / f"{fastq.unit_id}.bw"
    output.write_bytes(f"{genome.accession}:{cpus}".encode())
    return ProcessingResult(True, outputs=(output,), metrics={"cpus": cpus})


def catalog(*experiment_ids):
    return RunCatalog.from_records(
        [
            {
                "Run": f"SRR{index}",
                "Experiment": experiment,
                "SRA Sample": f"SRS{index}",
                "BioSample": f"SAMN{index}",
                "TaxID": 9606,
                "ScientificName": "Homo sapiens",
                "size_MB": 1,
            }
            for index, experiment in enumerate(experiment_ids, 1)
        ]
    )


def builder(tmp_path):
    return DatasetBuilder(
        BuilderConfig(tmp_path, show_progress=False),
        fastq_provider=FakeFastqProvider(),
        genome_manager=FakeGenomeManager(tmp_path / "genome-cache"),
    )


def test_local_sample_queue_builds_and_resumes(tmp_path):
    instance = builder(tmp_path)
    execution = LocalExecution(
        total_cpus=8,
        min_cpus_per_job=2,
        max_cpus_per_job=4,
        max_running_jobs=2,
        storage=FilesystemStorage(reserve_free_gb=0),
    )
    queue = QueuePolicy(download_workers=2, cleanup="after_success")
    first = instance.build(catalog("SRX1", "SRX2"), processor, execution=execution, queue=queue)
    assert first.succeeded == 2
    assert first.failed == 0
    assert not (tmp_path / "fastq" / "SRX1").exists()
    second = instance.build(catalog("SRX1", "SRX2"), processor, execution=execution, queue=queue)
    assert second.skipped == 2
    assert instance.status()["counts"]["succeeded"] == 2
    assert list((tmp_path / "executions").glob("execution-*.json"))


def test_changed_processor_identity_rebuilds_sample(tmp_path):
    instance = builder(tmp_path)
    instance.build(catalog("SRX1"), processor, processor_id="processor-v1")
    rebuilt = instance.build(catalog("SRX1"), processor, processor_id="processor-v2")
    assert rebuilt.succeeded == 1
    assert list((tmp_path / "state" / "history" / "SRX1").glob("*.json"))


def test_catalog_loads_from_csv_without_ncbi_credentials(tmp_path):
    csv = tmp_path / "runs.csv"
    csv.write_text(
        "Run,Experiment,TaxID,ScientificName,size_MB\nSRR1,SRX1,9606,Homo sapiens,1.5\n",
        encoding="utf-8",
    )
    loaded = DatasetBuilder.load_runs(csv)
    assert loaded.processing_units()[0].total_size_gb == 0.0015


def test_fetch_runs_uses_ncbi_client_and_catalog_cache(tmp_path):
    class FakeSra:
        def __init__(self):
            self.calls = 0

        def fetch_runinfo(self, query, *, refresh):
            self.calls += 1
            assert query == '"ATAC-seq"[Strategy]'
            return catalog("SRX1")

    instance = builder(tmp_path)
    instance.sra = FakeSra()
    first = instance.fetch_runs('"ATAC-seq"[Strategy]')
    second = instance.fetch_runs('"ATAC-seq"[Strategy]')
    assert first.frame.equals(second.frame)
    assert instance.sra.calls == 1
    manifest = json.loads((tmp_path / "manifest.json").read_text(encoding="utf-8")) if (tmp_path / "manifest.json").exists() else None
    assert manifest is None

