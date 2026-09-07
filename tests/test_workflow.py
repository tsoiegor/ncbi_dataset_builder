import pytest

from ncbi_dataset_builder.catalog import RunCatalog
from ncbi_dataset_builder.models import (
    FastqLayout,
    FastqSet,
    GenomeRef,
    ProcessingResult,
    ResourceSpec,
)
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
    builder = DatasetBuilder(
        BuilderConfig(tmp_path, "test@example.org", max_workers=2, total_threads=4),
        fastq_provider=FakeFastqProvider(),
        genome_manager=FakeGenomeManager(tmp_path / "genomes"),
    )
    plan = builder.plan(catalog, resources=ResourceSpec(2, 4, "01:00:00"))
    report = builder.build(plan, processor)
    assert report.succeeded == 1
    assert report.failed == 0
    assert builder.status(plan)["counts"]["succeeded"] == 1
    resumed = builder.build(plan, processor, retry_failed=True)
    assert resumed.skipped == 1
    with pytest.raises(ValueError, match="already bound to processor"):
        builder.build(plan, other_processor)


def test_offline_builder_can_load_and_plan_without_ncbi_email(tmp_path):
    csv_path = tmp_path / "runs.csv"
    csv_path.write_text(
        "Run,Experiment,TaxID,ScientificName,size_MB\nSRR1,SRX1,9606,Homo sapiens,1.5\n",
        encoding="utf-8",
    )
    builder = DatasetBuilder(BuilderConfig(tmp_path / "workspace"))
    plan = builder.plan(builder.load_runs(csv_path))
    assert plan.tasks[0].unit.total_bytes == 1_500_000
