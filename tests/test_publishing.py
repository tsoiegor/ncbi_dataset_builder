import json

from test_metadata import BIOSAMPLE_XML, SRA_XML

from ncbi_dataset_builder import BuilderConfig, DatasetBuilder, RunCatalog
from ncbi_dataset_builder.metadata import BioSampleClient, SraClient
from ncbi_dataset_builder.models import FastqLayout, FastqSet, GenomeRef, ProcessingResult


class Provider:
    def fetch(self, unit, destination, *, threads):
        root = destination / unit.unit_id
        root.mkdir(parents=True, exist_ok=True)
        read = root / "reads.fastq.gz"
        read.write_bytes(b"reads")
        return FastqSet(unit.unit_id, FastqLayout.SINGLE, unit.run_accessions, single=(read,))


class Genomes:
    def __init__(self, root):
        self.root = root

    def resolve(self, *, taxid, scientific_name, pin=None):
        self.root.mkdir(parents=True, exist_ok=True)
        fasta = self.root / "genome.fna"
        fasta.write_text(">chr1\nACGT\n", encoding="utf-8")
        return GenomeRef(taxid, scientific_name, pin or "GCF_TEST", fasta, "sha")


def bigwig_processor(fastq, genome, cpus):
    fastq.output_dir.mkdir(parents=True, exist_ok=True)
    output = fastq.output_dir / f"{fastq.unit_id}.bw"
    output.write_bytes(b"bigwig")
    return ProcessingResult(True, outputs=(output,))


def test_publish_completed_experiment_dataset(tmp_path):
    workspace = tmp_path / "workspace"
    metadata = SraClient.parse_packages(SRA_XML)
    metadata.biosamples = BioSampleClient.parse(BIOSAMPLE_XML)
    metadata.save(workspace / "metadata")
    catalog = RunCatalog.from_records(
        [
            {
                "Run": "SRR9032674",
                "Experiment": "SRX5809925",
                "SRA Sample": "SRS4739189",
                "BioSample": "SAMN11608754",
                "TaxID": 7955,
                "ScientificName": "Danio rerio",
                "size_MB": 1,
            }
        ]
    )
    builder = DatasetBuilder(
        BuilderConfig(workspace, show_progress=False),
        fastq_provider=Provider(),
        genome_manager=Genomes(workspace / "genome-cache"),
    )
    report = builder.build(catalog, bigwig_processor)
    export = builder.publish_dataset(tmp_path / "published", execution_id=report.execution_id)
    manifest = json.loads(export.manifest.read_text(encoding="utf-8"))
    assert export.experiments == 1
    assert (export.destination / "bigWig" / "SRX5809925.bw").is_file()
    assert (export.destination / "genomes" / "Danio_rerio.fasta.gz").is_file()
    description = json.loads(
        (export.destination / "descriptions" / "SRX5809925.json").read_text(encoding="utf-8")
    )
    assert description["Experiment ID"] == "SRX5809925"
    assert manifest["execution_id"] == report.execution_id

